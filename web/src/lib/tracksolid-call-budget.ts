import { execute, query } from '@/lib/db';

/** HK, EU, and US nodes. TrackSolid restores this 24 hours after the first call of the window. */
export const TRACKSOLID_DAILY_CALL_LIMIT = 8640;
const WINDOW_MS = 24 * 60 * 60 * 1000;
const TOKEN_MS = 7200 * 1000;
/** Device list plus one location call. The account is under the 100-IMEI limit, so one location call covers it. */
const CALLS_PER_POLL = 2;
const SETTINGS_TYPE = 'TracksolidCallWindow';

let ensured = false;

export type TracksolidCallBudget = {
  limit: number;
  used: number;
  forecast: number;
  fullDayAtInterval: number;
  pollIntervalSeconds: number;
  pollRunning: boolean;
  callsPerPoll: number;
  windowStartedAt: string | null;
  resetsAt: string | null;
  resetsAtMs: number | null;
  byMethod: { method: string; count: number }[];
};

function localClock(ms: number): string {
  const now = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

async function ensureSchema(): Promise<void> {
  if (ensured) return;
  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_tracksolid_api_calls (
      id            bigserial PRIMARY KEY,
      called_at_ms  bigint NOT NULL,
      method        text NOT NULL,
      endpoint      text NOT NULL,
      ok            boolean NOT NULL
    )`);
  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_tracksolid_api_calls_endpoint_at
      ON tbl_tracksolid_api_calls (endpoint, called_at_ms)`);
  ensured = true;
}

async function readWindowStart(endpointKey: string): Promise<number | null> {
  const rows = await query<{ settingvalue: string | null }>(
    'SELECT settingvalue FROM tbl_settings WHERE type = $1 AND settingname = $2 LIMIT 1',
    [SETTINGS_TYPE, endpointKey],
  );
  const n = Number(rows[0]?.settingvalue);
  return Number.isFinite(n) && n > 0 ? n : null;
}

async function writeWindowStart(endpointKey: string, startedAt: number): Promise<void> {
  await execute(
    `INSERT INTO tbl_settings (type, settingname, settingvalue) VALUES ($1, $2, $3)
     ON CONFLICT (type, settingname) DO UPDATE SET settingvalue = EXCLUDED.settingvalue`,
    [SETTINGS_TYPE, endpointKey, String(startedAt)],
  );
}

let recordChain: Promise<void> = Promise.resolve();

/** Count one TrackSolid HTTP call toward the 24-hour account limit. */
export function recordTracksolidCall(input: { endpointKey: string; method: string; ok: boolean }): Promise<void> {
  const run = recordChain.then(() => recordTracksolidCallInner(input));
  recordChain = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}

async function recordTracksolidCallInner(input: { endpointKey: string; method: string; ok: boolean }): Promise<void> {
  await ensureSchema();
  const now = Date.now();
  let start = await readWindowStart(input.endpointKey);
  if (start == null || now >= start + WINDOW_MS) {
    start = now;
    await writeWindowStart(input.endpointKey, start);
    await execute('DELETE FROM tbl_tracksolid_api_calls WHERE endpoint = $1 AND called_at_ms < $2', [
      input.endpointKey,
      start,
    ]);
  }
  await execute(
    'INSERT INTO tbl_tracksolid_api_calls (called_at_ms, method, endpoint, ok) VALUES ($1, $2, $3, $4)',
    [now, input.method, input.endpointKey, input.ok],
  );
}

function pollsIn(ms: number, intervalSeconds: number): number {
  if (ms <= 0 || intervalSeconds <= 0) return 0;
  return Math.floor(ms / (intervalSeconds * 1000));
}

function tokenCallsUntil(now: number, end: number, expiresAtMs: number | null, pollRunning: boolean): number {
  if (!pollRunning || end <= now) return 0;
  let next = expiresAtMs != null && expiresAtMs > now ? expiresAtMs : now;
  let count = 0;
  while (next < end && count < 100) {
    count += 1;
    next += TOKEN_MS;
  }
  return count;
}

export async function tracksolidCallBudget(input: {
  endpointKey: string;
  pollIntervalSeconds: number;
  pollRunning: boolean;
  tokenExpiresAtMs: number | null;
}): Promise<TracksolidCallBudget> {
  await ensureSchema();
  const now = Date.now();
  const start = await readWindowStart(input.endpointKey);
  const windowOpen = start != null && now < start + WINDOW_MS;
  const windowStart = windowOpen ? start : null;
  const rows = windowStart == null
    ? []
    : await query<{ method: string; count: number }>(
        `SELECT method, count(*)::int AS count
         FROM tbl_tracksolid_api_calls
         WHERE endpoint = $1 AND called_at_ms >= $2
         GROUP BY method
         ORDER BY count DESC, method`,
        [input.endpointKey, windowStart],
      );
  const used = rows.reduce((sum, row) => sum + Number(row.count), 0);
  const resetsAtMs = windowStart == null ? null : windowStart + WINDOW_MS;
  const remainingMs = resetsAtMs == null ? WINDOW_MS : Math.max(0, resetsAtMs - now);
  const futurePolls = input.pollRunning ? pollsIn(remainingMs, input.pollIntervalSeconds) : 0;
  const futureTokens = tokenCallsUntil(now, now + remainingMs, input.tokenExpiresAtMs, input.pollRunning);
  const forecast = windowStart == null && !input.pollRunning
    ? 0
    : used + futurePolls * CALLS_PER_POLL + futureTokens;
  const fullDayAtInterval =
    pollsIn(WINDOW_MS, input.pollIntervalSeconds) * CALLS_PER_POLL + tokenCallsUntil(0, WINDOW_MS, 0, true);

  return {
    limit: TRACKSOLID_DAILY_CALL_LIMIT,
    used,
    forecast,
    fullDayAtInterval,
    pollIntervalSeconds: input.pollIntervalSeconds,
    pollRunning: input.pollRunning,
    callsPerPoll: CALLS_PER_POLL,
    windowStartedAt: windowStart == null ? null : localClock(windowStart),
    resetsAt: resetsAtMs == null ? null : localClock(resetsAtMs),
    resetsAtMs,
    byMethod: rows.map((row) => ({ method: row.method, count: Number(row.count) })),
  };
}

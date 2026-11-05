import { execute, getClient, query } from '@/lib/db';
import { checkVworkJobFeed } from '@/lib/vwork-job-feed';
import { enqueueVworkFeedJobs, type VworkFeedQueueResult } from '@/lib/vwork-feed-queue';
import { vworkAccountStatus, vworkApiConfigured, vworkApiKeyState, type VworkAccountStatus } from '@/lib/vwork-api';
import type { VworkFeedAccountResult } from '@/lib/vwork-job-feed';

const LOGTYPE = 'vwork_api_poll';
const SETTINGS_TYPE = 'VworkApiPoll';

export type VworkApiSettings = {
  pollIntervalSeconds: number;
  lookbackDays: number;
};

const DEFAULT_SETTINGS: VworkApiSettings = {
  pollIntervalSeconds: 60,
  lookbackDays: 2,
};

type PollState = {
  timer: ReturnType<typeof setInterval> | null;
  running: boolean;
  inFlight: boolean;
  settings: VworkApiSettings;
  lastPolltime: string | null;
  lastSummary: string | null;
  lastError: string | null;
  nextPollAtMs: number | null;
  heartbeatMs: number;
};

const globalKey = '__geodataVworkApiPoll';

function localClock(now = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

function clampInt(value: unknown, min: number, max: number, fallback: number): number {
  const n = typeof value === 'number' ? value : parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(n)));
}

function normalizeSettings(input?: Partial<VworkApiSettings> | null): VworkApiSettings {
  return {
    pollIntervalSeconds: clampInt(input?.pollIntervalSeconds, 30, 3600, DEFAULT_SETTINGS.pollIntervalSeconds),
    lookbackDays: clampInt(input?.lookbackDays, 1, 30, DEFAULT_SETTINGS.lookbackDays),
  };
}

function state(): PollState {
  const g = globalThis as typeof globalThis & { [globalKey]?: PollState };
  if (!g[globalKey]) {
    g[globalKey] = {
      timer: null,
      running: false,
      inFlight: false,
      settings: { ...DEFAULT_SETTINGS },
      lastPolltime: null,
      lastSummary: null,
      lastError: null,
      nextPollAtMs: null,
      heartbeatMs: 0,
    };
  }
  const s = g[globalKey];
  if (s.running && s.timer) {
    const intervalMs = Math.max(30, s.settings.pollIntervalSeconds) * 1000;
    const stalled = s.heartbeatMs === 0 || Date.now() - s.heartbeatMs > intervalMs + 15_000;
    if (stalled) armTimer(s);
  }
  return s;
}

function armTimer(s: PollState): void {
  if (s.timer) clearInterval(s.timer);
  s.heartbeatMs = Date.now();
  s.nextPollAtMs = Date.now() + s.settings.pollIntervalSeconds * 1000;
  s.timer = setInterval(() => {
    s.heartbeatMs = Date.now();
    s.nextPollAtMs = Date.now() + s.settings.pollIntervalSeconds * 1000;
    void pollVworkApiOnce();
  }, s.settings.pollIntervalSeconds * 1000);
}

async function readSettings(): Promise<VworkApiSettings> {
  const rows = await query<{ settingname: string; settingvalue: string | null }>(
    `SELECT settingname, settingvalue
     FROM tbl_settings
     WHERE type = $1 AND settingname IN ('pollIntervalSeconds', 'lookbackDays')`,
    [SETTINGS_TYPE],
  );
  const found: Partial<VworkApiSettings> = {};
  for (const row of rows) {
    if (row.settingname === 'pollIntervalSeconds') found.pollIntervalSeconds = Number(row.settingvalue);
    if (row.settingname === 'lookbackDays') found.lookbackDays = Number(row.settingvalue);
  }
  return normalizeSettings({ ...DEFAULT_SETTINGS, ...found });
}

async function writeSettings(settings: VworkApiSettings): Promise<void> {
  const client = await getClient();
  try {
    await client.query('BEGIN');
    for (const [name, value] of Object.entries(settings)) {
      await client.query('DELETE FROM tbl_settings WHERE type = $1 AND settingname = $2', [SETTINGS_TYPE, name]);
      await client.query('INSERT INTO tbl_settings (type, settingname, settingvalue) VALUES ($1, $2, $3)', [
        SETTINGS_TYPE,
        name,
        String(value),
      ]);
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

async function writeLog(stage: string, details: Record<string, unknown>): Promise<void> {
  try {
    await execute(
      'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4)',
      [LOGTYPE, 'all', stage, JSON.stringify(details)],
    );
  } catch (err) {
    console.error('[vwork-api-poll] log failed', err);
  }
}

function summaryText(input: {
  configured: boolean;
  accounts: VworkFeedAccountResult[];
  queue: VworkFeedQueueResult;
}): string {
  const waiting = vworkAccountStatus()
    .filter((account) => account.state !== 'ready')
    .map((account) => `${account.label}: key ${account.state}`);
  const pulled = input.accounts.map((account) =>
    account.error
      ? `${account.label}: ${account.error}`
      : `${account.label}: fetched ${account.fetched}, added ${account.inserted}${account.truncated ? ' (page limit reached)' : ''}`,
  );
  const live = input.configured || waiting.length > 0 ? [...pulled, ...waiting].join('. ') : 'No VWork API key is ready';
  const more = input.queue.remaining > 0 ? `, ${input.queue.remaining} still waiting` : '';
  return `${live}. Queued ${input.queue.queued}, skipped ${input.queue.skippedClosed} closed${more}.`;
}

/** One pass: pull new VWork jobs into the feed when the key is ready, then queue open feed jobs. */
export async function pollVworkApiOnce(): Promise<{ summary: string; queued: number }> {
  const s = state();
  if (s.inFlight) return { summary: 'Skipped: previous poll still running', queued: 0 };
  s.inFlight = true;
  const at = localClock();
  s.lastPolltime = at;
  try {
    const configured = vworkApiConfigured();
    const feedAccounts: VworkFeedAccountResult[] = configured ? (await checkVworkJobFeed(s.settings.lookbackDays)).accounts : [];
    const queue = await enqueueVworkFeedJobs();
    const summary = summaryText({ configured, accounts: feedAccounts, queue });
    const accountErrors = feedAccounts.filter((account) => account.error);
    s.lastSummary = summary;
    s.lastError =
      accountErrors.length > 0 && accountErrors.length === feedAccounts.length
        ? accountErrors.map((account) => `${account.label}: ${account.error}`).join('; ')
        : null;
    await writeLog(configured ? 'ok' : 'waiting-key', {
      at,
      keyState: vworkApiKeyState(),
      accounts: vworkAccountStatus(),
      lookbackDays: s.settings.lookbackDays,
      fetched: feedAccounts.reduce((sum, account) => sum + account.fetched, 0),
      inserted: feedAccounts.reduce((sum, account) => sum + account.inserted, 0),
      accountResults: feedAccounts,
      queued: queue.queued,
      skippedClosed: queue.skippedClosed,
      remaining: queue.remaining,
      summary,
    });
    return { summary, queued: queue.queued };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    s.lastError = message;
    s.lastSummary = null;
    await writeLog('error', { at, error: message });
    return { summary: message, queued: 0 };
  } finally {
    s.inFlight = false;
  }
}

export async function loadVworkApiSettings(): Promise<VworkApiSettings> {
  const settings = await readSettings();
  state().settings = settings;
  return settings;
}

export async function saveVworkApiSettings(input: Partial<VworkApiSettings>): Promise<VworkApiSettings> {
  const settings = normalizeSettings({ ...state().settings, ...input });
  await writeSettings(settings);
  const s = state();
  s.settings = settings;
  if (s.running) armTimer(s);
  return settings;
}

export async function startVworkApiPoll(input?: Partial<VworkApiSettings>): Promise<{ running: boolean; settings: VworkApiSettings }> {
  const s = state();
  if (input) await saveVworkApiSettings(input);
  else s.settings = await readSettings();
  if (s.running && s.timer) return { running: true, settings: s.settings };
  s.running = true;
  armTimer(s);
  void pollVworkApiOnce();
  return { running: true, settings: s.settings };
}

export function stopVworkApiPoll(): { running: boolean; settings: VworkApiSettings } {
  const s = state();
  if (s.timer) clearInterval(s.timer);
  s.timer = null;
  s.running = false;
  s.nextPollAtMs = null;
  return { running: false, settings: s.settings };
}

export function vworkApiPollStatus(): {
  running: boolean;
  intervalMs: number;
  settings: VworkApiSettings;
  keyState: 'missing' | 'placeholder' | 'ready';
  accounts: VworkAccountStatus[];
  configured: boolean;
  lastPolltime: string | null;
  lastSummary: string | null;
  lastError: string | null;
  nextPollAtMs: number | null;
  serverNowMs: number;
} {
  const s = state();
  const running = s.running && s.timer != null;
  return {
    running,
    intervalMs: s.settings.pollIntervalSeconds * 1000,
    settings: s.settings,
    keyState: vworkApiKeyState(),
    accounts: vworkAccountStatus(),
    configured: vworkApiConfigured(),
    lastPolltime: s.lastPolltime,
    lastSummary: s.lastSummary,
    lastError: s.lastError,
    nextPollAtMs: running ? s.nextPollAtMs : null,
    serverNowMs: Date.now(),
  };
}

export async function listVworkApiLogs(limit = 40): Promise<{ logid: number; logdatetime: string | null; logcat2: string | null; logdetails: string | null }[]> {
  const cap = Math.min(Math.max(limit, 1), 100);
  return query(
    `SELECT logid, logdatetime::text AS logdatetime, logcat2, logdetails
     FROM tbl_logs
     WHERE logtype = $1
     ORDER BY logid DESC
     LIMIT $2`,
    [LOGTYPE, cap],
  );
}

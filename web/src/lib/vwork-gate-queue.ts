import { execute, getClient, query } from '@/lib/db';
import { ensureVworkGateUpdateSchema } from '@/lib/ensure-vwork-gate-update-schema';
import { normalizeVworkAccount } from '@/lib/vwork-api';
import { pushDriverGateToVwork } from '@/lib/vwork-job-steps';

const SETTINGS_TYPE = 'VworkGateUpdates';
const MAX_ATTEMPTS = 5;
const BATCH = 8;
const PASS_LIMIT = 40;
const MANUAL_LIMIT = 500;
const MIN_INTERVAL_SECONDS = 5;
const MAX_INTERVAL_SECONDS = 3600;
const DEFAULT_INTERVAL_SECONDS = 60;

export type VworkGateUpdateRow = {
  id: string;
  driver_job_id: number;
  job_id: string | null;
  vwork_account: string | null;
  step: number;
  step_title: string;
  stage_at: string;
  received_at: string;
  status: string;
  executed_at: string | null;
  error: string | null;
  attempts: number;
  winery_name: string | null;
  vineyard_name: string | null;
  truck_id: string | null;
};

export type VworkGateUpdateCounts = {
  queued: number;
  sending: number;
  sent: number;
  failed: number;
  skipped: number;
};

type WorkerState = {
  timer: ReturnType<typeof setInterval> | null;
  running: boolean;
  inFlight: boolean;
  intervalSeconds: number;
  lastRunAt: string | null;
  lastError: string | null;
  lastSummary: string | null;
  nextRunAtMs: number | null;
  heartbeatMs: number;
};

const globalKey = '__geodataVworkGateUpdates';

function localClock(now = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

function clockMinus(ms: number): string {
  return localClock(new Date(Date.now() - ms));
}

function clampInt(value: unknown, min: number, max: number, fallback: number): number {
  const n = typeof value === 'number' ? value : parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(n)));
}

function state(): WorkerState {
  const g = globalThis as typeof globalThis & { [globalKey]?: WorkerState };
  if (!g[globalKey]) {
    g[globalKey] = {
      timer: null,
      running: false,
      inFlight: false,
      intervalSeconds: DEFAULT_INTERVAL_SECONDS,
      lastRunAt: null,
      lastError: null,
      lastSummary: null,
      nextRunAtMs: null,
      heartbeatMs: 0,
    };
  }
  const s = g[globalKey];
  if (s.running && s.timer) {
    const intervalMs = Math.max(MIN_INTERVAL_SECONDS, s.intervalSeconds) * 1000;
    const stalled = s.heartbeatMs === 0 || Date.now() - s.heartbeatMs > intervalMs + 15_000;
    if (stalled) armTimer(s);
  }
  return s;
}

function armTimer(s: WorkerState): void {
  if (s.timer) clearInterval(s.timer);
  s.heartbeatMs = Date.now();
  s.nextRunAtMs = Date.now() + s.intervalSeconds * 1000;
  s.timer = setInterval(() => {
    s.heartbeatMs = Date.now();
    s.nextRunAtMs = Date.now() + s.intervalSeconds * 1000;
    void drainVworkGateUpdates();
  }, s.intervalSeconds * 1000);
}

const EMPTY_COUNTS: VworkGateUpdateCounts = { queued: 0, sending: 0, sent: 0, failed: 0, skipped: 0 };

/** Driver tap path. Stores the stage and returns. Does not call VWork. */
export async function enqueueDriverGateUpdate(input: {
  driverJobId: number;
  jobId: string;
  step: number;
  stepTitle: string;
  stageAt: string;
}): Promise<void> {
  await ensureVworkGateUpdateSchema();
  const jobs = await query<{ vwork_account: string | null }>(
    `SELECT vwork_account FROM tbl_driver_jobs WHERE id = $1`,
    [input.driverJobId],
  );
  const account = normalizeVworkAccount(jobs[0]?.vwork_account);
  await execute(
    `INSERT INTO vwork_gate_updates
       (driver_job_id, job_id, vwork_account, step, step_title, stage_at, received_at, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, 'queued')
     ON CONFLICT (driver_job_id, step) DO NOTHING`,
    [input.driverJobId, input.jobId, account, input.step, input.stepTitle, input.stageAt, localClock()],
  );
}

async function claimBatch(): Promise<VworkGateUpdateRow[]> {
  const client = await getClient();
  try {
    await client.query('BEGIN');
    await client.query(
      `UPDATE vwork_gate_updates
       SET status = 'queued', claimed_at = NULL,
           error = 'Send was interrupted and will be tried again'
       WHERE status = 'sending'
         AND (claimed_at IS NULL OR claimed_at < $1)`,
      [clockMinus(120_000)],
    );
    const picked = await client.query<{ id: string }>(
      `SELECT id::text AS id
       FROM vwork_gate_updates
       WHERE status IN ('queued', 'failed') AND attempts < $1
       ORDER BY id
       LIMIT $2
       FOR UPDATE SKIP LOCKED`,
      [MAX_ATTEMPTS, BATCH],
    );
    const ids = picked.rows.map((row) => row.id);
    if (ids.length === 0) {
      await client.query('COMMIT');
      return [];
    }
    const claimed = await client.query<VworkGateUpdateRow>(
      `UPDATE vwork_gate_updates
       SET status = 'sending', attempts = attempts + 1, claimed_at = $2, error = NULL
       WHERE id = ANY($1::bigint[])
       RETURNING id::text AS id, driver_job_id, job_id, vwork_account, step, step_title, stage_at, received_at,
                 status, executed_at, error, attempts`,
      [ids, localClock()],
    );
    await client.query('COMMIT');
    return claimed.rows;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

let drainChain: Promise<unknown> = Promise.resolve();

/** Sends waiting rows to VWork. A manual pass keeps going until the queue is empty. */
export function drainVworkGateUpdates(options?: {
  untilEmpty?: boolean;
}): Promise<{ sent: number; failed: number; skipped: number; waiting: number }> {
  const run = drainChain.then(() => drainVworkGateUpdatesNow(options));
  drainChain = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}

async function drainVworkGateUpdatesNow(options?: {
  untilEmpty?: boolean;
}): Promise<{ sent: number; failed: number; skipped: number; waiting: number }> {
  const s = state();
  s.inFlight = true;
  s.lastRunAt = localClock();
  if (s.running) s.nextRunAtMs = Date.now();
  let sent = 0;
  let failed = 0;
  let skipped = 0;
  const limit = options?.untilEmpty ? MANUAL_LIMIT : PASS_LIMIT;
  try {
    await ensureVworkGateUpdateSchema();
    let processed = 0;
    while (processed < limit) {
      const claimed = await claimBatch();
      if (claimed.length === 0) break;
      processed += claimed.length;
      for (const row of claimed) {
        const result = await pushDriverGateToVwork(row.driver_job_id, row.step, row.stage_at, row.vwork_account);
        if (result.sent) {
          sent += 1;
          await execute(
            `UPDATE vwork_gate_updates
             SET status = 'sent', executed_at = $2, error = NULL, claimed_at = NULL
             WHERE id = $1::bigint`,
            [row.id, localClock()],
          );
        } else if (result.error) {
          failed += 1;
          await execute(
            `UPDATE vwork_gate_updates
             SET status = 'failed', error = $2, claimed_at = NULL
             WHERE id = $1::bigint`,
            [row.id, result.error.slice(0, 4000)],
          );
        } else {
          skipped += 1;
          await execute(
            `UPDATE vwork_gate_updates
             SET status = 'skipped', error = $2, claimed_at = NULL
             WHERE id = $1::bigint`,
            [row.id, 'This job has no VWork step to update'],
          );
        }
      }
    }
    const waitingRows = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM vwork_gate_updates WHERE status IN ('queued', 'failed') AND attempts < $1`,
      [MAX_ATTEMPTS],
    );
    const waiting = waitingRows[0]?.n ?? 0;
    s.lastError = failed > 0 ? `${failed} update${failed === 1 ? '' : 's'} failed` : null;
    s.lastSummary = `Sent ${sent}, failed ${failed}, skipped ${skipped}. ${waiting} still waiting.`;
    return { sent, failed, skipped, waiting };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    s.lastError = message;
    s.lastSummary = null;
    return { sent, failed, skipped, waiting: 0 };
  } finally {
    s.inFlight = false;
    if (options?.untilEmpty && s.running && s.timer) armTimer(s);
  }
}

export async function loadVworkGateUpdateSettings(): Promise<{ intervalSeconds: number }> {
  const rows = await query<{ settingvalue: string | null }>(
    `SELECT settingvalue FROM tbl_settings WHERE type = $1 AND settingname = 'intervalSeconds'`,
    [SETTINGS_TYPE],
  );
  const intervalSeconds = clampInt(rows[0]?.settingvalue, MIN_INTERVAL_SECONDS, MAX_INTERVAL_SECONDS, DEFAULT_INTERVAL_SECONDS);
  state().intervalSeconds = intervalSeconds;
  return { intervalSeconds };
}

export async function saveVworkGateUpdateSettings(input: { intervalSeconds?: number }): Promise<{ intervalSeconds: number }> {
  const intervalSeconds = clampInt(input.intervalSeconds, MIN_INTERVAL_SECONDS, MAX_INTERVAL_SECONDS, state().intervalSeconds);
  await execute('DELETE FROM tbl_settings WHERE type = $1 AND settingname = $2', [SETTINGS_TYPE, 'intervalSeconds']);
  await execute('INSERT INTO tbl_settings (type, settingname, settingvalue) VALUES ($1, $2, $3)', [
    SETTINGS_TYPE,
    'intervalSeconds',
    String(intervalSeconds),
  ]);
  const s = state();
  s.intervalSeconds = intervalSeconds;
  if (s.running) armTimer(s);
  return { intervalSeconds };
}

export async function startVworkGateUpdates(input?: { intervalSeconds?: number }): Promise<{ running: boolean; intervalSeconds: number }> {
  const s = state();
  if (input?.intervalSeconds != null) await saveVworkGateUpdateSettings(input);
  else s.intervalSeconds = (await loadVworkGateUpdateSettings()).intervalSeconds;
  if (s.running && s.timer) return { running: true, intervalSeconds: s.intervalSeconds };
  s.running = true;
  armTimer(s);
  void drainVworkGateUpdates();
  return { running: true, intervalSeconds: s.intervalSeconds };
}

export function stopVworkGateUpdates(): { running: boolean; intervalSeconds: number } {
  const s = state();
  if (s.timer) clearInterval(s.timer);
  s.timer = null;
  s.running = false;
  s.nextRunAtMs = null;
  return { running: false, intervalSeconds: s.intervalSeconds };
}

export function vworkGateUpdateStatus(): {
  running: boolean;
  intervalSeconds: number;
  lastRunAt: string | null;
  lastError: string | null;
  lastSummary: string | null;
  nextRunAtMs: number | null;
  serverNowMs: number;
} {
  const s = state();
  const running = s.running && s.timer != null;
  return {
    running,
    intervalSeconds: s.intervalSeconds,
    lastRunAt: s.lastRunAt,
    lastError: s.lastError,
    lastSummary: s.lastSummary,
    nextRunAtMs: running ? s.nextRunAtMs : null,
    serverNowMs: Date.now(),
  };
}

export async function listVworkGateUpdates(limit = 100): Promise<{ rows: VworkGateUpdateRow[]; counts: VworkGateUpdateCounts }> {
  await ensureVworkGateUpdateSchema();
  const cap = Math.min(Math.max(limit, 1), 200);
  const [rows, grouped] = await Promise.all([
    query<VworkGateUpdateRow>(
      `SELECT g.id::text AS id, g.driver_job_id, g.job_id, g.vwork_account, g.step, g.step_title, g.stage_at, g.received_at,
              g.status, g.executed_at, g.error, g.attempts,
              j.winery_name, j.vineyard_name, j.truck_id
       FROM vwork_gate_updates g
       LEFT JOIN tbl_driver_jobs j ON j.id = g.driver_job_id
       ORDER BY g.id DESC
       LIMIT $1`,
      [cap],
    ),
    query<{ status: string; n: number }>(
      `SELECT status, count(*)::int AS n FROM vwork_gate_updates GROUP BY status`,
    ),
  ]);
  const counts = { ...EMPTY_COUNTS };
  for (const row of grouped) {
    if (row.status === 'queued' || row.status === 'sending' || row.status === 'sent' || row.status === 'failed' || row.status === 'skipped') {
      counts[row.status] = row.n;
    }
  }
  return { rows, counts };
}

/** Puts failed and skipped rows back in the queue for another send. */
export async function retryFailedVworkGateUpdates(): Promise<number> {
  await ensureVworkGateUpdateSchema();
  return execute(
    `UPDATE vwork_gate_updates
     SET status = 'queued', attempts = 0, error = NULL, claimed_at = NULL
     WHERE status IN ('failed', 'skipped')`,
  );
}

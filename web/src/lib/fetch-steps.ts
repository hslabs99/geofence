/**
 * Single place for "fetch GPS steps" logic: run derived-steps for one or many jobs.
 * Used by Admin → API GPS Import (Step 4) and Query → Inspect (refetch single job).
 * Optional args: jobId (single job), force (ignore steps_fetched filter when loading job list).
 *
 * When editing code here: never adjust timezones or add/subtract hours for display. All times
 * in the DB are already NZT regardless of any timezone stamps they may have.
 *
 * All timestamps are treated as raw (YYYY-MM-DD HH:mm:ss). No timezone conversion or +/- offset
 * is ever applied — same scale as position_time_nz and vwork job times.
 */

/**
 * Add deltaMinutes to a timestamp string. Input and output are YYYY-MM-DD HH:mm:ss (or ISO with T).
 * Uses only string parsing and arithmetic — no Date, no timezone, no UTC. Raw times only.
 */
export function addMinutesToRawNZ(ts: string, deltaMinutes: number): string {
  const s = String(ts).trim().replace('T', ' ').slice(0, 19);
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return s;
  const [, yStr, moStr, dStr, hStr, minStr, secStr] = m;
  const year = parseInt(yStr!, 10);
  const month = parseInt(moStr!, 10);
  const day = parseInt(dStr!, 10);
  const hour = parseInt(hStr!, 10);
  const minute = parseInt(minStr!, 10);
  const second = parseInt(secStr!, 10);
  // Treat digits as single scale: interpret as UTC instant, add delta minutes, format back (no TZ offset).
  const epochMs = Date.UTC(year, month - 1, day, hour, minute, second);
  const out = new Date(epochMs + deltaMinutes * 60 * 1000);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${out.getUTCFullYear()}-${pad(out.getUTCMonth() + 1)}-${pad(out.getUTCDate())} ${pad(out.getUTCHours())}:${pad(out.getUTCMinutes())}:${pad(out.getUTCSeconds())}`;
}

/** Alias: raw NZ in, add minutes, raw NZ out. Use for positionAfter/positionBefore. */
export const addMinutesToTimestampAsNZ = addMinutesToRawNZ;

/** Add deltaMinutes to a timestamp string (local time); return YYYY-MM-DD HH:mm:ss. Use for display only. */
export function addMinutesToTimestamp(ts: string, deltaMinutes: number): string {
  const s = String(ts).trim();
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return s;
  d.setMinutes(d.getMinutes() + deltaMinutes);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  const h = String(d.getHours()).padStart(2, '0');
  const min = String(d.getMinutes()).padStart(2, '0');
  const sec = String(d.getSeconds()).padStart(2, '0');
  return `${y}-${m}-${day} ${h}:${min}:${sec}`;
}

/** Minimal job shape for derived-steps: job_id, worker (device_name for tbl_tracking), actual_start_time, actual_end_time (or gps_end_time). */
export type FetchStepsJob = Record<string, unknown>;

export type FetchStepsLogEntry = {
  job_id: string;
  status: 'ok' | 'skip' | 'error';
  message?: string;
};

export type RunFetchStepsOptions = {
  /** Jobs to process. If empty, nothing runs. */
  jobs: FetchStepsJob[];
  startLessMinutes: number;
  endPlusMinutes: number;
  /** Called after each job with (currentIndex1Based, total, logSoFar). */
  onProgress?: (current: number, total: number, log: FetchStepsLogEntry[]) => void;
};

export type RunFetchStepsResult = {
  log: FetchStepsLogEntry[];
  /** When jobs.length === 1, the raw API response for debug (e.g. Inspect log). */
  lastResult?: unknown;
};

/**
 * Run derived-steps (with writeBack) for each job. Single code path for both
 * "fetch steps for date" (many jobs) and "refetch this job" (one job, force).
 * Each call clears GPS step fields 1–5 and final steps first, then runs part 1 (standard fence)
 * and part 2 (Steps2 buffered vineyard fence if step 2/3 missing). Used by Inspect, Tagging, API test.
 */
export async function runFetchStepsForJobs(options: RunFetchStepsOptions): Promise<RunFetchStepsResult> {
  const { jobs, startLessMinutes, endPlusMinutes, onProgress } = options;
  const log: FetchStepsLogEntry[] = [];
  let lastResult: unknown;

  /** Normalize field from job (API/DB may return mixed-case keys e.g. Worker, Actual_Start_Time). */
  const get = (j: Record<string, unknown>, ...keys: string[]): string => {
    for (const k of keys) {
      const v = j[k];
      if (v != null && v !== '') return String(v).trim();
    }
    return '';
  };

  for (let i = 0; i < jobs.length; i++) {
    const job = jobs[i] as Record<string, unknown>;
    const jobIdStr = job.job_id != null ? String(job.job_id) : '';
    const device = get(job, 'worker', 'Worker') || get(job, 'truck_id', 'Truck_ID');
    const actualStart = get(job, 'actual_start_time', 'Actual_Start_Time');
    const actualEnd =
      get(job, 'actual_end_time', 'Actual_End_Time') ||
      get(job, 'gps_end_time', 'Gps_End_Time');

    if (!device || !actualStart) {
      log.push({ job_id: jobIdStr || '?', status: 'skip', message: 'no worker (device_name) or actual_start_time' });
    } else {
      const positionAfter = addMinutesToTimestampAsNZ(actualStart, -startLessMinutes);
      const positionBefore = actualEnd ? addMinutesToTimestampAsNZ(actualEnd, endPlusMinutes) : '';
      const params = new URLSearchParams({ jobId: jobIdStr, device, positionAfter });
      if (positionBefore) params.set('positionBefore', positionBefore);
      params.set('writeBack', '1');
      try {
        const r = await fetch(`/api/tracking/derived-steps?${params}`);
        const result = await r.json().catch(() => ({}));
        lastResult = result;
        log.push({
          job_id: jobIdStr || '?',
          status: r.ok ? 'ok' : 'error',
          message: r.ok ? undefined : (result?.error ?? String(r.status)),
        });
      } catch (e) {
        log.push({
          job_id: jobIdStr || '?',
          status: 'error',
          message: e instanceof Error ? e.message : String(e),
        });
      }
    }
    onProgress?.(i + 1, jobs.length, [...log]);
  }

  return {
    log,
    lastResult: jobs.length === 1 ? lastResult : undefined,
  };
}

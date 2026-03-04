/**
 * Single place for "fetch GPS steps" logic: run derived-steps for one or many jobs.
 * Used by Admin → API GPS Import (Step 4) and Query → Inspect (refetch single job).
 * Optional args: jobId (single job), force (ignore steps_fetched filter when loading job list).
 */

/** Add deltaMinutes to a timestamp string; return YYYY-MM-DD HH:mm:ss for derived-steps API. */
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
 */
export async function runFetchStepsForJobs(options: RunFetchStepsOptions): Promise<RunFetchStepsResult> {
  const { jobs, startLessMinutes, endPlusMinutes, onProgress } = options;
  const log: FetchStepsLogEntry[] = [];
  let lastResult: unknown;

  for (let i = 0; i < jobs.length; i++) {
    const job = jobs[i];
    const jobIdStr = job.job_id != null ? String(job.job_id) : '';
    const device = (job.worker != null ? String(job.worker).trim() : '') || (job.truck_id != null ? String(job.truck_id).trim() : '');
    const actualStart = job.actual_start_time != null ? String(job.actual_start_time).trim() : '';
    const actualEnd =
      job.actual_end_time != null
        ? String(job.actual_end_time).trim()
        : job.gps_end_time != null
          ? String(job.gps_end_time).trim()
          : '';

    if (!device || !actualStart) {
      log.push({ job_id: jobIdStr || '?', status: 'skip', message: 'no worker (device_name) or actual_start_time' });
    } else {
      const positionAfter = addMinutesToTimestamp(actualStart, -startLessMinutes);
      const positionBefore = actualEnd ? addMinutesToTimestamp(actualEnd, endPlusMinutes) : '';
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

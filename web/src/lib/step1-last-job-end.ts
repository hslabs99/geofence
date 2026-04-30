/**
 * Step1(lastJobEnd): set step_1_completed_at from the previous same-day job’s Step 5 (GPS-backed) when the
 * outbound vineyard story is broken or missing a morning winery exit, and preserve the imported Step 1 in
 * step_1_safe (once).
 *
 * - **Legacy path:** no morning winery EXIT before vineyard ENTER; previous Step 5 strictly before
 *   actual_start_time; gap within limit (move “backward” in time vs a late import).
 * - **Missing vineyard Step 2+3 path:** no polygon vineyard ENTER and no vineyard EXIT in the GPS window;
 *   then allow moving Step 1 **forward or backward** to previous job Step 5 if within symmetric gap limit
 *   (e.g. VWork start before previous job could have finished).
 */
import { execute, query } from '@/lib/db';
import {
  fetchOutboundLegAnchors,
  type JobForDerivedSteps,
  normalizeTimestampString,
  type DerivedStepsOptions,
} from '@/lib/derived-steps';

/** Snapshot for Inspect → Explanation → “Acid Test Step1”. */
export type Step1LastJobEndDebug = {
  thisJobActualStartAt?: string;
  missingVineyardStep23?: boolean;
  morningWineryExitFound?: boolean;
  vineyardEnterFound?: boolean;
  vineyardExitFound?: boolean;
  /** Previous job id when a chain candidate was loaded. */
  previousJobId?: string;
  /** Candidate Step 5 end from previous row (step_5_actual_time preferred, else step_5_gps_completed_at). */
  previousJobStep5At?: string;
  limitMinutes: number;
};

export type Step1LastJobEndResult = {
  applied: boolean;
  reason?:
    | 'write_back_disabled'
    | 'step1_oride'
    | 'step_1_safe_set'
    | 'no_worker'
    | 'no_delivery_winery'
    | 'no_actual_start'
    | 'winery_exit_found'
    | 'no_previous_job'
    | 'previous_no_step5_gps'
    | 'no_step5_time'
    | 'previous_end_not_before_current_start'
    | 'previous_job_gap_exceeds_limit'
    | 'gap_minutes_unresolved'
    | 'no_time_change'
    | 'db_update_skipped';
  previousJobId?: string;
  /** Minutes cap from tbl_settings (echoed for Inspect/debug). */
  limitMinutes?: number;
  /** Which rule branch was evaluated (when not write_back_disabled). */
  path?: 'legacy_no_morning_exit' | 'missing_vineyard_step23';
  debug?: Step1LastJobEndDebug;
};

/** Match tbl_vworkjobs row keys regardless of PostgreSQL / driver casing (e.g. step_5_gps_completed_at). */
function getColumnCaseInsensitive(row: Record<string, unknown>, logicalName: string): unknown {
  const want = logicalName.toLowerCase();
  for (const [k, v] of Object.entries(row)) {
    if (k.toLowerCase() === want) return v;
  }
  return undefined;
}

/** Minutes from tsEarlier to tsLater (naive UTC on YYYY-MM-DD HH:mm:ss). */
function minutesBetween(tsEarlier: string, tsLater: string): number | null {
  const a = normalizeTimestampString(tsEarlier);
  const b = normalizeTimestampString(tsLater);
  if (!a || !b) return null;
  const ma = a.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  const mb = b.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  if (!ma || !mb) return null;
  const msA = Date.UTC(Number(ma[1]), Number(ma[2]) - 1, Number(ma[3]), Number(ma[4]), Number(ma[5]), Number(ma[6]));
  const msB = Date.UTC(Number(mb[1]), Number(mb[2]) - 1, Number(mb[3]), Number(mb[4]), Number(mb[5]), Number(mb[6]));
  return Math.round((msB - msA) / (60 * 1000));
}

/** Non-negative minutes between two timestamps (order-independent). */
function minutesApart(tsA: string, tsB: string): number | null {
  const a = normalizeTimestampString(tsA);
  const b = normalizeTimestampString(tsB);
  if (!a || !b) return null;
  return a <= b ? minutesBetween(a, b) : minutesBetween(b, a);
}

export async function applyStep1LastJobEndIfEligible(
  jobId: string,
  job: JobForDerivedSteps,
  rawJob: Record<string, unknown>,
  deviceForTracking: string,
  opts: DerivedStepsOptions,
  writeBack: boolean,
  /** Max minutes between previous job Step 5 and this job actual_start_time (tbl_settings). */
  step1FromPreviousJobLimitMinutes: number
): Promise<Step1LastJobEndResult> {
  const limitEcho = { limitMinutes: step1FromPreviousJobLimitMinutes };
  const limOnly = (extra?: Partial<Step1LastJobEndDebug>): Step1LastJobEndDebug => ({
    limitMinutes: step1FromPreviousJobLimitMinutes,
    ...extra,
  });

  if (!writeBack) {
    return { applied: false, reason: 'write_back_disabled', ...limitEcho, debug: limOnly() };
  }

  const step1oride = getColumnCaseInsensitive(rawJob, 'step1oride');
  if (step1oride != null && String(step1oride).trim() !== '') {
    return { applied: false, reason: 'step1_oride', ...limitEcho, debug: limOnly() };
  }

  const safeExisting = getColumnCaseInsensitive(rawJob, 'step_1_safe');
  if (safeExisting != null && String(safeExisting).trim() !== '') {
    return { applied: false, reason: 'step_1_safe_set', ...limitEcho, debug: limOnly() };
  }

  const workerTrim =
    job.worker != null && String(job.worker).trim() !== ''
      ? String(job.worker).trim()
      : deviceForTracking.trim();
  if (!workerTrim) {
    return { applied: false, reason: 'no_worker', ...limitEcho, debug: limOnly() };
  }

  const deliveryWinery = job.delivery_winery != null ? String(job.delivery_winery).trim() : '';
  if (!deliveryWinery) {
    return { applied: false, reason: 'no_delivery_winery', ...limitEcho, debug: limOnly() };
  }

  const actualStartRaw = getColumnCaseInsensitive(rawJob, 'actual_start_time');
  if (actualStartRaw == null || String(actualStartRaw).trim() === '') {
    return { applied: false, reason: 'no_actual_start', ...limitEcho, debug: limOnly() };
  }
  const actualStartNorm = normalizeTimestampString(actualStartRaw as string | Date);
  if (!actualStartNorm) {
    return { applied: false, reason: 'no_actual_start', ...limitEcho, debug: limOnly() };
  }

  const anchors = await fetchOutboundLegAnchors(job, opts);
  const missingVineyardStep23 = anchors.vineyardEnter == null && anchors.vineyardExit == null;

  const dbgAnchors: Step1LastJobEndDebug = limOnly({
    thisJobActualStartAt: actualStartNorm,
    missingVineyardStep23,
    morningWineryExitFound: anchors.wineryExit != null,
    vineyardEnterFound: anchors.vineyardEnter != null,
    vineyardExitFound: anchors.vineyardExit != null,
  });

  if (!missingVineyardStep23 && anchors.wineryExit != null) {
    return {
      applied: false,
      reason: 'winery_exit_found',
      ...limitEcho,
      debug: dbgAnchors,
    };
  }

  const dayRows = await query<Record<string, unknown>>(
    `SELECT job_id, step_5_actual_time, step_5_gps_completed_at, actual_start_time
     FROM tbl_vworkjobs
     WHERE LOWER(TRIM(COALESCE(worker::text, ''))) = LOWER(TRIM($1::text))
       AND actual_start_time IS NOT NULL
       AND (actual_start_time::date) = ($2::timestamp)::date
       AND LOWER(TRIM(COALESCE(delivery_winery, ''))) = LOWER(TRIM($3::text))
     ORDER BY actual_start_time ASC, trim(job_id::text) ASC`,
    [workerTrim, actualStartNorm, deliveryWinery]
  );

  const selfId = String(jobId).trim();
  const idx = dayRows.findIndex((r) => String(getColumnCaseInsensitive(r, 'job_id') ?? '').trim() === selfId);
  if (idx <= 0) {
    return { applied: false, reason: 'no_previous_job', ...limitEcho, debug: dbgAnchors };
  }
  const prev = dayRows[idx - 1]!;
  const prevJobId = String(getColumnCaseInsensitive(prev, 'job_id') ?? '').trim();
  const prevStep5Gps = getColumnCaseInsensitive(prev, 'step_5_gps_completed_at');
  if (prevStep5Gps == null || String(prevStep5Gps).trim() === '') {
    return {
      applied: false,
      reason: 'previous_no_step5_gps',
      ...limitEcho,
      debug: { ...dbgAnchors, previousJobId: prevJobId },
    };
  }

  const prevStep5Actual = getColumnCaseInsensitive(prev, 'step_5_actual_time');
  const step5ForStep1 =
    prevStep5Actual != null && String(prevStep5Actual).trim() !== ''
      ? normalizeTimestampString(prevStep5Actual as string | Date)
      : normalizeTimestampString(prevStep5Gps as string | Date);

  if (!step5ForStep1) {
    return {
      applied: false,
      reason: 'no_step5_time',
      ...limitEcho,
      debug: { ...dbgAnchors, previousJobId: prevJobId },
    };
  }

  const dbgWithPrev: Step1LastJobEndDebug = {
    ...dbgAnchors,
    previousJobId: prevJobId,
    previousJobStep5At: step5ForStep1,
  };

  if (step5ForStep1 === actualStartNorm) {
    return {
      applied: false,
      reason: 'no_time_change',
      ...limitEcho,
      path: missingVineyardStep23 ? 'missing_vineyard_step23' : 'legacy_no_morning_exit',
      debug: dbgWithPrev,
    };
  }

  let path: Step1LastJobEndResult['path'];

  if (missingVineyardStep23) {
    path = 'missing_vineyard_step23';
    const gapApart = minutesApart(step5ForStep1, actualStartNorm);
    if (gapApart == null) {
      return { applied: false, reason: 'gap_minutes_unresolved', ...limitEcho, path, debug: dbgWithPrev };
    }
    if (gapApart > step1FromPreviousJobLimitMinutes) {
      return { applied: false, reason: 'previous_job_gap_exceeds_limit', ...limitEcho, path, debug: dbgWithPrev };
    }
  } else {
    path = 'legacy_no_morning_exit';
    if (step5ForStep1 >= actualStartNorm) {
      return {
        applied: false,
        reason: 'previous_end_not_before_current_start',
        ...limitEcho,
        path,
        debug: dbgWithPrev,
      };
    }
    const gapMinutes = minutesBetween(step5ForStep1, actualStartNorm);
    if (gapMinutes == null) {
      return { applied: false, reason: 'gap_minutes_unresolved', ...limitEcho, path, debug: dbgWithPrev };
    }
    if (gapMinutes > step1FromPreviousJobLimitMinutes) {
      return { applied: false, reason: 'previous_job_gap_exceeds_limit', ...limitEcho, path, debug: dbgWithPrev };
    }
  }

  const n = await execute(
    `UPDATE tbl_vworkjobs SET
       step_1_safe = COALESCE(step_1_safe, step_1_completed_at),
       step_1_completed_at = $1::timestamp,
       calcnotes = COALESCE(TRIM(calcnotes) || ' ', '') || 'Step1(lastJobEnd):'
     WHERE trim(job_id::text) = trim($2::text)
       AND step_1_safe IS NULL`,
    [step5ForStep1, jobId]
  );

  if (n === 0) {
    return { applied: false, reason: 'db_update_skipped', ...limitEcho, path, debug: dbgWithPrev };
  }

  return {
    applied: true,
    previousJobId: prevJobId,
    ...limitEcho,
    path,
    debug: dbgWithPrev,
  };
}

/**
 * Jobs vs tbl_tracking GPS window coverage — uses the same window as Query → Inspect (see inspect-gps-window.ts).
 * Gap rule: any hole of ≥ threshold minutes inside the open NZ window counts as missing coverage.
 * When Inspect has no upper bound (no actual_end), we cap ordered samples for gap math (see INSPECTION_UNBOUNDED_NZ_SAMPLE_CAP).
 */

import { query } from '@/lib/db';
import { minutesBetweenNz } from '@/lib/distances-gps-harvest';
import { buildInspectGpsWindowForJob, type InspectGpsWindowOptions } from '@/lib/inspect-gps-window';
import { normalizeTimestampString } from '@/lib/derived-steps';
import { mapApiStepViaToDisplay } from '@/lib/fetch-steps';

export const JOBS_GPS_GAP_THRESHOLD_MINUTES = 10;

/** Max ordered points loaded when GPS To is open (Inspect has no end) — keeps report bounded. */
const INSPECTION_UNBOUNDED_NZ_SAMPLE_CAP = 25_000;

/** One tbl_tracking row snapshot for gap popup (verbatim times from DB). */
export type JobsGpsGapTrackingRow = {
  id: number | null;
  position_time_nz: string | null;
  position_time: string | null;
  lat: number | null;
  lon: number | null;
  fence_name: string | null;
  geofence_type: string | null;
};

/** One hole between consecutive coverage: last fix before gap → next fix after gap. */
export type JobsGpsGapSegment = {
  kind: 'start' | 'between' | 'end' | 'empty_window';
  /** Last GPS time before the gap (null = before first point / window start). */
  gps_last: string | null;
  /** First GPS time after the gap (null = after last point / window end). */
  gps_next: string | null;
  /** tbl_tracking row before the gap; null when that side is a window bound, not a ping. */
  prev_row: JobsGpsGapTrackingRow | null;
  /** tbl_tracking row after the gap; null when that side is a window bound, not a ping. */
  next_row: JobsGpsGapTrackingRow | null;
  /**
   * Ping immediately before `prev_row` in time order (extra context). Null if there is no earlier ping
   * (e.g. first in-window ping for a start gap, or second point for a between gap).
   */
  row_before_prev: JobsGpsGapTrackingRow | null;
  /** Minutes with no GPS ping; null only when window has no upper bound and no points (unbounded). */
  gap_minutes: number | null;
  /** True when gap_minutes ≥ threshold (same rule as report). */
  is_issue: boolean;
};

function mapQueryRowToGapTrackingRow(r: Record<string, unknown>): JobsGpsGapTrackingRow {
  const idRaw = r.id;
  const id =
    idRaw != null && String(idRaw).trim() !== ''
      ? parseInt(String(idRaw), 10)
      : null;
  const latRaw = r.lat;
  const lonRaw = r.lon;
  const lat =
    latRaw != null && latRaw !== '' && !Number.isNaN(Number(latRaw)) ? Number(latRaw) : null;
  const lon =
    lonRaw != null && lonRaw !== '' && !Number.isNaN(Number(lonRaw)) ? Number(lonRaw) : null;
  const fn = r.fence_name != null ? String(r.fence_name).trim() : '';
  const gt = r.geofence_type != null ? String(r.geofence_type).trim() : '';
  return {
    id: Number.isFinite(id as number) ? (id as number) : null,
    position_time_nz:
      r.position_time_nz != null && String(r.position_time_nz).trim() !== ''
        ? String(r.position_time_nz).trim()
        : null,
    position_time:
      r.position_time != null && String(r.position_time).trim() !== ''
        ? String(r.position_time).trim()
        : null,
    lat,
    lon,
    fence_name: fn || null,
    geofence_type: gt || null,
  };
}

export type JobsGpsGapsDetailResult = {
  ok: true;
  job_id: string;
  gps_from: string;
  gps_to: string | null;
  gap_threshold_minutes: number;
  truncated_sample: boolean;
  gaps: JobsGpsGapSegment[];
};

export type JobsGpsGapsDetailError = {
  ok: false;
  error: string;
};

export type JobsGpsReportRow = {
  job_id: string;
  customer: string | null;
  delivery_winery: string | null;
  vineyard_name: string | null;
  actual_start_time: string | null;
  /** Lower bound of tracking lookup window (NZ), exclusive for points: &gt; this. */
  gps_from: string | null;
  /** Upper bound (NZ), exclusive: &lt; this; null = open-ended (Inspect when no actual_end). */
  gps_to: string | null;
  /** Minutes from gps_from (exclusive) to first NZ fix; null if no points in window. */
  minutes_after_gps_from_to_first_fix: number | null;
  /** Minutes from last NZ fix to gps_to (exclusive); null if open-ended or no last fix. */
  minutes_from_last_fix_to_gps_to: number | null;
  min_gps_nz: string | null;
  max_gps_nz: string | null;
  max_gap_minutes: number | null;
  gap_missing: boolean;
  critical_null_nz_count: number;
  critical_null_nz: boolean;
  incomplete: boolean;
  incomplete_reason: string | null;
  step1_via: string;
  step2_via: string;
  step3_via: string;
  step4_via: string;
  step5_via: string;
  worker: string | null;
  /** Device used for tbl_tracking lookup (worker or truck_id), same as Inspect window. */
  device_name: string | null;
  /** Rows in the GPS gaps popup for this job (same Inspect window segment list). */
  gap_segment_count: number;
  points_in_window: number;
};

export { jobsGpsRowBreachesLimits, type JobsGpsReportRowBreachInput } from './jobs-gps-breach';

function viaLabel(v: unknown): string {
  const s = v != null && String(v).trim() !== '' ? String(v).trim() : '';
  return mapApiStepViaToDisplay(s || null);
}

function computeMaxGapMinutes(
  positionAfter: string,
  positionBefore: string | null,
  nzTimes: string[],
  threshold: number
): { maxGap: number | null; gapMissing: boolean } {
  const after = normalizeTimestampString(positionAfter) ?? positionAfter.slice(0, 19);
  if (nzTimes.length === 0) {
    if (positionBefore) {
      const before = normalizeTimestampString(positionBefore) ?? positionBefore.slice(0, 19);
      const full = minutesBetweenNz(after, before);
      const w = full ?? 0;
      return { maxGap: full, gapMissing: w >= threshold };
    }
    return { maxGap: null, gapMissing: true };
  }
  let maxG = 0;
  const startGap = minutesBetweenNz(after, nzTimes[0]!);
  if (startGap != null && startGap > maxG) maxG = startGap;
  for (let i = 1; i < nzTimes.length; i++) {
    const g = minutesBetweenNz(nzTimes[i - 1]!, nzTimes[i]!);
    if (g != null && g > maxG) maxG = g;
  }
  if (positionBefore) {
    const before = normalizeTimestampString(positionBefore) ?? positionBefore.slice(0, 19);
    const endGap = minutesBetweenNz(nzTimes[nzTimes.length - 1]!, before);
    if (endGap != null && endGap > maxG) maxG = endGap;
  }
  return { maxGap: maxG, gapMissing: maxG >= threshold };
}

function roundMin(m: number): number {
  return Math.round(m * 100) / 100;
}

/**
 * Enumerates gaps (holes) between window bounds and GPS points — same ordering as max-gap logic.
 * `nzRows` are ordered tbl_tracking rows with position_time_nz set (same window query as the report).
 */
export function buildGapSegmentsList(
  rawAfter: string,
  positionBefore: string | null,
  nzRows: JobsGpsGapTrackingRow[],
  threshold: number
): JobsGpsGapSegment[] {
  const afterN = normalizeTimestampString(rawAfter) ?? rawAfter.slice(0, 19);
  const gaps: JobsGpsGapSegment[] = [];

  if (nzRows.length === 0) {
    if (positionBefore) {
      const beforeN = normalizeTimestampString(positionBefore) ?? positionBefore.slice(0, 19);
      const g = minutesBetweenNz(afterN, beforeN) ?? 0;
      gaps.push({
        kind: 'empty_window',
        gps_last: null,
        gps_next: null,
        prev_row: null,
        next_row: null,
        row_before_prev: null,
        gap_minutes: roundMin(g),
        is_issue: g >= threshold,
      });
    } else {
      gaps.push({
        kind: 'empty_window',
        gps_last: null,
        gps_next: null,
        prev_row: null,
        next_row: null,
        row_before_prev: null,
        gap_minutes: null,
        is_issue: true,
      });
    }
    return gaps;
  }

  const nz0 = nzRows[0]!.position_time_nz;
  if (nz0) {
    const gStart = minutesBetweenNz(afterN, nz0);
    if (gStart != null) {
      gaps.push({
        kind: 'start',
        gps_last: rawAfter,
        gps_next: nz0,
        prev_row: null,
        next_row: nzRows[0]!,
        row_before_prev: null,
        gap_minutes: roundMin(gStart),
        is_issue: gStart >= threshold,
      });
    }
  }
  for (let i = 1; i < nzRows.length; i++) {
    const a = nzRows[i - 1]!.position_time_nz;
    const b = nzRows[i]!.position_time_nz;
    if (!a || !b) continue;
    const g = minutesBetweenNz(a, b);
    if (g != null) {
      gaps.push({
        kind: 'between',
        gps_last: a,
        gps_next: b,
        prev_row: nzRows[i - 1]!,
        next_row: nzRows[i]!,
        row_before_prev: i >= 2 ? nzRows[i - 2]! : null,
        gap_minutes: roundMin(g),
        is_issue: g >= threshold,
      });
    }
  }
  if (positionBefore) {
    const beforeN = normalizeTimestampString(positionBefore) ?? positionBefore.slice(0, 19);
    const lastNz = nzRows[nzRows.length - 1]!.position_time_nz;
    if (lastNz) {
      const gEnd = minutesBetweenNz(lastNz, beforeN);
      if (gEnd != null) {
        gaps.push({
          kind: 'end',
          gps_last: lastNz,
          gps_next: normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19),
          prev_row: nzRows[nzRows.length - 1]!,
          next_row: null,
          row_before_prev:
            nzRows.length >= 2 ? nzRows[nzRows.length - 2]! : null,
          gap_minutes: roundMin(gEnd),
          is_issue: gEnd >= threshold,
        });
      }
    }
  }
  return gaps;
}

/**
 * NZ bounding interval for each **issue** gap (≥ threshold), for reimport / daily hull.
 * Same segment rules as the gaps popup; uses window bounds for empty-window holes.
 */
export function extractIssueGapNZSpansFromSegments(
  segments: JobsGpsGapSegment[],
  rawAfter: string,
  positionBefore: string | null
): Array<{ from: string; to: string }> {
  const afterN = normalizeTimestampString(rawAfter) ?? rawAfter.slice(0, 19);
  const beforeN = positionBefore
    ? normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19)
    : null;

  const out: Array<{ from: string; to: string }> = [];
  for (const seg of segments) {
    if (!seg.is_issue) continue;

    if (seg.kind === 'empty_window') {
      if (beforeN != null) {
        out.push({ from: afterN, to: beforeN });
      }
      continue;
    }

    const fromRaw = seg.gps_last;
    const toRaw = seg.gps_next;
    const from =
      fromRaw != null
        ? normalizeTimestampString(fromRaw) ?? String(fromRaw).trim().slice(0, 19)
        : afterN;
    let to: string | null =
      toRaw != null
        ? normalizeTimestampString(toRaw) ?? String(toRaw).trim().slice(0, 19)
        : beforeN != null
          ? beforeN
          : null;
    if (to == null) continue;
    out.push({ from, to });
  }
  return out;
}

export type JobsGpsReportBuild = {
  row: JobsGpsReportRow;
  /** Issue gap segments (NZ) for hull aggregate by calendar day. */
  gapIssueNZSpans: Array<{ from: string; to: string }>;
  /** From `actual_start_time` (YYYY-MM-DD), or "—". */
  jobCalendarDay: string;
};

/**
 * Load ordered NZ times (same queries as the report) and return gap segments for the popup.
 */
export async function listJobsGpsWindowGaps(
  rawJob: Record<string, unknown>,
  windowOpts: InspectGpsWindowOptions,
  gapThresholdMinutes: number = JOBS_GPS_GAP_THRESHOLD_MINUTES
): Promise<JobsGpsGapsDetailResult | JobsGpsGapsDetailError> {
  const jobId = rawJob.job_id != null ? String(rawJob.job_id).trim() : '';
  const win = buildInspectGpsWindowForJob(rawJob, windowOpts);
  if (win.error) {
    return { ok: false, error: win.error };
  }
  const device = win.device;
  const positionAfter = win.positionAfter;
  const positionBefore = win.positionBefore;
  const rawAfter = normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19);

  const trackingSelect = `SELECT t.id,
       to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz,
       to_char(t.position_time, 'YYYY-MM-DD HH24:MI:SS') AS position_time,
       t.lat,
       t.lon,
       COALESCE(g.fence_name, '') AS fence_name,
       COALESCE(t.geofence_type::text, '') AS geofence_type
     FROM tbl_tracking t
     LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id`;

  let nzPingRows: JobsGpsGapTrackingRow[];
  let truncatedSample = false;

  if (positionBefore) {
    const rawBefore = normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19);
    const nzRaw = await query<Record<string, unknown>>(
      `${trackingSelect}
       WHERE t.device_name = $1
         AND t.position_time_nz IS NOT NULL
         AND t.position_time_nz > $2::timestamp
         AND t.position_time_nz < $3::timestamp
       ORDER BY t.position_time_nz ASC, t.id ASC`,
      [device, rawAfter, rawBefore]
    );
    nzPingRows = nzRaw.map(mapQueryRowToGapTrackingRow);
  } else {
    const cntRow = await query<{ c: string }>(
      `SELECT COUNT(*)::text AS c
       FROM tbl_tracking t
       WHERE t.device_name = $1
         AND t.position_time_nz IS NOT NULL
         AND t.position_time_nz > $2::timestamp`,
      [device, rawAfter]
    );
    const total = parseInt(cntRow[0]?.c ?? '0', 10) || 0;
    truncatedSample = total > INSPECTION_UNBOUNDED_NZ_SAMPLE_CAP;

    const nzRaw = await query<Record<string, unknown>>(
      `${trackingSelect}
       WHERE t.device_name = $1
         AND t.position_time_nz IS NOT NULL
         AND t.position_time_nz > $2::timestamp
       ORDER BY t.position_time_nz ASC, t.id ASC
       LIMIT ${INSPECTION_UNBOUNDED_NZ_SAMPLE_CAP}`,
      [device, rawAfter]
    );
    nzPingRows = nzRaw.map(mapQueryRowToGapTrackingRow);
  }

  const gaps = buildGapSegmentsList(rawAfter, positionBefore, nzPingRows, gapThresholdMinutes);
  const gpsToDisplay = positionBefore
    ? normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19)
    : null;

  return {
    ok: true,
    job_id: jobId,
    gps_from: rawAfter,
    gps_to: gpsToDisplay,
    gap_threshold_minutes: gapThresholdMinutes,
    truncated_sample: truncatedSample,
    gaps,
  };
}

/**
 * One job row from tbl_vworkjobs. Window matches Inspect: optional display expand, no 24h synthetic end.
 * Includes issue-gap NZ spans for daily hull (same pass as segment count).
 */
export async function buildJobsGpsReportRowBuild(
  rawJob: Record<string, unknown>,
  windowOpts: InspectGpsWindowOptions,
  gapThresholdMinutes: number = JOBS_GPS_GAP_THRESHOLD_MINUTES
): Promise<JobsGpsReportBuild> {
  const jobId = rawJob.job_id != null ? String(rawJob.job_id).trim() : '';
  const customer = rawJob.customer != null ? String(rawJob.customer).trim() : null;
  const delivery_winery =
    rawJob.delivery_winery != null ? String(rawJob.delivery_winery).trim() : null;
  const vineyard_name = rawJob.vineyard_name != null ? String(rawJob.vineyard_name).trim() : null;
  const worker = rawJob.worker != null && String(rawJob.worker).trim() !== '' ? String(rawJob.worker).trim() : null;

  const actualRaw =
    rawJob.actual_start_time != null ? String(rawJob.actual_start_time).trim() : '';
  const actual_start_time =
    actualRaw.length >= 19 ? actualRaw.slice(0, 19).replace('T', ' ') : actualRaw || null;

  const win = buildInspectGpsWindowForJob(rawJob, windowOpts);
  const device_name = win.device.trim() !== '' ? win.device.trim() : null;

  const base: JobsGpsReportRow = {
    job_id: jobId,
    customer: customer || null,
    delivery_winery,
    vineyard_name,
    actual_start_time,
    device_name,
    gps_from: null,
    gps_to: null,
    minutes_after_gps_from_to_first_fix: null,
    minutes_from_last_fix_to_gps_to: null,
    min_gps_nz: null,
    max_gps_nz: null,
    max_gap_minutes: null,
    gap_missing: false,
    critical_null_nz_count: 0,
    critical_null_nz: false,
    incomplete: false,
    incomplete_reason: null,
    step1_via: viaLabel(rawJob.step_1_via),
    step2_via: viaLabel(rawJob.step_2_via),
    step3_via: viaLabel(rawJob.step_3_via),
    step4_via: viaLabel(rawJob.step_4_via),
    step5_via: viaLabel(rawJob.step_5_via),
    worker,
    gap_segment_count: 0,
    points_in_window: 0,
  };

  const jobCalendarDay =
    actual_start_time != null && actual_start_time.length >= 10 ? actual_start_time.slice(0, 10) : '—';

  if (win.error) {
    return {
      row: {
        ...base,
        incomplete: true,
        incomplete_reason: win.error,
        gps_from: win.positionAfter || null,
        gps_to: win.positionBefore,
        gap_segment_count: 0,
      },
      gapIssueNZSpans: [],
      jobCalendarDay,
    };
  }

  const device = win.device;
  const positionAfter = win.positionAfter;
  const positionBefore = win.positionBefore;

  const rawAfter = normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19);

  let nzTimes: string[];
  let min_gps_nz: string | null = null;
  let max_gps_nz: string | null = null;
  let points_in_window: number;

  if (positionBefore) {
    const rawBefore = normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19);
    const nzRows = await query<{ nz: string }>(
      `SELECT to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS nz
       FROM tbl_tracking t
       WHERE t.device_name = $1
         AND t.position_time_nz IS NOT NULL
         AND t.position_time_nz > $2::timestamp
         AND t.position_time_nz < $3::timestamp
       ORDER BY t.position_time_nz ASC, t.id ASC`,
      [device, rawAfter, rawBefore]
    );
    nzTimes = nzRows.map((r) => r.nz).filter(Boolean);
    points_in_window = nzTimes.length;
    if (nzTimes.length > 0) {
      min_gps_nz = nzTimes[0]!;
      max_gps_nz = nzTimes[nzTimes.length - 1]!;
    }
  } else {
    const stats = await query<{ min_nz: string | null; max_nz: string | null; c: string }>(
      `SELECT
         (SELECT to_char(MIN(t.position_time_nz), 'YYYY-MM-DD HH24:MI:SS')
          FROM tbl_tracking t
          WHERE t.device_name = $1
            AND t.position_time_nz IS NOT NULL
            AND t.position_time_nz > $2::timestamp) AS min_nz,
         (SELECT to_char(MAX(t.position_time_nz), 'YYYY-MM-DD HH24:MI:SS')
          FROM tbl_tracking t
          WHERE t.device_name = $1
            AND t.position_time_nz IS NOT NULL
            AND t.position_time_nz > $2::timestamp) AS max_nz,
         (SELECT COUNT(*)::text
          FROM tbl_tracking t
          WHERE t.device_name = $1
            AND t.position_time_nz IS NOT NULL
            AND t.position_time_nz > $2::timestamp) AS c`,
      [device, rawAfter]
    );
    min_gps_nz = stats[0]?.min_nz ?? null;
    max_gps_nz = stats[0]?.max_nz ?? null;
    points_in_window = parseInt(stats[0]?.c ?? '0', 10) || 0;

    const nzRows = await query<{ nz: string }>(
      `SELECT to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS nz
       FROM tbl_tracking t
       WHERE t.device_name = $1
         AND t.position_time_nz IS NOT NULL
         AND t.position_time_nz > $2::timestamp
       ORDER BY t.position_time_nz ASC, t.id ASC
       LIMIT ${INSPECTION_UNBOUNDED_NZ_SAMPLE_CAP}`,
      [device, rawAfter]
    );
    nzTimes = nzRows.map((r) => r.nz).filter(Boolean);
  }

  let nullRows: { c: string }[];
  if (positionBefore) {
    const rawBefore = normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19);
    nullRows = await query<{ c: string }>(
      `SELECT COUNT(*)::text AS c
       FROM tbl_tracking t
       WHERE t.device_name = $1
         AND t.position_time IS NOT NULL
         AND t.position_time_nz IS NULL
         AND t.position_time::timestamp > $2::timestamp
         AND t.position_time::timestamp < $3::timestamp`,
      [device, rawAfter, rawBefore]
    );
  } else {
    nullRows = await query<{ c: string }>(
      `SELECT COUNT(*)::text AS c
       FROM tbl_tracking t
       WHERE t.device_name = $1
         AND t.position_time IS NOT NULL
         AND t.position_time_nz IS NULL
         AND t.position_time::timestamp > $2::timestamp`,
      [device, rawAfter]
    );
  }
  const critical_null_nz_count = parseInt(nullRows[0]?.c ?? '0', 10) || 0;
  const critical_null_nz = critical_null_nz_count > 0;

  const { maxGap, gapMissing } = computeMaxGapMinutes(positionAfter, positionBefore, nzTimes, gapThresholdMinutes);

  const rawBeforeDisplay = positionBefore
    ? normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19)
    : null;

  let minutes_after_gps_from_to_first_fix: number | null = null;
  if (min_gps_nz) {
    const m = minutesBetweenNz(rawAfter, min_gps_nz);
    minutes_after_gps_from_to_first_fix = m != null ? Math.round(m * 100) / 100 : null;
  }

  let minutes_from_last_fix_to_gps_to: number | null = null;
  if (rawBeforeDisplay && max_gps_nz) {
    const m = minutesBetweenNz(max_gps_nz, rawBeforeDisplay);
    minutes_from_last_fix_to_gps_to = m != null ? Math.round(m * 100) / 100 : null;
  }

  const nzPingStubs: JobsGpsGapTrackingRow[] = nzTimes.map((nz) => ({
    id: null,
    position_time_nz: nz,
    position_time: null,
    lat: null,
    lon: null,
    fence_name: null,
    geofence_type: null,
  }));
  const fullSegments = buildGapSegmentsList(
    rawAfter,
    positionBefore,
    nzPingStubs,
    gapThresholdMinutes
  );
  const gap_segment_count = fullSegments.length;
  const gapIssueNZSpans = extractIssueGapNZSpansFromSegments(fullSegments, rawAfter, positionBefore);

  return {
    row: {
      ...base,
      gps_from: rawAfter,
      gps_to: rawBeforeDisplay,
      minutes_after_gps_from_to_first_fix,
      minutes_from_last_fix_to_gps_to,
      min_gps_nz,
      max_gps_nz,
      max_gap_minutes: maxGap != null ? Math.round(maxGap * 100) / 100 : null,
      gap_missing: gapMissing,
      critical_null_nz_count,
      critical_null_nz,
      incomplete: false,
      incomplete_reason: null,
      gap_segment_count,
      points_in_window,
    },
    gapIssueNZSpans,
    jobCalendarDay,
  };
}

export async function buildJobsGpsReportRow(
  rawJob: Record<string, unknown>,
  windowOpts: InspectGpsWindowOptions,
  gapThresholdMinutes: number = JOBS_GPS_GAP_THRESHOLD_MINUTES
): Promise<JobsGpsReportRow> {
  return (await buildJobsGpsReportRowBuild(rawJob, windowOpts, gapThresholdMinutes)).row;
}

export async function buildJobsGpsReportRowsParallel(
  jobs: Record<string, unknown>[],
  windowOpts: InspectGpsWindowOptions,
  gapThresholdMinutes: number = JOBS_GPS_GAP_THRESHOLD_MINUTES,
  concurrency: number = 12
): Promise<JobsGpsReportBuild[]> {
  const out: JobsGpsReportBuild[] = [];
  for (let i = 0; i < jobs.length; i += concurrency) {
    const slice = jobs.slice(i, i + concurrency);
    const part = await Promise.all(
      slice.map((j) => buildJobsGpsReportRowBuild(j, windowOpts, gapThresholdMinutes))
    );
    out.push(...part);
  }
  return out;
}

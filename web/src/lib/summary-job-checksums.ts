/**
 * Admin custom report: whole-customer checksum of consecutive same-truck jobs.
 * Pairing uses our actual step 1 / step 5 (not VWork taps).
 *
 * Forward: minutes from this actual end to the next included job’s actual start.
 *   Negative → overlap (next job starts before this job ends).
 * Short gap: 0 < minutes < 30. Gaps of 30 minutes or more are omitted.
 */
import { normalizeTimestampString } from './normalize-timestamp-string';

/** Short unbilled gaps are strictly less than this many minutes. */
export const JOB_CHECKSUM_SHORT_GAP_MAX_MINUTES = 30;

export type SummaryChecksumJob = Record<string, unknown>;

export type JobChecksumLink = {
  excluded: boolean;
  startFinal: string | null;
  endFinal: string | null;
  nextJobId: string | null;
  nextStartFinal: string | null;
  /** next_start − this_end. Negative = overlap. */
  minsEndToNextStart: number | null;
  overlapFlag: boolean;
  overlapMins: number | null;
  prevJobId: string | null;
  prevEndFinal: string | null;
  /** this_start − prev_end. Negative = this job started before previous ended. */
  minsPrevEndToThisStart: number | null;
  overlapFromPrevFlag: boolean;
  overlapFromPrevMins: number | null;
  shortGapFlag: boolean;
  shortGapMins: number | null;
};

export type JobChecksumTotals = {
  includedJobs: number;
  forwardPairs: number;
  overlapPairs: number;
  overlapMinutes: number;
  overlapFromPrevPairs: number;
  overlapFromPrevMinutes: number;
  shortGapPairs: number;
  shortGapMinutes: number;
};

export type JobChecksumExportRow = {
  row: SummaryChecksumJob;
  checksum: JobChecksumLink;
};

/** VWork step 1 tap (not our final clock). */
export function jobVworkStep1Clock(row: SummaryChecksumJob): unknown {
  return row.step_1_completed_at ?? row.actual_start_time ?? null;
}

/** VWork step 5 tap (not our final clock). */
export function jobVworkStep5Clock(row: SummaryChecksumJob): unknown {
  return row.step_5_completed_at ?? row.actual_end_time ?? null;
}

export const CHECKSUM_CUSTOM_REPORT_HEADERS = [
  'customer',
  'template',
  'job_id',
  'worker',
  'excluded',
  'vwork_step_1',
  'vwork_step_5',
  'actual_step_1',
  'actual_step_5',
  'next_job_id',
  'mins_end_to_next_start',
  'overlap_flag',
  'overlap_mins',
  'gap_flag',
  'gap_mins',
] as const;

const CHECKSUM_CUSTOM_LEAD_COUNT = 9;

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}

function clockNorm(v: unknown): string | null {
  if (v == null || v === '') return null;
  if (typeof v === 'string' || v instanceof Date) return normalizeTimestampString(v);
  return normalizeTimestampString(String(v));
}

/** Parse a stored clock as a naive YYYY-MM-DD HH:mm:ss and take wall-clock ms (no TZ shift). */
export function clockStringToUtcMs(v: unknown): number | null {
  const norm = clockNorm(v);
  if (!norm) return null;
  const ms = Date.parse(norm.replace(' ', 'T') + 'Z');
  return Number.isFinite(ms) ? ms : null;
}

export function minutesBetweenClocks(later: unknown, earlier: unknown): number | null {
  const a = clockStringToUtcMs(earlier);
  const b = clockStringToUtcMs(later);
  if (a == null || b == null) return null;
  return round2((b - a) / 60000);
}

export function jobIsIncludedInChecksum(row: SummaryChecksumJob): boolean {
  const ex = row.excluded;
  if (ex == null || ex === '') return true;
  return Number(ex) !== 1;
}

export function jobWorkerKey(row: SummaryChecksumJob): string {
  return String(row.worker ?? '').trim();
}

export function jobIdKey(row: SummaryChecksumJob): string {
  return String(row.job_id ?? '').trim();
}

/** Final start: step 1 actual, else GPS, else VWork completed, else VWork tap. */
export function jobFinalStartClock(row: SummaryChecksumJob): unknown {
  return (
    row.step_1_actual_time ??
    row.step_1_gps_completed_at ??
    row.step_1_completed_at ??
    row.actual_start_time ??
    null
  );
}

/** Final end: step 5 actual, else GPS, else VWork completed, else VWork tap. */
export function jobFinalEndClock(row: SummaryChecksumJob): unknown {
  return (
    row.step_5_actual_time ??
    row.step_5_gps_completed_at ??
    row.step_5_completed_at ??
    row.actual_end_time ??
    null
  );
}

export function formatChecksumClock(v: unknown): string {
  const norm = clockNorm(v);
  if (!norm) return '';
  const m = norm.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})/);
  if (!m) return norm;
  return `${m[3]}/${m[2]}/${m[1]} ${m[4]}:${m[5]}:${m[6]}`;
}

export function compareChecksumJobOrder(a: SummaryChecksumJob, b: SummaryChecksumJob): number {
  const wa = jobWorkerKey(a);
  const wb = jobWorkerKey(b);
  if (wa !== wb) return wa.localeCompare(wb);
  const sa = clockStringToUtcMs(jobFinalStartClock(a));
  const sb = clockStringToUtcMs(jobFinalStartClock(b));
  const na = sa ?? Number.POSITIVE_INFINITY;
  const nb = sb ?? Number.POSITIVE_INFINITY;
  if (na !== nb) return na - nb;
  return jobIdKey(a).localeCompare(jobIdKey(b));
}

function emptyChecksum(row: SummaryChecksumJob): JobChecksumLink {
  return {
    excluded: !jobIsIncludedInChecksum(row),
    startFinal: clockNorm(jobFinalStartClock(row)),
    endFinal: clockNorm(jobFinalEndClock(row)),
    nextJobId: null,
    nextStartFinal: null,
    minsEndToNextStart: null,
    overlapFlag: false,
    overlapMins: null,
    prevJobId: null,
    prevEndFinal: null,
    minsPrevEndToThisStart: null,
    overlapFromPrevFlag: false,
    overlapFromPrevMins: null,
    shortGapFlag: false,
    shortGapMins: null,
  };
}

function applyForwardPair(
  prev: JobChecksumLink,
  next: JobChecksumLink,
  prevRow: SummaryChecksumJob,
  nextRow: SummaryChecksumJob,
): void {
  const nextId = jobIdKey(nextRow);
  const prevId = jobIdKey(prevRow);
  const nextStart = jobFinalStartClock(nextRow);
  const prevEnd = jobFinalEndClock(prevRow);
  const delta = minutesBetweenClocks(nextStart, prevEnd);

  prev.nextJobId = nextId || null;
  prev.nextStartFinal = clockNorm(nextStart);
  prev.minsEndToNextStart = delta;
  if (delta != null && delta < 0) {
    prev.overlapFlag = true;
    prev.overlapMins = round2(-delta);
  } else if (delta != null && delta > 0 && delta < JOB_CHECKSUM_SHORT_GAP_MAX_MINUTES) {
    prev.shortGapFlag = true;
    prev.shortGapMins = delta;
  }

  next.prevJobId = prevId || null;
  next.prevEndFinal = clockNorm(prevEnd);
  next.minsPrevEndToThisStart = delta;
  if (delta != null && delta < 0) {
    next.overlapFromPrevFlag = true;
    next.overlapFromPrevMins = round2(-delta);
  }
}

/**
 * Sort jobs by truck then final start. Pair consecutive **included** jobs on the same worker.
 * Excluded jobs stay in the file (so middle exclusions are visible) but are skipped when chaining.
 */
export function buildJobChecksumExport(rows: readonly SummaryChecksumJob[]): {
  rows: JobChecksumExportRow[];
  totals: JobChecksumTotals;
} {
  const ordered = [...rows].sort(compareChecksumJobOrder);
  const checksums = ordered.map((row) => emptyChecksum(row));

  const includedIdx: number[] = [];
  for (let i = 0; i < ordered.length; i++) {
    if (!jobIsIncludedInChecksum(ordered[i])) continue;
    if (!jobWorkerKey(ordered[i])) continue;
    includedIdx.push(i);
  }

  for (let k = 0; k < includedIdx.length - 1; k++) {
    const i = includedIdx[k];
    const j = includedIdx[k + 1];
    if (jobWorkerKey(ordered[i]) !== jobWorkerKey(ordered[j])) continue;
    applyForwardPair(checksums[i], checksums[j], ordered[i], ordered[j]);
  }

  const totals: JobChecksumTotals = {
    includedJobs: ordered.filter(jobIsIncludedInChecksum).length,
    forwardPairs: 0,
    overlapPairs: 0,
    overlapMinutes: 0,
    overlapFromPrevPairs: 0,
    overlapFromPrevMinutes: 0,
    shortGapPairs: 0,
    shortGapMinutes: 0,
  };

  for (const c of checksums) {
    if (c.nextJobId) totals.forwardPairs += 1;
    if (c.overlapFlag && c.overlapMins != null) {
      totals.overlapPairs += 1;
      totals.overlapMinutes = round2(totals.overlapMinutes + c.overlapMins);
    }
    if (c.overlapFromPrevFlag && c.overlapFromPrevMins != null) {
      totals.overlapFromPrevPairs += 1;
      totals.overlapFromPrevMinutes = round2(totals.overlapFromPrevMinutes + c.overlapFromPrevMins);
    }
    if (c.shortGapFlag && c.shortGapMins != null) {
      totals.shortGapPairs += 1;
      totals.shortGapMinutes = round2(totals.shortGapMinutes + c.shortGapMins);
    }
  }

  return {
    rows: ordered.map((row, i) => ({ row, checksum: checksums[i] })),
    totals,
  };
}

function checksumCustomLeadCells(row: SummaryChecksumJob): (string | number)[] {
  const customer = String(row.Customer ?? row.customer ?? '').trim();
  const template = String(row.template ?? '').trim();
  return [
    customer,
    template,
    jobIdKey(row),
    jobWorkerKey(row),
    jobIsIncludedInChecksum(row) ? 'N' : 'Y',
    formatChecksumClock(jobVworkStep1Clock(row)),
    formatChecksumClock(jobVworkStep5Clock(row)),
    formatChecksumClock(jobFinalStartClock(row)),
    formatChecksumClock(jobFinalEndClock(row)),
  ];
}

/** Overlap + short-gap cells. Gaps of 30 minutes or more are left blank. */
function checksumCustomMetricCells(c: JobChecksumLink): (string | number)[] {
  const yn = (v: boolean) => (v ? 'Y' : '');
  const delta = c.minsEndToNextStart;
  const showDelta = delta != null && delta < JOB_CHECKSUM_SHORT_GAP_MAX_MINUTES;
  return [
    c.nextJobId ?? '',
    showDelta ? delta : '',
    yn(c.overlapFlag),
    c.overlapMins ?? '',
    yn(c.shortGapFlag),
    c.shortGapMins ?? '',
  ];
}

export function checksumCustomTotalsRow(totals: JobChecksumTotals): (string | number)[] {
  const lead = Array.from({ length: CHECKSUM_CUSTOM_LEAD_COUNT }, () => '');
  lead[0] = 'TOTALS';
  return [...lead, '', '', totals.overlapPairs, totals.overlapMinutes, totals.shortGapPairs, totals.shortGapMinutes];
}

export function checksumSummaryAoa(totals: JobChecksumTotals): (string | number)[][] {
  return [
    [],
    ['Checksum summary'],
    ['Scope', 'Whole customer (all templates), included jobs, same truck, consecutive by actual start'],
    ['Overlap clocks', 'Our actual step 1 and actual step 5 — not VWork taps'],
    ['Included jobs', totals.includedJobs],
    ['Forward pairs (job has a next included job on same truck)', totals.forwardPairs],
    ['Overlap pairs (next actual start before this actual end)', totals.overlapPairs],
    ['Overlap minutes (fact-check vs 129)', totals.overlapMinutes],
    ['Gap pairs (0 < actual end to next actual start < 30 min)', totals.shortGapPairs],
    ['Gap minutes', totals.shortGapMinutes],
    [
      'Notes',
      'Rows are ordered by truck then actual start. Negative mins_end_to_next_start is an overlap. Gaps of 30 minutes or more are omitted.',
    ],
  ];
}

/** Custom admin report: customer/template/job, VWork vs actual 1/5, then overlaps, then short gaps. */
export function buildChecksumCustomReportAoa(rows: readonly SummaryChecksumJob[]): (string | number | null | undefined)[][] {
  const built = buildJobChecksumExport(rows);
  const aoa: (string | number | null | undefined)[][] = [[...CHECKSUM_CUSTOM_REPORT_HEADERS]];
  for (const { row, checksum } of built.rows) {
    aoa.push([...checksumCustomLeadCells(row), ...checksumCustomMetricCells(checksum)]);
  }
  aoa.push(checksumCustomTotalsRow(built.totals));
  aoa.push(...checksumSummaryAoa(built.totals));
  return aoa;
}

'use client';

import Link from 'next/link';
import { Suspense, useState, useEffect, useCallback, useRef } from 'react';
import { useSearchParams } from 'next/navigation';
import {
  JOBS_GPS_TAB_SETTINGS_NAME,
  JOBS_GPS_TAB_SETTINGS_TYPE,
} from '@/lib/jobs-gps-tab-settings';
import { jobsGpsRowBreachesLimits, type JobsGpsReportRowBreachInput } from '@/lib/jobs-gps-breach';
import type { JobsGpsGapsDetailResult, JobsGpsGapTrackingRow } from '@/lib/jobs-gps-window-report';

type SummaryRow = {
  report_date: string;
  device_name: string;
  max_gap_minutes: string;
  gap_count: number;
  day_count: number;
  gap_from: string | null;
  gap_to: string | null;
  gap_from_nz: string;
  gap_to_nz: string;
  gap_start_lat?: number | null;
  gap_start_lon?: number | null;
  gap_end_lat?: number | null;
  gap_end_lon?: number | null;
};

/**
 * Rough ETA for Step4→5 bulk UPDATE (many columns per row).
 * Tuned for batches around 500–2000 rows; actual time varies with DB load, disk, and network (if remote).
 */
function estimateStep4to5FixRange(todoCount: number, lightStep4TimeOnly?: boolean): { lowSec: number; highSec: number } {
  if (todoCount <= 0) return { lowSec: 1, highSec: 3 };
  const scale = lightStep4TimeOnly ? 0.12 : 1;
  const low = Math.max(1, Math.round((0.3 + todoCount * 0.003) * scale));
  const high = Math.max(2, Math.ceil((0.8 + todoCount * 0.012) * scale));
  return { lowSec: low, highSec: high };
}

/** True if device moved between outage start and end (lat/lon at From !== at To). */
function deviceMovedDuringOutage(row: SummaryRow): boolean {
  const slat = row.gap_start_lat;
  const slon = row.gap_start_lon;
  const elat = row.gap_end_lat;
  const elon = row.gap_end_lon;
  if (slat == null || slon == null || elat == null || elon == null) return false;
  return slat !== elat || slon !== elon;
}

type TrackingWindowRow = {
  device_name: string;
  position_time: string | null;
  position_time_nz: string | null;
  fence_name: string | null;
  geofence_type: string | null;
  lat: number | null;
  lon: number | null;
};

type DetailRow = {
  device_name: string;
  gap_start: string;
  gap_end: string;
  gap_minutes: string;
};

type GapsResult = {
  summary: SummaryRow[];
  detail: DetailRow[];
  minGapMinutes: number;
  dateFrom: string | null;
  dateTo: string | null;
};

type JobsGpsReportApiRow = {
  job_id: string;
  customer: string | null;
  delivery_winery: string | null;
  vineyard_name: string | null;
  actual_start_time: string | null;
  gps_from: string | null;
  gps_to: string | null;
  /** Minutes after window start (exclusive) until first NZ fix; omitted in JSON if null. */
  minutes_after_gps_from_to_first_fix: number | null;
  /** Minutes from last NZ fix to window end (exclusive); null if open-ended or no last fix. */
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
  /** Inspect / tbl_tracking device (worker or truck). */
  device_name: string | null;
  gap_segment_count: number;
  points_in_window: number;
};

type JobsGpsReportResponse = {
  ok: true;
  /** Jobs included in this response (`rows.length`); paging is over this set. */
  total: number;
  /** SQL count of jobs matching filters (may exceed `total` when scan hits cap). */
  totalMatchingJobs: number;
  limit: number;
  offset: number;
  totalJobsScanned: number;
  breachJobCount: number;
  jobsWithoutIssues: number;
  totalGapSegmentRows: number;
  uniqueDayWorkerWithIssues: number;
  truncated: boolean;
  maxJobsScan: number;
  /** Calendar day + tbl_vworkjobs.worker: counts for the Days / Worker tab (filled in the same scan as rows). */
  dayWorkerSummary: { day: string; worker: string; total_jobs: number; bad_jobs: number }[];
  /** Per calendar day (actual_start): hull of issue-gap NZ windows + counts (same scan). */
  gapProbabilityByDay: {
    day: string;
    gap_from_nz: string | null;
    gap_to_nz: string | null;
    issue_gap_segments: number;
    jobs_with_issue_gaps: number;
  }[];
  window: {
    startLessMinutes: number;
    endPlusMinutes: number;
    displayExpandBefore: number;
    displayExpandAfter: number;
    gapThresholdMinutes: number;
    dateFrom: string;
    dateTo: string;
  };
  rows: JobsGpsReportApiRow[];
};

/** Jobs & GPS table: filter by (x) / window start edge. */
type JobsGpsEdgeColumnFilter = 'all' | 'breach' | 'ok' | 'na';

/** Jobs & GPS table: filter Flags column triggers. */
type JobsGpsFlagsColumnFilter =
  | 'all'
  | 'ok'
  | 'any_issue'
  | 'incomplete'
  | 'critical_nz'
  | 'gap';

function jobsGpsFromBreach(r: JobsGpsReportApiRow, thr: number): boolean {
  const x = r.minutes_after_gps_from_to_first_fix;
  return r.gps_from != null && (x != null ? x > thr : r.min_gps_nz == null);
}

function jobsGpsToBreach(r: JobsGpsReportApiRow, thr: number): boolean {
  const y = r.minutes_from_last_fix_to_gps_to;
  return r.gps_to != null && (y != null ? y > thr : r.max_gps_nz == null);
}

function jobsGpsMatchesEdgeFromFilter(
  r: JobsGpsReportApiRow,
  thr: number,
  f: JobsGpsEdgeColumnFilter
): boolean {
  if (f === 'all') return true;
  const breach = jobsGpsFromBreach(r, thr);
  if (f === 'breach') return breach;
  if (f === 'ok') return r.gps_from != null && !breach;
  if (f === 'na') return r.gps_from == null;
  return true;
}

function jobsGpsMatchesEdgeToFilter(
  r: JobsGpsReportApiRow,
  thr: number,
  f: JobsGpsEdgeColumnFilter
): boolean {
  if (f === 'all') return true;
  const breach = jobsGpsToBreach(r, thr);
  if (f === 'breach') return breach;
  if (f === 'ok') return r.gps_to != null && !breach;
  if (f === 'na') return r.gps_to == null;
  return true;
}

function jobsGpsMatchesFlagsFilter(
  r: JobsGpsReportApiRow,
  f: JobsGpsFlagsColumnFilter,
  thr: number
): boolean {
  if (f === 'all') return true;
  const hasIssue = jobsGpsRowBreachesLimits(r as JobsGpsReportRowBreachInput, thr);
  if (f === 'ok') return !hasIssue;
  if (f === 'any_issue') return hasIssue;
  if (f === 'incomplete') return r.incomplete;
  if (f === 'critical_nz') return r.critical_null_nz;
  if (f === 'gap') return r.gap_missing;
  return true;
}

function coerceJobsGpsEdgeFilter(v: unknown): JobsGpsEdgeColumnFilter {
  return v === 'breach' || v === 'ok' || v === 'na' || v === 'all' ? v : 'all';
}

function coerceJobsGpsFlagsFilter(v: unknown): JobsGpsFlagsColumnFilter {
  return v === 'ok' ||
    v === 'any_issue' ||
    v === 'incomplete' ||
    v === 'critical_nz' ||
    v === 'gap' ||
    v === 'all'
    ? v
    : 'all';
}

/** Response from GET /api/admin/data-checks/jobs-gps-gaps-detail */
type JobsGpsGapsDetailOk = JobsGpsGapsDetailResult;

type WineMappRow = {
  id: number;
  oldvworkname: string;
  newvworkname: string;
  created_at: string;
};

type VineyardGroupRow = {
  id: number;
  winery_name: string | null;
  vineyard_name: string;
  vineyard_group: string;
  created_at: string;
};

function formatDateInput(value: string): string {
  if (!value) return '';
  const d = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
  return value;
}

/** Format YYYY-MM-DD as dd/mm/yy */
function formatDateDDMMYY(s: string | null | undefined): string {
  if (!s || typeof s !== 'string') return '—';
  const m = s.trim().match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return s;
  const yy = m[1].slice(-2);
  return `${m[3]}/${m[2]}/${yy}`;
}

/** Format YYYY-MM-DD HH:MI:SS as dd/mm hh:mm */
function formatGapDateTime(s: string | null | undefined): string {
  if (!s || typeof s !== 'string') return '—';
  const m = s.trim().match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})/);
  if (!m) return s;
  const yy = m[1].slice(-2);
  const h = m[4].padStart(2, '0');
  return `${m[3]}/${m[2]}/${yy} ${h}:${m[5]}`;
}

function jobsGpsGapKindLabel(kind: string): string {
  switch (kind) {
    case 'start':
      return 'Window start → first fix';
    case 'between':
      return 'Between consecutive fixes';
    case 'end':
      return 'Last fix → window end';
    case 'empty_window':
      return 'No GPS in window';
    default:
      return kind;
  }
}

function JobsGpsGapTrackingCell({ row }: { row: JobsGpsGapTrackingRow | null | undefined }) {
  if (!row) return <span className="text-zinc-400">—</span>;
  const fence = [row.fence_name, row.geofence_type].filter(Boolean).join(' · ') || '—';
  const ll = row.lat != null && row.lon != null ? `${row.lat}, ${row.lon}` : '—';
  return (
    <div className="max-w-[12rem] space-y-0.5 text-[11px] leading-snug text-zinc-800 dark:text-zinc-200">
      <div className="font-mono text-zinc-900 dark:text-zinc-100">
        {row.position_time_nz ? formatGapDateTime(row.position_time_nz) : '—'}{' '}
        <span className="text-zinc-500 dark:text-zinc-400">NZ</span>
      </div>
      <div className="font-mono text-zinc-500 dark:text-zinc-400">raw {row.position_time ?? '—'}</div>
      <div>
        id {row.id ?? '—'} · {ll}
      </div>
      <div className="text-zinc-600 dark:text-zinc-400">{fence}</div>
    </div>
  );
}

function JobsGpsGapWindowBoundCell({ nzTime }: { nzTime: string | null }) {
  if (!nzTime) return <span className="text-zinc-400">—</span>;
  return (
    <div className="max-w-[12rem] text-[11px] leading-snug text-zinc-600 dark:text-zinc-400">
      <div className="mb-0.5 font-medium text-zinc-700 dark:text-zinc-300">Window bound (not a ping)</div>
      <div className="font-mono text-zinc-800 dark:text-zinc-200">{formatGapDateTime(nzTime)} NZ</div>
    </div>
  );
}

/** Haversine distance in meters between two lat/lon points. */
function distanceMeters(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const R = 6371000; // Earth radius in meters
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

/** Format meters as "X m" or "X.X km". */
function formatDistance(m: number): string {
  if (m >= 1000) return `${(m / 1000).toFixed(1)} km`;
  return `${Math.round(m)} m`;
}

/** Fixed page size for tab 13 — Jobs & GPS window (matches API default). */
const JOBS_GPS_PAGE_SIZE = 200;

const SORT_KEYS = [
  { value: 'report_date', label: 'Date' },
  { value: 'device_name', label: 'Device' },
  { value: 'max_gap_minutes', label: 'Max gap (min)' },
  { value: 'gap_count', label: '# gaps' },
  { value: 'day_count', label: 'DayCount' },
  { value: 'gap_from', label: 'From' },
  { value: 'gap_to', label: 'To' },
  { value: 'gap_from_nz', label: 'From_NZ' },
  { value: 'gap_to_nz', label: 'To_NZ' },
] as const;

type SortKey = (typeof SORT_KEYS)[number]['value'];

function getSortValue(row: SummaryRow, key: SortKey): string | number {
  const v = row[key];
  if (key === 'max_gap_minutes') return parseFloat(String(v)) ?? 0;
  if (key === 'gap_count' || key === 'day_count') return Number(v) ?? 0;
  return (v ?? '') as string;
}

function sortSummary(rows: SummaryRow[], sortBy1: SortKey, sortBy2: SortKey, dir: 'asc' | 'desc'): SummaryRow[] {
  const mult = dir === 'asc' ? 1 : -1;
  return [...rows].sort((a, b) => {
    const v1a = getSortValue(a, sortBy1);
    const v1b = getSortValue(b, sortBy1);
    let cmp = v1a < v1b ? -1 : v1a > v1b ? 1 : 0;
    if (cmp !== 0) return mult * cmp;
    const v2a = getSortValue(a, sortBy2);
    const v2b = getSortValue(b, sortBy2);
    cmp = v2a < v2b ? -1 : v2a > v2b ? 1 : 0;
    return mult * cmp;
  });
}

type GpsIntegrityCell =
  | { status: 'no_data' }
  | {
      status: 'data';
      firstOk: boolean;
      lastOk: boolean;
      countOk: boolean;
      firstTime: string;
      lastTime: string;
      apiCount: number;
      dbCount: number;
      dbFirstTime: string | null;
      dbLastTime: string | null;
    };

type GpsIntegrityGridResponse = {
  dates: string[];
  devices: string[];
  rows: Array<{ device: string; cells: GpsIntegrityCell[] }>;
};

type DbCheckCell =
  | { status: 'empty' }
  | { status: 'data'; count: number; minTime: string; maxTime: string };

type DbCheckGridResponse = {
  dates: string[];
  devices: string[];
  rows: Array<{ device: string; cells: DbCheckCell[] }>;
};

type DbCheckSimpleRow =
  | { date: string; status: 'empty' }
  | { date: string; status: 'data'; count: number; minTime: string; maxTime: string };

type DbCheckSimpleResponse = {
  dates: string[];
  rows: DbCheckSimpleRow[];
};

type VworkStaleRow = {
  job_id: string;
  worker: string | null;
  vineyard_name: string | null;
  delivery_winery: string | null;
  actual_start_time: string | null;
  steps_fetched: boolean | null;
  steps_fetched_when: string | null;
  issue: 'never_fetched' | 'no_timestamp' | 'stale' | 'fresh';
  hours_since_steps: number | null;
};

type VworkStaleResponse = {
  staleHours: number;
  /** When true, list is jobs with all step_1–5 actual times null (not stale-filtered). */
  noStepActuals?: boolean;
  dateFrom: string | null;
  dateTo: string | null;
  count: number;
  rows: VworkStaleRow[];
};

type Step4to5PreviewResponse = {
  ok: true;
  /** Rerun = only recalc step_4_completed_at on done rows (Arrive Winery + Job Completed on 5 + step4to5=1). */
  rerun: boolean;
  /** Subset rerun: only rows where step_4 is not strictly before step_5 (same capped recalc). */
  orderingFix?: boolean;
  customer: string;
  template: string;
  /** Normal: step4to5=0 pool. Rerun: done rows eligible for time-only fix. */
  potentialCount: number;
  /** Normal: wrong step_4, or step_5 name Job Completed, or step_5_completed_at set. Rerun: step4to5=1 but not done layout. */
  blockedCount: number;
  /** Normal: potential − blocked. Rerun: same as rows updated. */
  willDoCount: number;
  /** Rows this Fix UPDATE touches. */
  todoCount: number;
  selectSql: string;
  selectSqlParams: string[];
  selectSqlLiteral: string;
  updateSql: string;
  updateSqlParams: string[];
  updateSqlLiteral: string;
  countPotentialSql: string;
  countPotentialSqlLiteral: string;
  countBlockedSql: string;
  countBlockedSqlLiteral: string;
  countEligibleSql: string;
  countEligibleSqlLiteral: string;
};

type Step4to5FixResponse = {
  ok: true;
  updated: number;
  customer: string;
  template: string;
  rerun?: boolean;
  orderingFix?: boolean;
  afterPreview: Step4to5PreviewResponse;
};

function step4to5PreviewMatchesMode(
  p: Step4to5PreviewResponse,
  mode: 'normal' | 'rerun' | 'ordering',
): boolean {
  const ordering = p.orderingFix === true;
  if (mode === 'ordering') return ordering;
  if (mode === 'rerun') return p.rerun === true && !ordering;
  return !p.rerun && !ordering;
}

type GeofenceGapsRow = {
  device_name: string;
  day: string;
  point_count: number;
  unattempted_count: number;
  attempted_count: number;
};

type GeofenceGapsDayRow = {
  day: string;
  point_count: number;
  unattempted_count: number;
  attempted_count: number;
  device_count: number;
};

type GeofenceGapsResponse =
  | {
      dateFrom: string;
      dateTo: string;
      minPoints: number;
      view: 'by-device-day';
      count: number;
      rows: GeofenceGapsRow[];
    }
  | {
      dateFrom: string;
      dateTo: string;
      minPoints: number;
      view: 'by-day';
      count: number;
      rows: GeofenceGapsDayRow[];
    };

type GeofenceEnterExitGapsRow = {
  device_name: string;
  day: string;
  point_count: number;
  enter_exit_count: number;
};

type GeofenceEnterExitGapsResponse = {
  dateFrom: string;
  dateTo: string;
  minPoints: number;
  count: number;
  rows: GeofenceEnterExitGapsRow[];
};

type DataHealthOverviewResponse = {
  ok: true;
  generatedAt: string;
  window: { dateFrom: string; dateTo: string; minPointsEnterExit: number };
  tracking: {
    totalRows: number;
    maxPositionTime: string | null;
    maxPositionTimeNz: string | null;
    rowsMissingPositionTimeNz: number;
    rowsGeofenceNotAttempted: number;
    maxPositionTimeFenceAttempted: string | null;
    maxPositionTimeNzFenceAttempted: string | null;
  };
  vworkjobs: {
    jobsWithActualStart: number;
    maxActualStartTime: string | null;
    maxActualStartWithStepData: string | null;
    jobsWithStartStepsNotFetched: number;
  };
  windowMetrics: {
    deviceDaysWithGeofenceGap: number;
    deviceDaysWithEnterExitGap: number;
  };
};

type DataChecksTab =
  | 'data-health-overview'
  | 'gps-gaps'
  | 'jobs-gps'
  | 'winery-fixes'
  | 'data-fixes'
  | 'varied'
  | 'gps-integrity'
  | 'db-check'
  | 'db-check-simple'
  | 'vineyard-mappings'
  | 'vwork-stale'
  | 'geofence-gaps'
  | 'geofence-enter-exit-gaps'
  | 'step4to5'
  | 'sql-updates';

type LoadsizeBackfillFileRow = {
  fileName: string;
  rowsInFile: number;
  rowsIndevinCustomer: number;
  rowsUpdated: number;
  rowsSkippedNoJobId: number;
  rowsSkippedNoLoadSize: number;
  rowsNotFoundInDb: number;
  errorsTruncated: boolean;
  errorSampleCount: number;
};

type SqlRunRow = {
  id: number;
  sql_name: string;
  sql_command: string;
  sql_description: string | null;
  run_order: number;
  created_at: string;
  updated_at: string;
};

type LoadsizeQuickTestResponse = {
  ok: boolean;
  importHost: string;
  health: { ok: boolean; status?: number; error?: string };
  driveList: {
    ok: boolean;
    status?: number;
    filesTotal?: number;
    sampleNames?: string[];
    driveFolderId?: string;
    error?: string;
  };
  customerScan?: {
    ok: boolean;
    skippedNoFiles?: boolean;
    notRun?: boolean;
    notRunReason?: string;
    error?: string;
    rowsScanned?: number;
    filesTouched?: number;
    rowsWithCustomerText?: number;
    indevinMatchCount?: number;
    firstMatchCustomerPreview?: string | null;
    maxRowsBudget?: number;
    failedResolver?: boolean;
    failedIndevinSubstring?: boolean;
    warning?: string;
  };
  hint?: string;
};

function DataChecksPageContent() {
  const searchParams = useSearchParams();
  const tabParam = searchParams.get('tab');
  const [activeTab, setActiveTab] = useState<DataChecksTab>(() => {
    if (tabParam === 'data-health-overview') return 'data-health-overview';
    if (tabParam === 'vineyard-mappings') return 'vineyard-mappings';
    if (tabParam === 'vwork-stale') return 'vwork-stale';
    if (tabParam === 'geofence-enter-exit-gaps') return 'geofence-enter-exit-gaps';
    if (tabParam === 'step4to5') return 'step4to5';
    if (tabParam === 'sql-updates') return 'sql-updates';
    if (tabParam === 'geofence-gaps' || tabParam === 'enter-exit-coverage') return 'geofence-gaps';
    if (tabParam === 'db-check-simple') return 'db-check-simple';
    if (tabParam === 'db-check') return 'db-check';
    if (tabParam === 'gps-integrity') return 'gps-integrity';
    if (tabParam === 'winery-fixes') return 'winery-fixes';
    if (tabParam === 'data-fixes') return 'data-fixes';
    if (tabParam === 'varied') return 'varied';
    if (tabParam === 'gps-gaps') return 'gps-gaps';
    if (tabParam === 'jobs-gps') return 'jobs-gps';
    return 'gps-gaps';
  });

  useEffect(() => {
    if (tabParam === 'data-health-overview') setActiveTab('data-health-overview');
    else if (tabParam === 'vineyard-mappings') setActiveTab('vineyard-mappings');
    else if (tabParam === 'vwork-stale') setActiveTab('vwork-stale');
    else if (tabParam === 'geofence-enter-exit-gaps') setActiveTab('geofence-enter-exit-gaps');
    else if (tabParam === 'step4to5') setActiveTab('step4to5');
    else if (tabParam === 'sql-updates') setActiveTab('sql-updates');
    else if (tabParam === 'geofence-gaps' || tabParam === 'enter-exit-coverage') setActiveTab('geofence-gaps');
    else if (tabParam === 'db-check-simple') setActiveTab('db-check-simple');
    else if (tabParam === 'db-check') setActiveTab('db-check');
    else if (tabParam === 'gps-integrity') setActiveTab('gps-integrity');
    else if (tabParam === 'winery-fixes') setActiveTab('winery-fixes');
    else if (tabParam === 'data-fixes') setActiveTab('data-fixes');
    else if (tabParam === 'varied') setActiveTab('varied');
    else if (tabParam === 'gps-gaps') setActiveTab('gps-gaps');
    else if (tabParam === 'jobs-gps') setActiveTab('jobs-gps');
  }, [tabParam]);
  const [minGapMinutes, setMinGapMinutes] = useState<string>('60');
  const [dateFrom, setDateFrom] = useState<string>('');
  const [dateTo, setDateTo] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<GapsResult | null>(null);
  const [showDetail, setShowDetail] = useState(false);
  const [popupRow, setPopupRow] = useState<SummaryRow | null>(null);
  const [popupLoading, setPopupLoading] = useState(false);
  const [popupRows, setPopupRows] = useState<TrackingWindowRow[]>([]);
  const [popupError, setPopupError] = useState<string | null>(null);
  const [deviceFilter, setDeviceFilter] = useState('');
  const [sortBy1, setSortBy1] = useState<SortKey>('device_name');
  const [sortBy2, setSortBy2] = useState<SortKey>('gap_from');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  const [jobsGpsDateFrom, setJobsGpsDateFrom] = useState('');
  const [jobsGpsDateTo, setJobsGpsDateTo] = useState('');
  const [jobsGpsCustomer, setJobsGpsCustomer] = useState('');
  const [jobsGpsTemplate, setJobsGpsTemplate] = useState('');
  const [jobsGpsWinery, setJobsGpsWinery] = useState('');
  const [jobsGpsVineyard, setJobsGpsVineyard] = useState('');
  const [jobsGpsStartLess, setJobsGpsStartLess] = useState('10');
  const [jobsGpsEndPlus, setJobsGpsEndPlus] = useState('60');
  const [jobsGpsGapThreshold, setJobsGpsGapThreshold] = useState('10');
  /** 0-based page index; 200 jobs per page. */
  const [jobsGpsPage, setJobsGpsPage] = useState(0);
  const [jobsGpsLoading, setJobsGpsLoading] = useState(false);
  const [jobsGpsError, setJobsGpsError] = useState<string | null>(null);
  const [jobsGpsResult, setJobsGpsResult] = useState<JobsGpsReportResponse | null>(null);
  const [jobsGpsIssuesOnly, setJobsGpsIssuesOnly] = useState(false);
  const [jobsGpsFilterFrom, setJobsGpsFilterFrom] = useState<JobsGpsEdgeColumnFilter>('all');
  const [jobsGpsFilterTo, setJobsGpsFilterTo] = useState<JobsGpsEdgeColumnFilter>('all');
  const [jobsGpsFilterFlags, setJobsGpsFilterFlags] = useState<JobsGpsFlagsColumnFilter>('all');
  const [jobsGpsGapsModalBreachedOnly, setJobsGpsGapsModalBreachedOnly] = useState(false);
  /** Index in current gaps view for “Next breach row” scroll + highlight (show-all mode). */
  const [jobsGpsGapScrollIdx, setJobsGpsGapScrollIdx] = useState(-1);
  const [jobsGpsPrefsHydrated, setJobsGpsPrefsHydrated] = useState(false);
  /** One load from tbl_settings per browser session (avoids Strict Mode double-fetch overwriting). */
  const jobsGpsPrefsSessionLoadedRef = useRef(false);
  const [jobsGpsGapModalOpen, setJobsGpsGapModalOpen] = useState(false);
  const [jobsGpsGapModalJobId, setJobsGpsGapModalJobId] = useState('');
  const [jobsGpsGapDetailLoading, setJobsGpsGapDetailLoading] = useState(false);
  const [jobsGpsGapDetailError, setJobsGpsGapDetailError] = useState<string | null>(null);
  const [jobsGpsGapDetailData, setJobsGpsGapDetailData] = useState<JobsGpsGapsDetailOk | null>(null);
  const [jobsGpsSubTab, setJobsGpsSubTab] = useState<'report' | 'days-devices' | 'gap-probability'>(
    'report'
  );

  const [healthDateFrom, setHealthDateFrom] = useState(() => {
    const to = new Date();
    const from = new Date(to);
    from.setUTCDate(from.getUTCDate() - 13);
    return from.toISOString().slice(0, 10);
  });
  const [healthDateTo, setHealthDateTo] = useState(() => new Date().toISOString().slice(0, 10));
  const [healthMinPointsEE, setHealthMinPointsEE] = useState(25);
  const [healthData, setHealthData] = useState<DataHealthOverviewResponse | null>(null);
  const [healthLoading, setHealthLoading] = useState(false);
  const [healthError, setHealthError] = useState<string | null>(null);
  const [healthFillNzLoading, setHealthFillNzLoading] = useState(false);
  const [healthFillNzResult, setHealthFillNzResult] = useState<{ ok: boolean; text: string } | null>(null);

  // Winery / vineyard name fixes (tab 2)
  const [wineMappRows, setWineMappRows] = useState<WineMappRow[]>([]);
  const [wineMappLoading, setWineMappLoading] = useState(false);
  const [wineMappError, setWineMappError] = useState<string | null>(null);
  const [wineMappNewOld, setWineMappNewOld] = useState('');
  const [wineMappNewNew, setWineMappNewNew] = useState('');
  const [wineMappSaving, setWineMappSaving] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editOld, setEditOld] = useState('');
  const [editNew, setEditNew] = useState('');
  const [deliveryWineries, setDeliveryWineries] = useState<string[]>([]);
  const [runFixesLoading, setRunFixesLoading] = useState(false);
  const [runFixesResult, setRunFixesResult] = useState<{ totalUpdated: number; perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[] } | null>(null);

  // Vineyard name fixes (same tab as winery)
  const [vineMappRows, setVineMappRows] = useState<WineMappRow[]>([]);
  const [vineMappLoading, setVineMappLoading] = useState(false);
  const [vineMappError, setVineMappError] = useState<string | null>(null);
  const [vineMappNewOld, setVineMappNewOld] = useState('');
  const [vineMappNewNew, setVineMappNewNew] = useState('');
  const [vineMappSaving, setVineMappSaving] = useState(false);
  const [vineEditingId, setVineEditingId] = useState<number | null>(null);
  const [vineEditOld, setVineEditOld] = useState('');
  const [vineEditNew, setVineEditNew] = useState('');
  const [vineyardNamesList, setVineyardNamesList] = useState<string[]>([]);
  const [runVineFixesLoading, setRunVineFixesLoading] = useState(false);
  const [runVineFixesResult, setRunVineFixesResult] = useState<{
    totalUpdated: number;
    perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[];
  } | null>(null);

  // Driver name fixes (same tab)
  const [driverMappRows, setDriverMappRows] = useState<WineMappRow[]>([]);
  const [driverMappLoading, setDriverMappLoading] = useState(false);
  const [driverMappError, setDriverMappError] = useState<string | null>(null);
  const [driverMappNewOld, setDriverMappNewOld] = useState('');
  const [driverMappNewNew, setDriverMappNewNew] = useState('');
  const [driverMappSaving, setDriverMappSaving] = useState(false);
  const [driverEditingId, setDriverEditingId] = useState<number | null>(null);
  const [driverEditOld, setDriverEditOld] = useState('');
  const [driverEditNew, setDriverEditNew] = useState('');
  const [workerNamesList, setWorkerNamesList] = useState<string[]>([]);
  const [runDriverFixesLoading, setRunDriverFixesLoading] = useState(false);
  const [runDriverFixesResult, setRunDriverFixesResult] = useState<{
    totalUpdated: number;
    perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[];
  } | null>(null);

  // Varied tab: Set Trailer Type
  const [setTrailerLoading, setSetTrailerLoading] = useState(false);
  const [setTrailerError, setSetTrailerError] = useState<string | null>(null);
  const [setTrailerResult, setSetTrailerResult] = useState<{ updatedTT: number; updatedT: number; totalUpdated: number } | null>(null);
  const [loadsizeTmLoading, setLoadsizeTmLoading] = useState(false);
  const [loadsizeTmError, setLoadsizeTmError] = useState<string | null>(null);
  const [loadsizeTmResult, setLoadsizeTmResult] = useState<{
    threshold: number;
    updatedTT: number;
    updatedT: number;
    totalUpdated: number;
  } | null>(null);

  // Varied tab: Update Vineyard_Group (also used in vineyard-mappings tab)
  const [vineyardGroupLoading, setVineyardGroupLoading] = useState(false);
  const [vineyardGroupError, setVineyardGroupError] = useState<string | null>(null);
  const [vineyardGroupResult, setVineyardGroupResult] = useState<{ setToNa: number; matched: number; totalRows: number } | null>(null);

  // Vineyard Mappings tab (tab 4)
  const [vgRows, setVgRows] = useState<VineyardGroupRow[]>([]);
  const [vgLoading, setVgLoading] = useState(false);
  const [vgError, setVgError] = useState<string | null>(null);
  const [vgSaving, setVgSaving] = useState(false);
  const [vgDeliveryWineries, setVgDeliveryWineries] = useState<string[]>([]);
  const [vgVineyardNames, setVgVineyardNames] = useState<string[]>([]);
  const [vgNewWinery, setVgNewWinery] = useState('');
  const [vgNewVineyard, setVgNewVineyard] = useState('');
  const [vgNewGroup, setVgNewGroup] = useState('');
  const [vgEditingId, setVgEditingId] = useState<number | null>(null);
  const [vgEditWinery, setVgEditWinery] = useState('');
  const [vgEditVineyard, setVgEditVineyard] = useState('');
  const [vgEditGroup, setVgEditGroup] = useState('');

  // GPS Integrity Check: devices × dates, API vs tbl_tracking.position_time (streams NDJSON)
  const [gpsIntegrityFrom, setGpsIntegrityFrom] = useState('');
  const [gpsIntegrityTo, setGpsIntegrityTo] = useState('');
  const [gpsIntegrityLoading, setGpsIntegrityLoading] = useState(false);
  const [gpsIntegrityError, setGpsIntegrityError] = useState<string | null>(null);
  const [gpsIntegrityGrid, setGpsIntegrityGrid] = useState<GpsIntegrityGridResponse | null>(null);

  const [dbCheckFrom, setDbCheckFrom] = useState('');
  const [dbCheckTo, setDbCheckTo] = useState('');
  const [dbCheckLoading, setDbCheckLoading] = useState(false);
  const [dbCheckError, setDbCheckError] = useState<string | null>(null);
  const [dbCheckGrid, setDbCheckGrid] = useState<DbCheckGridResponse | null>(null);

  const [dbCheckSimpleFrom, setDbCheckSimpleFrom] = useState('');
  const [dbCheckSimpleTo, setDbCheckSimpleTo] = useState('');
  const [dbCheckSimpleLoading, setDbCheckSimpleLoading] = useState(false);
  const [dbCheckSimpleError, setDbCheckSimpleError] = useState<string | null>(null);
  const [dbCheckSimpleResult, setDbCheckSimpleResult] = useState<DbCheckSimpleResponse | null>(null);

  const [vworkStaleHours, setVworkStaleHours] = useState('48');
  const [vworkStaleFrom, setVworkStaleFrom] = useState('');
  const [vworkStaleTo, setVworkStaleTo] = useState('');
  /** Stage 1 = not stepped (no step actuals). Stage 2 = aging / stale threshold only — mutually exclusive. */
  const [vworkStaleMode, setVworkStaleMode] = useState<'stage1' | 'stage2'>('stage1');
  const [vworkStaleLoading, setVworkStaleLoading] = useState(false);
  const [vworkStaleError, setVworkStaleError] = useState<string | null>(null);
  const [vworkStaleResult, setVworkStaleResult] = useState<VworkStaleResponse | null>(null);

  const [geofenceGapsFrom, setGeofenceGapsFrom] = useState('');
  const [geofenceGapsTo, setGeofenceGapsTo] = useState('');
  const [geofenceGapsMinPoints, setGeofenceGapsMinPoints] = useState('1');
  /** Rerun list: one row per calendar day (whole-day tagging/steps/mapping). */
  const [geofenceGapsDaysOnly, setGeofenceGapsDaysOnly] = useState(false);
  const [geofenceGapsLoading, setGeofenceGapsLoading] = useState(false);
  const [geofenceGapsError, setGeofenceGapsError] = useState<string | null>(null);
  const [geofenceGapsResult, setGeofenceGapsResult] = useState<GeofenceGapsResponse | null>(null);

  const [eeGapsFrom, setEeGapsFrom] = useState('');
  const [eeGapsTo, setEeGapsTo] = useState('');
  const [eeGapsMinPoints, setEeGapsMinPoints] = useState('25');
  const [eeGapsLoading, setEeGapsLoading] = useState(false);
  const [eeGapsError, setEeGapsError] = useState<string | null>(null);
  const [eeGapsResult, setEeGapsResult] = useState<GeofenceEnterExitGapsResponse | null>(null);

  const [step4to5Customer, setStep4to5Customer] = useState('');
  const [step4to5Template, setStep4to5Template] = useState('');
  const [step4to5Customers, setStep4to5Customers] = useState<string[]>([]);
  const [step4to5Templates, setStep4to5Templates] = useState<string[]>([]);
  const [step4to5Preview, setStep4to5Preview] = useState<Step4to5PreviewResponse | null>(null);
  const [step4to5PreviewLoading, setStep4to5PreviewLoading] = useState(false);
  const [step4to5PreviewError, setStep4to5PreviewError] = useState<string | null>(null);
  const [step4to5FixLoading, setStep4to5FixLoading] = useState(false);
  const [step4to5FixError, setStep4to5FixError] = useState<string | null>(null);
  const [step4to5FixResult, setStep4to5FixResult] = useState<Step4to5FixResponse | null>(null);
  const [step4to5FixConfirmOpen, setStep4to5FixConfirmOpen] = useState(false);
  const [step4to5Mode, setStep4to5Mode] = useState<'normal' | 'rerun' | 'ordering'>('normal');

  const [sqlRuns, setSqlRuns] = useState<SqlRunRow[]>([]);
  const [sqlRunsLoading, setSqlRunsLoading] = useState(false);
  const [sqlRunsError, setSqlRunsError] = useState<string | null>(null);
  const [sqlRunsSaving, setSqlRunsSaving] = useState(false);
  const [sqlRunsRunLoading, setSqlRunsRunLoading] = useState(false);
  const [sqlRunsSingleLoadingId, setSqlRunsSingleLoadingId] = useState<number | null>(null);
  type SqlRunLogStep = {
    id: number;
    sql_name: string;
    ok: boolean;
    command?: string | null;
    rowCount?: number | null;
    summary?: string;
    error?: string;
  };
  const [sqlRunsRunResult, setSqlRunsRunResult] = useState<{
    ok?: boolean;
    error?: string;
    ranAt?: string;
    summary?: string;
    mode?: 'all' | 'single';
    steps?: SqlRunLogStep[];
    stoppedAt?: number;
  } | null>(null);
  const [sqlRunLogModalOpen, setSqlRunLogModalOpen] = useState(false);
  const [sqlConfirmModal, setSqlConfirmModal] = useState<
    | null
    | { kind: 'delete'; id: number; name: string }
    | { kind: 'runAll'; total: number }
    | { kind: 'runOne'; id: number; name: string }
  >(null);
  const [sqlNew, setSqlNew] = useState({ sql_name: '', sql_command: '', sql_description: '' });
  const [sqlEditingId, setSqlEditingId] = useState<number | null>(null);
  const [sqlEdit, setSqlEdit] = useState({ sql_name: '', sql_command: '', sql_description: '' });

  const [loadsizeBackfillConfirm, setLoadsizeBackfillConfirm] = useState(false);
  const [loadsizeBackfillRunning, setLoadsizeBackfillRunning] = useState(false);
  const [loadsizeBackfillError, setLoadsizeBackfillError] = useState<string | null>(null);
  const [loadsizeBackfillPhase, setLoadsizeBackfillPhase] = useState<'idle' | 'listing' | 'processing' | 'done'>('idle');
  const [loadsizeBackfillFilesTotal, setLoadsizeBackfillFilesTotal] = useState(0);
  const [loadsizeBackfillFileIndex, setLoadsizeBackfillFileIndex] = useState(-1);
  const [loadsizeBackfillCurrentName, setLoadsizeBackfillCurrentName] = useState('');
  const [loadsizeBackfillTotals, setLoadsizeBackfillTotals] = useState({
    rowsUpdated: 0,
    rowsIndevinCustomer: 0,
    rowsSkippedNoJobId: 0,
    rowsSkippedNoLoadSize: 0,
    rowsNotFoundInDb: 0,
    rowsInAllFiles: 0,
  });
  const [loadsizeBackfillPerFile, setLoadsizeBackfillPerFile] = useState<LoadsizeBackfillFileRow[]>([]);
  const [loadsizeBackfillLastErrors, setLoadsizeBackfillLastErrors] = useState<string[]>([]);
  const [loadsizeQuickTestLoading, setLoadsizeQuickTestLoading] = useState(false);
  const [loadsizeQuickTestResult, setLoadsizeQuickTestResult] = useState<LoadsizeQuickTestResponse | null>(null);
  const [loadsizeQuickTestError, setLoadsizeQuickTestError] = useState<string | null>(null);

  const runLoadsizeQuickTest = useCallback(async () => {
    setLoadsizeQuickTestLoading(true);
    setLoadsizeQuickTestError(null);
    setLoadsizeQuickTestResult(null);
    try {
      const r = await fetch('/api/admin/data-checks/vwork-loadsize-backfill/test', { cache: 'no-store' });
      const d = (await r.json()) as LoadsizeQuickTestResponse & { error?: string };
      if (!r.ok) {
        setLoadsizeQuickTestError(d?.error ?? `HTTP ${r.status}`);
        return;
      }
      setLoadsizeQuickTestResult(d);
    } catch (e) {
      setLoadsizeQuickTestError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoadsizeQuickTestLoading(false);
    }
  }, []);

  const runLoadsizeBackfill = useCallback(async () => {
    if (!loadsizeBackfillConfirm) {
      setLoadsizeBackfillError('Confirm the checklist below before running.');
      return;
    }
    setLoadsizeBackfillRunning(true);
    setLoadsizeBackfillError(null);
    setLoadsizeBackfillLastErrors([]);
    setLoadsizeBackfillPhase('listing');
    setLoadsizeBackfillPerFile([]);
    setLoadsizeBackfillFileIndex(-1);
    setLoadsizeBackfillCurrentName('');
    const emptyTotals = {
      rowsUpdated: 0,
      rowsIndevinCustomer: 0,
      rowsSkippedNoJobId: 0,
      rowsSkippedNoLoadSize: 0,
      rowsNotFoundInDb: 0,
      rowsInAllFiles: 0,
    };
    setLoadsizeBackfillTotals(emptyTotals);

    try {
      const listRes = await fetch('/api/admin/data-checks/vwork-loadsize-backfill', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'list' }),
      });
      const listJson = (await listRes.json()) as {
        ok?: boolean;
        filesTotal?: number;
        files?: { id: string; name: string }[];
        error?: string;
      };
      if (!listRes.ok) throw new Error(listJson.error ?? `List failed (${listRes.status})`);
      const total = listJson.filesTotal ?? 0;
      setLoadsizeBackfillFilesTotal(total);
      if (total === 0) {
        setLoadsizeBackfillPhase('done');
        setLoadsizeBackfillRunning(false);
        setLoadsizeBackfillConfirm(false);
        return;
      }

      setLoadsizeBackfillPhase('processing');
      const perFile: LoadsizeBackfillFileRow[] = [];
      const acc = { ...emptyTotals };

      for (let i = 0; i < total; i++) {
        setLoadsizeBackfillFileIndex(i);
        const nameHint = listJson.files?.[i]?.name ?? `file #${i + 1}`;
        setLoadsizeBackfillCurrentName(nameHint);

        const fr = await fetch('/api/admin/data-checks/vwork-loadsize-backfill', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'file', fileIndex: i }),
        });
        const fj = (await fr.json()) as {
          ok?: boolean;
          fileName?: string;
          rowsInFile?: number;
          rowsIndevinCustomer?: number;
          rowsUpdated?: number;
          rowsSkippedNoJobId?: number;
          rowsSkippedNoLoadSize?: number;
          rowsNotFoundInDb?: number;
          errors?: string[];
          errorsTruncated?: boolean;
          error?: string;
        };
        if (!fr.ok) throw new Error(fj.error ?? `File step failed (${fr.status})`);

        const rowsInFile = fj.rowsInFile ?? 0;
        const rowsIndevin = fj.rowsIndevinCustomer ?? 0;
        const upd = fj.rowsUpdated ?? 0;
        const skipPk = fj.rowsSkippedNoJobId ?? 0;
        const skipLoad = fj.rowsSkippedNoLoadSize ?? 0;
        const notFound = fj.rowsNotFoundInDb ?? 0;

        acc.rowsInAllFiles += rowsInFile;
        acc.rowsIndevinCustomer += rowsIndevin;
        acc.rowsUpdated += upd;
        acc.rowsSkippedNoJobId += skipPk;
        acc.rowsSkippedNoLoadSize += skipLoad;
        acc.rowsNotFoundInDb += notFound;

        const errList = Array.isArray(fj.errors) ? fj.errors : [];
        if (errList.length > 0) {
          setLoadsizeBackfillLastErrors(errList);
        }

        perFile.push({
          fileName: fj.fileName ?? nameHint,
          rowsInFile,
          rowsIndevinCustomer: rowsIndevin,
          rowsUpdated: upd,
          rowsSkippedNoJobId: skipPk,
          rowsSkippedNoLoadSize: skipLoad,
          rowsNotFoundInDb: notFound,
          errorsTruncated: Boolean(fj.errorsTruncated),
          errorSampleCount: errList.length,
        });
        setLoadsizeBackfillPerFile([...perFile]);
        setLoadsizeBackfillTotals({ ...acc });
      }

      setLoadsizeBackfillPhase('done');
      setLoadsizeBackfillConfirm(false);
    } catch (e) {
      setLoadsizeBackfillError(e instanceof Error ? e.message : String(e));
      setLoadsizeBackfillPhase('idle');
    } finally {
      setLoadsizeBackfillRunning(false);
    }
  }, [loadsizeBackfillConfirm]);

  const runStep4to5Fix = useCallback((c: string, t: string, mode: 'normal' | 'rerun' | 'ordering') => {
    setStep4to5FixError(null);
    setStep4to5FixLoading(true);
    const body =
      mode === 'ordering'
        ? { customer: c, template: t, orderingFix: true }
        : { customer: c, template: t, rerun: mode === 'rerun' };
    fetch('/api/admin/vworkjobs/step4to5', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
      .then(async (r) => {
        const d = (await r.json()) as Step4to5FixResponse & { ok?: boolean; error?: string };
        if (!r.ok) throw new Error(d?.error ?? r.statusText);
        if (!d.ok) throw new Error('Fix failed');
        return d;
      })
      .then((data) => {
        setStep4to5FixResult(data);
        setStep4to5Preview(data.afterPreview);
      })
      .catch((e) => setStep4to5FixError(e instanceof Error ? e.message : String(e)))
      .finally(() => {
        setStep4to5FixLoading(false);
        setStep4to5FixConfirmOpen(false);
      });
  }, []);

  useEffect(() => {
    if (activeTab !== 'step4to5') return;
    let cancelled = false;
    fetch('/api/vworkjobs/customers', { cache: 'no-store' })
      .then((r) => r.json())
      .then((d: { customers?: string[] }) => {
        if (!cancelled && Array.isArray(d.customers)) setStep4to5Customers(d.customers);
      })
      .catch(() => {
        if (!cancelled) setStep4to5Customers([]);
      });
    return () => {
      cancelled = true;
    };
  }, [activeTab]);

  useEffect(() => {
    if (activeTab !== 'step4to5') return;
    const c = step4to5Customer.trim();
    if (!c) {
      setStep4to5Templates([]);
      return;
    }
    let cancelled = false;
    fetch(`/api/vworkjobs/templates?customer=${encodeURIComponent(c)}`, { cache: 'no-store' })
      .then((r) => r.json())
      .then((d: { templates?: string[] }) => {
        if (!cancelled && Array.isArray(d.templates)) setStep4to5Templates(d.templates);
      })
      .catch(() => {
        if (!cancelled) setStep4to5Templates([]);
      });
    return () => {
      cancelled = true;
    };
  }, [activeTab, step4to5Customer]);

  useEffect(() => {
    if (activeTab !== 'step4to5') setStep4to5FixConfirmOpen(false);
  }, [activeTab]);

  const fetchSqlRuns = useCallback(() => {
    setSqlRunsError(null);
    setSqlRunsLoading(true);
    fetch('/api/admin/data-checks/sql-runs', { cache: 'no-store' })
      .then((r) => r.json())
      .then((data: { rows?: SqlRunRow[]; error?: string }) => {
        if (data.error) throw new Error(data.error);
        setSqlRuns(data.rows ?? []);
      })
      .catch((e) => setSqlRunsError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSqlRunsLoading(false));
  }, []);

  useEffect(() => {
    if (activeTab !== 'sql-updates') return;
    fetchSqlRuns();
  }, [activeTab, fetchSqlRuns]);

  useEffect(() => {
    if (activeTab === 'sql-updates') return;
    setSqlConfirmModal(null);
    setSqlRunLogModalOpen(false);
  }, [activeTab]);

  const addSqlRun = () => {
    const name = sqlNew.sql_name.trim();
    const cmd = sqlNew.sql_command.trim();
    if (!name || !cmd) {
      setSqlRunsError('SQL name and SQL command are required');
      return;
    }
    setSqlRunsSaving(true);
    setSqlRunsError(null);
    fetch('/api/admin/data-checks/sql-runs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql_name: name,
        sql_command: cmd,
        sql_description: sqlNew.sql_description.trim() || null,
      }),
    })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        setSqlNew({ sql_name: '', sql_command: '', sql_description: '' });
        fetchSqlRuns();
      })
      .catch((e) => setSqlRunsError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSqlRunsSaving(false));
  };

  const saveSqlRun = (id: number) => {
    const name = sqlEdit.sql_name.trim();
    const cmd = sqlEdit.sql_command.trim();
    if (!name || !cmd) {
      setSqlRunsError('SQL name and SQL command are required');
      return;
    }
    setSqlRunsSaving(true);
    setSqlRunsError(null);
    fetch(`/api/admin/data-checks/sql-runs/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql_name: name,
        sql_command: cmd,
        sql_description: sqlEdit.sql_description.trim() || null,
      }),
    })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        setSqlEditingId(null);
        fetchSqlRuns();
      })
      .catch((e) => setSqlRunsError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSqlRunsSaving(false));
  };

  const startSqlEdit = (row: SqlRunRow) => {
    setSqlEditingId(row.id);
    setSqlEdit({
      sql_name: row.sql_name,
      sql_command: row.sql_command,
      sql_description: row.sql_description ?? '',
    });
  };

  const executeDeleteSqlRun = (id: number) => {
    setSqlRunsSaving(true);
    setSqlRunsError(null);
    fetch(`/api/admin/data-checks/sql-runs/${id}`, { method: 'DELETE' })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        if (sqlEditingId === id) setSqlEditingId(null);
        setSqlConfirmModal(null);
        fetchSqlRuns();
      })
      .catch((e) => setSqlRunsError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSqlRunsSaving(false));
  };

  const reorderSqlRun = (id: number, direction: 'up' | 'down') => {
    setSqlRunsSaving(true);
    setSqlRunsError(null);
    fetch('/api/admin/data-checks/sql-runs/reorder', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id, direction }),
    })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        fetchSqlRuns();
      })
      .catch((e) => setSqlRunsError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSqlRunsSaving(false));
  };

  const executeRunAllSqlRuns = () => {
    setSqlConfirmModal(null);
    setSqlRunsRunLoading(true);
    setSqlRunsRunResult(null);
    setSqlRunsError(null);
    fetch('/api/admin/data-checks/sql-runs/run', { method: 'POST' })
      .then(async (r) => {
        const data = (await r.json()) as {
          ok?: boolean;
          error?: string;
          ranAt?: string;
          summary?: string;
          mode?: 'all';
          steps?: SqlRunLogStep[];
          stoppedAt?: number;
        };
        setSqlRunsRunResult(data);
        setSqlRunLogModalOpen(true);
        if (data.ok === true) {
          setSqlRunsError(null);
        } else {
          const errStep = Array.isArray(data.steps) ? data.steps.find((s) => !s.ok) : undefined;
          setSqlRunsError(errStep?.error ?? data.error ?? `Request failed (${r.status})`);
        }
      })
      .catch((e) => setSqlRunsError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSqlRunsRunLoading(false));
  };

  const executeRunOneSqlRun = (id: number) => {
    setSqlConfirmModal(null);
    setSqlRunsSingleLoadingId(id);
    setSqlRunsRunResult(null);
    setSqlRunsError(null);
    fetch(`/api/admin/data-checks/sql-runs/${id}/run`, { method: 'POST' })
      .then(async (r) => {
        const data = (await r.json()) as {
          ok?: boolean;
          error?: string;
          ranAt?: string;
          summary?: string;
          mode?: 'single';
          step?: SqlRunLogStep;
        };
        const step = data.step;
        const normalized =
          step != null
            ? {
                ok: data.ok,
                ranAt: data.ranAt,
                mode: 'single' as const,
                summary:
                  data.summary ??
                  (data.ok
                    ? `Statement "${step.sql_name}" completed.`
                    : `Statement "${step.sql_name}" failed.`),
                steps: [step],
              }
            : {
                ok: false,
                mode: 'single' as const,
                summary: data.error ?? 'Run failed',
                steps: [] as SqlRunLogStep[],
              };
        setSqlRunsRunResult(normalized);
        setSqlRunLogModalOpen(true);
        if (data.ok === true) {
          setSqlRunsError(null);
        } else {
          setSqlRunsError(step?.error ?? data.error ?? 'Statement failed');
        }
      })
      .catch((e) => setSqlRunsError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSqlRunsSingleLoadingId(null));
  };

  const runGpsIntegrityCheck = useCallback(async () => {
    setGpsIntegrityError(null);
    setGpsIntegrityGrid(null);
    if (!gpsIntegrityFrom || !gpsIntegrityTo) {
      setGpsIntegrityError('From and To dates required');
      return;
    }
    setGpsIntegrityLoading(true);
    try {
      const res = await fetch('/api/admin/data-checks/gps-integrity', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dateFrom: gpsIntegrityFrom,
          dateTo: gpsIntegrityTo,
          grid: true,
          stream: true,
        }),
      });
      const ct = res.headers.get('content-type') ?? '';
      if (!res.ok) {
        const d = ct.includes('application/json') ? await res.json() : {};
        throw new Error((d as { error?: string })?.error ?? res.statusText);
      }
      if (!res.body) throw new Error('No response body');
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';
      let metaDates: string[] = [];
      let metaDevices: string[] = [];
      const rows: Array<{ device: string; cells: GpsIntegrityCell[] }> = [];
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const lines = buf.split('\n');
        buf = lines.pop() ?? '';
        for (const line of lines) {
          if (!line.trim()) continue;
          const msg = JSON.parse(line) as
            | { type: 'meta'; dates: string[]; devices: string[] }
            | { type: 'deviceRow'; device: string; cells: GpsIntegrityCell[] }
            | { type: 'done' }
            | { type: 'error'; message: string };
          if (msg.type === 'meta') {
            metaDates = msg.dates;
            metaDevices = msg.devices;
            setGpsIntegrityGrid({ dates: metaDates, devices: metaDevices, rows: [] });
          } else if (msg.type === 'deviceRow') {
            rows.push({ device: msg.device, cells: msg.cells });
            setGpsIntegrityGrid({
              dates: metaDates,
              devices: metaDevices,
              rows: [...rows],
            });
          } else if (msg.type === 'error') {
            throw new Error(msg.message);
          }
        }
      }
      if (buf.trim()) {
        const msg = JSON.parse(buf) as { type?: string; message?: string };
        if (msg.type === 'error' && msg.message) throw new Error(msg.message);
      }
    } catch (e) {
      setGpsIntegrityError(e instanceof Error ? e.message : String(e));
    } finally {
      setGpsIntegrityLoading(false);
    }
  }, [gpsIntegrityFrom, gpsIntegrityTo]);

  const fetchHealthOverview = useCallback(async () => {
    setHealthLoading(true);
    setHealthError(null);
    try {
      const p = new URLSearchParams();
      p.set('dateFrom', formatDateInput(healthDateFrom.trim()) || healthDateFrom.trim());
      p.set('dateTo', formatDateInput(healthDateTo.trim()) || healthDateTo.trim());
      p.set('minPointsEnterExit', String(healthMinPointsEE));
      const r = await fetch(`/api/admin/data-checks/data-health-overview?${p.toString()}`);
      const d = (await r.json()) as DataHealthOverviewResponse & { ok?: boolean; error?: string };
      if (!r.ok || d.ok !== true) {
        throw new Error(typeof d.error === 'string' ? d.error : r.statusText);
      }
      setHealthData(d);
    } catch (e) {
      setHealthError(e instanceof Error ? e.message : String(e));
      setHealthData(null);
    } finally {
      setHealthLoading(false);
    }
  }, [healthDateFrom, healthDateTo, healthMinPointsEE]);

  const runHealthFillPositionTimeNz = useCallback(async () => {
    setHealthFillNzResult(null);
    setHealthFillNzLoading(true);
    try {
      const r = await fetch('/api/admin/data-checks/fill-position-time-nz', { method: 'POST' });
      const d = (await r.json()) as { ok?: boolean; updated?: number; error?: string };
      if (!r.ok || d.ok !== true) {
        throw new Error(typeof d.error === 'string' ? d.error : r.statusText);
      }
      const n = d.updated ?? 0;
      setHealthFillNzResult({ ok: true, text: `Updated ${n.toLocaleString()} row(s).` });
      await fetchHealthOverview();
    } catch (e) {
      setHealthFillNzResult({ ok: false, text: e instanceof Error ? e.message : String(e) });
    } finally {
      setHealthFillNzLoading(false);
    }
  }, [fetchHealthOverview]);

  useEffect(() => {
    if (activeTab === 'data-health-overview') {
      void fetchHealthOverview();
    }
  }, [activeTab, fetchHealthOverview]);

  const fetchWineMapp = useCallback(() => {
    setWineMappLoading(true);
    setWineMappError(null);
    fetch('/api/admin/data-checks/wine-mapp')
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: WineMappRow[] }) => setWineMappRows(data.rows ?? []))
      .catch((e) => setWineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setWineMappLoading(false));
  }, []);

  const fetchVineMapp = useCallback(() => {
    setVineMappLoading(true);
    setVineMappError(null);
    fetch('/api/admin/data-checks/vine-mapp')
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: WineMappRow[] }) => setVineMappRows(data.rows ?? []))
      .catch((e) => setVineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVineMappLoading(false));
  }, []);

  const fetchDriverMapp = useCallback(() => {
    setDriverMappLoading(true);
    setDriverMappError(null);
    fetch('/api/admin/data-checks/driver-mapp')
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: WineMappRow[] }) => setDriverMappRows(data.rows ?? []))
      .catch((e) => setDriverMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setDriverMappLoading(false));
  }, []);

  useEffect(() => {
    if (activeTab === 'winery-fixes') {
      fetchWineMapp();
      fetchVineMapp();
      fetchDriverMapp();
      fetch('/api/admin/data-checks/wine-mapp/delivery-wineries')
        .then((r) => {
          if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
          return r.json();
        })
        .then((data: { rows: string[] }) => setDeliveryWineries(data.rows ?? []))
        .catch(() => setDeliveryWineries([]));
      fetch('/api/admin/data-checks/vine-mapp/vineyard-names')
        .then((r) => {
          if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
          return r.json();
        })
        .then((data: { rows: string[] }) => setVineyardNamesList(data.rows ?? []))
        .catch(() => setVineyardNamesList([]));
      fetch('/api/admin/data-checks/driver-mapp/worker-names')
        .then((r) => {
          if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
          return r.json();
        })
        .then((data: { rows: string[] }) => setWorkerNamesList(data.rows ?? []))
        .catch(() => setWorkerNamesList([]));
    }
  }, [activeTab, fetchWineMapp, fetchVineMapp, fetchDriverMapp]);

  const runJobsGpsReport = useCallback(async () => {
    setJobsGpsError(null);
    const df = formatDateInput(jobsGpsDateFrom.trim());
    const dt = formatDateInput(jobsGpsDateTo.trim());
    if (!df || !dt) {
      setJobsGpsError('Date from and date to are required (filters jobs by actual_start_time in that range).');
      return;
    }
    setJobsGpsLoading(true);
    try {
      const p = new URLSearchParams();
      p.set('dateFrom', df);
      p.set('dateTo', dt);
      if (jobsGpsCustomer.trim()) p.set('customer', jobsGpsCustomer.trim());
      if (jobsGpsTemplate.trim()) p.set('template', jobsGpsTemplate.trim());
      if (jobsGpsWinery.trim()) p.set('winery', jobsGpsWinery.trim());
      if (jobsGpsVineyard.trim()) p.set('vineyard', jobsGpsVineyard.trim());
      p.set('startLessMinutes', jobsGpsStartLess.trim() || '10');
      p.set('endPlusMinutes', jobsGpsEndPlus.trim() || '60');
      p.set('gapThresholdMinutes', jobsGpsGapThreshold.trim() || '10');
      const r = await fetch(`/api/admin/data-checks/jobs-gps-report?${p.toString()}`);
      const d = (await r.json()) as JobsGpsReportResponse & { ok?: boolean; error?: string };
      if (!r.ok || d.ok !== true) {
        throw new Error(typeof d.error === 'string' ? d.error : r.statusText);
      }
      setJobsGpsResult(d);
      setJobsGpsPage(0);
    } catch (e) {
      setJobsGpsError(e instanceof Error ? e.message : String(e));
      setJobsGpsResult(null);
    } finally {
      setJobsGpsLoading(false);
    }
  }, [
    jobsGpsDateFrom,
    jobsGpsDateTo,
    jobsGpsCustomer,
    jobsGpsTemplate,
    jobsGpsWinery,
    jobsGpsVineyard,
    jobsGpsStartLess,
    jobsGpsEndPlus,
    jobsGpsGapThreshold,
  ]);

  const openJobsGpsGapDetail = useCallback(
    async (jobId: string) => {
      if (!jobsGpsResult?.window) return;
      setJobsGpsGapModalJobId(jobId);
      setJobsGpsGapModalOpen(true);
      setJobsGpsGapDetailLoading(true);
      setJobsGpsGapDetailError(null);
      setJobsGpsGapDetailData(null);
      try {
        const w = jobsGpsResult.window;
        const p = new URLSearchParams();
        p.set('jobId', jobId);
        p.set('startLessMinutes', String(w.startLessMinutes));
        p.set('endPlusMinutes', String(w.endPlusMinutes));
        p.set('displayExpandBefore', String(w.displayExpandBefore ?? 0));
        p.set('displayExpandAfter', String(w.displayExpandAfter ?? 0));
        p.set('gapThresholdMinutes', String(w.gapThresholdMinutes));
        const r = await fetch(`/api/admin/data-checks/jobs-gps-gaps-detail?${p.toString()}`);
        const d = (await r.json()) as JobsGpsGapsDetailOk & { ok?: boolean; error?: string };
        if (!r.ok || d.ok !== true) {
          throw new Error(typeof d.error === 'string' ? d.error : r.statusText);
        }
        setJobsGpsGapDetailData(d);
      } catch (e) {
        setJobsGpsGapDetailError(e instanceof Error ? e.message : String(e));
      } finally {
        setJobsGpsGapDetailLoading(false);
      }
    },
    [jobsGpsResult]
  );

  useEffect(() => {
    if (!jobsGpsGapModalOpen) return;
    setJobsGpsGapScrollIdx(-1);
  }, [jobsGpsGapModalOpen, jobsGpsGapsModalBreachedOnly, jobsGpsGapDetailData?.job_id]);

  useEffect(() => {
    if (activeTab !== 'jobs-gps' || jobsGpsPrefsSessionLoadedRef.current) return;
    let cancelled = false;
    void (async () => {
      try {
        const r = await fetch(
          `/api/settings?type=${encodeURIComponent(JOBS_GPS_TAB_SETTINGS_TYPE)}&name=${encodeURIComponent(JOBS_GPS_TAB_SETTINGS_NAME)}`
        );
        const d = (await r.json()) as { settingvalue?: string | null };
        if (cancelled) return;
        const raw = d?.settingvalue;
        if (raw && typeof raw === 'string') {
          try {
            const o = JSON.parse(raw) as Record<string, unknown>;
            if (typeof o.dateFrom === 'string') setJobsGpsDateFrom(o.dateFrom);
            if (typeof o.dateTo === 'string') setJobsGpsDateTo(o.dateTo);
            if (typeof o.customer === 'string') setJobsGpsCustomer(o.customer);
            if (typeof o.template === 'string') setJobsGpsTemplate(o.template);
            if (typeof o.winery === 'string') setJobsGpsWinery(o.winery);
            if (typeof o.vineyard === 'string') setJobsGpsVineyard(o.vineyard);
            if (typeof o.startLess === 'string') setJobsGpsStartLess(o.startLess);
            if (typeof o.endPlus === 'string') setJobsGpsEndPlus(o.endPlus);
            if (typeof o.gapThreshold === 'string') setJobsGpsGapThreshold(o.gapThreshold);
            if (typeof o.issuesOnly === 'boolean') setJobsGpsIssuesOnly(o.issuesOnly);
            setJobsGpsFilterFrom(coerceJobsGpsEdgeFilter(o.filterFrom));
            setJobsGpsFilterTo(coerceJobsGpsEdgeFilter(o.filterTo));
            setJobsGpsFilterFlags(coerceJobsGpsFlagsFilter(o.filterFlags));
            if (typeof o.gapsModalBreachedOnly === 'boolean')
              setJobsGpsGapsModalBreachedOnly(o.gapsModalBreachedOnly);
          } catch {
            /* ignore invalid JSON */
          }
        }
      } catch {
        /* ignore network error */
      } finally {
        if (!cancelled) {
          jobsGpsPrefsSessionLoadedRef.current = true;
          setJobsGpsPrefsHydrated(true);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [activeTab]);

  useEffect(() => {
    if (!jobsGpsPrefsHydrated) return;
    const t = setTimeout(() => {
      const payload = {
        v: 1,
        dateFrom: jobsGpsDateFrom,
        dateTo: jobsGpsDateTo,
        customer: jobsGpsCustomer,
        template: jobsGpsTemplate,
        winery: jobsGpsWinery,
        vineyard: jobsGpsVineyard,
        startLess: jobsGpsStartLess,
        endPlus: jobsGpsEndPlus,
        gapThreshold: jobsGpsGapThreshold,
        issuesOnly: jobsGpsIssuesOnly,
        filterFrom: jobsGpsFilterFrom,
        filterTo: jobsGpsFilterTo,
        filterFlags: jobsGpsFilterFlags,
        gapsModalBreachedOnly: jobsGpsGapsModalBreachedOnly,
      };
      void fetch('/api/settings', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type: JOBS_GPS_TAB_SETTINGS_TYPE,
          settingname: JOBS_GPS_TAB_SETTINGS_NAME,
          settingvalue: JSON.stringify(payload),
        }),
      });
    }, 600);
    return () => clearTimeout(t);
  }, [
    jobsGpsPrefsHydrated,
    jobsGpsDateFrom,
    jobsGpsDateTo,
    jobsGpsCustomer,
    jobsGpsTemplate,
    jobsGpsWinery,
    jobsGpsVineyard,
    jobsGpsStartLess,
    jobsGpsEndPlus,
    jobsGpsGapThreshold,
    jobsGpsIssuesOnly,
    jobsGpsFilterFrom,
    jobsGpsFilterTo,
    jobsGpsFilterFlags,
    jobsGpsGapsModalBreachedOnly,
  ]);

  const runGapsScan = () => {
    const minutes = parseFloat(minGapMinutes);
    if (Number.isNaN(minutes) || minutes < 0) {
      setError('Enter a valid gap threshold (minutes).');
      return;
    }
    setError(null);
    setLoading(true);
    setResult(null);
    const params = new URLSearchParams({ minGapMinutes: String(minutes) });
    if (dateFrom.trim()) params.set('dateFrom', formatDateInput(dateFrom.trim()));
    if (dateTo.trim()) params.set('dateTo', formatDateInput(dateTo.trim()));
    fetch(`/api/admin/data-checks/gps-gaps?${params.toString()}`)
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: GapsResult) => {
        setResult(data);
      })
      .catch((e) => {
        setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => setLoading(false));
  };

  const openTrackingPopup = (row: SummaryRow) => {
    if (row.gap_from == null || row.gap_to == null) return;
    setPopupRow(row);
    setPopupError(null);
    setPopupRows([]);
    setPopupLoading(true);
    const params = new URLSearchParams({
      device: row.device_name,
      from: row.gap_from,
      to: row.gap_to,
    });
    fetch(`/api/admin/data-checks/tracking-window?${params.toString()}`)
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: TrackingWindowRow[] }) => {
        setPopupRows(data.rows ?? []);
      })
      .catch((e) => {
        setPopupError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => setPopupLoading(false));
  };

  const createWineMapp = () => {
    const oldV = wineMappNewOld.trim();
    const newV = wineMappNewNew.trim();
    if (!oldV || !newV) {
      setWineMappError('Old name and new name are required.');
      return;
    }
    setWineMappError(null);
    setWineMappSaving(true);
    fetch('/api/admin/data-checks/wine-mapp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oldvworkname: oldV, newvworkname: newV }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setWineMappNewOld('');
        setWineMappNewNew('');
        fetchWineMapp();
      })
      .catch((e) => setWineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setWineMappSaving(false));
  };

  const updateWineMapp = (id: number) => {
    const oldV = editOld.trim();
    const newV = editNew.trim();
    if (!oldV || !newV) {
      setWineMappError('Old name and new name are required.');
      return;
    }
    setWineMappError(null);
    setWineMappSaving(true);
    fetch(`/api/admin/data-checks/wine-mapp/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oldvworkname: oldV, newvworkname: newV }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setEditingId(null);
        fetchWineMapp();
      })
      .catch((e) => setWineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setWineMappSaving(false));
  };

  const deleteWineMapp = (id: number) => {
    if (!confirm('Delete this winery name fix?')) return;
    setWineMappError(null);
    setWineMappSaving(true);
    fetch(`/api/admin/data-checks/wine-mapp/${id}`, { method: 'DELETE' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => fetchWineMapp())
      .catch((e) => setWineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setWineMappSaving(false));
  };

  const startEdit = (row: WineMappRow) => {
    setEditingId(row.id);
    setEditOld(row.oldvworkname);
    setEditNew(row.newvworkname);
  };

  const runFixes = () => {
    setWineMappError(null);
    setRunFixesResult(null);
    setRunFixesLoading(true);
    fetch('/api/admin/data-checks/wine-mapp/run-fixes', { method: 'POST' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { ok: boolean; totalUpdated: number; perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[] }) => {
        setRunFixesResult({ totalUpdated: data.totalUpdated, perMapping: data.perMapping ?? [] });
        setDeliveryWineries([]);
        fetch('/api/admin/data-checks/wine-mapp/delivery-wineries')
          .then((res) => res.ok ? res.json() : { rows: [] })
          .then((d: { rows: string[] }) => setDeliveryWineries(d.rows ?? []));
      })
      .catch((e) => setWineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setRunFixesLoading(false));
  };

  const createVineMapp = () => {
    const oldV = vineMappNewOld.trim();
    const newV = vineMappNewNew.trim();
    if (!oldV || !newV) {
      setVineMappError('Old name and new name are required.');
      return;
    }
    setVineMappError(null);
    setVineMappSaving(true);
    fetch('/api/admin/data-checks/vine-mapp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oldvworkname: oldV, newvworkname: newV }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setVineMappNewOld('');
        setVineMappNewNew('');
        fetchVineMapp();
      })
      .catch((e) => setVineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVineMappSaving(false));
  };

  const updateVineMapp = (id: number) => {
    const oldV = vineEditOld.trim();
    const newV = vineEditNew.trim();
    if (!oldV || !newV) {
      setVineMappError('Old name and new name are required.');
      return;
    }
    setVineMappError(null);
    setVineMappSaving(true);
    fetch(`/api/admin/data-checks/vine-mapp/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oldvworkname: oldV, newvworkname: newV }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setVineEditingId(null);
        fetchVineMapp();
      })
      .catch((e) => setVineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVineMappSaving(false));
  };

  const deleteVineMapp = (id: number) => {
    if (!confirm('Delete this vineyard name fix?')) return;
    setVineMappError(null);
    setVineMappSaving(true);
    fetch(`/api/admin/data-checks/vine-mapp/${id}`, { method: 'DELETE' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => fetchVineMapp())
      .catch((e) => setVineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVineMappSaving(false));
  };

  const startEditVine = (row: WineMappRow) => {
    setVineEditingId(row.id);
    setVineEditOld(row.oldvworkname);
    setVineEditNew(row.newvworkname);
  };

  const runVineFixes = () => {
    setVineMappError(null);
    setRunVineFixesResult(null);
    setRunVineFixesLoading(true);
    fetch('/api/admin/data-checks/vine-mapp/run-fixes', { method: 'POST' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { ok: boolean; totalUpdated: number; perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[] }) => {
        setRunVineFixesResult({ totalUpdated: data.totalUpdated, perMapping: data.perMapping ?? [] });
        setVineyardNamesList([]);
        fetch('/api/admin/data-checks/vine-mapp/vineyard-names')
          .then((res) => (res.ok ? res.json() : { rows: [] }))
          .then((d: { rows: string[] }) => setVineyardNamesList(d.rows ?? []));
      })
      .catch((e) => setVineMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setRunVineFixesLoading(false));
  };

  const createDriverMapp = () => {
    const oldV = driverMappNewOld.trim();
    const newV = driverMappNewNew.trim();
    if (!oldV || !newV) {
      setDriverMappError('Old name and new name are required.');
      return;
    }
    setDriverMappError(null);
    setDriverMappSaving(true);
    fetch('/api/admin/data-checks/driver-mapp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oldvworkname: oldV, newvworkname: newV }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setDriverMappNewOld('');
        setDriverMappNewNew('');
        fetchDriverMapp();
      })
      .catch((e) => setDriverMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setDriverMappSaving(false));
  };

  const updateDriverMapp = (id: number) => {
    const oldV = driverEditOld.trim();
    const newV = driverEditNew.trim();
    if (!oldV || !newV) {
      setDriverMappError('Old name and new name are required.');
      return;
    }
    setDriverMappError(null);
    setDriverMappSaving(true);
    fetch(`/api/admin/data-checks/driver-mapp/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oldvworkname: oldV, newvworkname: newV }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setDriverEditingId(null);
        fetchDriverMapp();
      })
      .catch((e) => setDriverMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setDriverMappSaving(false));
  };

  const deleteDriverMapp = (id: number) => {
    if (!confirm('Delete this driver name fix?')) return;
    setDriverMappError(null);
    setDriverMappSaving(true);
    fetch(`/api/admin/data-checks/driver-mapp/${id}`, { method: 'DELETE' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => fetchDriverMapp())
      .catch((e) => setDriverMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setDriverMappSaving(false));
  };

  const startEditDriver = (row: WineMappRow) => {
    setDriverEditingId(row.id);
    setDriverEditOld(row.oldvworkname);
    setDriverEditNew(row.newvworkname);
  };

  const runDriverFixes = () => {
    setDriverMappError(null);
    setRunDriverFixesResult(null);
    setRunDriverFixesLoading(true);
    fetch('/api/admin/data-checks/driver-mapp/run-fixes', { method: 'POST' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { ok: boolean; totalUpdated: number; perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[] }) => {
        setRunDriverFixesResult({ totalUpdated: data.totalUpdated, perMapping: data.perMapping ?? [] });
        setWorkerNamesList([]);
        fetch('/api/admin/data-checks/driver-mapp/worker-names')
          .then((res) => (res.ok ? res.json() : { rows: [] }))
          .then((d: { rows: string[] }) => setWorkerNamesList(d.rows ?? []));
      })
      .catch((e) => setDriverMappError(e instanceof Error ? e.message : String(e)))
      .finally(() => setRunDriverFixesLoading(false));
  };

  const runSetTrailerType = () => {
    setSetTrailerError(null);
    setSetTrailerResult(null);
    setSetTrailerLoading(true);
    fetch('/api/admin/data-checks/set-trailer-type', { method: 'POST' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { ok: boolean; updatedTT: number; updatedT: number; totalUpdated: number }) => {
        setSetTrailerResult({
          updatedTT: data.updatedTT,
          updatedT: data.updatedT,
          totalUpdated: data.totalUpdated,
        });
      })
      .catch((e) => setSetTrailerError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSetTrailerLoading(false));
  };

  const runSetTrailermodeFromLoadsize = () => {
    setLoadsizeTmError(null);
    setLoadsizeTmResult(null);
    setLoadsizeTmLoading(true);
    fetch('/api/admin/data-checks/set-trailermode-from-loadsize', { method: 'POST' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(
        (data: {
          ok: boolean;
          threshold: number;
          updatedTT: number;
          updatedT: number;
          totalUpdated: number;
        }) => {
          setLoadsizeTmResult({
            threshold: data.threshold,
            updatedTT: data.updatedTT,
            updatedT: data.updatedT,
            totalUpdated: data.totalUpdated,
          });
        }
      )
      .catch((e) => setLoadsizeTmError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoadsizeTmLoading(false));
  };

  const runUpdateVineyardGroup = () => {
    setVineyardGroupError(null);
    setVineyardGroupResult(null);
    setVineyardGroupLoading(true);
    fetch('/api/admin/data-checks/update-vineyard-group', { method: 'POST' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { ok: boolean; setToNa: number; matched: number; totalRows: number }) => {
        setVineyardGroupResult({
          setToNa: data.setToNa,
          matched: data.matched,
          totalRows: data.totalRows ?? 0,
        });
      })
      .catch((e) => setVineyardGroupError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVineyardGroupLoading(false));
  };

  const fetchVgRows = useCallback(() => {
    setVgLoading(true);
    setVgError(null);
    fetch('/api/admin/vineyardgroups')
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { rows: VineyardGroupRow[] }) => setVgRows(data.rows ?? []))
      .catch((e) => setVgError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVgLoading(false));
  }, []);

  const fetchVgOptions = useCallback(() => {
    fetch('/api/admin/vineyardgroups/options')
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then((data: { deliveryWineries?: string[]; vineyardNames?: string[] }) => {
        setVgDeliveryWineries(data.deliveryWineries ?? []);
        setVgVineyardNames(data.vineyardNames ?? []);
      })
      .catch(() => {
        setVgDeliveryWineries([]);
        setVgVineyardNames([]);
      });
  }, []);

  useEffect(() => {
    if (activeTab === 'vineyard-mappings') {
      fetchVgRows();
      fetchVgOptions();
    }
  }, [activeTab, fetchVgRows, fetchVgOptions]);

  const createVgRow = () => {
    const vineyard = vgNewVineyard.trim();
    const group = vgNewGroup.trim();
    if (!vineyard || !group) {
      setVgError('Vineyard name and group are required.');
      return;
    }
    setVgError(null);
    setVgSaving(true);
    fetch('/api/admin/vineyardgroups', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        winery_name: vgNewWinery.trim() || null,
        vineyard_name: vineyard,
        vineyard_group: group,
      }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setVgNewWinery('');
        setVgNewVineyard('');
        setVgNewGroup('');
        fetchVgRows();
      })
      .catch((e) => setVgError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVgSaving(false));
  };

  const updateVgRow = (id: number) => {
    const vineyard = vgEditVineyard.trim();
    const group = vgEditGroup.trim();
    if (!vineyard || !group) {
      setVgError('Vineyard name and group are required.');
      return;
    }
    setVgError(null);
    setVgSaving(true);
    fetch(`/api/admin/vineyardgroups/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        winery_name: vgEditWinery.trim() || null,
        vineyard_name: vineyard,
        vineyard_group: group,
      }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => {
        setVgEditingId(null);
        fetchVgRows();
      })
      .catch((e) => setVgError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVgSaving(false));
  };

  const deleteVgRow = (id: number) => {
    if (!confirm('Delete this vineyard mapping?')) return;
    setVgError(null);
    setVgSaving(true);
    fetch(`/api/admin/vineyardgroups/${id}`, { method: 'DELETE' })
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
        return r.json();
      })
      .then(() => fetchVgRows())
      .catch((e) => setVgError(e instanceof Error ? e.message : String(e)))
      .finally(() => setVgSaving(false));
  };

  const startEditVg = (row: VineyardGroupRow) => {
    setVgEditingId(row.id);
    setVgEditWinery(row.winery_name ?? '');
    setVgEditVineyard(row.vineyard_name);
    setVgEditGroup(row.vineyard_group);
  };

  const cloneVgRow = (row: VineyardGroupRow) => {
    setVgEditingId(null);
    setVgNewWinery(row.winery_name ?? '');
    setVgNewVineyard(row.vineyard_name);
    setVgNewGroup(row.vineyard_group);
    setVgError(null);
  };

  return (
    <div className="min-h-screen min-w-0 bg-zinc-50 dark:bg-zinc-950">
      <div className="w-full min-w-0 px-4 py-8 text-left">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Data Checks</h1>
        <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
          Scan and report on data quality (tbl_tracking).
        </p>

        <div className="mt-6 flex flex-wrap gap-1 border-b border-zinc-200 dark:border-zinc-700">
          <button
            type="button"
            onClick={() => setActiveTab('data-health-overview')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'data-health-overview' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            0. Data Health Overview
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('gps-gaps')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'gps-gaps' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            1. GPS Tracking Gaps
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('winery-fixes')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'winery-fixes' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            2. Winery/Vineyard/Driver Name Fixes
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('data-fixes')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'data-fixes' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            2b. Data fixes
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('varied')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'varied' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            3. Varied
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('gps-integrity')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'gps-integrity' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            4. GPS Integrity (API)
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('db-check')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'db-check' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            5. DB Check
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('db-check-simple')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'db-check-simple' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            6. DB Check Simple
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('vineyard-mappings')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'vineyard-mappings' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            7. Vineyard Mappings
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('vwork-stale')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'vwork-stale' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            8. VWork steps
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('geofence-gaps')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'geofence-gaps' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            9. Geofence gaps
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('geofence-enter-exit-gaps')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'geofence-enter-exit-gaps' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            10. Geofence Enter/Exit gaps
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('step4to5')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'step4to5' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            11. Step4→5 (VWork jobs)
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('sql-updates')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'sql-updates' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            12. SQL Updates
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('jobs-gps')}
            className={`rounded-t px-4 py-2 text-sm font-medium ${activeTab === 'jobs-gps' ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100' : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'}`}
          >
            13. Jobs &amp; GPS window
          </button>
        </div>

        {activeTab === 'data-health-overview' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
            Data Health Overview
          </h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            DB-only snapshot: tracking coverage, fence assignment backlog, ENTER/EXIT gaps in a date window, and vworkjobs steps coverage. Use the numbered tabs below for detail and actions. If rows lack{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time_nz</code>, you can run the same NZ backfill as after GPS import (below). Day boundaries for window metrics use{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time::date</code> (verbatim API date).
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4">
            <div>
              <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">Window from</label>
              <input
                type="date"
                value={healthDateFrom}
                onChange={(e) => setHealthDateFrom(e.target.value)}
                className="mt-0.5 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">Window to</label>
              <input
                type="date"
                value={healthDateTo}
                onChange={(e) => setHealthDateTo(e.target.value)}
                className="mt-0.5 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">Min points (Enter/Exit gap)</label>
              <input
                type="number"
                min={1}
                max={50000}
                value={healthMinPointsEE}
                onChange={(e) => setHealthMinPointsEE(Math.max(1, Math.min(50000, parseInt(e.target.value, 10) || 25)))}
                className="mt-0.5 w-24 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </div>
            <button
              type="button"
              onClick={() => void fetchHealthOverview()}
              disabled={healthLoading}
              className="rounded bg-zinc-800 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-200 dark:text-zinc-900 dark:hover:bg-zinc-300"
            >
              {healthLoading ? 'Loading…' : 'Refresh'}
            </button>
          </div>

          {healthError && (
            <p className="mt-4 text-sm text-red-600 dark:text-red-400">{healthError}</p>
          )}

          {healthData && !healthError && (
            <>
              <p className="mt-3 text-xs text-zinc-500 dark:text-zinc-400">
                Snapshot:{' '}
                <span className="font-mono text-zinc-700 dark:text-zinc-300">
                  {new Date(healthData.generatedAt).toLocaleString()}
                </span>
                {' · '}
                Window{' '}
                <span className="font-mono">
                  {healthData.window.dateFrom} → {healthData.window.dateTo}
                </span>
              </p>

              <div className="mt-6 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
                <div className="rounded-lg border border-zinc-200 bg-zinc-50/80 p-4 dark:border-zinc-600 dark:bg-zinc-800/40">
                  <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">tbl_tracking (all rows)</h3>
                  <dl className="mt-3 space-y-2 text-sm text-zinc-700 dark:text-zinc-300">
                    <div className="flex justify-between gap-2">
                      <dt>Total rows</dt>
                      <dd className="font-mono tabular-nums">{healthData.tracking.totalRows.toLocaleString()}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt>max(position_time)</dt>
                      <dd className="font-mono text-xs">{healthData.tracking.maxPositionTime ?? '—'}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt>max(position_time_nz)</dt>
                      <dd className="font-mono text-xs">{healthData.tracking.maxPositionTimeNz ?? '—'}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt>Rows missing NZ time</dt>
                      <dd className={`font-mono tabular-nums ${healthData.tracking.rowsMissingPositionTimeNz > 0 ? 'text-amber-700 dark:text-amber-400' : ''}`}>
                        {healthData.tracking.rowsMissingPositionTimeNz.toLocaleString()}
                      </dd>
                    </div>
                    <div className="mt-3 rounded-md border border-zinc-200 bg-white/80 p-3 dark:border-zinc-600 dark:bg-zinc-900/40">
                      <p className="text-xs text-zinc-600 dark:text-zinc-400">
                        Set <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">position_time_nz</code> from{' '}
                        <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">position_time</code> using{' '}
                        <span className="font-mono text-[11px] text-zinc-700 dark:text-zinc-300">UTC → Pacific/Auckland</span>{' '}
                        (DST-aware). Same as post-import backfill; only fills where NZ is null; does not change{' '}
                        <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">position_time</code>.
                      </p>
                      <div className="mt-2 flex flex-wrap items-center gap-2">
                        <button
                          type="button"
                          onClick={() => void runHealthFillPositionTimeNz()}
                          disabled={healthFillNzLoading || healthLoading}
                          className="rounded bg-amber-700 px-3 py-1.5 text-xs font-medium text-white hover:bg-amber-800 disabled:opacity-50 dark:bg-amber-600 dark:hover:bg-amber-500"
                        >
                          {healthFillNzLoading ? 'Running…' : 'Set position_time_nz (missing rows)'}
                        </button>
                        {healthFillNzResult && (
                          <span
                            className={`text-xs ${healthFillNzResult.ok ? 'text-green-700 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}
                          >
                            {healthFillNzResult.text}
                          </span>
                        )}
                      </div>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt>Geofence not attempted</dt>
                      <dd className={`font-mono tabular-nums ${healthData.tracking.rowsGeofenceNotAttempted > 0 ? 'text-amber-700 dark:text-amber-400' : ''}`}>
                        {healthData.tracking.rowsGeofenceNotAttempted.toLocaleString()}
                      </dd>
                    </div>
                    <div className="flex justify-between gap-2 border-t border-zinc-200 pt-2 dark:border-zinc-600">
                      <dt className="text-xs">max PT (geofence_attempted)</dt>
                      <dd className="font-mono text-xs">{healthData.tracking.maxPositionTimeFenceAttempted ?? '—'}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt className="text-xs">max PT NZ (geofence_attempted)</dt>
                      <dd className="font-mono text-xs">{healthData.tracking.maxPositionTimeNzFenceAttempted ?? '—'}</dd>
                    </div>
                  </dl>
                  <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs">
                    <Link href="/admin/data-checks?tab=db-check-simple" className="text-blue-600 hover:underline dark:text-blue-400">
                      DB Check Simple
                    </Link>
                    <Link href="/admin/data-checks?tab=geofence-gaps" className="text-blue-600 hover:underline dark:text-blue-400">
                      Geofence gaps
                    </Link>
                    <Link href="/admin/tagging" className="text-blue-600 hover:underline dark:text-blue-400">
                      Tagging / fence prep
                    </Link>
                  </div>
                </div>

                <div className="rounded-lg border border-zinc-200 bg-zinc-50/80 p-4 dark:border-zinc-600 dark:bg-zinc-800/40">
                  <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Window: fence &amp; ENTER/EXIT</h3>
                  <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
                    Device-days in range with issues (see tabs 9–10 for lists).
                  </p>
                  <dl className="mt-3 space-y-2 text-sm text-zinc-700 dark:text-zinc-300">
                    <div className="flex justify-between gap-2">
                      <dt>Device-days w/ geofence gap</dt>
                      <dd className={`font-mono tabular-nums ${healthData.windowMetrics.deviceDaysWithGeofenceGap > 0 ? 'font-semibold text-amber-800 dark:text-amber-300' : ''}`}>
                        {healthData.windowMetrics.deviceDaysWithGeofenceGap.toLocaleString()}
                      </dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt>Device-days w/ Enter/Exit gap</dt>
                      <dd className={`font-mono tabular-nums ${healthData.windowMetrics.deviceDaysWithEnterExitGap > 0 ? 'font-semibold text-amber-800 dark:text-amber-300' : ''}`}>
                        {healthData.windowMetrics.deviceDaysWithEnterExitGap.toLocaleString()}
                      </dd>
                    </div>
                    <div className="text-xs text-zinc-500 dark:text-zinc-400">
                      Enter/Exit threshold: ≥ {healthData.window.minPointsEnterExit} points/day, zero ENTER/EXIT.
                    </div>
                  </dl>
                  <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs">
                    <Link href="/admin/data-checks?tab=geofence-gaps" className="text-blue-600 hover:underline dark:text-blue-400">
                      Tab 9 — Geofence gaps
                    </Link>
                    <Link href="/admin/data-checks?tab=geofence-enter-exit-gaps" className="text-blue-600 hover:underline dark:text-blue-400">
                      Tab 10 — Enter/Exit gaps
                    </Link>
                  </div>
                </div>

                <div className="rounded-lg border border-zinc-200 bg-zinc-50/80 p-4 dark:border-zinc-600 dark:bg-zinc-800/40 sm:col-span-2 xl:col-span-1">
                  <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">tbl_vworkjobs (steps)</h3>
                  <dl className="mt-3 space-y-2 text-sm text-zinc-700 dark:text-zinc-300">
                    <div className="flex justify-between gap-2">
                      <dt>Jobs with actual_start</dt>
                      <dd className="font-mono tabular-nums">{healthData.vworkjobs.jobsWithActualStart.toLocaleString()}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt>max(actual_start_time)</dt>
                      <dd className="font-mono text-xs">{healthData.vworkjobs.maxActualStartTime ?? '—'}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt>max start w/ step_*_actual_time</dt>
                      <dd className="font-mono text-xs">{healthData.vworkjobs.maxActualStartWithStepData ?? '—'}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt>Started but steps_fetched ≠ true</dt>
                      <dd className={`font-mono tabular-nums ${healthData.vworkjobs.jobsWithStartStepsNotFetched > 0 ? 'text-amber-700 dark:text-amber-400' : ''}`}>
                        {healthData.vworkjobs.jobsWithStartStepsNotFetched.toLocaleString()}
                      </dd>
                    </div>
                  </dl>
                  <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs">
                    <Link href="/admin/data-checks?tab=vwork-stale" className="text-blue-600 hover:underline dark:text-blue-400">
                      Tab 8 — VWork steps
                    </Link>
                    <Link href="/admin/tagging" className="text-blue-600 hover:underline dark:text-blue-400">
                      Tagging (run steps)
                    </Link>
                  </div>
                </div>
              </div>

              <div className="mt-6 rounded-md border border-blue-200 bg-blue-50/60 p-3 text-sm text-zinc-700 dark:border-blue-900 dark:bg-blue-950/30 dark:text-zinc-300">
                <strong className="text-zinc-900 dark:text-zinc-100">Pipeline reminder:</strong> import tracking →{' '}
                <code className="rounded bg-white/80 px-1 dark:bg-zinc-800">position_time_nz</code> + fence (
                <code className="rounded bg-white/80 px-1 dark:bg-zinc-800">geofence_attempted</code>) → ENTER/EXIT tagging (
                <code className="rounded bg-white/80 px-1 dark:bg-zinc-800">position_time_nz</code>) → derived steps on jobs. Temporal GPS gaps are on{' '}
                <Link href="/admin/data-checks?tab=gps-gaps" className="text-blue-600 hover:underline dark:text-blue-400">
                  Tab 1 — GPS Tracking Gaps
                </Link>
                ; per-job window coverage vs tagging:{' '}
                <Link href="/admin/data-checks?tab=jobs-gps" className="text-blue-600 hover:underline dark:text-blue-400">
                  Tab 13 — Jobs &amp; GPS window
                </Link>
                .
              </div>
            </>
          )}
        </section>
        )}

        {activeTab === 'gps-gaps' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
            GPS tracking gaps
          </h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Find breaks in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time</code> per device
            (ordered by device_name, position_time). Gap length uses consecutive points in that order. Date filter and report
            date use <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time::date</code>. From_NZ / To_NZ
            columns show the NZ timestamps on the same boundary rows for reference.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">
                Gap threshold (minutes)
              </span>
              <input
                type="number"
                min={0}
                step={1}
                value={minGapMinutes}
                onChange={(e) => setMinGapMinutes(e.target.value)}
                className="w-28 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                placeholder="60"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Date from (optional)</span>
              <input
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Date to (optional)</span>
              <input
                type="date"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={runGapsScan}
              disabled={loading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {loading ? 'Scanning…' : 'Run scan'}
            </button>
          </div>

          {error && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {error}
            </div>
          )}

          {result && (
            <div className="mt-6">
              <div className="flex flex-wrap items-end justify-between gap-4">
                <p className="text-sm text-zinc-600 dark:text-zinc-400">
                  Gaps &gt; {result.minGapMinutes} min
                  {result.dateFrom || result.dateTo
                    ? ` (${result.dateFrom ?? '…'} to ${result.dateTo ?? '…'})`
                    : ''}
                  . Summary: {result.summary.length} date/device row(s).
                </p>
                <div className="flex flex-wrap items-center gap-4">
                  <label className="flex flex-col gap-1">
                    <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Filter by device</span>
                    <input
                      type="text"
                      value={deviceFilter}
                      onChange={(e) => setDeviceFilter(e.target.value)}
                      placeholder="Device name..."
                      className="w-40 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                    />
                  </label>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Sort 1</span>
                    <select
                      value={sortBy1}
                      onChange={(e) => setSortBy1(e.target.value as SortKey)}
                      className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                    >
                      {SORT_KEYS.map((o) => (
                        <option key={o.value} value={o.value}>{o.label}</option>
                      ))}
                    </select>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Sort 2</span>
                    <select
                      value={sortBy2}
                      onChange={(e) => setSortBy2(e.target.value as SortKey)}
                      className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                    >
                      {SORT_KEYS.map((o) => (
                        <option key={o.value} value={o.value}>{o.label}</option>
                      ))}
                    </select>
                  </div>
                  <select
                    value={sortDir}
                    onChange={(e) => setSortDir(e.target.value as 'asc' | 'desc')}
                    className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  >
                    <option value="asc">Ascending</option>
                    <option value="desc">Descending</option>
                  </select>
                </div>
                <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-600 dark:text-zinc-400" title="List every gap above the threshold (up to 500 rows)">
                  <input
                    type="checkbox"
                    checked={showDetail}
                    onChange={(e) => setShowDetail(e.target.checked)}
                    className="rounded border-zinc-300 text-blue-600 dark:border-zinc-600"
                  />
                  Show all gaps &gt; {result.minGapMinutes} min (up to 500)
                </label>
              </div>

              {(() => {
                const filtered = deviceFilter.trim()
                  ? result.summary.filter((r) =>
                      r.device_name.toLowerCase().includes(deviceFilter.trim().toLowerCase())
                    )
                  : result.summary;
                const sorted = sortSummary(filtered, sortBy1, sortBy2, sortDir);
                const displaySummary = sorted;
                return (
                  <>
              <p className="mt-2 text-xs text-zinc-500 dark:text-zinc-400">
                Click a row to load tbl_tracking data in a popup (5 min before From to 90 min after To). Red rows in popup = outage boundaries.
              </p>
              <div className="mt-4 overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[900px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300" title="Date from position_time (query and grouping use position_time)">Date</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Device</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">
                        Max gap (min)
                      </th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">
                        # gaps &gt; threshold
                      </th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right" title="Total tbl_tracking rows for this device on this date (reconcile with GPS import)">
                        DayCount
                      </th>
                      <th className="min-w-[5rem] px-3 py-2 text-center font-medium text-zinc-700 dark:text-zinc-300" title="!! = device moved between outage start and end (lat/lon at From ≠ at To); distance shown when moved">
                        !!
                      </th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">From</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">To</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">From_NZ</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">To_NZ</th>
                    </tr>
                  </thead>
                  <tbody>
                    {displaySummary.length === 0 ? (
                      <tr>
                        <td colSpan={10} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          {result.summary.length === 0
                            ? 'No gaps above threshold in the selected range.'
                            : 'No rows match the device filter.'}
                        </td>
                      </tr>
                    ) : (
                      displaySummary.map((row, i) => (
                        <tr
                          key={`${row.report_date}-${row.device_name}-${i}`}
                          onClick={() => openTrackingPopup(row)}
                          className={`cursor-pointer border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-100 dark:hover:bg-zinc-800/70 ${row.gap_from != null && row.gap_to != null ? '' : 'opacity-70'}`}
                          title={row.gap_from != null && row.gap_to != null ? 'Click to view tbl_tracking data around this gap' : 'No position_time for this gap'}
                        >
                          <td className="px-3 py-2 font-mono text-zinc-900 dark:text-zinc-100">
                            {formatDateDDMMYY(row.report_date)}
                          </td>
                          <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.device_name}</td>
                          <td className="px-3 py-2 text-right font-mono text-zinc-800 dark:text-zinc-200">
                            {row.max_gap_minutes}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-zinc-800 dark:text-zinc-200">
                            {row.gap_count}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-zinc-800 dark:text-zinc-200" title="Total rows for this device on this date (reconcile with GPS import)">
                            {row.day_count}
                          </td>
                          <td className="px-3 py-2 text-center font-bold text-amber-600 dark:text-amber-400" title={deviceMovedDuringOutage(row) ? 'Device moved between outage start and end' : 'Same position at start and end of gap'}>
                            {deviceMovedDuringOutage(row) && row.gap_start_lat != null && row.gap_start_lon != null && row.gap_end_lat != null && row.gap_end_lon != null
                              ? `!! ${formatDistance(distanceMeters(row.gap_start_lat, row.gap_start_lon, row.gap_end_lat, row.gap_end_lon))}`
                              : deviceMovedDuringOutage(row)
                                ? '!!'
                                : '—'}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time last read before gap">
                            {row.gap_from != null ? formatGapDateTime(row.gap_from) : '—'}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time first read after gap">
                            {row.gap_to != null ? formatGapDateTime(row.gap_to) : '—'}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time_nz on row before gap (reference)">
                            {formatGapDateTime(row.gap_from_nz)}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-700 dark:text-zinc-300" title="position_time_nz on row after gap (reference)">
                            {formatGapDateTime(row.gap_to_nz)}
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
                  </>
                );
              })()}

              {popupRow && (
                <div
                  className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
                  role="dialog"
                  aria-modal="true"
                  aria-labelledby="popup-title"
                  onClick={(e) => e.target === e.currentTarget && setPopupRow(null)}
                >
                  <div
                    className="max-h-[90vh] w-full max-w-4xl overflow-hidden rounded-lg border border-zinc-200 bg-white shadow-xl dark:border-zinc-700 dark:bg-zinc-900"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="flex items-center justify-between border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
                      <h3 id="popup-title" className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
                        tbl_tracking: {popupRow.device_name} (5 min before From → 90 min after To)
                      </h3>
                      <button
                        type="button"
                        onClick={() => setPopupRow(null)}
                        className="rounded p-1 text-zinc-500 hover:bg-zinc-100 hover:text-zinc-700 dark:hover:bg-zinc-800 dark:hover:text-zinc-300"
                        aria-label="Close"
                      >
                        ✕
                      </button>
                    </div>
                    <div className="max-h-[70vh] overflow-auto p-4">
                      {popupLoading && (
                        <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading…</p>
                      )}
                      {popupError && (
                        <p className="text-sm text-red-600 dark:text-red-400">{popupError}</p>
                      )}
                      {!popupLoading && !popupError && popupRows.length === 0 && (
                        <p className="text-sm text-zinc-500 dark:text-zinc-400">No rows in this window.</p>
                      )}
                      {!popupLoading && !popupError && popupRows.length > 0 && (
                        <table className="w-full min-w-[640px] border-collapse text-left text-sm">
                          <thead>
                            <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">position_time</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">position_time_nz</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">fence_name</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">geofence_type</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300 text-right">lat</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300 text-right">lon</th>
                              <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300 text-center">Map</th>
                              <th className="min-w-[4rem] px-2 py-1.5 text-center font-medium text-zinc-700 dark:text-zinc-300" title="Distance from previous row (→ moved, ● same position)">Move</th>
                            </tr>
                          </thead>
                          <tbody>
                            {popupRows.map((r, i) => {
                              const pt = (r.position_time ?? '').trim();
                              const isFrom = pt && popupRow.gap_from != null && pt === popupRow.gap_from.trim();
                              const isTo = pt && popupRow.gap_to != null && pt === popupRow.gap_to.trim();
                              const isOutageBoundary = isFrom || isTo;
                              const prev = i > 0 ? popupRows[i - 1] : null;
                              const sameAsAbove = prev != null && r.lat != null && r.lon != null && prev.lat != null && prev.lon != null && prev.lat === r.lat && prev.lon === r.lon;
                              const moveSymbol = prev == null ? '—' : sameAsAbove ? '●' : '→';
                              const distM =
                                prev != null && r.lat != null && r.lon != null && prev.lat != null && prev.lon != null
                                  ? distanceMeters(prev.lat, prev.lon, r.lat, r.lon)
                                  : null;
                              const distStr = distM != null ? formatDistance(distM) : '';
                              const moveTitle =
                                prev == null
                                  ? 'First row'
                                  : sameAsAbove
                                    ? `Stationary (0 m from row above)`
                                    : `Moved ${distStr} from row above`;
                              return (
                              <tr
                                key={i}
                                className={`border-b border-zinc-100 dark:border-zinc-800 ${isOutageBoundary ? 'bg-red-200 dark:bg-red-950/80' : ''}`}
                                title={isFrom ? 'Last read before gap (outage start)' : isTo ? 'First read after gap (outage end)' : undefined}
                              >
                                <td className="px-2 py-1 font-mono text-xs text-zinc-800 dark:text-zinc-200">
                                  {r.position_time ?? '—'}
                                </td>
                                <td className="px-2 py-1 font-mono text-xs text-zinc-800 dark:text-zinc-200">
                                  {r.position_time_nz ?? '—'}
                                </td>
                                <td className="px-2 py-1 text-zinc-700 dark:text-zinc-300">{r.fence_name ?? '—'}</td>
                                <td className="px-2 py-1 text-zinc-700 dark:text-zinc-300">{r.geofence_type ?? '—'}</td>
                                <td className="px-2 py-1 text-right font-mono text-xs text-zinc-600 dark:text-zinc-400">
                                  {r.lat != null ? r.lat : '—'}
                                </td>
                                <td className="px-2 py-1 text-right font-mono text-xs text-zinc-600 dark:text-zinc-400">
                                  {r.lon != null ? r.lon : '—'}
                                </td>
                                <td className="px-2 py-1 text-center">
                                  {r.lat != null && r.lon != null ? (
                                    <a
                                      href={`https://www.google.com/maps?q=${r.lat},${r.lon}`}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-blue-600 dark:text-blue-400 hover:underline"
                                      title={`Open ${r.lat}, ${r.lon} in Google Maps`}
                                    >
                                      Map
                                    </a>
                                  ) : (
                                    '—'
                                  )}
                                </td>
                                <td className="px-2 py-1 text-center text-sm font-mono text-xs" title={moveTitle}>
                                  {prev == null ? (
                                    '—'
                                  ) : (
                                    <>
                                      <span className={sameAsAbove ? 'text-zinc-400 dark:text-zinc-500' : 'text-emerald-600 dark:text-emerald-400'}>{moveSymbol}</span>
                                      {' '}
                                      <span className="text-zinc-600 dark:text-zinc-400">{distStr}</span>
                                    </>
                                  )}
                                </td>
                              </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      )}
                    </div>
                  </div>
                </div>
              )}

              {showDetail && result.detail.length > 0 && (
                <div className="mt-6">
                  <h3 className="text-sm font-medium text-zinc-700 dark:text-zinc-300">All gaps &gt; {result.minGapMinutes} min ({result.detail.length} shown)</h3>
                  <div className="mt-2 max-h-[400px] overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
                    <table className="w-full min-w-[520px] border-collapse text-left text-sm">
                      <thead className="sticky top-0 bg-zinc-100/95 dark:bg-zinc-800/95">
                        <tr className="border-b border-zinc-200 dark:border-zinc-700">
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Device</th>
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Gap start</th>
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Gap end</th>
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">
                            Gap (min)
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {result.detail.map((row, i) => (
                          <tr
                            key={`${row.device_name}-${row.gap_start}-${i}`}
                            className="border-b border-zinc-100 dark:border-zinc-800"
                          >
                            <td className="px-3 py-1.5 text-zinc-900 dark:text-zinc-100">{row.device_name}</td>
                            <td className="px-3 py-1.5 font-mono text-xs text-zinc-700 dark:text-zinc-300">
                              {formatGapDateTime(row.gap_start)}
                            </td>
                            <td className="px-3 py-1.5 font-mono text-xs text-zinc-700 dark:text-zinc-300">
                              {formatGapDateTime(row.gap_end)}
                            </td>
                            <td className="px-3 py-1.5 text-right font-mono text-zinc-800 dark:text-zinc-200">
                              {row.gap_minutes}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}
        </section>
        )}

        {activeTab === 'jobs-gps' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">13. Jobs &amp; GPS window</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            For each job, uses the same GPS window as <strong className="font-medium text-zinc-800 dark:text-zinc-200">Query → Inspect</strong>{' '}
            (<code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">buildInspectGpsWindowForJob</code>):{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time_nz</code> strictly after GPS From, and strictly before GPS To when{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">actual_end_time</code> is set; if not, the upper bound is open (same as Inspect). Reports min/max NZ times in that
            window, largest internal gap in minutes, and whether any gap is ≥ the threshold (default 10 min). Gaps between two pings at the same lat/lon (stationary) do not count. Rows with{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time</code> but null{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time_nz</code> in the window are flagged as critical. Step columns show{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">step_*_via</code> only (GPS, VW, etc.).
          </p>

          <div className="mt-4 flex gap-1 border-b border-zinc-200 dark:border-zinc-700">
            <button
              type="button"
              onClick={() => setJobsGpsSubTab('report')}
              className={`rounded-t px-4 py-2 text-sm font-medium ${
                jobsGpsSubTab === 'report'
                  ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100'
                  : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              Report
            </button>
            <button
              type="button"
              onClick={() => setJobsGpsSubTab('days-devices')}
              className={`rounded-t px-4 py-2 text-sm font-medium ${
                jobsGpsSubTab === 'days-devices'
                  ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100'
                  : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              Day / Worker summary
            </button>
            <button
              type="button"
              onClick={() => setJobsGpsSubTab('gap-probability')}
              className={`rounded-t px-4 py-2 text-sm font-medium ${
                jobsGpsSubTab === 'gap-probability'
                  ? 'border border-b-0 border-zinc-200 bg-white text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100'
                  : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
              }`}
            >
              Gap Probability
            </button>
          </div>

          {jobsGpsSubTab === 'report' && (
            <>
          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Date from (actual_start)</span>
              <input
                type="date"
                value={jobsGpsDateFrom}
                onChange={(e) => setJobsGpsDateFrom(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Date to (actual_start)</span>
              <input
                type="date"
                value={jobsGpsDateTo}
                onChange={(e) => setJobsGpsDateTo(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Customer (exact)</span>
              <input
                type="text"
                value={jobsGpsCustomer}
                onChange={(e) => setJobsGpsCustomer(e.target.value)}
                className="w-40 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Template (exact)</span>
              <input
                type="text"
                value={jobsGpsTemplate}
                onChange={(e) => setJobsGpsTemplate(e.target.value)}
                className="w-36 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Winery (exact)</span>
              <input
                type="text"
                value={jobsGpsWinery}
                onChange={(e) => setJobsGpsWinery(e.target.value)}
                className="w-40 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Vineyard (exact)</span>
              <input
                type="text"
                value={jobsGpsVineyard}
                onChange={(e) => setJobsGpsVineyard(e.target.value)}
                className="w-44 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Start −min</span>
              <input
                type="number"
                min={0}
                value={jobsGpsStartLess}
                onChange={(e) => setJobsGpsStartLess(e.target.value)}
                className="w-20 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">End +min</span>
              <input
                type="number"
                min={0}
                value={jobsGpsEndPlus}
                onChange={(e) => setJobsGpsEndPlus(e.target.value)}
                className="w-20 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Gap threshold (min)</span>
              <input
                type="number"
                min={1}
                value={jobsGpsGapThreshold}
                onChange={(e) => setJobsGpsGapThreshold(e.target.value)}
                className="w-24 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={() => void runJobsGpsReport()}
              disabled={jobsGpsLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {jobsGpsLoading ? 'Running…' : 'Run report'}
            </button>
          </div>
          <p className="mt-2 text-xs text-zinc-500 dark:text-zinc-500">
            Dates, optional filters, window minutes, column filters, and the gaps-popup breach filter are stored in{' '}
            <code className="rounded bg-zinc-100 px-1 text-[11px] dark:bg-zinc-800">tbl_settings</code> (
            <span className="font-mono">{JOBS_GPS_TAB_SETTINGS_TYPE}</span> /{' '}
            <span className="font-mono">{JOBS_GPS_TAB_SETTINGS_NAME}</span>) and loaded once when you open this tab.
          </p>

          <label className="mt-4 flex cursor-pointer items-center gap-2 text-sm text-zinc-600 dark:text-zinc-400">
            <input
              type="checkbox"
              checked={jobsGpsIssuesOnly}
              onChange={(e) => setJobsGpsIssuesOnly(e.target.checked)}
              className="rounded border-zinc-300 text-blue-600 dark:border-zinc-600"
            />
            Show only rows with issues (GPS From edge, GPS To edge, or internal gap vs threshold; incomplete window or
            critical NZ null)
          </label>

          {jobsGpsError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {jobsGpsError}
            </div>
          )}

          {jobsGpsResult && (() => {
            const jg = jobsGpsResult;
            const thrWin = jg.window.gapThresholdMinutes;
            const pageStart = jobsGpsPage * JOBS_GPS_PAGE_SIZE;
            const pageChunk = jg.rows.slice(pageStart, pageStart + JOBS_GPS_PAGE_SIZE);
            const baseRows = jobsGpsIssuesOnly
              ? pageChunk.filter((r) => jobsGpsRowBreachesLimits(r, thrWin))
              : pageChunk;
            const tableRows = baseRows.filter(
              (r) =>
                jobsGpsMatchesEdgeFromFilter(r, thrWin, jobsGpsFilterFrom) &&
                jobsGpsMatchesEdgeToFilter(r, thrWin, jobsGpsFilterTo) &&
                jobsGpsMatchesFlagsFilter(r, jobsGpsFilterFlags, thrWin)
            );
            const colFiltersActive =
              jobsGpsFilterFrom !== 'all' ||
              jobsGpsFilterTo !== 'all' ||
              jobsGpsFilterFlags !== 'all';
            const canPrev = jobsGpsPage > 0;
            const canNext = pageStart + JOBS_GPS_PAGE_SIZE < jg.total;
            const totalPages = Math.max(1, Math.ceil(jg.total / JOBS_GPS_PAGE_SIZE));
            const paginationBar = (
              <div className="flex flex-wrap items-center justify-between gap-3 border-zinc-200 dark:border-zinc-700">
                <p className="text-sm text-zinc-600 dark:text-zinc-400">
                  Page <span className="font-mono tabular-nums">{jobsGpsPage + 1}</span> of{' '}
                  <span className="font-mono tabular-nums">{totalPages}</span>
                  <span className="text-zinc-500 dark:text-zinc-500"> ({JOBS_GPS_PAGE_SIZE} jobs per page)</span>
                </p>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    disabled={!canPrev || jobsGpsLoading}
                    onClick={() => setJobsGpsPage((p) => Math.max(0, p - 1))}
                    className="rounded border border-zinc-300 bg-white px-3 py-1.5 text-sm font-medium text-zinc-800 hover:bg-zinc-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700"
                  >
                    Previous
                  </button>
                  <button
                    type="button"
                    disabled={!canNext || jobsGpsLoading}
                    onClick={() => setJobsGpsPage((p) => p + 1)}
                    className="rounded border border-zinc-300 bg-white px-3 py-1.5 text-sm font-medium text-zinc-800 hover:bg-zinc-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700"
                  >
                    Next
                  </button>
                </div>
              </div>
            );
            return (
            <div className="mt-6">
              <p className="text-sm text-zinc-600 dark:text-zinc-400">
                Jobs matching filters:{' '}
                <span className="font-mono tabular-nums">{jg.totalMatchingJobs.toLocaleString()}</span>
                {jg.truncated && jg.totalMatchingJobs > jg.total ? (
                  <>
                    {' '}
                    <span className="font-medium text-amber-800 dark:text-amber-300">
                      (loaded {jg.total.toLocaleString()} — scan cap {jg.maxJobsScan.toLocaleString()})
                    </span>
                  </>
                ) : null}
                . Jobs with at least one issue:{' '}
                <span className="font-mono tabular-nums font-medium text-zinc-900 dark:text-zinc-100">
                  {jg.breachJobCount.toLocaleString()}
                </span>{' '}
                <span className="text-zinc-500 dark:text-zinc-500">
                  (GPS From edge, GPS To edge, or internal gap ≥ threshold; or incomplete / critical NZ null)
                </span>
                . Window (Inspect): start −{jg.window.startLessMinutes + (jg.window.displayExpandBefore ?? 0)} min / end +
                {jg.window.endPlusMinutes + (jg.window.displayExpandAfter ?? 0)} min, gap threshold {jg.window.gapThresholdMinutes}{' '}
                min.
                {colFiltersActive && (
                  <>
                    {' '}
                    <span className="text-zinc-700 dark:text-zinc-300">
                      This page after column filters:{' '}
                      <span className="font-mono tabular-nums">{tableRows.length}</span> of{' '}
                      <span className="font-mono tabular-nums">{baseRows.length}</span> rows.
                    </span>
                  </>
                )}
              </p>
              <div className="mt-3 border-b pb-3 dark:border-zinc-700">{paginationBar}</div>
              <div className="mt-4 overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[1200px] border-collapse text-left text-xs">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">Job</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">Worker</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">Customer</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">Winery</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">Vineyard</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">actual_start</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">GPS From</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">GPS To</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">Min GPS (NZ)</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">Max GPS (NZ)</th>
                      <th className="px-2 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Max gap (min)</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">Flags</th>
                      <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">S1–S5 via</th>
                    </tr>
                    <tr className="border-b border-zinc-200 bg-zinc-50/90 dark:border-zinc-700 dark:bg-zinc-900/50">
                      <th
                        colSpan={6}
                        className="px-2 py-1.5 align-bottom text-left text-[10px] font-normal text-zinc-500 dark:text-zinc-500"
                      >
                        {colFiltersActive ? (
                          <button
                            type="button"
                            onClick={() => {
                              setJobsGpsFilterFrom('all');
                              setJobsGpsFilterTo('all');
                              setJobsGpsFilterFlags('all');
                            }}
                            className="text-blue-600 underline decoration-dotted underline-offset-2 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
                          >
                            Clear column filters
                          </button>
                        ) : (
                          <span>Column filters</span>
                        )}
                      </th>
                      <th className="px-1 py-1.5 align-bottom">
                        <label className="sr-only" htmlFor="jobs-gps-filter-from">
                          Filter GPS From (x)
                        </label>
                        <select
                          id="jobs-gps-filter-from"
                          value={jobsGpsFilterFrom}
                          onChange={(e) =>
                            setJobsGpsFilterFrom(e.target.value as JobsGpsEdgeColumnFilter)
                          }
                          className="w-full max-w-[9.5rem] rounded border border-zinc-300 bg-white px-1 py-1 text-[10px] text-zinc-800 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                          title="(x) edge vs threshold"
                        >
                          <option value="all">From: all</option>
                          <option value="breach">From: breach</option>
                          <option value="ok">From: OK</option>
                          <option value="na">From: n/a</option>
                        </select>
                      </th>
                      <th className="px-1 py-1.5 align-bottom">
                        <label className="sr-only" htmlFor="jobs-gps-filter-to">
                          Filter GPS To (y)
                        </label>
                        <select
                          id="jobs-gps-filter-to"
                          value={jobsGpsFilterTo}
                          onChange={(e) =>
                            setJobsGpsFilterTo(e.target.value as JobsGpsEdgeColumnFilter)
                          }
                          className="w-full max-w-[9.5rem] rounded border border-zinc-300 bg-white px-1 py-1 text-[10px] text-zinc-800 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                          title="(y) edge vs threshold"
                        >
                          <option value="all">To: all</option>
                          <option value="breach">To: breach</option>
                          <option value="ok">To: OK</option>
                          <option value="na">To: n/a</option>
                        </select>
                      </th>
                      <th colSpan={3} className="px-2 py-1.5" aria-hidden />
                      <th className="px-1 py-1.5 align-bottom">
                        <label className="sr-only" htmlFor="jobs-gps-filter-flags">
                          Filter Flags
                        </label>
                        <select
                          id="jobs-gps-filter-flags"
                          value={jobsGpsFilterFlags}
                          onChange={(e) =>
                            setJobsGpsFilterFlags(e.target.value as JobsGpsFlagsColumnFilter)
                          }
                          className="w-full max-w-[11rem] rounded border border-zinc-300 bg-white px-1 py-1 text-[10px] text-zinc-800 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                        >
                          <option value="all">Flags: all</option>
                          <option value="ok">Flags: OK</option>
                          <option value="any_issue">Flags: any issue</option>
                          <option value="incomplete">Flags: incomplete</option>
                          <option value="critical_nz">Flags: critical NZ null</option>
                          <option value="gap">Flags: gap ≥ threshold</option>
                        </select>
                      </th>
                      <th className="px-2 py-1.5" aria-hidden />
                    </tr>
                  </thead>
                  <tbody>
                    {tableRows.length === 0 ? (
                      <tr>
                        <td
                          colSpan={13}
                          className="px-3 py-6 text-center text-sm text-zinc-600 dark:text-zinc-400"
                        >
                          {baseRows.length === 0
                            ? 'No rows on this page.'
                            : `No rows match the column filters (${baseRows.length} row${
                                baseRows.length !== 1 ? 's' : ''
                              } on this page hidden).`}
                        </td>
                      </tr>
                    ) : (
                      tableRows.map((r) => {
                      const xEdge = r.minutes_after_gps_from_to_first_fix;
                      const yEdge = r.minutes_from_last_fix_to_gps_to;
                      const gpsFromBreach =
                        r.gps_from != null &&
                        (xEdge != null ? xEdge > thrWin : r.min_gps_nz == null);
                      const gpsToBreach =
                        r.gps_to != null &&
                        (yEdge != null ? yEdge > thrWin : r.max_gps_nz == null);
                      const hasIssue = jobsGpsRowBreachesLimits(r, thrWin);

                      const flags: string[] = [];
                      if (r.incomplete) flags.push(`incomplete: ${r.incomplete_reason ?? '—'}`);
                      if (r.critical_null_nz) flags.push(`critical NZ null ×${r.critical_null_nz_count}`);
                      if (r.gap_missing) flags.push(`gap ≥ ${jg.window.gapThresholdMinutes}m`);
                      if (gpsFromBreach) flags.push(`From edge >${thrWin}m`);
                      if (gpsToBreach) flags.push(`To edge >${thrWin}m`);
                      if (flags.length === 0) flags.push('OK');

                      const rowBg = r.critical_null_nz
                        ? 'bg-red-50/90 dark:bg-red-950/35'
                        : r.incomplete
                          ? 'bg-amber-50/80 dark:bg-amber-950/25'
                          : hasIssue
                            ? 'bg-red-50/90 dark:bg-red-950/35'
                            : 'bg-emerald-50/85 dark:bg-emerald-950/28';

                      const minGpsCellBg =
                        r.gps_from == null
                          ? ''
                          : xEdge != null
                            ? xEdge > thrWin
                              ? 'bg-red-100/90 dark:bg-red-950/40'
                              : 'bg-emerald-100/80 dark:bg-emerald-950/30'
                            : r.min_gps_nz == null
                              ? 'bg-red-100/90 dark:bg-red-950/40'
                              : '';

                      const maxGpsCellBg =
                        r.gps_to == null
                          ? ''
                          : yEdge != null
                            ? yEdge > thrWin
                              ? 'bg-red-100/90 dark:bg-red-950/40'
                              : 'bg-emerald-100/80 dark:bg-emerald-950/30'
                            : r.max_gps_nz == null
                              ? 'bg-red-100/90 dark:bg-red-950/40'
                              : '';

                      const gpsFromCellBg = gpsFromBreach
                        ? 'bg-red-100/90 dark:bg-red-950/40'
                        : '';
                      const gpsToCellBg = gpsToBreach
                        ? 'bg-red-100/90 dark:bg-red-950/40'
                        : '';
                      return (
                        <tr
                          key={r.job_id}
                          className={`border-b border-zinc-100 dark:border-zinc-800 ${rowBg}`}
                        >
                          <td className="px-2 py-1.5 font-mono text-zinc-900 dark:text-zinc-100">{r.job_id}</td>
                          <td className="px-2 py-1.5 text-zinc-800 dark:text-zinc-200">{r.worker ?? '—'}</td>
                          <td className="px-2 py-1.5 text-zinc-800 dark:text-zinc-200">{r.customer ?? '—'}</td>
                          <td className="px-2 py-1.5 text-zinc-800 dark:text-zinc-200">{r.delivery_winery ?? '—'}</td>
                          <td className="px-2 py-1.5 text-zinc-800 dark:text-zinc-200">{r.vineyard_name ?? '—'}</td>
                          <td className="px-2 py-1.5 font-mono text-zinc-700 dark:text-zinc-300">{r.actual_start_time ?? '—'}</td>
                          <td
                            className={`px-2 py-1.5 font-mono text-zinc-700 dark:text-zinc-300 ${gpsFromCellBg}`}
                          >
                            {r.gps_from ?? '—'}
                            {r.minutes_after_gps_from_to_first_fix != null
                              ? ` (${r.minutes_after_gps_from_to_first_fix})`
                              : r.gps_from != null && r.min_gps_nz == null
                                ? ' (—)'
                                : ''}
                          </td>
                          <td
                            className={`px-2 py-1.5 font-mono text-zinc-700 dark:text-zinc-300 ${gpsToCellBg}`}
                          >
                            {r.gps_to ?? '—'}
                            {r.minutes_from_last_fix_to_gps_to != null
                              ? ` (${r.minutes_from_last_fix_to_gps_to})`
                              : r.gps_to != null && r.max_gps_nz == null
                                ? ' (—)'
                                : ''}
                          </td>
                          <td
                            className={`px-2 py-1.5 font-mono text-zinc-700 dark:text-zinc-300 ${minGpsCellBg}`}
                          >
                            {r.min_gps_nz ?? '—'}
                          </td>
                          <td
                            className={`px-2 py-1.5 font-mono text-zinc-700 dark:text-zinc-300 ${maxGpsCellBg}`}
                          >
                            {r.max_gps_nz ?? '—'}
                          </td>
                          <td className="px-2 py-1.5 text-right font-mono tabular-nums text-zinc-800 dark:text-zinc-200">
                            {r.max_gap_minutes != null ? r.max_gap_minutes : '—'}
                          </td>
                          <td
                            className="max-w-[14rem] cursor-pointer px-2 py-1.5 text-zinc-700 underline decoration-zinc-400 decoration-dotted underline-offset-2 hover:text-zinc-900 dark:text-zinc-300 dark:hover:text-zinc-100"
                            title="Show GPS gaps (last / next fix and minutes)"
                            onClick={(e) => {
                              e.stopPropagation();
                              void openJobsGpsGapDetail(r.job_id);
                            }}
                          >
                            {flags.join(' · ')}
                          </td>
                          <td className="max-w-[18rem] px-2 py-1.5 font-mono text-[10px] text-zinc-700 dark:text-zinc-300">
                            {r.step1_via} / {r.step2_via} / {r.step3_via} / {r.step4_via} / {r.step5_via}
                          </td>
                        </tr>
                      );
                    })
                    )}
                  </tbody>
                </table>
              </div>
              <div className="mt-3 border-t pt-3 dark:border-zinc-700">{paginationBar}</div>
            </div>
            );
          })()}
            </>
          )}

          {jobsGpsSubTab === 'days-devices' && (
            <div className="mt-6 space-y-4">
              <p className="text-sm text-zinc-600 dark:text-zinc-400">
                Built in the <strong className="font-medium text-zinc-800 dark:text-zinc-200">same run</strong> as the Report
                tab (no second scan). Each row is a calendar <strong className="font-medium text-zinc-800 dark:text-zinc-200">day</strong>{' '}
                (from <code className="rounded bg-zinc-100 px-1 text-xs dark:bg-zinc-800">actual_start_time</code>) and{' '}
                <strong className="font-medium text-zinc-800 dark:text-zinc-200">worker</strong> from{' '}
                <code className="rounded bg-zinc-100 px-1 text-xs dark:bg-zinc-800">tbl_vworkjobs.worker</code>.{' '}
                <strong className="font-medium text-zinc-800 dark:text-zinc-200">Total jobs</strong> counts every job in that
                pair; <strong className="font-medium text-zinc-800 dark:text-zinc-200">bad jobs</strong> counts jobs that breach
                the same limits as the main report (edges vs threshold, gap ≥ threshold, critical NZ null, incomplete window).
                Only pairs that have at least one bad job are listed.
              </p>
              <div className="flex flex-wrap items-center gap-3">
                <button
                  type="button"
                  onClick={() => void runJobsGpsReport()}
                  disabled={jobsGpsLoading}
                  className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
                >
                  {jobsGpsLoading ? 'Scanning…' : 'Run report again'}
                </button>
              </div>
              {!jobsGpsResult && (
                <p className="text-sm text-zinc-600 dark:text-zinc-400">
                  Run the <strong className="font-medium text-zinc-800 dark:text-zinc-200">Report</strong> tab to load jobs —
                  the summary appears here automatically.
                </p>
              )}
              {jobsGpsResult && (
                <div className="space-y-3">
                  <p className="text-sm text-zinc-700 dark:text-zinc-300">
                    <span className="font-mono tabular-nums">
                      {jobsGpsResult.totalJobsScanned.toLocaleString()}
                    </span>{' '}
                    jobs (
                    <span className="font-mono tabular-nums">
                      {jobsGpsResult.breachJobCount.toLocaleString()}
                    </span>{' '}
                    with issues,{' '}
                    <span className="font-mono tabular-nums">
                      {jobsGpsResult.jobsWithoutIssues.toLocaleString()}
                    </span>{' '}
                    OK).{' '}
                    <span className="font-mono tabular-nums">
                      {jobsGpsResult.totalGapSegmentRows.toLocaleString()}
                    </span>{' '}
                    gap rows total (popup segment rows summed over all scanned jobs).{' '}
                    <span className="font-mono tabular-nums">
                      {jobsGpsResult.uniqueDayWorkerWithIssues.toLocaleString()}
                    </span>{' '}
                    unique day/worker pairs with ≥1 bad job.
                  </p>
                  {jobsGpsResult.truncated && (
                    <p className="text-sm font-medium text-amber-800 dark:text-amber-300">
                      Scan stopped at {jobsGpsResult.maxJobsScan.toLocaleString()} jobs — narrow the date range or filters and
                      run again.
                    </p>
                  )}
                  <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                    <table className="w-full min-w-[560px] border-collapse text-left text-sm">
                      <caption className="caption-top border-b border-zinc-200 px-3 py-2 text-left text-sm text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                        Total jobs in this scan:{' '}
                        <span className="font-mono tabular-nums font-medium text-zinc-900 dark:text-zinc-100">
                          {jobsGpsResult.totalJobsScanned.toLocaleString()}
                        </span>
                        {' '}
                        <span className="text-zinc-500 dark:text-zinc-500">
                          (listed rows: day/worker pairs with at least one bad job)
                        </span>
                      </caption>
                      <thead>
                        <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Day (actual_start)</th>
                          <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Worker</th>
                          <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Total jobs</th>
                          <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Bad jobs</th>
                        </tr>
                      </thead>
                      <tbody>
                        {jobsGpsResult.dayWorkerSummary.length === 0 ? (
                          <tr>
                            <td colSpan={4} className="px-3 py-4 text-zinc-500 dark:text-zinc-400">
                              No day/worker pairs with bad jobs — every job passed the limits (or none matched filters).
                            </td>
                          </tr>
                        ) : (
                          jobsGpsResult.dayWorkerSummary.map((row) => (
                            <tr
                              key={`${row.day}-${row.worker}`}
                              className="border-b border-zinc-100 dark:border-zinc-800"
                            >
                              <td className="px-3 py-2 font-mono text-zinc-900 dark:text-zinc-100">{row.day}</td>
                              <td className="px-3 py-2 font-mono text-zinc-800 dark:text-zinc-200">{row.worker}</td>
                              <td className="px-3 py-2 text-right font-mono tabular-nums text-zinc-800 dark:text-zinc-200">
                                {row.total_jobs}
                              </td>
                              <td className="px-3 py-2 text-right font-mono tabular-nums text-zinc-800 dark:text-zinc-200">
                                {row.bad_jobs}
                              </td>
                            </tr>
                          ))
                        )}
                      </tbody>
                      {jobsGpsResult.dayWorkerSummary.length > 0 && (
                        <tfoot>
                          <tr className="border-t-2 border-zinc-200 bg-zinc-50/90 dark:border-zinc-600 dark:bg-zinc-800/60">
                            <td
                              colSpan={2}
                              className="px-3 py-2 text-sm font-medium text-zinc-800 dark:text-zinc-200"
                            >
                              Sums (rows above)
                            </td>
                            <td className="px-3 py-2 text-right font-mono text-sm font-medium tabular-nums text-zinc-900 dark:text-zinc-100">
                              {jobsGpsResult.dayWorkerSummary.reduce((s, r) => s + r.total_jobs, 0).toLocaleString()}
                            </td>
                            <td className="px-3 py-2 text-right font-mono text-sm font-medium tabular-nums text-zinc-900 dark:text-zinc-100">
                              {jobsGpsResult.dayWorkerSummary.reduce((s, r) => s + r.bad_jobs, 0).toLocaleString()}
                            </td>
                          </tr>
                        </tfoot>
                      )}
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}

          {jobsGpsSubTab === 'gap-probability' && (
            <div className="mt-6 space-y-4">
              <p className="text-sm text-zinc-600 dark:text-zinc-400">
                One row per <strong className="font-medium text-zinc-800 dark:text-zinc-200">calendar day</strong> (from job{' '}
                <code className="rounded bg-zinc-100 px-1 text-xs dark:bg-zinc-800">actual_start_time</code>).{' '}
                <strong className="font-medium text-zinc-800 dark:text-zinc-200">Gap from</strong> /{' '}
                <strong className="font-medium text-zinc-800 dark:text-zinc-200">Gap to</strong> are the earliest and latest
                NZ timestamps bounding any <strong className="font-medium text-zinc-800 dark:text-zinc-200">issue</strong> gap
                (≥ threshold) across all jobs scheduled that day — a coarse reimport window.{' '}
                <strong className="font-medium text-zinc-800 dark:text-zinc-200">Issue gap segments</strong> is how many
                gap holes contributed; <strong className="font-medium text-zinc-800 dark:text-zinc-200">Jobs</strong> is how
                many distinct jobs had at least one issue gap that day.
              </p>
              <div className="flex flex-wrap items-center gap-3">
                <button
                  type="button"
                  onClick={() => void runJobsGpsReport()}
                  disabled={jobsGpsLoading}
                  className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
                >
                  {jobsGpsLoading ? 'Scanning…' : 'Run report again'}
                </button>
              </div>
              {!jobsGpsResult && (
                <p className="text-sm text-zinc-600 dark:text-zinc-400">
                  Run the <strong className="font-medium text-zinc-800 dark:text-zinc-200">Report</strong> tab to load data.
                </p>
              )}
              {jobsGpsResult && (
                <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                  <table className="w-full min-w-[720px] border-collapse text-left text-sm">
                    <thead>
                      <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                        <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Day (actual_start)</th>
                        <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Gap from (NZ)</th>
                        <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Gap to (NZ)</th>
                        <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">
                          Issue gap segments
                        </th>
                        <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Jobs</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(jobsGpsResult.gapProbabilityByDay ?? []).length === 0 ? (
                        <tr>
                          <td colSpan={5} className="px-3 py-4 text-zinc-500 dark:text-zinc-400">
                            No issue gaps in this scan — every job had only sub-threshold holes (or no holes) in the Inspect
                            window.
                          </td>
                        </tr>
                      ) : (
                        (jobsGpsResult.gapProbabilityByDay ?? []).map((row) => (
                          <tr
                            key={row.day}
                            className="border-b border-zinc-100 dark:border-zinc-800"
                          >
                            <td className="px-3 py-2 font-mono text-zinc-900 dark:text-zinc-100">{row.day}</td>
                            <td className="px-3 py-2 font-mono text-xs text-zinc-800 dark:text-zinc-200">
                              {row.gap_from_nz ?? '—'}
                            </td>
                            <td className="px-3 py-2 font-mono text-xs text-zinc-800 dark:text-zinc-200">
                              {row.gap_to_nz ?? '—'}
                            </td>
                            <td className="px-3 py-2 text-right font-mono tabular-nums text-zinc-800 dark:text-zinc-200">
                              {row.issue_gap_segments}
                            </td>
                            <td className="px-3 py-2 text-right font-mono tabular-nums text-zinc-800 dark:text-zinc-200">
                              {row.jobs_with_issue_gaps}
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                    {(jobsGpsResult.gapProbabilityByDay ?? []).length > 0 && (
                      <tfoot>
                        <tr className="border-t-2 border-zinc-200 bg-zinc-50/90 dark:border-zinc-600 dark:bg-zinc-800/60">
                          <td
                            colSpan={3}
                            className="px-3 py-2 text-sm font-medium text-zinc-800 dark:text-zinc-200"
                          >
                            Sums (segments; job counts are per-day, not deduped across days)
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-sm font-medium tabular-nums text-zinc-900 dark:text-zinc-100">
                            {(jobsGpsResult.gapProbabilityByDay ?? [])
                              .reduce((s, r) => s + r.issue_gap_segments, 0)
                              .toLocaleString()}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-sm font-medium tabular-nums text-zinc-900 dark:text-zinc-100">
                            {(jobsGpsResult.gapProbabilityByDay ?? [])
                              .reduce((s, r) => s + r.jobs_with_issue_gaps, 0)
                              .toLocaleString()}
                          </td>
                        </tr>
                      </tfoot>
                    )}
                  </table>
                </div>
              )}
            </div>
          )}

          {jobsGpsGapModalOpen && (
            <div
              className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
              role="dialog"
              aria-modal="true"
              aria-labelledby="jobs-gps-gap-title"
              onClick={(e) => e.target === e.currentTarget && setJobsGpsGapModalOpen(false)}
            >
              <div
                className="max-h-[90vh] w-full max-w-6xl overflow-hidden rounded-lg border border-zinc-200 bg-white shadow-xl dark:border-zinc-700 dark:bg-zinc-900"
                onClick={(e) => e.stopPropagation()}
              >
                <div className="flex items-center justify-between border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
                  <h3 id="jobs-gps-gap-title" className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
                    GPS gaps — job {jobsGpsGapDetailData?.job_id || jobsGpsGapModalJobId || '…'}
                  </h3>
                  <button
                    type="button"
                    onClick={() => setJobsGpsGapModalOpen(false)}
                    className="rounded p-1 text-zinc-500 hover:bg-zinc-100 hover:text-zinc-700 dark:hover:bg-zinc-800 dark:hover:text-zinc-300"
                    aria-label="Close"
                  >
                    ✕
                  </button>
                </div>
                <div className="max-h-[75vh] overflow-auto p-4">
                  {jobsGpsGapDetailLoading && (
                    <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading gaps…</p>
                  )}
                  {jobsGpsGapDetailError && (
                    <p className="text-sm text-red-600 dark:text-red-400">{jobsGpsGapDetailError}</p>
                  )}
                  {!jobsGpsGapDetailLoading && !jobsGpsGapDetailError && jobsGpsGapDetailData && (
                    <>
                      <p className="mb-3 text-xs text-zinc-600 dark:text-zinc-400">
                        Same Inspect window as the report. Threshold:{' '}
                        <span className="font-mono">{jobsGpsGapDetailData.gap_threshold_minutes}</span> min. Red = gap ≥
                        threshold; green = below threshold.
                        {jobsGpsGapDetailData.truncated_sample && (
                          <span className="ml-2 font-medium text-amber-700 dark:text-amber-400">
                            Open-ended window: gap segments use an ordered sample of points; the main report still uses full
                            min/max/count for coverage.
                          </span>
                        )}
                      </p>
                      <div className="mb-3 flex flex-wrap items-center gap-3">
                        <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-800 dark:text-zinc-200">
                          <input
                            type="checkbox"
                            checked={jobsGpsGapsModalBreachedOnly}
                            onChange={(e) => setJobsGpsGapsModalBreachedOnly(e.target.checked)}
                            className="rounded border-zinc-300 text-blue-600 dark:border-zinc-600"
                          />
                          Show only gaps ≥ threshold (breaches)
                        </label>
                        {!jobsGpsGapsModalBreachedOnly &&
                          jobsGpsGapDetailData.gaps.filter((g) => g.is_issue).length > 1 && (
                            <button
                              type="button"
                              onClick={() => {
                                const d = jobsGpsGapDetailData;
                                const gapsView = d.gaps;
                                const breachIdxs = gapsView
                                  .map((g, i) => (g.is_issue ? i : -1))
                                  .filter((i): i is number => i >= 0);
                                if (breachIdxs.length <= 1) return;
                                setJobsGpsGapScrollIdx((prev) => {
                                  const nextHigher = breachIdxs.find((i) => i > prev);
                                  const next =
                                    nextHigher !== undefined ? nextHigher : breachIdxs[0]!;
                                  requestAnimationFrame(() => {
                                    document
                                      .getElementById(`jobs-gps-gap-row-${next}`)
                                      ?.scrollIntoView({ behavior: 'smooth', block: 'center' });
                                  });
                                  return next;
                                });
                              }}
                              className="rounded border border-zinc-300 bg-white px-3 py-1.5 text-sm font-medium text-zinc-800 hover:bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700"
                            >
                              Next breach row
                            </button>
                          )}
                      </div>
                      <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                        <table className="w-full min-w-[960px] border-collapse text-left text-sm">
                          <thead>
                            <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                              <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">Segment</th>
                              <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">
                                Ping before start
                              </th>
                              <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">
                                Ping at gap start
                              </th>
                              <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">
                                Ping at gap end
                              </th>
                              <th className="px-2 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">
                                Gap (min)
                              </th>
                              <th className="px-2 py-2 font-medium text-zinc-700 dark:text-zinc-300">Status</th>
                            </tr>
                          </thead>
                          <tbody>
                            {(() => {
                              const allG = jobsGpsGapDetailData.gaps;
                              const gapsView = jobsGpsGapsModalBreachedOnly
                                ? allG.filter((g) => g.is_issue)
                                : allG;
                              if (allG.length === 0) {
                                return (
                                  <tr>
                                    <td colSpan={6} className="px-2 py-3 text-zinc-500 dark:text-zinc-400">
                                      No gap rows (full coverage with zero-length segments only — unlikely).
                                    </td>
                                  </tr>
                                );
                              }
                              if (gapsView.length === 0) {
                                return (
                                  <tr>
                                    <td colSpan={6} className="px-2 py-3 text-zinc-500 dark:text-zinc-400">
                                      No breaches — all gaps are below the threshold. Turn off the filter to see every
                                      segment.
                                    </td>
                                  </tr>
                                );
                              }
                              return gapsView.map((g, i) => (
                                <tr
                                  id={`jobs-gps-gap-row-${i}`}
                                  key={`${g.kind}-${i}-${g.gap_minutes ?? 'x'}`}
                                  className={`${
                                    g.is_issue
                                      ? 'bg-red-50/95 dark:bg-red-950/40'
                                      : 'bg-green-50/90 dark:bg-green-950/25'
                                  } ${
                                    !jobsGpsGapsModalBreachedOnly &&
                                    jobsGpsGapScrollIdx === i &&
                                    g.is_issue
                                      ? 'ring-2 ring-inset ring-blue-500/70 dark:ring-blue-400/50'
                                      : ''
                                  }`}
                                >
                                  <td className="border-b border-zinc-100 px-2 py-1.5 align-top text-zinc-800 dark:text-zinc-200">
                                    {jobsGpsGapKindLabel(g.kind)}
                                  </td>
                                  <td className="border-b border-zinc-100 px-2 py-1.5 align-top">
                                    <JobsGpsGapTrackingCell row={g.row_before_prev} />
                                  </td>
                                  <td className="border-b border-zinc-100 px-2 py-1.5 align-top">
                                    {g.prev_row != null ? (
                                      <JobsGpsGapTrackingCell row={g.prev_row} />
                                    ) : g.kind === 'start' && g.gps_last != null ? (
                                      <JobsGpsGapWindowBoundCell nzTime={g.gps_last} />
                                    ) : (
                                      <span className="text-zinc-400">—</span>
                                    )}
                                  </td>
                                  <td className="border-b border-zinc-100 px-2 py-1.5 align-top">
                                    {g.next_row != null ? (
                                      <JobsGpsGapTrackingCell row={g.next_row} />
                                    ) : g.kind === 'end' && g.gps_next != null ? (
                                      <JobsGpsGapWindowBoundCell nzTime={g.gps_next} />
                                    ) : (
                                      <span className="text-zinc-400">—</span>
                                    )}
                                  </td>
                                  <td className="border-b border-zinc-100 px-2 py-1.5 text-right font-mono tabular-nums text-zinc-900 dark:text-zinc-100">
                                    {g.gap_minutes != null ? g.gap_minutes : '—'}
                                  </td>
                                  <td className="border-b border-zinc-100 px-2 py-1.5 font-medium text-zinc-800 dark:text-zinc-200">
                                    {g.is_issue ? 'Issue' : 'OK'}
                                  </td>
                                </tr>
                              ));
                            })()}
                          </tbody>
                        </table>
                      </div>
                    </>
                  )}
                </div>
              </div>
            </div>
          )}
        </section>
        )}

        {activeTab === 'winery-fixes' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">Winery / Vineyard / Driver name fixes</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Map old vwork names to new names. Winery: <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_wine_mapp</code> → <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">delivery_winery</code>; vineyard: <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vine_mapp</code> → <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">vineyard_name</code>; driver: <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_driver_mapp</code> → <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">worker</code>. Run SQL migrations once if columns or tables are missing.
          </p>

          <h3 className="mt-6 text-base font-medium text-zinc-800 dark:text-zinc-200">Winery name fixes</h3>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">Run Fixes</code> sets <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">delivery_winery_old</code>, then <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">delivery_winery</code>. Run <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">add_delivery_winery_old_tbl_vworkjobs.sql</code> if needed.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4 rounded border border-zinc-200 bg-zinc-50/50 p-4 dark:border-zinc-700 dark:bg-zinc-800/30">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Old name</span>
              <input
                type="text"
                list="wine-mapp-old-list"
                value={wineMappNewOld}
                onChange={(e) => setWineMappNewOld(e.target.value)}
                placeholder="Select or type old winery name"
                className="w-56 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="wine-mapp-old-list">
                {deliveryWineries.map((w) => (
                  <option key={w} value={w} />
                ))}
              </datalist>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">New name</span>
              <input
                type="text"
                list="wine-mapp-new-list"
                value={wineMappNewNew}
                onChange={(e) => setWineMappNewNew(e.target.value)}
                placeholder="Select or type new winery name"
                className="w-56 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="wine-mapp-new-list">
                {deliveryWineries.map((w) => (
                  <option key={w} value={w} />
                ))}
              </datalist>
            </label>
            <button
              type="button"
              onClick={createWineMapp}
              disabled={wineMappSaving}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {wineMappSaving ? 'Saving…' : 'Add fix'}
            </button>
            <button
              type="button"
              onClick={runFixes}
              disabled={runFixesLoading || wineMappRows.length === 0}
              className="rounded bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50 dark:bg-amber-700 dark:hover:bg-amber-600"
              title="Update tbl_vworkjobs: set delivery_winery_old = current delivery_winery, delivery_winery = new name where delivery_winery = old name"
            >
              {runFixesLoading ? 'Running…' : 'Run Fixes'}
            </button>
          </div>

          {runFixesResult != null && (
            <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
              Updated {runFixesResult.totalUpdated} row(s) in tbl_vworkjobs.
              {runFixesResult.perMapping.length > 0 && (
                <ul className="mt-1 list-inside list-disc">
                  {runFixesResult.perMapping.map((m) => (
                    <li key={m.id}>
                      <code className="rounded bg-emerald-100 px-0.5 dark:bg-emerald-900/50">{m.oldvworkname}</code> → <code className="rounded bg-emerald-100 px-0.5 dark:bg-emerald-900/50">{m.newvworkname}</code>: {m.updated} row(s)
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}

          {wineMappError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {wineMappError}
            </div>
          )}

          <div className="mt-6">
            {wineMappLoading ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading…</p>
            ) : (
              <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[480px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Old name</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">New name</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {wineMappRows.length === 0 ? (
                      <tr>
                        <td colSpan={3} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          No winery name fixes yet. Add one above.
                        </td>
                      </tr>
                    ) : (
                      wineMappRows.map((row) => (
                        <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                          {editingId === row.id ? (
                            <>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`wine-mapp-edit-old-${row.id}`}
                                  value={editOld}
                                  onChange={(e) => setEditOld(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`wine-mapp-edit-old-${row.id}`}>
                                  {deliveryWineries.map((w) => (
                                    <option key={w} value={w} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`wine-mapp-edit-new-${row.id}`}
                                  value={editNew}
                                  onChange={(e) => setEditNew(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`wine-mapp-edit-new-${row.id}`}>
                                  {deliveryWineries.map((w) => (
                                    <option key={w} value={w} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => updateWineMapp(row.id)}
                                  disabled={wineMappSaving}
                                  className="mr-2 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
                                >
                                  Save
                                </button>
                                <button
                                  type="button"
                                  onClick={() => setEditingId(null)}
                                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Cancel
                                </button>
                              </td>
                            </>
                          ) : (
                            <>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.oldvworkname}</td>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.newvworkname}</td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => startEdit(row)}
                                  disabled={wineMappSaving}
                                  className="mr-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Edit
                                </button>
                                <button
                                  type="button"
                                  onClick={() => deleteWineMapp(row.id)}
                                  disabled={wineMappSaving}
                                  className="rounded border border-red-200 bg-white px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-50 dark:border-red-800 dark:bg-zinc-800 dark:text-red-300 dark:hover:bg-red-950/30"
                                >
                                  Delete
                                </button>
                              </td>
                            </>
                          )}
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          <h3 className="mt-10 border-t border-zinc-200 pt-8 text-base font-medium text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
            Vineyard name fixes
          </h3>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Map old vineyard names to new names in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vine_mapp</code>. <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">Run Fixes</code> sets <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">vineyard_name_old</code>, then <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">vineyard_name</code>. Create the table with <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">create_tbl_vine_mapp.sql</code>; run <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">add_vineyard_name_old_tbl_vworkjobs.sql</code> if <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">vineyard_name_old</code> is missing.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4 rounded border border-zinc-200 bg-zinc-50/50 p-4 dark:border-zinc-700 dark:bg-zinc-800/30">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Old name</span>
              <input
                type="text"
                list="vine-mapp-old-list"
                value={vineMappNewOld}
                onChange={(e) => setVineMappNewOld(e.target.value)}
                placeholder="Select or type old vineyard name"
                className="w-56 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="vine-mapp-old-list">
                {vineyardNamesList.map((w) => (
                  <option key={w} value={w} />
                ))}
              </datalist>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">New name</span>
              <input
                type="text"
                list="vine-mapp-new-list"
                value={vineMappNewNew}
                onChange={(e) => setVineMappNewNew(e.target.value)}
                placeholder="Select or type new vineyard name"
                className="w-56 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="vine-mapp-new-list">
                {vineyardNamesList.map((w) => (
                  <option key={w} value={w} />
                ))}
              </datalist>
            </label>
            <button
              type="button"
              onClick={createVineMapp}
              disabled={vineMappSaving}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {vineMappSaving ? 'Saving…' : 'Add fix'}
            </button>
            <button
              type="button"
              onClick={runVineFixes}
              disabled={runVineFixesLoading || vineMappRows.length === 0}
              className="rounded bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50 dark:bg-amber-700 dark:hover:bg-amber-600"
              title="Update tbl_vworkjobs: set vineyard_name_old = current vineyard_name, vineyard_name = new name where vineyard_name = old name"
            >
              {runVineFixesLoading ? 'Running…' : 'Run Fixes'}
            </button>
          </div>

          {runVineFixesResult != null && (
            <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
              Updated {runVineFixesResult.totalUpdated} row(s) in tbl_vworkjobs (vineyard_name).
              {runVineFixesResult.perMapping.length > 0 && (
                <ul className="mt-1 list-inside list-disc">
                  {runVineFixesResult.perMapping.map((m) => (
                    <li key={m.id}>
                      <code className="rounded bg-emerald-100 px-0.5 dark:bg-emerald-900/50">{m.oldvworkname}</code> →{' '}
                      <code className="rounded bg-emerald-100 px-0.5 dark:bg-emerald-900/50">{m.newvworkname}</code>: {m.updated}{' '}
                      row(s)
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}

          {vineMappError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {vineMappError}
            </div>
          )}

          <div className="mt-6">
            {vineMappLoading ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading vineyard fixes…</p>
            ) : (
              <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[480px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Old name</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">New name</th>
                      <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {vineMappRows.length === 0 ? (
                      <tr>
                        <td colSpan={3} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          No vineyard name fixes yet. Add one above.
                        </td>
                      </tr>
                    ) : (
                      vineMappRows.map((row) => (
                        <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                          {vineEditingId === row.id ? (
                            <>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`vine-mapp-edit-old-${row.id}`}
                                  value={vineEditOld}
                                  onChange={(e) => setVineEditOld(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`vine-mapp-edit-old-${row.id}`}>
                                  {vineyardNamesList.map((w) => (
                                    <option key={w} value={w} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`vine-mapp-edit-new-${row.id}`}
                                  value={vineEditNew}
                                  onChange={(e) => setVineEditNew(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`vine-mapp-edit-new-${row.id}`}>
                                  {vineyardNamesList.map((w) => (
                                    <option key={w} value={w} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => updateVineMapp(row.id)}
                                  disabled={vineMappSaving}
                                  className="mr-2 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
                                >
                                  Save
                                </button>
                                <button
                                  type="button"
                                  onClick={() => setVineEditingId(null)}
                                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Cancel
                                </button>
                              </td>
                            </>
                          ) : (
                            <>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.oldvworkname}</td>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.newvworkname}</td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => startEditVine(row)}
                                  disabled={vineMappSaving}
                                  className="mr-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Edit
                                </button>
                                <button
                                  type="button"
                                  onClick={() => deleteVineMapp(row.id)}
                                  disabled={vineMappSaving}
                                  className="rounded border border-red-200 bg-white px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-50 dark:border-red-800 dark:bg-zinc-800 dark:text-red-300 dark:hover:bg-red-950/30"
                                >
                                  Delete
                                </button>
                              </td>
                            </>
                          )}
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          <h3 className="mt-10 border-t border-zinc-200 pt-8 text-base font-medium text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
            Driver name fixes
          </h3>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Map old driver (worker) names to new names in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_driver_mapp</code>.{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">Run Fixes</code> sets <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">worker_old</code>, then <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">worker</code>. Use <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">add_worker_old_tbl_vworkjobs.sql</code> if <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">worker_old</code> is missing.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4 rounded border border-zinc-200 bg-zinc-50/50 p-4 dark:border-zinc-700 dark:bg-zinc-800/30">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Old name</span>
              <input
                type="text"
                list="driver-mapp-old-list"
                value={driverMappNewOld}
                onChange={(e) => setDriverMappNewOld(e.target.value)}
                placeholder="Select or type old driver name"
                className="w-56 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="driver-mapp-old-list">
                {workerNamesList.map((w) => (
                  <option key={w} value={w} />
                ))}
              </datalist>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">New name</span>
              <input
                type="text"
                list="driver-mapp-new-list"
                value={driverMappNewNew}
                onChange={(e) => setDriverMappNewNew(e.target.value)}
                placeholder="Select or type new driver name"
                className="w-56 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="driver-mapp-new-list">
                {workerNamesList.map((w) => (
                  <option key={w} value={w} />
                ))}
              </datalist>
            </label>
            <button
              type="button"
              onClick={createDriverMapp}
              disabled={driverMappSaving}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {driverMappSaving ? 'Saving…' : 'Add fix'}
            </button>
            <button
              type="button"
              onClick={runDriverFixes}
              disabled={runDriverFixesLoading || driverMappRows.length === 0}
              className="rounded bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50 dark:bg-amber-700 dark:hover:bg-amber-600"
              title="Update tbl_vworkjobs: set worker_old = current worker, worker = new name where worker = old name"
            >
              {runDriverFixesLoading ? 'Running…' : 'Run Fixes'}
            </button>
          </div>

          {runDriverFixesResult != null && (
            <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
              Updated {runDriverFixesResult.totalUpdated} row(s) in tbl_vworkjobs (worker).
              {runDriverFixesResult.perMapping.length > 0 && (
                <ul className="mt-1 list-inside list-disc">
                  {runDriverFixesResult.perMapping.map((m) => (
                    <li key={m.id}>
                      <code className="rounded bg-emerald-100 px-0.5 dark:bg-emerald-900/50">{m.oldvworkname}</code> →{' '}
                      <code className="rounded bg-emerald-100 px-0.5 dark:bg-emerald-900/50">{m.newvworkname}</code>: {m.updated}{' '}
                      row(s)
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}

          {driverMappError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {driverMappError}
            </div>
          )}

          <div className="mt-6">
            {driverMappLoading ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading driver fixes…</p>
            ) : (
              <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[480px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Old name</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">New name</th>
                      <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {driverMappRows.length === 0 ? (
                      <tr>
                        <td colSpan={3} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          No driver name fixes yet. Add one above.
                        </td>
                      </tr>
                    ) : (
                      driverMappRows.map((row) => (
                        <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                          {driverEditingId === row.id ? (
                            <>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`driver-mapp-edit-old-${row.id}`}
                                  value={driverEditOld}
                                  onChange={(e) => setDriverEditOld(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`driver-mapp-edit-old-${row.id}`}>
                                  {workerNamesList.map((w) => (
                                    <option key={w} value={w} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`driver-mapp-edit-new-${row.id}`}
                                  value={driverEditNew}
                                  onChange={(e) => setDriverEditNew(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`driver-mapp-edit-new-${row.id}`}>
                                  {workerNamesList.map((w) => (
                                    <option key={w} value={w} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => updateDriverMapp(row.id)}
                                  disabled={driverMappSaving}
                                  className="mr-2 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
                                >
                                  Save
                                </button>
                                <button
                                  type="button"
                                  onClick={() => setDriverEditingId(null)}
                                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Cancel
                                </button>
                              </td>
                            </>
                          ) : (
                            <>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.oldvworkname}</td>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.newvworkname}</td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => startEditDriver(row)}
                                  disabled={driverMappSaving}
                                  className="mr-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Edit
                                </button>
                                <button
                                  type="button"
                                  onClick={() => deleteDriverMapp(row.id)}
                                  disabled={driverMappSaving}
                                  className="rounded border border-red-200 bg-white px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-50 dark:border-red-800 dark:bg-zinc-800 dark:text-red-300 dark:hover:bg-red-950/30"
                                >
                                  Delete
                                </button>
                              </td>
                            </>
                          )}
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </section>
        )}

        {activeTab === 'varied' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">Varied</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Set <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vworkjobs.trailermode</code> from <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">trailer_rego</code>: <strong>TT</strong> when a valid trailer rego is present (truck + trailer), <strong>T</strong> when no trailer (truck only).
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4">
            <button
              type="button"
              onClick={runSetTrailerType}
              disabled={setTrailerLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
              title="UPDATE tbl_vworkjobs: trailermode = 'TT' where trailer_rego valid, trailermode = 'T' (truck only) otherwise"
            >
              {setTrailerLoading ? 'Running…' : 'Set Trailer Type'}
            </button>
          </div>

          {setTrailerResult != null && (
            <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
              Updated {setTrailerResult.totalUpdated} row(s): {setTrailerResult.updatedTT} set to TT (trailer), {setTrailerResult.updatedT} set to T (truck only).
            </div>
          )}

          {setTrailerError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {setTrailerError}
            </div>
          )}

          <hr className="my-8 border-zinc-200 dark:border-zinc-700" />

          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Then set <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">trailermode</code> from <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">loadsize</code> and Admin → Settings → <strong>TT Load Size</strong> (tbl_settings) <em>only where loadsize is not null and greater than zero</em>: <strong>TT</strong> if loadsize is greater than the threshold, <strong>T</strong> if it is less than or equal to the threshold. Rows with null or zero loadsize are left unchanged (run “Set Trailer Type” first). In AutoRuns, “Set trailer type” runs before this step.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4">
            <button
              type="button"
              onClick={runSetTrailermodeFromLoadsize}
              disabled={loadsizeTmLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
              title="UPDATE tbl_vworkjobs trailermode from loadsize vs Settings TT Load Size"
            >
              {loadsizeTmLoading ? 'Running…' : 'Trailermode from load size'}
            </button>
          </div>

          {loadsizeTmResult != null && (
            <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
              Threshold {loadsizeTmResult.threshold}: updated {loadsizeTmResult.totalUpdated} row(s) — {loadsizeTmResult.updatedTT} TT, {loadsizeTmResult.updatedT} T.
            </div>
          )}

          {loadsizeTmError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {loadsizeTmError}
            </div>
          )}
        </section>
        )}

        {activeTab === 'gps-integrity' && (
        <section className="mt-0 min-w-0 max-w-full rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">GPS Integrity (API)</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Rows = devices (HK), columns = UTC dates. Each cell: 3 checks vs{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_tracking.position_time</code> — first time, last time, row count (API vs DB). Green = pass, red = fail, — = no API data. The table fills row-by-row as each device finishes (several API calls run in parallel per device).
          </p>
          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">From</span>
              <input
                type="date"
                value={gpsIntegrityFrom}
                onChange={(e) => setGpsIntegrityFrom(e.target.value)}
                className="w-40 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">To</span>
              <input
                type="date"
                value={gpsIntegrityTo}
                onChange={(e) => setGpsIntegrityTo(e.target.value)}
                className="w-40 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={() => void runGpsIntegrityCheck()}
              disabled={gpsIntegrityLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {gpsIntegrityLoading ? 'Running…' : 'Run check'}
            </button>
          </div>
          {gpsIntegrityError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {gpsIntegrityError}
            </div>
          )}
          {gpsIntegrityGrid && gpsIntegrityGrid.dates.length > 0 && (
            <div className="mt-4">
              {gpsIntegrityLoading && (
                <p className="mb-2 text-xs text-zinc-500 dark:text-zinc-400">
                  Loading… {gpsIntegrityGrid.rows.length} / {gpsIntegrityGrid.devices.length} device(s) complete.
                </p>
              )}
              <p className="mb-2 text-xs text-zinc-500 dark:text-zinc-400">
                Each cell shows 1st time, last time, and row count (or API/DB when mismatch). Green = pass, red = fail. — = no API data.
              </p>
              <div className="max-w-full overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-max min-w-full border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="sticky left-0 z-10 border-r border-zinc-200 bg-zinc-100/80 px-2 py-2 font-medium text-zinc-700 dark:border-zinc-700 dark:bg-zinc-800/80 dark:text-zinc-300">Device</th>
                      {gpsIntegrityGrid.dates.map((d) => (
                        <th key={d} className="whitespace-nowrap px-2 py-2 font-mono text-xs font-medium text-zinc-700 dark:text-zinc-300">
                          {d}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {gpsIntegrityGrid.rows.map((row) => (
                      <tr key={row.device} className="border-b border-zinc-100 dark:border-zinc-700">
                        <td className="sticky left-0 z-10 max-w-[12rem] border-r border-zinc-200 bg-white px-2 py-1.5 text-xs text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100">
                          {row.device}
                        </td>
                        {row.cells.map((cell, j) => {
                          if (cell.status === 'no_data') {
                            return (
                              <td key={j} className="px-2 py-1.5 text-center text-zinc-400 dark:text-zinc-500">
                                —
                              </td>
                            );
                          }
                          const title = `1st: ${cell.firstTime} | Last: ${cell.lastTime} | API: ${cell.apiCount} DB: ${cell.dbCount}`;
                          const firstText = cell.firstOk ? cell.firstTime : `${cell.firstTime} (${cell.dbFirstTime ?? ''})`;
                          const lastText = cell.lastOk ? cell.lastTime : `${cell.lastTime} (${cell.dbLastTime ?? ''})`;
                          const countText = cell.countOk ? String(cell.apiCount) : `${cell.apiCount} / (${cell.dbCount})`;
                          return (
                            <td key={j} className="px-2 py-1.5 font-mono text-xs" title={title}>
                              <div className="flex flex-col gap-0.5">
                                <span className={cell.firstOk ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'} title="1st in DB">{firstText}</span>
                                <span className={cell.lastOk ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'} title="Last in DB">{lastText}</span>
                                <span className={cell.countOk ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'} title="API vs DB row count">{countText}</span>
                              </div>
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </section>
        )}

        {activeTab === 'db-check' && (
        <section className="mt-0 min-w-0 max-w-full rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">DB Check</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            No API calls. Per device and UTC calendar day: row count and min/max{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time</code> in{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_tracking</code>. Empty cells = no rows that day (gap alert).
          </p>
          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">From</span>
              <input
                type="date"
                value={dbCheckFrom}
                onChange={(e) => setDbCheckFrom(e.target.value)}
                className="w-40 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">To</span>
              <input
                type="date"
                value={dbCheckTo}
                onChange={(e) => setDbCheckTo(e.target.value)}
                className="w-40 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={() => {
                setDbCheckError(null);
                setDbCheckGrid(null);
                if (!dbCheckFrom || !dbCheckTo) {
                  setDbCheckError('From and To dates required');
                  return;
                }
                setDbCheckLoading(true);
                fetch('/api/admin/data-checks/db-check', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ dateFrom: dbCheckFrom, dateTo: dbCheckTo }),
                })
                  .then((r) => {
                    if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
                    return r.json();
                  })
                  .then((data: DbCheckGridResponse) => setDbCheckGrid(data))
                  .catch((e) => setDbCheckError(e instanceof Error ? e.message : String(e)))
                  .finally(() => setDbCheckLoading(false));
              }}
              disabled={dbCheckLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {dbCheckLoading ? 'Running…' : 'Run check'}
            </button>
          </div>
          {dbCheckError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {dbCheckError}
            </div>
          )}
          {dbCheckGrid && dbCheckGrid.rows.length > 0 && (
            <div className="mt-4 max-w-full overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
              <table className="w-max min-w-full border-collapse text-left text-sm">
                <thead>
                  <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                    <th className="sticky left-0 z-10 border-r border-zinc-200 bg-zinc-100/80 px-2 py-2 font-medium text-zinc-700 dark:border-zinc-700 dark:bg-zinc-800/80 dark:text-zinc-300">Device</th>
                    {dbCheckGrid.dates.map((d) => (
                      <th key={d} className="whitespace-nowrap px-2 py-2 font-mono text-xs font-medium text-zinc-700 dark:text-zinc-300">{d}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {dbCheckGrid.rows.map((row) => (
                    <tr key={row.device} className="border-b border-zinc-100 dark:border-zinc-700">
                      <td className="sticky left-0 z-10 max-w-[12rem] border-r border-zinc-200 bg-white px-2 py-1.5 text-xs text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100">{row.device}</td>
                      {row.cells.map((cell, j) =>
                        cell.status === 'empty' ? (
                          <td key={j} className="px-2 py-1.5 text-center text-zinc-400 dark:text-zinc-500">—</td>
                        ) : (
                          <td
                            key={j}
                            className="px-2 py-1.5 font-mono text-xs text-zinc-800 dark:text-zinc-200"
                            title={`min ${cell.minTime} | max ${cell.maxTime}`}
                          >
                            <div className="flex flex-col gap-0.5">
                              <span className="font-medium">{cell.count}</span>
                              <span className="text-[10px] leading-tight text-zinc-500 dark:text-zinc-400">{formatGapDateTime(cell.minTime)}</span>
                              <span className="text-[10px] leading-tight text-zinc-500 dark:text-zinc-400">{formatGapDateTime(cell.maxTime)}</span>
                            </div>
                          </td>
                        )
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {dbCheckGrid && dbCheckGrid.rows.length === 0 && !dbCheckLoading && (
            <p className="mt-4 text-sm text-zinc-500 dark:text-zinc-400">No devices with tracking rows in this range.</p>
          )}
        </section>
        )}

        {activeTab === 'db-check-simple' && (
        <section className="mt-0 min-w-0 max-w-full rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">DB Check Simple</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            High-level check: one row per UTC day — total row count and min/max{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time</code> across all devices in{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_tracking</code>. Use this first; then use DB Check for per-device gaps.
          </p>
          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">From</span>
              <input
                type="date"
                value={dbCheckSimpleFrom}
                onChange={(e) => setDbCheckSimpleFrom(e.target.value)}
                className="w-40 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">To</span>
              <input
                type="date"
                value={dbCheckSimpleTo}
                onChange={(e) => setDbCheckSimpleTo(e.target.value)}
                className="w-40 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={() => {
                setDbCheckSimpleError(null);
                setDbCheckSimpleResult(null);
                if (!dbCheckSimpleFrom || !dbCheckSimpleTo) {
                  setDbCheckSimpleError('From and To dates required');
                  return;
                }
                setDbCheckSimpleLoading(true);
                fetch('/api/admin/data-checks/db-check-simple', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ dateFrom: dbCheckSimpleFrom, dateTo: dbCheckSimpleTo }),
                })
                  .then((r) => {
                    if (!r.ok) return r.json().then((d) => Promise.reject(new Error(d?.error ?? r.statusText)));
                    return r.json();
                  })
                  .then((data: DbCheckSimpleResponse) => setDbCheckSimpleResult(data))
                  .catch((e) => setDbCheckSimpleError(e instanceof Error ? e.message : String(e)))
                  .finally(() => setDbCheckSimpleLoading(false));
              }}
              disabled={dbCheckSimpleLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {dbCheckSimpleLoading ? 'Running…' : 'Run check'}
            </button>
          </div>
          {dbCheckSimpleError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {dbCheckSimpleError}
            </div>
          )}
          {dbCheckSimpleResult && dbCheckSimpleResult.rows.length > 0 && (
            <div className="mt-4 max-w-full overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
              <table className="w-full min-w-[480px] border-collapse text-left text-sm">
                <thead>
                  <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                    <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Date (UTC)</th>
                    <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Rows</th>
                    <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Min position_time</th>
                    <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Max position_time</th>
                  </tr>
                </thead>
                <tbody>
                  {dbCheckSimpleResult.rows.map((row) => (
                    <tr key={row.date} className="border-b border-zinc-100 dark:border-zinc-700">
                      <td className="px-3 py-2 font-mono text-xs text-zinc-900 dark:text-zinc-100">{row.date}</td>
                      {row.status === 'empty' ? (
                        <>
                          <td className="px-3 py-2 text-right text-zinc-400 dark:text-zinc-500">—</td>
                          <td className="px-3 py-2 text-zinc-400 dark:text-zinc-500">—</td>
                          <td className="px-3 py-2 text-zinc-400 dark:text-zinc-500">—</td>
                        </>
                      ) : (
                        <>
                          <td className="px-3 py-2 text-right font-mono text-zinc-800 dark:text-zinc-200">{row.count}</td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-800 dark:text-zinc-200">{row.minTime}</td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-800 dark:text-zinc-200">{row.maxTime}</td>
                        </>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
        )}

        {activeTab === 'vineyard-mappings' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">Vineyard Mappings</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Map vineyard names to groups in <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vineyardgroups</code>. Winery from <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vworkjobs.delivery_winery</code>; vineyard name and group are free text with suggestions from vworkjobs.
          </p>

          <div className="mt-4 flex flex-wrap items-end gap-4 rounded border border-zinc-200 bg-zinc-50/50 p-4 dark:border-zinc-700 dark:bg-zinc-800/30">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Winery name</span>
              <select
                value={vgNewWinery}
                onChange={(e) => setVgNewWinery(e.target.value)}
                className="w-48 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              >
                <option value="">— Select —</option>
                {vgDeliveryWineries.map((w) => (
                  <option key={w} value={w}>{w}</option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Vineyard name</span>
              <input
                type="text"
                list="vg-new-vineyard-list"
                value={vgNewVineyard}
                onChange={(e) => setVgNewVineyard(e.target.value)}
                placeholder="Type or pick from list"
                className="w-48 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
              <datalist id="vg-new-vineyard-list">
                {vgVineyardNames.map((v) => (
                  <option key={v} value={v} />
                ))}
              </datalist>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Group</span>
              <input
                type="text"
                value={vgNewGroup}
                onChange={(e) => setVgNewGroup(e.target.value)}
                placeholder="Free text"
                className="w-40 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={createVgRow}
              disabled={vgSaving}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {vgSaving ? 'Saving…' : 'Add'}
            </button>
          </div>

          {vgError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {vgError}
            </div>
          )}

          <div className="mt-6">
            {vgLoading ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading…</p>
            ) : (
              <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[520px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Winery</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Vineyard</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Group</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {vgRows.length === 0 ? (
                      <tr>
                        <td colSpan={4} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          No mappings yet. Add one above.
                        </td>
                      </tr>
                    ) : (
                      vgRows.map((row) => (
                        <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                          {vgEditingId === row.id ? (
                            <>
                              <td className="px-3 py-2">
                                <select
                                  value={vgEditWinery}
                                  onChange={(e) => setVgEditWinery(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                >
                                  <option value="">—</option>
                                  {vgDeliveryWineries.map((w) => (
                                    <option key={w} value={w}>{w}</option>
                                  ))}
                                </select>
                              </td>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  list={`vg-edit-vineyard-${row.id}`}
                                  value={vgEditVineyard}
                                  onChange={(e) => setVgEditVineyard(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                                <datalist id={`vg-edit-vineyard-${row.id}`}>
                                  {vgVineyardNames.map((v) => (
                                    <option key={v} value={v} />
                                  ))}
                                </datalist>
                              </td>
                              <td className="px-3 py-2">
                                <input
                                  type="text"
                                  value={vgEditGroup}
                                  onChange={(e) => setVgEditGroup(e.target.value)}
                                  className="w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                                />
                              </td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => updateVgRow(row.id)}
                                  disabled={vgSaving}
                                  className="mr-2 rounded bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
                                >
                                  Save
                                </button>
                                <button
                                  type="button"
                                  onClick={() => setVgEditingId(null)}
                                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Cancel
                                </button>
                              </td>
                            </>
                          ) : (
                            <>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.winery_name ?? '—'}</td>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.vineyard_name}</td>
                              <td className="px-3 py-2 text-zinc-900 dark:text-zinc-100">{row.vineyard_group}</td>
                              <td className="px-3 py-2 text-right">
                                <button
                                  type="button"
                                  onClick={() => startEditVg(row)}
                                  disabled={vgSaving}
                                  className="mr-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                >
                                  Edit
                                </button>
                                <button
                                  type="button"
                                  onClick={() => cloneVgRow(row)}
                                  disabled={vgSaving}
                                  className="mr-2 rounded border border-zinc-300 bg-white px-2 py-1 text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                                  title="Copy this row into the Add form"
                                >
                                  Clone
                                </button>
                                <button
                                  type="button"
                                  onClick={() => deleteVgRow(row.id)}
                                  disabled={vgSaving}
                                  className="rounded border border-red-200 bg-white px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-50 dark:border-red-800 dark:bg-zinc-800 dark:text-red-300 dark:hover:bg-red-950/30"
                                >
                                  Delete
                                </button>
                              </td>
                            </>
                          )}
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          <div className="mt-8 border-t border-zinc-200 pt-6 dark:border-zinc-700">
            <h3 className="text-base font-medium text-zinc-900 dark:text-zinc-100">Update Vineyard_Group</h3>
            <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
              Set all <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vworkjobs.vineyard_group</code> to <strong>NA</strong>, then set rows that match <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_vineyardgroups</code> (winery + vineyard) to the mapped <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">vineyard_group</code>. Run <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">add_vineyard_group_tbl_vworkjobs.sql</code> once if the column is missing.
            </p>
            <div className="mt-4">
              <button
                type="button"
                onClick={runUpdateVineyardGroup}
                disabled={vineyardGroupLoading}
                className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
                title="SET vineyard_group = 'NA' for all, then set from tbl_vineyardgroups where delivery_winery/vineyard_name match"
              >
                {vineyardGroupLoading ? 'Running…' : 'Update Vineyard_Group'}
              </button>
            </div>
            {vineyardGroupResult != null && (
              <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
                Set {vineyardGroupResult.setToNa} row(s) to NA; {vineyardGroupResult.matched} row(s) updated from tbl_vineyardgroups (total vwork rows: {vineyardGroupResult.totalRows}).
              </div>
            )}
            {vineyardGroupError && (
              <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
                {vineyardGroupError}
              </div>
            )}
          </div>
        </section>
        )}

        {activeTab === 'vwork-stale' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">VWork GPS steps</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Two separate checks (pick one): <strong>Stage 1</strong> lists jobs with no derived step times yet (not stepped).{' '}
            <strong>Stage 2</strong> lists jobs that need a re-run by aging (<code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">steps_fetched</code> /{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">steps_fetched_when</code> threshold). Both require a worker and{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">actual_start_time</code>. Use{' '}
            <a href="/query/inspect" className="text-blue-600 underline dark:text-blue-400">Inspect</a> or API Test Step 4 to run derived steps.
          </p>
          <fieldset className="mt-4 space-y-2">
            <legend className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Mode</legend>
            <div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap sm:gap-x-6 sm:gap-y-2">
              <label className="flex cursor-pointer items-start gap-2 text-sm text-zinc-800 dark:text-zinc-200">
                <input
                  type="radio"
                  name="vworkStaleMode"
                  checked={vworkStaleMode === 'stage1'}
                  onChange={() => setVworkStaleMode('stage1')}
                  className="mt-1"
                />
                <span>
                  <strong>Stage 1 — Not stepped</strong> —{' '}
                  <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">step_1_actual_time</code> through{' '}
                  <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">step_5_actual_time</code> all null. Optional date range. Does not use stale/aging logic.
                </span>
              </label>
              <label className="flex cursor-pointer items-start gap-2 text-sm text-zinc-800 dark:text-zinc-200">
                <input
                  type="radio"
                  name="vworkStaleMode"
                  checked={vworkStaleMode === 'stage2'}
                  onChange={() => setVworkStaleMode('stage2')}
                  className="mt-1"
                />
                <span>
                  <strong>Stage 2 — Stale / aging</strong> — jobs needing a GPS steps run by threshold (never fetched, missing timestamp, or last run older than N hours).
                </span>
              </label>
            </div>
          </fieldset>
          <div className="mt-4 flex flex-wrap items-end gap-4">
            {vworkStaleMode === 'stage2' && (
              <label className="flex flex-col gap-1">
                <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Stale if older than (hours)</span>
                <input
                  type="number"
                  min={0}
                  value={vworkStaleHours}
                  onChange={(e) => setVworkStaleHours(e.target.value)}
                  className="w-28 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                />
              </label>
            )}
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">actual_start from (optional)</span>
              <input
                type="date"
                value={vworkStaleFrom}
                onChange={(e) => setVworkStaleFrom(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">actual_start to (optional)</span>
              <input
                type="date"
                value={vworkStaleTo}
                onChange={(e) => setVworkStaleTo(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={() => {
                setVworkStaleError(null);
                setVworkStaleResult(null);
                const q = new URLSearchParams();
                if (vworkStaleMode === 'stage1') {
                  q.set('noStepActuals', '1');
                  q.set('staleHours', '48');
                } else {
                  const h = parseInt(vworkStaleHours, 10);
                  if (Number.isNaN(h) || h < 0) {
                    setVworkStaleError('Stale hours must be a non-negative number');
                    return;
                  }
                  q.set('staleHours', String(h));
                }
                if (vworkStaleFrom) q.set('dateFrom', vworkStaleFrom);
                if (vworkStaleTo) q.set('dateTo', vworkStaleTo);
                setVworkStaleLoading(true);
                fetch(`/api/admin/data-checks/vwork-stale?${q}`)
                  .then((r) => {
                    if (!r.ok) return r.json().then((d) => Promise.reject(new Error((d as { error?: string })?.error ?? r.statusText)));
                    return r.json();
                  })
                  .then((data: VworkStaleResponse) => setVworkStaleResult(data))
                  .catch((e) => setVworkStaleError(e instanceof Error ? e.message : String(e)))
                  .finally(() => setVworkStaleLoading(false));
              }}
              disabled={vworkStaleLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {vworkStaleLoading ? 'Running…' : vworkStaleMode === 'stage1' ? 'Run stage 1' : 'Run stage 2'}
            </button>
          </div>
          {vworkStaleError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {vworkStaleError}
            </div>
          )}
          {vworkStaleResult && (
            <div className="mt-4">
              <p className="text-sm text-zinc-600 dark:text-zinc-400">
                {vworkStaleResult.noStepActuals ? (
                  <>
                    <strong>Stage 1 — Not stepped:</strong> all of{' '}
                    <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">step_1_actual_time</code>–
                    <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">step_5_actual_time</code> are null —{' '}
                    {vworkStaleResult.count} job(s)
                    {vworkStaleResult.dateFrom || vworkStaleResult.dateTo
                      ? ` (actual_start ${vworkStaleResult.dateFrom ?? '…'} … ${vworkStaleResult.dateTo ?? '…'})`
                      : ''}
                    . (Stage 1 only — no aging threshold.)
                  </>
                ) : (
                  <>
                    <strong>Stage 2 — Stale / aging:</strong> threshold {vworkStaleResult.staleHours} h since{' '}
                    <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">steps_fetched_when</code> — {vworkStaleResult.count} job(s)
                    {vworkStaleResult.dateFrom || vworkStaleResult.dateTo
                      ? ` (actual_start ${vworkStaleResult.dateFrom ?? '…'} … ${vworkStaleResult.dateTo ?? '…'})`
                      : ''}
                    .
                  </>
                )}
              </p>
              <div className="mt-4 max-w-full overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[960px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Issue</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">job_id</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Worker</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Vineyard</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Winery</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">actual_start</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">steps_fetched</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">steps_fetched_when</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Hours ago</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Open</th>
                    </tr>
                  </thead>
                  <tbody>
                    {vworkStaleResult.rows.length === 0 ? (
                      <tr>
                        <td colSpan={10} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          {vworkStaleResult.noStepActuals
                            ? 'No jobs with all step 1–5 actual times null in this filter (limit 500).'
                            : 'No stale jobs in this filter (limit 500).'}
                        </td>
                      </tr>
                    ) : (
                      vworkStaleResult.rows.map((row) => (
                        <tr key={row.job_id} className="border-b border-zinc-100 dark:border-zinc-700">
                          <td className="px-3 py-2 text-xs text-zinc-800 dark:text-zinc-200">{row.issue}</td>
                          <td className="px-3 py-2 font-mono text-xs text-zinc-900 dark:text-zinc-100">{row.job_id}</td>
                          <td className="px-3 py-2 text-xs">{row.worker ?? '—'}</td>
                          <td className="px-3 py-2 text-xs">{row.vineyard_name ?? '—'}</td>
                          <td className="px-3 py-2 text-xs">{row.delivery_winery ?? '—'}</td>
                          <td className="px-3 py-2 font-mono text-xs">{row.actual_start_time ?? '—'}</td>
                          <td className="px-3 py-2 text-xs">{row.steps_fetched === true ? 'true' : row.steps_fetched === false ? 'false' : '—'}</td>
                          <td className="px-3 py-2 font-mono text-xs">{row.steps_fetched_when ?? '—'}</td>
                          <td className="px-3 py-2 text-right text-xs">{row.hours_since_steps != null ? row.hours_since_steps : '—'}</td>
                          <td className="px-3 py-2">
                            <a
                              href={`/query/inspect?jobId=${encodeURIComponent(row.job_id)}`}
                              className="text-blue-600 underline dark:text-blue-400"
                            >
                              Inspect
                            </a>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </section>
        )}

        {activeTab === 'geofence-gaps' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">Geofence gaps</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Device-days where any <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_tracking</code> row still has{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">geofence_attempted</code> false or null (same condition as{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">store_fences_for_date_scoped</code> for a normal run). Day boundary is{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time::date</code>. Use{' '}
            <strong>Days only</strong> for a short list of calendar days to rerun end-to-end (mapping, entry/exit, steps) for the whole day.
          </p>
          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">From (day)</span>
              <input
                type="date"
                value={geofenceGapsFrom}
                onChange={(e) => setGeofenceGapsFrom(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">To (day)</span>
              <input
                type="date"
                value={geofenceGapsTo}
                onChange={(e) => setGeofenceGapsTo(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">
                Min points ({geofenceGapsDaysOnly ? 'total that day' : 'per device-day'})
              </span>
              <input
                type="number"
                min={1}
                value={geofenceGapsMinPoints}
                onChange={(e) => setGeofenceGapsMinPoints(e.target.value)}
                className="w-28 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                title={
                  geofenceGapsDaysOnly
                    ? 'Minimum total tracking rows that calendar day (all devices)'
                    : 'Minimum rows per device per day'
                }
              />
            </label>
            <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
              <input
                type="checkbox"
                checked={geofenceGapsDaysOnly}
                onChange={(e) => setGeofenceGapsDaysOnly(e.target.checked)}
                className="rounded border-zinc-300 text-blue-600 dark:border-zinc-600"
              />
              Days only (rerun list)
            </label>
            <button
              type="button"
              onClick={() => {
                setGeofenceGapsError(null);
                setGeofenceGapsResult(null);
                if (!geofenceGapsFrom || !geofenceGapsTo) {
                  setGeofenceGapsError('From and To dates required');
                  return;
                }
                const mp = parseInt(geofenceGapsMinPoints, 10);
                if (Number.isNaN(mp) || mp < 1) {
                  setGeofenceGapsError('Min points must be at least 1');
                  return;
                }
                const q = new URLSearchParams({
                  dateFrom: geofenceGapsFrom,
                  dateTo: geofenceGapsTo,
                  minPoints: String(mp),
                });
                if (geofenceGapsDaysOnly) q.set('view', 'by-day');
                setGeofenceGapsLoading(true);
                fetch(`/api/admin/data-checks/geofence-gaps?${q}`)
                  .then((r) => {
                    if (!r.ok) return r.json().then((d) => Promise.reject(new Error((d as { error?: string })?.error ?? r.statusText)));
                    return r.json();
                  })
                  .then((data: GeofenceGapsResponse) => setGeofenceGapsResult(data))
                  .catch((e) => setGeofenceGapsError(e instanceof Error ? e.message : String(e)))
                  .finally(() => setGeofenceGapsLoading(false));
              }}
              disabled={geofenceGapsLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {geofenceGapsLoading ? 'Running…' : 'Run check'}
            </button>
          </div>
          {geofenceGapsError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {geofenceGapsError}
            </div>
          )}
          {geofenceGapsResult && (
            <div className="mt-4">
              <p className="text-sm text-zinc-600 dark:text-zinc-400">
                {geofenceGapsResult.view === 'by-day'
                  ? `${geofenceGapsResult.count} calendar day(s) with unattempted points (whole-day rerun list); min total points ${geofenceGapsResult.minPoints}.`
                  : `${geofenceGapsResult.count} device-day row(s); min points per device-day ${geofenceGapsResult.minPoints}.`}
              </p>
              <div className="mt-4 max-w-full overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                {geofenceGapsResult.view === 'by-day' ? (
                  <table className="w-full min-w-[560px] border-collapse text-left text-sm">
                    <thead>
                      <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                        <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Day</th>
                        <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Devices</th>
                        <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Points</th>
                        <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Unattempted</th>
                        <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Attempted</th>
                      </tr>
                    </thead>
                    <tbody>
                      {geofenceGapsResult.rows.length === 0 ? (
                        <tr>
                          <td colSpan={5} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                            No gaps found for this range (limit 500 rows).
                          </td>
                        </tr>
                      ) : (
                        geofenceGapsResult.rows.map((row) => (
                          <tr key={row.day} className="border-b border-zinc-100 dark:border-zinc-700">
                            <td className="px-3 py-2 font-mono text-xs font-medium text-zinc-900 dark:text-zinc-100">{row.day}</td>
                            <td className="px-3 py-2 text-right font-mono text-xs">{row.device_count}</td>
                            <td className="px-3 py-2 text-right font-mono text-xs">{row.point_count}</td>
                            <td className="px-3 py-2 text-right font-mono text-xs text-amber-700 dark:text-amber-400">{row.unattempted_count}</td>
                            <td className="px-3 py-2 text-right font-mono text-xs">{row.attempted_count}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                ) : (
                  <table className="w-full min-w-[720px] border-collapse text-left text-sm">
                    <thead>
                      <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                        <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Day (position_time)</th>
                        <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Device</th>
                        <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Points</th>
                        <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Unattempted</th>
                        <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">Attempted</th>
                      </tr>
                    </thead>
                    <tbody>
                      {geofenceGapsResult.rows.length === 0 ? (
                        <tr>
                          <td colSpan={5} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                            No gaps found for this range (limit 500 rows).
                          </td>
                        </tr>
                      ) : (
                        geofenceGapsResult.rows.map((row) => (
                          <tr key={`${row.device_name}-${row.day}`} className="border-b border-zinc-100 dark:border-zinc-700">
                            <td className="px-3 py-2 font-mono text-xs">{row.day}</td>
                            <td className="px-3 py-2 text-xs">{row.device_name}</td>
                            <td className="px-3 py-2 text-right font-mono text-xs">{row.point_count}</td>
                            <td className="px-3 py-2 text-right font-mono text-xs text-amber-700 dark:text-amber-400">{row.unattempted_count}</td>
                            <td className="px-3 py-2 text-right font-mono text-xs">{row.attempted_count}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          )}
        </section>
        )}

        {activeTab === 'geofence-enter-exit-gaps' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">Geofence Enter/Exit gaps</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">device_name</code> + calendar day (
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time::date</code>) with enough tracking
            points but <strong>no</strong> rows tagged <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">ENTER</code> or{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">EXIT</code>. Sorted by day (newest first), then driver.
            Usually means that day was not run through{' '}
            <a href="/admin/tagging" className="text-blue-600 underline dark:text-blue-400">Entry/Exit Tagging</a>
            , so GPS steps will be thin. Run geofence mapping first if needed. Click <strong>Points</strong> to open{' '}
            <Link href="/query/gpsdata" className="text-blue-600 underline dark:text-blue-400">GPS Tracking</Link> in a new tab
            (that device, calendar day from <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">position_time</code>, sorted ascending, paginated).
          </p>
          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">From (day)</span>
              <input
                type="date"
                value={eeGapsFrom}
                onChange={(e) => setEeGapsFrom(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">To (day)</span>
              <input
                type="date"
                value={eeGapsTo}
                onChange={(e) => setEeGapsTo(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Min points / device-day</span>
              <input
                type="number"
                min={1}
                value={eeGapsMinPoints}
                onChange={(e) => setEeGapsMinPoints(e.target.value)}
                className="w-28 rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              />
            </label>
            <button
              type="button"
              onClick={() => {
                setEeGapsError(null);
                setEeGapsResult(null);
                if (!eeGapsFrom || !eeGapsTo) {
                  setEeGapsError('From and To dates required');
                  return;
                }
                const mp = parseInt(eeGapsMinPoints, 10);
                if (Number.isNaN(mp) || mp < 1) {
                  setEeGapsError('Min points must be at least 1');
                  return;
                }
                const q = new URLSearchParams({
                  dateFrom: eeGapsFrom,
                  dateTo: eeGapsTo,
                  minPoints: String(mp),
                });
                setEeGapsLoading(true);
                fetch(`/api/admin/data-checks/geofence-enter-exit-gaps?${q}`)
                  .then((r) => {
                    if (!r.ok) return r.json().then((d) => Promise.reject(new Error((d as { error?: string })?.error ?? r.statusText)));
                    return r.json();
                  })
                  .then((data: GeofenceEnterExitGapsResponse) => setEeGapsResult(data))
                  .catch((e) => setEeGapsError(e instanceof Error ? e.message : String(e)))
                  .finally(() => setEeGapsLoading(false));
              }}
              disabled={eeGapsLoading}
              className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
            >
              {eeGapsLoading ? 'Running…' : 'Run check'}
            </button>
          </div>
          {eeGapsError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {eeGapsError}
            </div>
          )}
          {eeGapsResult && (
            <div className="mt-4">
              <p className="text-sm text-zinc-600 dark:text-zinc-400">
                {eeGapsResult.count} device-day row(s) with zero ENTER/EXIT; min points {eeGapsResult.minPoints}. Sorted by day
                (newest first), then driver.
              </p>
              <div className="mt-4 max-w-full overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full min-w-[640px] border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/80 dark:border-zinc-700 dark:bg-zinc-800/80">
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Day</th>
                      <th className="px-3 py-2 font-medium text-zinc-700 dark:text-zinc-300">Driver (device_name)</th>
                      <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300" title="Opens GPS Tracking (new tab)">
                        Points
                      </th>
                      <th className="px-3 py-2 text-right font-medium text-zinc-700 dark:text-zinc-300">ENTER/EXIT rows</th>
                    </tr>
                  </thead>
                  <tbody>
                    {eeGapsResult.rows.length === 0 ? (
                      <tr>
                        <td colSpan={4} className="px-3 py-4 text-center text-zinc-500 dark:text-zinc-400">
                          No gaps in this range (limit 500 rows).
                        </td>
                      </tr>
                    ) : (
                      eeGapsResult.rows.map((row) => (
                        <tr key={`${row.device_name}-${row.day}`} className="border-b border-zinc-100 dark:border-zinc-700">
                          <td className="px-3 py-2 font-mono text-xs">{row.day}</td>
                          <td className="px-3 py-2 text-xs text-zinc-900 dark:text-zinc-100">{row.device_name}</td>
                          <td className="px-3 py-2 text-right font-mono text-xs">
                            <Link
                              href={`/query/gpsdata?device=${encodeURIComponent(row.device_name)}&day=${encodeURIComponent(row.day)}&orderBy=position_time&orderDir=asc`}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-blue-600 underline dark:text-blue-400"
                              title="tbl_tracking: this device and day, position_time ASC (500 rows per page)"
                            >
                              {row.point_count}
                            </Link>
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-xs text-amber-700 dark:text-amber-400">{row.enter_exit_count}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </section>
        )}

        {activeTab === 'data-fixes' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">Data fixes</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Targeted repairs from Google Drive exports. Requires the import service (
            <code className="rounded bg-zinc-100 px-1 text-xs dark:bg-zinc-800">IMPORT_SERVICE_URL</code>) and Drive access
            as configured for vWork import.
          </p>

          <div className="mt-6 rounded-lg border border-amber-200 bg-amber-50/80 p-4 dark:border-amber-900/60 dark:bg-amber-950/30">
            <h3 className="text-base font-medium text-amber-950 dark:text-amber-100">Indevin — backfill Load Size → tbl_vworkjobs.loadsize</h3>
            <ul className="mt-2 list-inside list-disc space-y-1 text-sm text-amber-950/90 dark:text-amber-100/90">
              <li>
                Re-reads <strong>every</strong> vWork export CSV in the Drive folder (same rules as auto import: filename contains{' '}
                <code className="rounded bg-amber-100/80 px-1 text-xs dark:bg-amber-900/50">export</code>), including files already marked imported —{' '}
                <strong>no other table columns</strong> are written.
              </li>
              <li>
                Only rows where <strong>Customer</strong> (or Client name) contains <strong>Indevin</strong>, case-insensitive, are considered.
              </li>
              <li>
                For each matching row: finds <code className="rounded bg-amber-100/80 px-1 text-xs dark:bg-amber-900/50">job_id</code> and runs{' '}
                <code className="rounded bg-amber-100/80 px-1 text-xs dark:bg-amber-900/50">UPDATE tbl_vworkjobs SET loadsize = …</code> only. Rows
                without a parseable number in Load Size are skipped (existing <code className="rounded bg-amber-100/80 px-1 text-xs dark:bg-amber-900/50">loadsize</code> is left unchanged). Jobs not in the DB are counted but not inserted.
              </li>
              <li>
                Run <code className="rounded bg-amber-100/80 px-1 text-xs dark:bg-amber-900/50">web/sql/add_loadsize_tbl_vworkjobs.sql</code> if the column
                or mappings are missing. Going forward, normal vWork import populates <code className="rounded bg-amber-100/80 px-1 text-xs dark:bg-amber-900/50">loadsize</code> on <strong>new</strong> inserts only (existing jobs still need this backfill once).
              </li>
            </ul>

            <div className="mt-5 rounded-md border border-zinc-300/90 bg-white/70 p-3 dark:border-zinc-600 dark:bg-zinc-900/50">
              <p className="text-sm font-medium text-zinc-800 dark:text-zinc-200">Smoke test (safe)</p>
              <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                (1){' '}
                <code className="rounded bg-zinc-100 px-1 font-mono dark:bg-zinc-800">GET /health</code> on the import service.
                (2) Drive export file list. (3) Parse up to ~15k rows across CSVs (read-only) using the same Customer column
                resolution as the backfill and verify at least one cell contains <strong>indevin</strong> (case-insensitive)
                after a reasonable sample — so the primary filter logic is exercised before a long backfill.
              </p>
              <button
                type="button"
                onClick={() => void runLoadsizeQuickTest()}
                disabled={loadsizeQuickTestLoading || loadsizeBackfillRunning}
                className="mt-3 rounded bg-zinc-700 px-3 py-1.5 text-sm font-medium text-white hover:bg-zinc-800 disabled:cursor-not-allowed disabled:opacity-50 dark:bg-zinc-600 dark:hover:bg-zinc-500"
              >
                {loadsizeQuickTestLoading ? 'Running test…' : 'Quick test'}
              </button>
              {loadsizeQuickTestError && (
                <p className="mt-2 text-sm text-red-700 dark:text-red-300">{loadsizeQuickTestError}</p>
              )}
              {loadsizeQuickTestResult && (
                <div
                  className={`mt-3 rounded border px-3 py-2 text-sm ${
                    loadsizeQuickTestResult.ok
                      ? 'border-emerald-300 bg-emerald-50 text-emerald-900 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100'
                      : 'border-red-300 bg-red-50 text-red-950 dark:border-red-900 dark:bg-red-950/40 dark:text-red-100'
                  }`}
                >
                  <p className="font-mono text-xs text-zinc-600 dark:text-zinc-400">Import host: {loadsizeQuickTestResult.importHost}</p>
                  <p className="mt-1">
                    Health:{' '}
                    {loadsizeQuickTestResult.health.ok ? (
                      <span className="text-emerald-700 dark:text-emerald-400">OK</span>
                    ) : (
                      <span>
                        Failed
                        {loadsizeQuickTestResult.health.status != null && ` (HTTP ${loadsizeQuickTestResult.health.status})`}
                        {loadsizeQuickTestResult.health.error && ` — ${loadsizeQuickTestResult.health.error}`}
                      </span>
                    )}
                  </p>
                  <p className="mt-1">
                    Drive list:{' '}
                    {loadsizeQuickTestResult.driveList.ok ? (
                      <span className="text-emerald-700 dark:text-emerald-400">
                        OK — {loadsizeQuickTestResult.driveList.filesTotal ?? 0} export file(s)
                        {loadsizeQuickTestResult.driveList.driveFolderId && (
                          <span className="block font-mono text-xs text-zinc-600 dark:text-zinc-400">
                            folder id: {loadsizeQuickTestResult.driveList.driveFolderId}
                          </span>
                        )}
                      </span>
                    ) : (
                      <span>
                        Failed
                        {loadsizeQuickTestResult.driveList.status != null && ` (HTTP ${loadsizeQuickTestResult.driveList.status})`}
                        {loadsizeQuickTestResult.driveList.error && ` — ${loadsizeQuickTestResult.driveList.error}`}
                      </span>
                    )}
                  </p>
                  {loadsizeQuickTestResult.driveList.ok &&
                    (loadsizeQuickTestResult.driveList.sampleNames?.length ?? 0) > 0 && (
                      <ul className="mt-2 list-inside list-disc font-mono text-xs text-zinc-700 dark:text-zinc-300">
                        {loadsizeQuickTestResult.driveList.sampleNames!.map((n, si) => (
                          <li key={`${si}-${n}`}>{n}</li>
                        ))}
                        {(loadsizeQuickTestResult.driveList.filesTotal ?? 0) > 8 && (
                          <li className="list-none text-zinc-500">…and more</li>
                        )}
                      </ul>
                    )}
                  {loadsizeQuickTestResult.customerScan && (
                    <div className="mt-3 border-t border-zinc-200 pt-2 dark:border-zinc-600">
                      <p className="text-xs font-medium text-zinc-800 dark:text-zinc-200">Customer / “indevin” (backfill logic)</p>
                      {loadsizeQuickTestResult.customerScan.notRun ? (
                        <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                          Not run — {loadsizeQuickTestResult.customerScan.notRunReason ?? 'see above'}
                        </p>
                      ) : (
                        <>
                          <p className="mt-1">
                            {loadsizeQuickTestResult.customerScan.ok ? (
                              <span className="text-emerald-700 dark:text-emerald-400">OK</span>
                            ) : (
                              <span className="text-red-700 dark:text-red-300">Failed</span>
                            )}
                            {loadsizeQuickTestResult.customerScan.error && (
                              <span className="block text-xs text-red-800 dark:text-red-200">
                                {loadsizeQuickTestResult.customerScan.error}
                              </span>
                            )}
                          </p>
                          {loadsizeQuickTestResult.customerScan.rowsScanned != null && (
                            <p className="mt-1 font-mono text-xs text-zinc-700 dark:text-zinc-300">
                              Rows scanned: {loadsizeQuickTestResult.customerScan.rowsScanned.toLocaleString()}
                              {loadsizeQuickTestResult.customerScan.maxRowsBudget != null &&
                                ` (budget ${loadsizeQuickTestResult.customerScan.maxRowsBudget.toLocaleString()})`}
                              {loadsizeQuickTestResult.customerScan.filesTouched != null &&
                                ` · Files opened: ${loadsizeQuickTestResult.customerScan.filesTouched}`}
                            </p>
                          )}
                          {(loadsizeQuickTestResult.customerScan.rowsWithCustomerText != null ||
                            loadsizeQuickTestResult.customerScan.indevinMatchCount != null) && (
                            <p className="mt-1 font-mono text-xs text-zinc-700 dark:text-zinc-300">
                              Rows with Customer text:{' '}
                              {loadsizeQuickTestResult.customerScan.rowsWithCustomerText?.toLocaleString() ?? '—'} · Matches{' '}
                              <strong>indevin</strong>:{' '}
                              {loadsizeQuickTestResult.customerScan.indevinMatchCount?.toLocaleString() ?? '—'}
                            </p>
                          )}
                          {loadsizeQuickTestResult.customerScan.firstMatchCustomerPreview && (
                            <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                              First match preview:{' '}
                              <q className="font-mono text-zinc-800 dark:text-zinc-200">
                                {loadsizeQuickTestResult.customerScan.firstMatchCustomerPreview}
                              </q>
                            </p>
                          )}
                          {loadsizeQuickTestResult.customerScan.warning && (
                            <p className="mt-1 text-xs text-amber-800 dark:text-amber-200">
                              {loadsizeQuickTestResult.customerScan.warning}
                            </p>
                          )}
                        </>
                      )}
                    </div>
                  )}
                  {loadsizeQuickTestResult.hint && (
                    <p className="mt-2 text-xs opacity-90">{loadsizeQuickTestResult.hint}</p>
                  )}
                </div>
              )}
            </div>

            <label className="mt-4 flex cursor-pointer items-start gap-2 text-sm text-amber-950 dark:text-amber-100">
              <input
                type="checkbox"
                checked={loadsizeBackfillConfirm}
                onChange={(e) => setLoadsizeBackfillConfirm(e.target.checked)}
                disabled={loadsizeBackfillRunning}
                className="mt-1 rounded border-amber-400 text-amber-700 disabled:opacity-50 dark:border-amber-700 dark:text-amber-500"
              />
              <span>
                I understand this will update <strong>only</strong> the <code className="rounded bg-amber-100/80 px-0.5 font-mono text-xs dark:bg-amber-900/50">loadsize</code> column
                for existing jobs, from Drive, for Indevin customer rows as described above.
              </span>
            </label>

            <div className="mt-4 flex flex-wrap items-center gap-3">
              <button
                type="button"
                onClick={() => void runLoadsizeBackfill()}
                disabled={loadsizeBackfillRunning || !loadsizeBackfillConfirm}
                className="rounded bg-amber-700 px-4 py-2 text-sm font-medium text-white hover:bg-amber-800 disabled:cursor-not-allowed disabled:opacity-50 dark:bg-amber-600 dark:hover:bg-amber-500"
              >
                {loadsizeBackfillRunning
                  ? loadsizeBackfillPhase === 'listing'
                    ? 'Listing files…'
                    : 'Processing exports…'
                  : 'Run Load Size backfill'}
              </button>
              {loadsizeBackfillPhase !== 'idle' && (
                <span className="text-sm text-amber-950/80 dark:text-amber-200/80">
                  {loadsizeBackfillPhase === 'done'
                    ? 'Finished.'
                    : loadsizeBackfillFilesTotal > 0 && loadsizeBackfillFileIndex >= 0
                      ? `File ${loadsizeBackfillFileIndex + 1} of ${loadsizeBackfillFilesTotal}`
                      : loadsizeBackfillPhase === 'listing'
                        ? 'Contacting import service…'
                        : '—'}
                </span>
              )}
            </div>

            {loadsizeBackfillRunning && loadsizeBackfillPhase === 'processing' && loadsizeBackfillFilesTotal > 0 && (
              <div className="mt-4">
                <div className="h-2 w-full overflow-hidden rounded-full bg-amber-200/60 dark:bg-amber-900/40">
                  <div
                    className="h-full rounded-full bg-amber-600 transition-[width] duration-300 dark:bg-amber-500"
                    style={{
                      width: `${Math.min(100, ((loadsizeBackfillFileIndex + 1) / loadsizeBackfillFilesTotal) * 100)}%`,
                    }}
                  />
                </div>
                <p className="mt-2 font-mono text-xs text-amber-950/90 dark:text-amber-200/90" title={loadsizeBackfillCurrentName}>
                  Current: {loadsizeBackfillCurrentName || '—'}
                </p>
                <p className="mt-1 text-xs text-amber-900/80 dark:text-amber-300/80">
                  Updated so far: {loadsizeBackfillTotals.rowsUpdated.toLocaleString()} · Indevin rows seen:{' '}
                  {loadsizeBackfillTotals.rowsIndevinCustomer.toLocaleString()} · No matching job:{' '}
                  {loadsizeBackfillTotals.rowsNotFoundInDb.toLocaleString()}
                </p>
              </div>
            )}

            {loadsizeBackfillError && (
              <div className="mt-4 rounded border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-900 dark:border-red-800 dark:bg-red-950/40 dark:text-red-200">
                {loadsizeBackfillError}
              </div>
            )}

            {!loadsizeBackfillRunning && loadsizeBackfillPhase === 'done' && loadsizeBackfillFilesTotal === 0 && (
              <div className="mt-4 rounded border border-zinc-200 bg-zinc-50 px-3 py-2 text-sm text-zinc-700 dark:border-zinc-600 dark:bg-zinc-800/50 dark:text-zinc-300">
                No export CSV files found in the configured Drive folder. Nothing to do.
              </div>
            )}

            {!loadsizeBackfillRunning && loadsizeBackfillPhase === 'done' && loadsizeBackfillFilesTotal > 0 && (
              <div className="mt-4 space-y-3 rounded border border-emerald-200 bg-emerald-50/90 px-3 py-3 text-sm text-emerald-950 dark:border-emerald-900/50 dark:bg-emerald-950/30 dark:text-emerald-100">
                <p className="font-medium text-emerald-950 dark:text-emerald-50">Summary ({loadsizeBackfillFilesTotal} file(s))</p>
                <dl className="grid grid-cols-1 gap-1 sm:grid-cols-2">
                  <div>
                    <dt className="text-xs text-emerald-800/80 dark:text-emerald-300/80">Rows updated (loadsize set)</dt>
                    <dd className="font-mono text-base">{loadsizeBackfillTotals.rowsUpdated.toLocaleString()}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-emerald-800/80 dark:text-emerald-300/80">Indevin customer rows (in exports)</dt>
                    <dd className="font-mono text-base">{loadsizeBackfillTotals.rowsIndevinCustomer.toLocaleString()}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-emerald-800/80 dark:text-emerald-300/80">Skipped — no job id in row</dt>
                    <dd className="font-mono text-base">{loadsizeBackfillTotals.rowsSkippedNoJobId.toLocaleString()}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-emerald-800/80 dark:text-emerald-300/80">Skipped — no parseable Load Size</dt>
                    <dd className="font-mono text-base">{loadsizeBackfillTotals.rowsSkippedNoLoadSize.toLocaleString()}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-emerald-800/80 dark:text-emerald-300/80">No row in DB for job_id</dt>
                    <dd className="font-mono text-base">{loadsizeBackfillTotals.rowsNotFoundInDb.toLocaleString()}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-emerald-800/80 dark:text-emerald-300/80">Data rows scanned (all files)</dt>
                    <dd className="font-mono text-base">{loadsizeBackfillTotals.rowsInAllFiles.toLocaleString()}</dd>
                  </div>
                </dl>
                <p className="text-xs text-emerald-900/85 dark:text-emerald-300/85">
                  Each file is logged under <code className="rounded bg-emerald-100/80 px-1 dark:bg-emerald-900/40">tbl_logs</code> (
                  <code className="rounded bg-emerald-100/80 px-1 dark:bg-emerald-900/40">DataFix</code> /{' '}
                  <code className="rounded bg-emerald-100/80 px-1 dark:bg-emerald-900/40">vwork-loadsize-backfill</code>).
                </p>
              </div>
            )}

            {loadsizeBackfillLastErrors.length > 0 && (
              <div className="mt-4 rounded border border-amber-300 bg-white px-3 py-2 dark:border-amber-800 dark:bg-zinc-900">
                <p className="text-xs font-medium text-amber-900 dark:text-amber-200">Latest file row errors (sample)</p>
                <ul className="mt-1 max-h-32 list-inside list-disc overflow-y-auto font-mono text-xs text-amber-950 dark:text-amber-100">
                  {loadsizeBackfillLastErrors.map((line, i) => (
                    <li key={i}>{line}</li>
                  ))}
                </ul>
              </div>
            )}

            {loadsizeBackfillPerFile.length > 0 && !loadsizeBackfillRunning && loadsizeBackfillPhase === 'done' && (
              <div className="mt-4">
                <h4 className="text-sm font-medium text-zinc-800 dark:text-zinc-200">Per file</h4>
                <div className="mt-2 max-h-72 overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
                  <table className="w-full min-w-[640px] border-collapse text-left text-xs">
                    <thead className="sticky top-0 bg-zinc-100 dark:bg-zinc-800">
                      <tr>
                        <th className="border-b border-zinc-200 px-2 py-2 font-medium dark:border-zinc-600">File</th>
                        <th className="border-b border-zinc-200 px-2 py-2 text-right font-medium dark:border-zinc-600">Rows</th>
                        <th className="border-b border-zinc-200 px-2 py-2 text-right font-medium dark:border-zinc-600">Indevin</th>
                        <th className="border-b border-zinc-200 px-2 py-2 text-right font-medium dark:border-zinc-600">Updated</th>
                        <th className="border-b border-zinc-200 px-2 py-2 text-right font-medium dark:border-zinc-600">No job id</th>
                        <th className="border-b border-zinc-200 px-2 py-2 text-right font-medium dark:border-zinc-600">No load size</th>
                        <th className="border-b border-zinc-200 px-2 py-2 text-right font-medium dark:border-zinc-600">Not in DB</th>
                        <th className="border-b border-zinc-200 px-2 py-2 text-center font-medium dark:border-zinc-600">Errors</th>
                      </tr>
                    </thead>
                    <tbody>
                      {loadsizeBackfillPerFile.map((row, idx) => (
                        <tr key={`${idx}-${row.fileName}`} className="border-b border-zinc-100 dark:border-zinc-800">
                          <td className="max-w-[220px] truncate px-2 py-1.5 font-mono text-zinc-800 dark:text-zinc-200" title={row.fileName}>
                            {row.fileName}
                          </td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{row.rowsInFile}</td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{row.rowsIndevinCustomer}</td>
                          <td className="px-2 py-1.5 text-right tabular-nums text-emerald-700 dark:text-emerald-400">{row.rowsUpdated}</td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{row.rowsSkippedNoJobId}</td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{row.rowsSkippedNoLoadSize}</td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{row.rowsNotFoundInDb}</td>
                          <td className="px-2 py-1.5 text-center">
                            {row.errorSampleCount > 0 ? (
                              <span title={row.errorsTruncated ? 'More errors truncated on server' : undefined}>
                                {row.errorSampleCount}
                                {row.errorsTruncated ? '+' : ''}
                              </span>
                            ) : (
                              '—'
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </section>
        )}

        {activeTab === 'step4to5' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">Step4→5 migration</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            <strong className="font-medium text-zinc-800 dark:text-zinc-200">Normal</strong> — only when{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">step4to5 = 0</code>,{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">step_4_name</code> is <strong>Job Completed</strong>, and{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">step_5_name</code> is <strong>not</strong> Job Completed, and{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">step_5_completed_at</code> is <strong>empty</strong> (never push 4→5 if step 5 already has a completed time — rerun only recalculates step 4). Then: copy step 4 → 5, set step 4 to &quot;Arrive Winery&quot; with synthetic time from steps 1–3 (see guardrail below), clear GPS/actual/orides,{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">steps_fetched = false</code>, <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">step4to5 = 1</code>.{' '}
            <strong className="font-medium text-zinc-800 dark:text-zinc-200">Done</strong> = step 4 Arrive Winery, step 5 Job Completed, step4to5=1.{' '}
            <strong className="font-medium text-zinc-800 dark:text-zinc-200">Rerun</strong> — on those done rows only: updates <strong>step_4_completed_at</strong> (recalc capped synthetic time).{' '}
            <strong className="font-medium text-zinc-800 dark:text-zinc-200">Order fix</strong> — same recalc as Rerun, but only rows where <code className="rounded bg-zinc-100 px-0.5 font-mono text-xs dark:bg-zinc-800">step_4_completed_at</code> is null or not strictly before{' '}
            <code className="rounded bg-zinc-100 px-0.5 font-mono text-xs dark:bg-zinc-800">step_5_completed_at</code> (does not change step 5). Run{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">web/sql/add_step4to5_tbl_vworkjobs.sql</code> if columns are missing.
          </p>
          <p className="mt-3 rounded-md border border-zinc-200 bg-zinc-50 px-3 py-2 text-sm text-zinc-800 dark:border-zinc-600 dark:bg-zinc-900/40 dark:text-zinc-200">
            <strong className="font-medium">Synthetic step 4 (VWork only).</strong> Default: step 3 + (step 2 − step 1). If that would be on or after <code className="rounded bg-zinc-100 px-0.5 font-mono text-xs dark:bg-zinc-800">step_5_completed_at</code>, use halfway between step 3 and step 5 completion. Otherwise no synthetic time (steps 1–3 and valid outbound required).
          </p>
          <fieldset className="mt-4 space-y-2 text-sm text-zinc-800 dark:text-zinc-200">
            <legend className="sr-only">Step4→5 mode</legend>
            <label className="flex cursor-pointer items-start gap-2">
              <input
                type="radio"
                name="step4to5Mode"
                checked={step4to5Mode === 'normal'}
                onChange={() => {
                  setStep4to5Mode('normal');
                  setStep4to5Preview(null);
                  setStep4to5FixResult(null);
                }}
                className="mt-1 border-zinc-300 text-amber-600 dark:border-zinc-600"
              />
              <span>
                <strong>Normal</strong> — migrate Job Completed on step 4 into step 5 and set step 4 to Arrive Winery (full column update).
              </span>
            </label>
            <label className="flex cursor-pointer items-start gap-2">
              <input
                type="radio"
                name="step4to5Mode"
                checked={step4to5Mode === 'rerun'}
                onChange={() => {
                  setStep4to5Mode('rerun');
                  setStep4to5Preview(null);
                  setStep4to5FixResult(null);
                }}
                className="mt-1 border-zinc-300 text-amber-600 dark:border-zinc-600"
              />
              <span>
                <strong>Rerun</strong> — all <strong>done</strong> rows (step 4 = Arrive Winery, step 5 = Job Completed, step4to5 = 1): recalculate{' '}
                <code className="rounded bg-zinc-100 px-0.5 font-mono text-xs dark:bg-zinc-800">step_4_completed_at</code> only; step 5 unchanged.
              </span>
            </label>
            <label className="flex cursor-pointer items-start gap-2">
              <input
                type="radio"
                name="step4to5Mode"
                checked={step4to5Mode === 'ordering'}
                onChange={() => {
                  setStep4to5Mode('ordering');
                  setStep4to5Preview(null);
                  setStep4to5FixResult(null);
                }}
                className="mt-1 border-zinc-300 text-amber-600 dark:border-zinc-600"
              />
              <span>
                <strong>Order fix</strong> — done rows where <code className="rounded bg-zinc-100 px-0.5 font-mono text-xs dark:bg-zinc-800">step_5_completed_at</code> is set and{' '}
                <code className="rounded bg-zinc-100 px-0.5 font-mono text-xs dark:bg-zinc-800">step_4_completed_at</code> is missing or not strictly before it; same recalc as Rerun, only those rows.
              </span>
            </label>
          </fieldset>
          <div className="mt-4 flex flex-wrap items-end gap-4">
            <label className="flex min-w-[200px] flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Customer</span>
              <select
                value={step4to5Customer}
                onChange={(e) => {
                  setStep4to5Customer(e.target.value);
                  setStep4to5Template('');
                  setStep4to5Preview(null);
                  setStep4to5FixResult(null);
                }}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              >
                <option value="">— Select —</option>
                {step4to5Customers.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex min-w-[200px] flex-col gap-1">
              <span className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Template</span>
              <select
                value={step4to5Template}
                onChange={(e) => {
                  setStep4to5Template(e.target.value);
                  setStep4to5Preview(null);
                  setStep4to5FixResult(null);
                }}
                disabled={!step4to5Customer.trim()}
                className="rounded border border-zinc-300 bg-white px-3 py-2 text-sm disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
              >
                <option value="">— Select —</option>
                {step4to5Templates.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </label>
            <button
              type="button"
              onClick={() => {
                const c = step4to5Customer.trim();
                const t = step4to5Template.trim();
                if (!c || !t) {
                  setStep4to5PreviewError('Select customer and template');
                  return;
                }
                setStep4to5PreviewError(null);
                setStep4to5FixError(null);
                setStep4to5FixResult(null);
                setStep4to5PreviewLoading(true);
                const qs = new URLSearchParams({ customer: c, template: t });
                if (step4to5Mode === 'rerun') qs.set('rerun', '1');
                if (step4to5Mode === 'ordering') qs.set('orderingFix', '1');
                fetch(`/api/admin/vworkjobs/step4to5?${qs}`)
                  .then(async (r) => {
                    const d = (await r.json()) as Step4to5PreviewResponse & { ok?: boolean; error?: string };
                    if (!r.ok) throw new Error(d?.error ?? r.statusText);
                    if (!d.ok) throw new Error('Preview failed');
                    return d;
                  })
                  .then((data) => setStep4to5Preview(data))
                  .catch((e) => {
                    setStep4to5Preview(null);
                    setStep4to5PreviewError(e instanceof Error ? e.message : String(e));
                  })
                  .finally(() => setStep4to5PreviewLoading(false));
              }}
              disabled={step4to5PreviewLoading}
              className="rounded bg-zinc-200 px-4 py-2 text-sm font-medium text-zinc-900 hover:bg-zinc-300 disabled:opacity-50 dark:bg-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-600"
            >
              {step4to5PreviewLoading ? 'Loading…' : 'Preview'}
            </button>
            <button
              type="button"
              onClick={() => {
                const c = step4to5Customer.trim();
                const t = step4to5Template.trim();
                if (!c || !t) {
                  setStep4to5FixError('Select customer and template');
                  return;
                }
                if (!step4to5Preview || step4to5Preview.customer !== c || step4to5Preview.template !== t) {
                  setStep4to5FixError('Run Preview first for this customer and template');
                  return;
                }
                if (!step4to5PreviewMatchesMode(step4to5Preview, step4to5Mode)) {
                  setStep4to5FixError('Run Preview first for the selected mode (Normal / Rerun / Order fix)');
                  return;
                }
                setStep4to5FixError(null);
                setStep4to5FixConfirmOpen(true);
              }}
              disabled={step4to5FixLoading || !step4to5Preview}
              className="rounded bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50 dark:bg-amber-700 dark:hover:bg-amber-600"
            >
              {step4to5FixLoading ? 'Fixing…' : 'Fix'}
            </button>
          </div>
          {step4to5PreviewError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {step4to5PreviewError}
            </div>
          )}
          {step4to5FixError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/50 dark:text-red-200">
              {step4to5FixError}
            </div>
          )}
          {step4to5Preview && (
            <div className="mt-4 space-y-3 text-sm text-zinc-700 dark:text-zinc-300">
              <div className="max-w-3xl overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                <table className="w-full border-collapse text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 bg-zinc-100/90 dark:border-zinc-700 dark:bg-zinc-800/90">
                      <th className="px-3 py-2 font-medium text-zinc-800 dark:text-zinc-200">Metric</th>
                      <th className="px-3 py-2 text-right font-medium tabular-nums text-zinc-800 dark:text-zinc-200">Count</th>
                    </tr>
                  </thead>
                  <tbody className="text-zinc-700 dark:text-zinc-300">
                    <tr className="border-b border-zinc-100 dark:border-zinc-700/80">
                      <td className="px-3 py-2">
                        Potential rows{' '}
                        <span className="text-zinc-500 dark:text-zinc-400">
                          {step4to5Preview.orderingFix
                            ? '(done rows: same pool as Rerun — step4to5=1, Arrive Winery, Job Completed on 5)'
                            : step4to5Preview.rerun
                              ? '(done rows: step4to5=1, step_4 Arrive Winery, step_5 Job Completed)'
                              : '(match: customer, template, step4to5 = 0)'}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums font-medium">{step4to5Preview.potentialCount}</td>
                    </tr>
                    <tr
                      className="border-b border-zinc-100 cursor-pointer hover:bg-blue-50 dark:border-zinc-700/80 dark:hover:bg-blue-950/30"
                      title="Open in VWork (new tab) with blocked filter applied"
                      onClick={() => {
                        const p = new URLSearchParams({
                          customer: step4to5Preview.customer,
                          template: step4to5Preview.template,
                          autoLoad: '1',
                          blockedView: step4to5Preview.orderingFix
                            ? 'ordering'
                            : step4to5Preview.rerun
                              ? 'rerun'
                              : 'normal',
                        });
                        window.open(`/query/vwork?${p.toString()}`, '_blank', 'noopener,noreferrer');
                      }}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          (e.currentTarget as HTMLTableRowElement).click();
                        }
                      }}
                      tabIndex={0}
                      role="button"
                    >
                      <td className="px-3 py-2">
                        Blocked <span className="text-blue-600 dark:text-blue-400">(open in VWork)</span>{' '}
                        <span className="text-zinc-500 dark:text-zinc-400">
                          {step4to5Preview.orderingFix
                            ? '(done layout with step_4 already strictly before step_5 — nothing to fix)'
                            : step4to5Preview.rerun
                              ? '(step4to5=1 but not Arrive Winery + Job Completed on step 5)'
                              : '(step_4 ≠ Job Completed, or step_5 name/time already set — no 4→5 copy)'}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums font-medium">{step4to5Preview.blockedCount}</td>
                    </tr>
                    <tr className="border-b border-zinc-200 bg-zinc-50/80 dark:border-zinc-700 dark:bg-zinc-900/40">
                      <td className="px-3 py-2 font-medium">
                        Will do{' '}
                        <span className="font-normal text-zinc-500 dark:text-zinc-400">
                          {step4to5Preview.orderingFix || step4to5Preview.rerun
                            ? '(same as rows updated — see below)'
                            : '(potential − blocked)'}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums font-semibold text-zinc-900 dark:text-zinc-100">
                        {step4to5Preview.willDoCount}
                      </td>
                    </tr>
                    <tr className="border-b border-zinc-200 dark:border-zinc-700">
                      <td className="px-3 py-2">
                        Rows updated on Fix{' '}
                        <span className="text-zinc-500 dark:text-zinc-400">
                          {step4to5Preview.orderingFix
                            ? '(only step_4_completed_at — bad order rows only)'
                            : step4to5Preview.rerun
                              ? '(only step_4_completed_at recalculated)'
                              : '(step_4 Job Completed, step_5 name not Job Completed, step_5_completed_at null, step_4 time set)'}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums font-medium text-amber-800 dark:text-amber-300">
                        {step4to5Preview.todoCount}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <p className="text-xs text-zinc-500 dark:text-zinc-400">
                {(() => {
                  const est = estimateStep4to5FixRange(
                    step4to5Preview.todoCount,
                    step4to5Preview.rerun || step4to5Preview.orderingFix === true,
                  );
                  return (
                    <>
                      Rough <strong>Fix</strong> duration for <strong>{step4to5Preview.todoCount}</strong> row(s) touched by UPDATE: about{' '}
                      <strong>
                        {est.lowSec}–{est.highSec} s
                      </strong>{' '}
                      {step4to5Preview.rerun || step4to5Preview.orderingFix
                        ? '(rerun / order fix: one column per row — usually quicker.)'
                        : '(best guess, tuned for ~500–2000-row batches; remote or loaded DBs can take longer.)'}
                    </>
                  );
                })()}
              </p>
              <div>
                <div className="mb-1 text-xs font-medium text-zinc-500 dark:text-zinc-400">COUNT potential</div>
                <pre className="max-h-24 overflow-auto whitespace-pre-wrap break-all rounded border border-zinc-200 bg-zinc-50 p-3 font-mono text-xs dark:border-zinc-700 dark:bg-zinc-950">
                  {step4to5Preview.countPotentialSqlLiteral}
                </pre>
              </div>
              <div>
                <div className="mb-1 text-xs font-medium text-zinc-500 dark:text-zinc-400">COUNT blocked</div>
                <pre className="max-h-24 overflow-auto whitespace-pre-wrap break-all rounded border border-zinc-200 bg-zinc-50 p-3 font-mono text-xs dark:border-zinc-700 dark:bg-zinc-950">
                  {step4to5Preview.countBlockedSqlLiteral}
                </pre>
              </div>
              <div>
                <div className="mb-1 text-xs font-medium text-zinc-500 dark:text-zinc-400">SELECT rows updated on Fix (preview, limit 200)</div>
                <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-all rounded border border-zinc-200 bg-zinc-50 p-3 font-mono text-xs dark:border-zinc-700 dark:bg-zinc-950">
                  {step4to5Preview.selectSqlLiteral}
                </pre>
              </div>
              <div>
                <div className="mb-1 text-xs font-medium text-zinc-500 dark:text-zinc-400">COUNT rows updated on Fix</div>
                <pre className="max-h-24 overflow-auto whitespace-pre-wrap break-all rounded border border-zinc-200 bg-zinc-50 p-3 font-mono text-xs dark:border-zinc-700 dark:bg-zinc-950">
                  {step4to5Preview.countEligibleSqlLiteral}
                </pre>
              </div>
              <div>
                <div className="mb-1 text-xs font-medium text-zinc-500 dark:text-zinc-400">UPDATE (runs on Fix)</div>
                <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-all rounded border border-zinc-200 bg-zinc-50 p-3 font-mono text-xs dark:border-zinc-700 dark:bg-zinc-950">
                  {step4to5Preview.updateSqlLiteral}
                </pre>
              </div>
            </div>
          )}
          {step4to5FixResult && (
            <div className="mt-4 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-900 dark:border-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
              Updated <strong>{step4to5FixResult.updated}</strong> row(s)
              {step4to5FixResult.afterPreview.orderingFix
                ? ' (order fix)'
                : step4to5FixResult.afterPreview.rerun
                  ? ' (rerun mode)'
                  : ''}
              . Remaining rows updated on Fix for this filter:{' '}
              <strong>{step4to5FixResult.afterPreview.todoCount}</strong> (potential {step4to5FixResult.afterPreview.potentialCount}, blocked{' '}
              {step4to5FixResult.afterPreview.blockedCount}, will do {step4to5FixResult.afterPreview.willDoCount}).
            </div>
          )}

          {step4to5FixConfirmOpen && step4to5Preview && (
            <div
              className="fixed inset-0 z-[60] flex items-center justify-center bg-black/50 p-4"
              role="dialog"
              aria-modal="true"
              aria-labelledby="step4to5-confirm-title"
              onClick={(e) => e.target === e.currentTarget && !step4to5FixLoading && setStep4to5FixConfirmOpen(false)}
            >
              <div
                className="w-full max-w-md rounded-lg border border-zinc-200 bg-white shadow-xl dark:border-zinc-700 dark:bg-zinc-900"
                onClick={(e) => e.stopPropagation()}
              >
                <div className="border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
                  <h3 id="step4to5-confirm-title" className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
                    Run Step4→5 fix?
                  </h3>
                  <p className="mt-1 text-sm text-zinc-500 dark:text-zinc-400">
                    {step4to5Preview.customer} / {step4to5Preview.template}
                    {step4to5Preview.orderingFix
                      ? ' · Order fix'
                      : step4to5Preview.rerun
                        ? ' · Rerun mode'
                        : ''}
                  </p>
                </div>
                <div className="space-y-3 px-4 py-4 text-sm text-zinc-700 dark:text-zinc-300">
                  {step4to5Preview.orderingFix && (
                    <p className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-amber-950 dark:border-amber-700 dark:bg-amber-950/40 dark:text-amber-100">
                      Order fix: updates <strong>only</strong>{' '}
                      <code className="rounded bg-amber-100/80 px-1 dark:bg-amber-900/50">step_4_completed_at</code> on done rows where step 4 is not strictly before step 5 completion. Step 5 is not modified.
                    </p>
                  )}
                  {step4to5Preview.rerun && !step4to5Preview.orderingFix && (
                    <p className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-amber-950 dark:border-amber-700 dark:bg-amber-950/40 dark:text-amber-100">
                      Rerun: updates <strong>only</strong> <code className="rounded bg-amber-100/80 px-1 dark:bg-amber-900/50">step_4_completed_at</code> on rows that are already done (Arrive Winery / Job Completed / step4to5=1). Step 5 is not modified.
                    </p>
                  )}
                  <ul className="list-inside list-disc space-y-1">
                    <li>
                      Will do (potential − blocked): <strong className="tabular-nums">{step4to5Preview.willDoCount}</strong>
                    </li>
                    <li>
                      Rows this UPDATE will change: <strong className="tabular-nums">{step4to5Preview.todoCount}</strong>
                    </li>
                  </ul>
                  {(() => {
                    const est = estimateStep4to5FixRange(
                      step4to5Preview.todoCount,
                      step4to5Preview.rerun || step4to5Preview.orderingFix === true,
                    );
                    return (
                      <p className="rounded-md bg-zinc-100 px-3 py-2 text-sm text-zinc-800 dark:bg-zinc-800 dark:text-zinc-200">
                        <span className="font-medium">Estimated run time:</span> about{' '}
                        <strong>
                          {est.lowSec}–{est.highSec} seconds
                        </strong>
                        .{' '}
                        {step4to5Preview.rerun || step4to5Preview.orderingFix
                          ? 'Rerun and order fix only update one timestamp column per row (lighter than normal).'
                          : 'Heuristic for wide multi-column updates on ~500–2000 rows; slow network or a busy database can push this higher.'}
                      </p>
                    );
                  })()}
                  <p className="text-amber-800 dark:text-amber-200/90">
                    This cannot be undone. Run Preview again after if you need to verify counts.
                  </p>
                </div>
                <div className="flex justify-end gap-2 border-t border-zinc-200 px-4 py-3 dark:border-zinc-700">
                  <button
                    type="button"
                    onClick={() => !step4to5FixLoading && setStep4to5FixConfirmOpen(false)}
                    disabled={step4to5FixLoading}
                    className="rounded border border-zinc-300 bg-white px-4 py-2 text-sm font-medium text-zinc-800 hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700"
                  >
                    Cancel
                  </button>
                  <button
                    type="button"
                    onClick={() =>
                      runStep4to5Fix(step4to5Customer.trim(), step4to5Template.trim(), step4to5Mode)
                    }
                    disabled={step4to5FixLoading}
                    className="rounded bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50 dark:bg-amber-700 dark:hover:bg-amber-600"
                  >
                    {step4to5FixLoading ? 'Running…' : 'Run fix'}
                  </button>
                </div>
              </div>
            </div>
          )}
        </section>
        )}

        {activeTab === 'sql-updates' && (
        <section className="mt-0 rounded-b-lg border border-zinc-200 border-t-0 bg-white p-6 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
          <h2 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">SQL Updates</h2>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Store named SQL snippets in{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">tbl_sqlruns</code>. Order is{' '}
            <code className="rounded bg-zinc-100 px-1 dark:bg-zinc-800">run_order</code> (use arrows). Run all
            executes each statement in order on the database — use with care.
          </p>

          {sqlRunsError && (
            <div className="mt-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-900 dark:bg-red-950/40 dark:text-red-200">
              {sqlRunsError}
            </div>
          )}

          <div className="mt-4 flex flex-wrap items-center gap-3">
            <button
              type="button"
              onClick={() => fetchSqlRuns()}
              disabled={sqlRunsLoading}
              className="rounded border border-zinc-300 bg-white px-3 py-1.5 text-sm font-medium text-zinc-800 hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700"
            >
              {sqlRunsLoading ? 'Refreshing…' : 'Refresh'}
            </button>
            <button
              type="button"
              onClick={() => setSqlConfirmModal({ kind: 'runAll', total: sqlRuns.length })}
              disabled={sqlRunsRunLoading || sqlRuns.length === 0}
              className="rounded bg-amber-700 px-3 py-1.5 text-sm font-medium text-white hover:bg-amber-800 disabled:opacity-50 dark:bg-amber-600 dark:hover:bg-amber-500"
            >
              {sqlRunsRunLoading ? 'Running…' : 'Run all in order'}
            </button>
            {sqlRunsRunResult && (
              <button
                type="button"
                onClick={() => setSqlRunLogModalOpen(true)}
                className="rounded border border-zinc-300 bg-white px-3 py-1.5 text-sm font-medium text-zinc-800 hover:bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700"
              >
                View run log
              </button>
            )}
          </div>

          <div className="mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <h3 className="mb-3 text-sm font-medium text-zinc-800 dark:text-zinc-200">Add SQL run</h3>
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400 sm:col-span-2">
                SQL name
                <input
                  className="mt-1 w-full rounded border border-zinc-300 px-2 py-1.5 font-mono text-sm dark:border-zinc-600 dark:bg-zinc-800"
                  value={sqlNew.sql_name}
                  onChange={(e) => setSqlNew((x) => ({ ...x, sql_name: e.target.value }))}
                  placeholder="Short label"
                />
              </label>
              <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400 sm:col-span-2">
                SQL description
                <input
                  className="mt-1 w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                  value={sqlNew.sql_description}
                  onChange={(e) => setSqlNew((x) => ({ ...x, sql_description: e.target.value }))}
                  placeholder="Optional"
                />
              </label>
              <label className="block text-xs font-medium text-zinc-600 dark:text-zinc-400 sm:col-span-2">
                SQL command
                <textarea
                  className="mt-1 min-h-[100px] w-full rounded border border-zinc-300 px-2 py-1.5 font-mono text-sm dark:border-zinc-600 dark:bg-zinc-800"
                  value={sqlNew.sql_command}
                  onChange={(e) => setSqlNew((x) => ({ ...x, sql_command: e.target.value }))}
                  placeholder="Single statement recommended"
                />
              </label>
            </div>
            <button
              type="button"
              disabled={sqlRunsSaving}
              onClick={() => addSqlRun()}
              className="mt-3 rounded bg-zinc-900 px-3 py-1.5 text-sm font-medium text-white hover:bg-zinc-800 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-200"
            >
              Add
            </button>
          </div>

          <div className="mt-6 overflow-x-auto rounded-lg border border-zinc-200 dark:border-zinc-700">
            <table className="w-full min-w-[720px] border-collapse text-sm">
              <thead>
                <tr className="border-b border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-800/80">
                  <th className="px-2 py-2 text-left font-semibold text-zinc-700 dark:text-zinc-200">Order</th>
                  <th className="px-2 py-2 text-left font-semibold text-zinc-700 dark:text-zinc-200">SQL name</th>
                  <th className="px-2 py-2 text-left font-semibold text-zinc-700 dark:text-zinc-200">Description</th>
                  <th className="px-2 py-2 text-left font-semibold text-zinc-700 dark:text-zinc-200">SQL command</th>
                  <th className="px-2 py-2 text-right font-semibold text-zinc-700 dark:text-zinc-200">Actions</th>
                </tr>
              </thead>
              <tbody>
                {sqlRuns.map((row, i) =>
                  sqlEditingId === row.id ? (
                    <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                      <td className="px-2 py-2 align-top text-zinc-500">{row.run_order}</td>
                      <td className="px-2 py-2 align-top" colSpan={3}>
                        <div className="grid gap-2">
                          <input
                            className="w-full rounded border border-zinc-300 px-2 py-1 font-mono text-xs dark:border-zinc-600 dark:bg-zinc-800"
                            value={sqlEdit.sql_name}
                            onChange={(e) => setSqlEdit((x) => ({ ...x, sql_name: e.target.value }))}
                          />
                          <input
                            className="w-full rounded border border-zinc-300 px-2 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                            value={sqlEdit.sql_description}
                            onChange={(e) => setSqlEdit((x) => ({ ...x, sql_description: e.target.value }))}
                            placeholder="Description"
                          />
                          <textarea
                            className="min-h-[88px] w-full rounded border border-zinc-300 px-2 py-1 font-mono text-xs dark:border-zinc-600 dark:bg-zinc-800"
                            value={sqlEdit.sql_command}
                            onChange={(e) => setSqlEdit((x) => ({ ...x, sql_command: e.target.value }))}
                          />
                        </div>
                      </td>
                      <td className="px-2 py-2 align-top text-right">
                        <button
                          type="button"
                          disabled={sqlRunsSaving}
                          onClick={() => saveSqlRun(row.id)}
                          className="mr-1 rounded bg-zinc-800 px-2 py-1 text-xs text-white dark:bg-zinc-200 dark:text-zinc-900"
                        >
                          Save
                        </button>
                        <button
                          type="button"
                          disabled={sqlRunsSaving}
                          onClick={() => setSqlEditingId(null)}
                          className="rounded border border-zinc-300 px-2 py-1 text-xs dark:border-zinc-600"
                        >
                          Cancel
                        </button>
                      </td>
                    </tr>
                  ) : (
                    <tr key={row.id} className="border-b border-zinc-100 dark:border-zinc-800">
                      <td className="px-2 py-2 align-middle">
                        <div className="flex flex-col items-center gap-0.5">
                          <span className="text-xs tabular-nums text-zinc-500">{row.run_order}</span>
                          <div className="flex gap-0.5">
                            <button
                              type="button"
                              title="Move up"
                              disabled={sqlRunsSaving || i === 0}
                              onClick={() => reorderSqlRun(row.id, 'up')}
                              className="rounded border border-zinc-300 px-1.5 py-0 text-xs leading-none disabled:opacity-40 dark:border-zinc-600"
                            >
                              ↑
                            </button>
                            <button
                              type="button"
                              title="Move down"
                              disabled={sqlRunsSaving || i === sqlRuns.length - 1}
                              onClick={() => reorderSqlRun(row.id, 'down')}
                              className="rounded border border-zinc-300 px-1.5 py-0 text-xs leading-none disabled:opacity-40 dark:border-zinc-600"
                            >
                              ↓
                            </button>
                          </div>
                        </div>
                      </td>
                      <td className="px-2 py-2 align-top font-mono text-xs text-zinc-900 dark:text-zinc-100">
                        {row.sql_name}
                      </td>
                      <td className="max-w-[200px] px-2 py-2 align-top text-xs text-zinc-600 dark:text-zinc-400">
                        {row.sql_description ?? '—'}
                      </td>
                      <td className="max-w-md px-2 py-2 align-top">
                        <pre className="whitespace-pre-wrap break-all font-mono text-xs text-zinc-800 dark:text-zinc-200">
                          {row.sql_command}
                        </pre>
                      </td>
                      <td className="px-2 py-2 align-top text-right">
                        <button
                          type="button"
                          disabled={sqlRunsSaving}
                          onClick={() => startSqlEdit(row)}
                          className="mr-1 rounded border border-zinc-300 px-2 py-1 text-xs dark:border-zinc-600"
                        >
                          Edit
                        </button>
                        <button
                          type="button"
                          disabled={sqlRunsSaving || sqlRunsSingleLoadingId !== null}
                          onClick={() =>
                            setSqlConfirmModal({ kind: 'runOne', id: row.id, name: row.sql_name })
                          }
                          className="mr-1 rounded bg-emerald-800 px-2 py-1 text-xs text-white hover:bg-emerald-900 disabled:opacity-50 dark:bg-emerald-700 dark:hover:bg-emerald-600"
                        >
                          {sqlRunsSingleLoadingId === row.id ? 'Running…' : 'Run'}
                        </button>
                        <button
                          type="button"
                          disabled={sqlRunsSaving}
                          onClick={() =>
                            setSqlConfirmModal({ kind: 'delete', id: row.id, name: row.sql_name })
                          }
                          className="rounded border border-red-300 px-2 py-1 text-xs text-red-800 dark:border-red-800 dark:text-red-300"
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  )
                )}
              </tbody>
            </table>
            {!sqlRunsLoading && sqlRuns.length === 0 && (
              <p className="p-4 text-sm text-zinc-500">No rows yet. Add one above or run the table DDL.</p>
            )}
          </div>

          {sqlConfirmModal && (
            <div
              className="fixed inset-0 z-[60] flex items-center justify-center bg-black/50 p-4"
              role="dialog"
              aria-modal="true"
              onClick={(e) => e.target === e.currentTarget && setSqlConfirmModal(null)}
            >
              <div
                className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-lg border border-zinc-200 bg-white shadow-xl dark:border-zinc-600 dark:bg-zinc-900"
                onClick={(e) => e.stopPropagation()}
              >
                <div className="border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
                  <h3 className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
                    {sqlConfirmModal.kind === 'delete' && 'Delete SQL run?'}
                    {sqlConfirmModal.kind === 'runAll' && 'Run all SQL statements?'}
                    {sqlConfirmModal.kind === 'runOne' && 'Run this SQL statement?'}
                  </h3>
                </div>
                <div className="space-y-2 px-4 py-4 text-sm text-zinc-700 dark:text-zinc-300">
                  {sqlConfirmModal.kind === 'delete' && (
                    <p>
                      Remove <span className="font-mono font-medium">{sqlConfirmModal.name}</span>? This cannot be
                      undone.
                    </p>
                  )}
                  {sqlConfirmModal.kind === 'runAll' && (
                    <p>
                      Run <strong className="tabular-nums">{sqlConfirmModal.total}</strong> stored statement(s) on the
                      database in order. This cannot be undone.
                    </p>
                  )}
                  {sqlConfirmModal.kind === 'runOne' && (
                    <p>
                      Execute only <span className="font-mono font-medium">{sqlConfirmModal.name}</span> on the
                      database. This cannot be undone.
                    </p>
                  )}
                </div>
                <div className="flex justify-end gap-2 border-t border-zinc-200 px-4 py-3 dark:border-zinc-700">
                  <button
                    type="button"
                    onClick={() => setSqlConfirmModal(null)}
                    disabled={sqlRunsSaving || sqlRunsRunLoading || sqlRunsSingleLoadingId !== null}
                    className="rounded border border-zinc-300 bg-white px-4 py-2 text-sm font-medium text-zinc-800 hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700"
                  >
                    Cancel
                  </button>
                  {sqlConfirmModal.kind === 'delete' && (
                    <button
                      type="button"
                      onClick={() => executeDeleteSqlRun(sqlConfirmModal.id)}
                      disabled={sqlRunsSaving}
                      className="rounded bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700 disabled:opacity-50"
                    >
                      {sqlRunsSaving ? 'Deleting…' : 'Delete'}
                    </button>
                  )}
                  {sqlConfirmModal.kind === 'runAll' && (
                    <button
                      type="button"
                      onClick={() => executeRunAllSqlRuns()}
                      disabled={sqlRunsRunLoading}
                      className="rounded bg-amber-700 px-4 py-2 text-sm font-medium text-white hover:bg-amber-800 disabled:opacity-50"
                    >
                      {sqlRunsRunLoading ? 'Running…' : 'Run all'}
                    </button>
                  )}
                  {sqlConfirmModal.kind === 'runOne' && (
                    <button
                      type="button"
                      onClick={() => executeRunOneSqlRun(sqlConfirmModal.id)}
                      disabled={sqlRunsSingleLoadingId !== null}
                      className="rounded bg-emerald-700 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-800 disabled:opacity-50"
                    >
                      {sqlRunsSingleLoadingId === sqlConfirmModal.id ? 'Running…' : 'Run'}
                    </button>
                  )}
                </div>
              </div>
            </div>
          )}

          {sqlRunLogModalOpen && sqlRunsRunResult && (
            <div
              className="fixed inset-0 z-[61] flex items-center justify-center bg-black/50 p-4"
              role="dialog"
              aria-modal="true"
              aria-labelledby="sql-run-log-title"
              onClick={(e) => e.target === e.currentTarget && setSqlRunLogModalOpen(false)}
            >
              <div
                className="flex max-h-[90vh] w-full max-w-3xl flex-col overflow-hidden rounded-lg border border-zinc-200 bg-white shadow-xl dark:border-zinc-600 dark:bg-zinc-900"
                onClick={(e) => e.stopPropagation()}
              >
                <div className="border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
                  <h3 id="sql-run-log-title" className="text-lg font-medium text-zinc-900 dark:text-zinc-100">
                    Run log
                  </h3>
                  {sqlRunsRunResult.ranAt && (
                    <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
                      Server time:{' '}
                      <span className="font-mono">{new Date(sqlRunsRunResult.ranAt).toLocaleString()}</span>
                    </p>
                  )}
                </div>
                <div className="min-h-0 flex-1 overflow-y-auto px-4 py-3">
                  {sqlRunsRunResult.summary && (
                    <p
                      className={`mb-4 rounded-md px-3 py-2 text-sm font-medium ${
                        sqlRunsRunResult.ok === true
                          ? 'border border-green-200 bg-green-50 text-green-900 dark:border-green-800 dark:bg-green-950/50 dark:text-green-100'
                          : 'border border-amber-200 bg-amber-50 text-amber-950 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-100'
                      }`}
                    >
                      {sqlRunsRunResult.summary}
                    </p>
                  )}
                  {Array.isArray(sqlRunsRunResult.steps) && sqlRunsRunResult.steps.length > 0 ? (
                    <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                      <table className="w-full min-w-[560px] border-collapse text-left text-sm">
                        <thead>
                          <tr className="border-b border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-800/80">
                            <th className="px-2 py-2 font-semibold text-zinc-700 dark:text-zinc-200">Name</th>
                            <th className="px-2 py-2 font-semibold text-zinc-700 dark:text-zinc-200">Status</th>
                            <th className="px-2 py-2 font-semibold text-zinc-700 dark:text-zinc-200">Command</th>
                            <th className="px-2 py-2 font-semibold text-zinc-700 dark:text-zinc-200">Rows / detail</th>
                          </tr>
                        </thead>
                        <tbody>
                          {sqlRunsRunResult.steps.map((s) => (
                            <tr key={`${s.id}-${s.sql_name}`} className="border-b border-zinc-100 dark:border-zinc-800">
                              <td className="px-2 py-2 align-top font-mono text-xs text-zinc-900 dark:text-zinc-100">
                                {s.sql_name}
                              </td>
                              <td className="px-2 py-2 align-top">
                                {s.ok ? (
                                  <span className="text-green-700 dark:text-green-400">OK</span>
                                ) : (
                                  <span className="text-red-700 dark:text-red-400">Failed</span>
                                )}
                              </td>
                              <td className="px-2 py-2 align-top font-mono text-xs text-zinc-600 dark:text-zinc-400">
                                {s.command ?? '—'}
                              </td>
                              <td className="px-2 py-2 align-top text-xs text-zinc-800 dark:text-zinc-200">
                                {s.ok ? (
                                  <span>
                                    {s.summary ??
                                      (s.rowCount != null ? `${s.rowCount} row(s)` : '—')}
                                  </span>
                                ) : (
                                  <span className="text-red-700 dark:text-red-300">{s.error ?? '—'}</span>
                                )}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="text-sm text-zinc-600 dark:text-zinc-400">No step details.</p>
                  )}
                </div>
                <div className="border-t border-zinc-200 px-4 py-3 text-right dark:border-zinc-700">
                  <button
                    type="button"
                    onClick={() => setSqlRunLogModalOpen(false)}
                    className="rounded bg-zinc-800 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 dark:bg-zinc-200 dark:text-zinc-900 dark:hover:bg-zinc-300"
                  >
                    Close
                  </button>
                </div>
              </div>
            </div>
          )}
        </section>
        )}
      </div>
    </div>
  );
}

export default function DataChecksPage() {
  return (
    <Suspense fallback={<div className="min-h-screen bg-zinc-50 dark:bg-zinc-950 flex items-center justify-center"><p className="text-zinc-500 dark:text-zinc-400">Loading…</p></div>}>
      <DataChecksPageContent />
    </Suspense>
  );
}

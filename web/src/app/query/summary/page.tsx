'use client';

import React, { Suspense, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { useViewMode } from '@/contexts/ViewModeContext';
import { useSummaryHistory } from '@/contexts/SummaryHistoryContext';
import { getSummaryHistoryEntry, type SummaryHistoryPayload } from '@/lib/summary-history-storage';

type Row = Record<string, unknown>;

const SORT_SETTING_TYPE = 'System';
const SORT_SETTING_NAME = 'SummarySort';

const LEAD_COLUMNS = [
  { key: 'Customer', label: 'Customer' },
  { key: 'job_id', label: 'Job ID' },
  { key: 'worker', label: 'Worker' },
  { key: 'trailermode', label: 'TT' },
  { key: 'delivery_winery', label: 'Winery' },
  { key: 'vineyard_name', label: 'Vineyard' },
] as const;

/** By Job table: lead columns with Limits (X/-) to the right of TT. */
const BY_JOB_LEAD_COLUMNS = [
  { key: 'Customer', label: 'Customer' },
  { key: 'job_id', label: 'Job ID' },
  { key: 'worker', label: 'Worker' },
  { key: 'trailermode', label: 'TT' },
  { key: 'limits_breached', label: 'Limits' },
  { key: 'delivery_winery', label: 'Winery' },
  { key: 'vineyard_name', label: 'Vineyard' },
  { key: 'vineyard_group', label: 'Vineyard group' },
] as const;

const STEP_NUMS = [1, 2, 3, 4, 5] as const;

const STEP_LABELS: Record<number, string> = {
  1: '1. Start',
  2: '2. Travel To Vineyard',
  3: '3. Time In Vineyard',
  4: '4. Travel To Winery',
  5: '5. Unloading Winery',
};

/** Alternating background for the 5 step column groups (1,3,5 = A; 2,4 = B) for visual separation. */
const STEP_GROUP_BG: Record<number, string> = {
  1: 'bg-sky-50 dark:bg-sky-900/25',
  2: 'bg-amber-50 dark:bg-amber-900/25',
  3: 'bg-sky-50 dark:bg-sky-900/25',
  4: 'bg-amber-50 dark:bg-amber-900/25',
  5: 'bg-sky-50 dark:bg-sky-900/25',
};

function formatCell(v: unknown): string {
  if (v == null || v === '') return '—';
  if (typeof v === 'string') return v.trim() || '—';
  if (v instanceof Date) return v.toISOString().replace('T', ' ').slice(0, 19);
  return String(v);
}

/** Parse timestamp (ISO or YYYY-MM-DD HH:mm:ss) to ms. Returns null if invalid. */
function parseToMs(v: unknown): number | null {
  if (v == null || v === '') return null;
  const s = typeof v === 'string' ? v.trim() : v instanceof Date ? v.toISOString() : String(v);
  if (!s) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d.getTime();
}

/** Format as dd/mm for date-only, or dd/mm HH:mm for timestamps (compact). */
function formatDateDDMM(v: unknown, includeTime = true): string {
  if (v == null || v === '') return '—';
  const s = typeof v === 'string' ? v.trim() : v instanceof Date ? v.toISOString().replace('T', ' ') : String(v);
  if (!s) return '—';
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2})?:?(\d{2})?:?(\d{2})?)?/);
  if (!m) return '—';
  const [, y, mo, d, h = '00', min = '00', sec = '00'] = m;
  const dd = d!.padStart(2, '0');
  const mm = mo!.padStart(2, '0');
  if (!includeTime) return `${dd}/${mm}`;
  return `${dd}/${mm} ${h!.padStart(2, '0')}:${min.padStart(2, '0')}`;
}

/** Minutes between two timestamps (step_x - step_x-1). Returns null if either missing. */
function minsBetween(row: Row, stepPrev: number, stepCur: number): number | null {
  const prevKey = `step_${stepPrev}_actual_time`;
  const curKey = `step_${stepCur}_actual_time`;
  const prevVal = row[prevKey] ?? row[`step_${stepPrev}_gps_completed_at`] ?? row[`step_${stepPrev}_completed_at`];
  const curVal = row[curKey] ?? row[`step_${stepCur}_gps_completed_at`] ?? row[`step_${stepCur}_completed_at`];
  const prevMs = parseToMs(prevVal);
  const curMs = parseToMs(curVal);
  if (prevMs == null || curMs == null) return null;
  return Math.round((curMs - prevMs) / 60000);
}

/** Travel = sum of Mins for steps 2 and 4. */
function travelMins(row: Row): number | null {
  const m2 = minsBetween(row, 1, 2);
  const m4 = minsBetween(row, 3, 4);
  if (m2 == null && m4 == null) return null;
  return (m2 ?? 0) + (m4 ?? 0);
}

/** Total = sum of Mins for steps 2, 3, 4, 5. */
function totalMins(row: Row): number | null {
  const m2 = minsBetween(row, 1, 2);
  const m3 = minsBetween(row, 2, 3);
  const m4 = minsBetween(row, 3, 4);
  const m5 = minsBetween(row, 4, 5);
  if (m2 == null && m3 == null && m4 == null && m5 == null) return null;
  return (m2 ?? 0) + (m3 ?? 0) + (m4 ?? 0) + (m5 ?? 0);
}

/** Row-level outlier flag on Job ID / Daily date (not per-cell; keeps limit reds on cells). */
const SUMMARY_OUTLIER_RED = 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200';
const SUMMARY_OUTLIER_YELLOW = 'bg-yellow-200 text-yellow-900 dark:bg-yellow-900/35 dark:text-yellow-100';

/** Minute columns aligned with By Job: steps 2–5, travel, total. */
function jobRowMinuteValues(row: Row): number[] {
  const out: number[] = [];
  for (let n = 2; n <= 5; n++) {
    const m = minsBetween(row, n - 1, n);
    if (m != null) out.push(m);
  }
  const t = travelMins(row);
  if (t != null) out.push(t);
  const tot = totalMins(row);
  if (tot != null) out.push(tot);
  return out;
}

/** Any &lt; 0 → red; else any &lt; 5 → yellow. */
function timeOutlierSeverity(values: number[]): 'red' | 'yellow' | null {
  if (values.length === 0) return null;
  if (values.some((v) => v < 0)) return 'red';
  if (values.some((v) => v < 5)) return 'yellow';
  return null;
}

type RollupQuads = {
  jobs: Row[];
  mins_2: { total: number; max: number; min: number; av: number };
  mins_3: { total: number; max: number; min: number; av: number };
  mins_4: { total: number; max: number; min: number; av: number };
  mins_5: { total: number; max: number; min: number; av: number };
  travel: { total: number; max: number; min: number; av: number };
  total: { total: number; max: number; min: number; av: number };
};

function dayRollupOutlierSeverity(r: RollupQuads): 'red' | 'yellow' | null {
  if (r.jobs.length === 0) return null;
  const keys = ['mins_2', 'mins_3', 'mins_4', 'mins_5', 'travel', 'total'] as const;
  const vals: number[] = [];
  for (const k of keys) {
    const q = r[k];
    vals.push(q.total, q.max, q.min, q.av);
  }
  return timeOutlierSeverity(vals);
}

function footerStatsOutlierSeverity(
  stats: Record<string, { total: number; max: number; min: number; av: number }>,
  jobCount: number,
): 'red' | 'yellow' | null {
  if (jobCount === 0) return null;
  const vals: number[] = [];
  for (const q of Object.values(stats)) {
    vals.push(q.total, q.max, q.min, q.av);
  }
  return timeOutlierSeverity(vals);
}

function compare(a: unknown, b: unknown): number {
  const va = a == null ? '' : a;
  const vb = b == null ? '' : b;
  if (va === vb) return 0;
  if (typeof va === 'number' && typeof vb === 'number') return va - vb;
  return String(va).localeCompare(String(vb));
}

/** Resolve sort value for a row by key (handles mins_*, travel, total, Customer, limits_breached). */
function sortValueFor(row: Row, key: string): unknown {
  const minsMatch = key.match(/^mins_(\d+)$/);
  if (minsMatch) return minsBetween(row, parseInt(minsMatch[1], 10) - 1, parseInt(minsMatch[1], 10));
  if (key === 'travel') return travelMins(row);
  if (key === 'in_vineyard') return minsBetween(row, 2, 3);
  if (key === 'in_winery') return minsBetween(row, 4, 5);
  if (key === 'total') return totalMins(row);
  if (key === 'Customer') return row['Customer'] ?? row['customer'];
  if (key === 'limits_breached') return row.limits_breached ?? '-';
  return row[key];
}

function inspectJumpMetaForRow(row: Row): { jobId: string; truckId: string } | null {
  const jobId = row.job_id != null ? String(row.job_id).trim() : '';
  if (!jobId) return null;
  const truckId = row.truck_id != null ? String(row.truck_id).trim() : '';
  return { jobId, truckId };
}

/** Inspect: locate job + optional truck only (no date filters; Inspect clears UI filters when opening this link). */
function inspectUrlForRow(row: Row): string {
  const meta = inspectJumpMetaForRow(row);
  if (!meta) return '/query/inspect';
  const params = new URLSearchParams();
  params.set('locateJobId', meta.jobId);
  if (meta.truckId) params.set('truckId', meta.truckId);
  return `/query/inspect?${params.toString()}`;
}

function SummaryPageInner() {
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filterActualFrom, setFilterActualFrom] = useState('');
  const [filterActualTo, setFilterActualTo] = useState('');
  // Customer is always taken from sidebar (clientCustomer); no local dropdown to avoid disconnect in admin/super view
  const [filterTemplate, setFilterTemplate] = useState('');
  const [filterTruckId, setFilterTruckId] = useState('');
  const [filterWorker, setFilterWorker] = useState('');
  const [filterTrailermode, setFilterTrailermode] = useState('');
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [sortColumns, setSortColumns] = useState<[string, string, string]>(['', '', '']);
  const sortColumnsInitialized = useRef(false);
  const [sortSaveStatus, setSortSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const { viewMode, clientCustomer, setClientCustomer, clientCustomerLocked } = useViewMode();
  const router = useRouter();
  const searchParams = useSearchParams();
  const { registerSummarySnapshot, registerPendingInspectForHistory } = useSummaryHistory();
  /** Used when applying session restore (layout runs before locked state is wrong on first paint). */
  const clientCustomerLockedRef = useRef(clientCustomerLocked);
  clientCustomerLockedRef.current = clientCustomerLocked;
  /** Customer always mirrors sidebar selection (single source of truth; avoids disconnect in admin/super). */
  const effectiveCustomer = clientCustomer;
  const [summaryTab, setSummaryTab] = useState<'season' | 'by_day' | 'by_job'>('season');
  const [filterWinery, setFilterWinery] = useState('');
  const [filterVineyardGroup, setFilterVineyardGroup] = useState('');
  const [filterVineyard, setFilterVineyard] = useState('');
  const [splitByLimits, setSplitByLimits] = useState(false);
  const [minsThresholds, setMinsThresholds] = useState<Record<string, string>>({
    '2': '', '3': '', '4': '', '5': '', travel: '', in_vineyard: '', in_winery: '', total: '',
  });
  const setMinsThreshold = (key: string, value: string) => {
    setMinsThresholds((prev) => ({ ...prev, [key]: value }));
  };
  /** Time limit rows for the selected customer (from tbl_wineryminutes), shown in Time Limits section. */
  type TimeLimitRow = {
    id: number;
    Customer?: string | null;
    Template?: string | null;
    vineyardgroup?: string | null;
    Winery?: string | null;
    TT?: string | null;
    ToVineMins?: number | null;
    InVineMins?: number | null;
    ToWineMins?: number | null;
    InWineMins?: number | null;
    TotalMins?: number | null;
  };
  const [timeLimitRows, setTimeLimitRows] = useState<TimeLimitRow[]>([]);
  /** When user clicks a Time Limits row, we show that row's limits in the header white boxes (for info only). */
  const [selectedTimeLimitRowId, setSelectedTimeLimitRowId] = useState<number | null>(null);
  /** Show/hide the Time Limits table (can get in the way). */
  const [showLimitsTable, setShowLimitsTable] = useState(true);
  const [jobsPage, setJobsPage] = useState(0);
  const [jobsPageSize] = useState(500);
  const [totalJobsFromApi, setTotalJobsFromApi] = useState(0);
  /** Same WHERE as /api/vworkjobs for the current filters (for season/daily footer). */
  const [jobsQueryDebug, setJobsQueryDebug] = useState<{
    debugSql: string;
    debugSqlParams: unknown[];
    debugSqlLiteral: string;
  } | null>(null);

  /** After sidebar history restore (?sh=): suppress jobs reset + cascade wipes (same as Client hydrate). */
  const jobsPageResetSuppressCountRef = useRef(0);
  const skipCustomerCascadeRef = useRef(false);
  const skipTemplateCascadeRef = useRef(false);
  const appliedHistoryIdRef = useRef<string | null>(null);
  /** After ?sh= restore: scroll/highlight this job row once it appears in By Job. */
  const pendingScrollToJobIdRef = useRef<string | null>(null);
  const [focusHighlightJobId, setFocusHighlightJobId] = useState<string | null>(null);
  /** Scroll parent for By Job table (`max-h-[70vh] overflow-auto`); `scrollIntoView` on the row is unreliable here. */
  const byJobTableScrollRef = useRef<HTMLDivElement | null>(null);
  /** Avoid treating context filling `'' → customer` (Client refreshUser) as a user customer change. */
  const prevEffectiveCustomerForCascadeRef = useRef<string | null>(null);
  /** Avoid first template effect run wiping winery after programmatic restore. */
  const prevFilterTemplateForCascadeRef = useRef<string | null>(null);

  const shParam = searchParams.get('sh');

  useLayoutEffect(() => {
    const id = shParam?.trim() ?? '';
    if (!id) {
      appliedHistoryIdRef.current = null;
      pendingScrollToJobIdRef.current = null;
      return;
    }
    if (appliedHistoryIdRef.current === id) return;
    const entry = getSummaryHistoryEntry(id);
    if (!entry) {
      router.replace('/query/summary');
      return;
    }
    appliedHistoryIdRef.current = id;
    jobsPageResetSuppressCountRef.current = clientCustomerLockedRef.current ? 2 : 1;
    skipCustomerCascadeRef.current = true;
    skipTemplateCascadeRef.current = true;
    const s = entry.payload;
    if (!clientCustomerLockedRef.current && s.clientCustomer !== undefined) {
      setClientCustomer(s.clientCustomer);
    }
    setFilterActualFrom(s.filterActualFrom ?? '');
    setFilterActualTo(s.filterActualTo ?? '');
    setFilterTemplate(s.filterTemplate ?? '');
    setFilterTruckId(s.filterTruckId ?? '');
    setFilterWorker(s.filterWorker ?? '');
    setFilterTrailermode(s.filterTrailermode ?? '');
    setSummaryTab(s.summaryTab ?? 'season');
    setFilterWinery(s.filterWinery ?? '');
    setFilterVineyardGroup(s.filterVineyardGroup ?? '');
    setFilterVineyard(s.filterVineyard ?? '');
    setSplitByLimits(Boolean(s.splitByLimits));
    if (s.minsThresholds && typeof s.minsThresholds === 'object') {
      setMinsThresholds((prev) => ({ ...prev, ...s.minsThresholds }));
    }
    setSelectedTimeLimitRowId(
      typeof s.selectedTimeLimitRowId === 'number' ? s.selectedTimeLimitRowId : null,
    );
    setShowLimitsTable(s.showLimitsTable !== false);
    setSortKey(s.sortKey ?? null);
    setSortDir(s.sortDir === 'desc' ? 'desc' : 'asc');
    if (Array.isArray(s.sortColumns) && s.sortColumns.length === 3) {
      const trip = s.sortColumns as [string, string, string];
      setSortColumns(trip);
      if (trip.some(Boolean)) sortColumnsInitialized.current = true;
    }
    if (typeof s.jobsPage === 'number' && s.jobsPage >= 0) setJobsPage(s.jobsPage);
    const fj = s.focusJobId != null ? String(s.focusJobId).trim() : '';
    pendingScrollToJobIdRef.current = fj || null;
    router.replace('/query/summary');
  }, [shParam, router, setClientCustomer]);

  /** Reset to page 0 when filters change (suppress after history restore / Client hydrate). */
  useEffect(() => {
    if (jobsPageResetSuppressCountRef.current > 0) {
      jobsPageResetSuppressCountRef.current -= 1;
      return;
    }
    setJobsPage(0);
  }, [effectiveCustomer, filterTemplate, filterActualFrom, filterActualTo, filterWinery, filterVineyardGroup, filterVineyard, filterTruckId, filterWorker, filterTrailermode]);

  useEffect(() => {
    const params = new URLSearchParams();
    if (effectiveCustomer.trim()) params.set('customer', effectiveCustomer.trim());
    if (filterTemplate.trim()) params.set('template', filterTemplate.trim());
    if (filterActualFrom?.trim()) params.set('dateFrom', filterActualFrom.trim().slice(0, 10));
    if (filterActualTo?.trim()) params.set('dateTo', filterActualTo.trim().slice(0, 10));
    if (filterWinery.trim()) params.set('winery', filterWinery.trim());
    if (filterVineyardGroup.trim()) params.set('vineyard_group', filterVineyardGroup.trim());
    if (filterVineyard.trim()) params.set('vineyard', filterVineyard.trim());
    if (filterTruckId.trim()) params.set('truck_id', filterTruckId.trim());
    if (filterWorker.trim()) params.set('worker', filterWorker.trim());
    if (filterTrailermode.trim()) params.set('trailermode', filterTrailermode.trim());
    /** Full result set for rollups; By Job tab paginates client-side (was capped at 500 rows server-side). */
    setLoading(true);
    fetch(`/api/vworkjobs?${params}`)
      .then(async (res) => {
        const data = await res.json();
        if (!res.ok) throw new Error(data?.error ?? res.statusText);
        return data;
      })
      .then((data) => {
        setRows(data.rows ?? []);
        setTotalJobsFromApi(typeof data.total === 'number' ? data.total : (data.rows ?? []).length);
        if (typeof data.debugSql === 'string' && Array.isArray(data.debugSqlParams)) {
          setJobsQueryDebug({
            debugSql: data.debugSql,
            debugSqlParams: data.debugSqlParams,
            debugSqlLiteral: typeof data.debugSqlLiteral === 'string' ? data.debugSqlLiteral : data.debugSql,
          });
        } else {
          setJobsQueryDebug(null);
        }
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [effectiveCustomer, filterTemplate, filterActualFrom, filterActualTo, filterWinery, filterVineyardGroup, filterVineyard, filterTruckId, filterWorker, filterTrailermode]);

  /** Keep latest Summary state for sidebar history when navigating away from /query/summary. */
  useEffect(() => {
    if (clientCustomerLocked && !effectiveCustomer.trim()) {
      registerSummarySnapshot(null);
      return;
    }
    const payload: SummaryHistoryPayload = {
      filterActualFrom,
      filterActualTo,
      filterTemplate,
      filterTruckId,
      filterWorker,
      filterTrailermode,
      summaryTab,
      filterWinery,
      filterVineyardGroup,
      filterVineyard,
      splitByLimits,
      minsThresholds: { ...minsThresholds },
      selectedTimeLimitRowId,
      showLimitsTable,
      sortKey,
      sortDir,
      sortColumns: [...sortColumns] as [string, string, string],
      jobsPage,
      clientCustomer: effectiveCustomer,
    };
    registerSummarySnapshot(payload);
  }, [
    registerSummarySnapshot,
    effectiveCustomer,
    clientCustomerLocked,
    filterActualFrom,
    filterActualTo,
    filterTemplate,
    filterTruckId,
    filterWorker,
    filterTrailermode,
    summaryTab,
    filterWinery,
    filterVineyardGroup,
    filterVineyard,
    splitByLimits,
    minsThresholds,
    selectedTimeLimitRowId,
    showLimitsTable,
    sortKey,
    sortDir,
    sortColumns,
    jobsPage,
  ]);

  const distinctTruckIds = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      const v = row.truck_id;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows]);

  /** Distinct workers in the current summary result set: rows matching customer (and template if set), winery, vineyard, truck, date range — so dropdown only shows workers that exist in the filtered data. */
  const distinctWorkers = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      if (effectiveCustomer) {
        const v = row.Customer ?? row.customer;
        if (v == null || String(v).trim() !== effectiveCustomer) continue;
      }
      if (filterTemplate) {
        const v = row.template;
        if (v == null || String(v).trim() !== filterTemplate) continue;
      }
      if (filterWinery) {
        const v = row.delivery_winery;
        if (v == null || String(v).trim() !== filterWinery) continue;
      }
      if (filterVineyardGroup) {
        const v = (row as Record<string, unknown>).vineyard_group;
        if (v == null || String(v).trim() !== filterVineyardGroup) continue;
      }
      if (filterVineyard) {
        const v = row.vineyard_name;
        if (v == null || String(v).trim() !== filterVineyard) continue;
      }
      if (filterTruckId) {
        const v = row.truck_id;
        if (v == null || String(v).trim() !== filterTruckId) continue;
      }
      if (filterActualFrom || filterActualTo) {
        const v = row.actual_start_time;
        if (v == null || v === '') continue;
        const str = String(v).trim();
        const datePart = str.match(/^(\d{4})-(\d{2})-(\d{2})/)?.[0] ?? '';
        if (!datePart) continue;
        const from = filterActualFrom ? filterActualFrom.slice(0, 10) : '';
        const to = filterActualTo ? filterActualTo.slice(0, 10) : '';
        if (from && datePart < from) continue;
        if (to && datePart > to) continue;
      }
      const w = row.worker;
      if (w != null && String(w).trim() !== '') set.add(String(w).trim());
    }
    return Array.from(set).sort();
  }, [rows, effectiveCustomer, filterTemplate, filterWinery, filterVineyardGroup, filterVineyard, filterTruckId, filterActualFrom, filterActualTo]);

  /** Distinct trailermode values in the current summary result set (same filters as distinctWorkers). */
  const distinctTrailermodes = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      if (effectiveCustomer) {
        const v = row.Customer ?? row.customer;
        if (v == null || String(v).trim() !== effectiveCustomer) continue;
      }
      if (filterTemplate) {
        const v = row.template;
        if (v == null || String(v).trim() !== filterTemplate) continue;
      }
      if (filterWinery) {
        const v = row.delivery_winery;
        if (v == null || String(v).trim() !== filterWinery) continue;
      }
      if (filterVineyardGroup) {
        const v = (row as Record<string, unknown>).vineyard_group;
        if (v == null || String(v).trim() !== filterVineyardGroup) continue;
      }
      if (filterVineyard) {
        const v = row.vineyard_name;
        if (v == null || String(v).trim() !== filterVineyard) continue;
      }
      if (filterTruckId) {
        const v = row.truck_id;
        if (v == null || String(v).trim() !== filterTruckId) continue;
      }
      if (filterActualFrom || filterActualTo) {
        const v = row.actual_start_time;
        if (v == null || v === '') continue;
        const str = String(v).trim();
        const datePart = str.match(/^(\d{4})-(\d{2})-(\d{2})/)?.[0] ?? '';
        if (!datePart) continue;
        const from = filterActualFrom ? filterActualFrom.slice(0, 10) : '';
        const to = filterActualTo ? filterActualTo.slice(0, 10) : '';
        if (from && datePart < from) continue;
        if (to && datePart > to) continue;
      }
      const t = row.trailermode;
      if (t != null && String(t).trim() !== '') set.add(String(t).trim());
    }
    return Array.from(set).sort();
  }, [rows, effectiveCustomer, filterTemplate, filterWinery, filterVineyardGroup, filterVineyard, filterTruckId, filterActualFrom, filterActualTo]);

  const distinctCustomers = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      const v = row.Customer ?? row.customer;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows]);

  /** Templates only for the effective customer (template dropdown is dependent on customer). */
  const distinctTemplates = useMemo(() => {
    if (!effectiveCustomer.trim()) return [];
    const set = new Set<string>();
    for (const row of rows) {
      const cust = row.Customer ?? row.customer;
      if (cust == null || String(cust).trim() !== effectiveCustomer) continue;
      const v = row.template;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows, effectiveCustomer]);

  /** Wineries (delivery_winery) only for the effective customer. */
  const distinctWineries = useMemo(() => {
    if (!effectiveCustomer.trim()) return [];
    const set = new Set<string>();
    for (const row of rows) {
      const cust = row.Customer ?? row.customer;
      if (cust == null || String(cust).trim() !== effectiveCustomer) continue;
      const v = row.delivery_winery;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows, effectiveCustomer]);

  /** Vineyard groups (vineyard_group from tbl_vworkjobs) only for the effective customer. */
  const distinctVineyardGroups = useMemo(() => {
    if (!effectiveCustomer.trim()) return [];
    const set = new Set<string>();
    for (const row of rows) {
      const cust = row.Customer ?? row.customer;
      if (cust == null || String(cust).trim() !== effectiveCustomer) continue;
      const v = (row as Record<string, unknown>).vineyard_group;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows, effectiveCustomer]);

  /** Vineyards (vineyard_name) only for the effective customer. */
  const distinctVineyards = useMemo(() => {
    if (!effectiveCustomer.trim()) return [];
    const set = new Set<string>();
    for (const row of rows) {
      const cust = row.Customer ?? row.customer;
      if (cust == null || String(cust).trim() !== effectiveCustomer) continue;
      const v = row.vineyard_name;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows, effectiveCustomer]);

  /**
   * Clear template, winery, … when the *user* changes customer in the sidebar.
   * Skip: (1) once after session restore; (2) transition empty → non-empty (Client: refreshUser runs after
   * our layout restore and would otherwise wipe restored template / tab).
   */
  useEffect(() => {
    if (skipCustomerCascadeRef.current) {
      skipCustomerCascadeRef.current = false;
      prevEffectiveCustomerForCascadeRef.current = effectiveCustomer;
      return;
    }
    const prev = prevEffectiveCustomerForCascadeRef.current;
    if (prev === null) {
      prevEffectiveCustomerForCascadeRef.current = effectiveCustomer;
      return;
    }
    if (prev === effectiveCustomer) return;
    if (prev.trim() === '' && effectiveCustomer.trim() !== '') {
      prevEffectiveCustomerForCascadeRef.current = effectiveCustomer;
      return;
    }
    prevEffectiveCustomerForCascadeRef.current = effectiveCustomer;
    setFilterTemplate('');
    setFilterWinery('');
    setFilterVineyardGroup('');
    setFilterVineyard('');
    setFilterWorker('');
    setFilterTrailermode('');
  }, [effectiveCustomer]);

  /** Clear winery, … when template changes (skip once after session restore; skip first mount). */
  useEffect(() => {
    if (skipTemplateCascadeRef.current) {
      skipTemplateCascadeRef.current = false;
      prevFilterTemplateForCascadeRef.current = filterTemplate;
      return;
    }
    const prev = prevFilterTemplateForCascadeRef.current;
    if (prev === null) {
      prevFilterTemplateForCascadeRef.current = filterTemplate;
      return;
    }
    if (prev === filterTemplate) return;
    prevFilterTemplateForCascadeRef.current = filterTemplate;
    setFilterWinery('');
    setFilterVineyardGroup('');
    setFilterWorker('');
    setFilterTrailermode('');
  }, [filterTemplate]);

  /** When Customer + Template + Winery (and optionally Vineyard group) are selected, load minute limits from tbl_wineryminutes (2=ToVine, 3=InVine, 4=ToWine, 5=InWine, travel=ToVine+ToWine, in_vineyard=InVine, in_winery=InWine, total=TotalMins). */
  useEffect(() => {
    if (!effectiveCustomer.trim() || !filterTemplate.trim() || !filterWinery.trim()) return;
    const params = new URLSearchParams({ customer: effectiveCustomer, template: filterTemplate, winery: filterWinery });
    if (filterVineyardGroup.trim()) params.set('vineyard_group', filterVineyardGroup.trim());
    fetch(`/api/admin/wineryminutes?${params}`)
      .then((r) => r.json())
      .then((row) => {
        if (!row || row.error) return;
        const toV = (row.ToVineMins ?? row.tovinemins) != null ? String(row.ToVineMins ?? row.tovinemins) : '';
        const inV = (row.InVineMins ?? row.invinemins) != null ? String(row.InVineMins ?? row.invinemins) : '';
        const toW = (row.ToWineMins ?? row.towinemins) != null ? String(row.ToWineMins ?? row.towinemins) : '';
        const inW = (row.InWineMins ?? row.inwinemins) != null ? String(row.InWineMins ?? row.inwinemins) : '';
        const tv = row.ToVineMins ?? row.tovinemins;
        const tw = row.ToWineMins ?? row.towinemins;
        const travelVal = (tv != null && tw != null) ? String(Number(tv) + Number(tw)) : '';
        const totalVal = (row.TotalMins ?? row.totalmins) != null ? String(row.TotalMins ?? row.totalmins) : '';
        setMinsThresholds({
          '2': toV,
          '3': inV,
          '4': toW,
          '5': inW,
          travel: travelVal,
          in_vineyard: inV,
          in_winery: inW,
          total: totalVal,
        });
      })
      .catch(() => {});
  }, [effectiveCustomer, filterTemplate, filterWinery, filterVineyardGroup]);

  /** When a customer is selected, load time limit rows for the Time Limits section (tbl_wineryminutes by customer; if template selected, filter on template too). */
  useEffect(() => {
    if (!effectiveCustomer.trim()) {
      setTimeLimitRows([]);
      return;
    }
    const params = new URLSearchParams({ customer: effectiveCustomer });
    if (filterTemplate.trim()) params.set('template', filterTemplate.trim());
    fetch(`/api/admin/wineryminutes?${params}`)
      .then((r) => r.json())
      .then((data) => {
        if (data?.rows && Array.isArray(data.rows)) setTimeLimitRows(data.rows);
        else setTimeLimitRows([]);
      })
      .catch(() => setTimeLimitRows([]));
  }, [effectiveCustomer, filterTemplate]);

  const filteredRows = useMemo(() => {
    return rows.filter((row) => {
      if (effectiveCustomer) {
        const v = row.Customer ?? row.customer;
        if (v == null || String(v).trim() !== effectiveCustomer) return false;
      }
      if (filterTemplate) {
        const v = row.template;
        if (v == null || String(v).trim() !== filterTemplate) return false;
      }
      if (filterWinery) {
        const v = row.delivery_winery;
        if (v == null || String(v).trim() !== filterWinery) return false;
      }
      if (filterVineyardGroup) {
        const v = (row as Record<string, unknown>).vineyard_group;
        if (v == null || String(v).trim() !== filterVineyardGroup) return false;
      }
      if (filterVineyard) {
        const v = row.vineyard_name;
        if (v == null || String(v).trim() !== filterVineyard) return false;
      }
      if (filterTruckId) {
        const v = row.truck_id;
        if (v == null || String(v).trim() !== filterTruckId) return false;
      }
      if (filterWorker.trim()) {
        const v = row.worker;
        const s = v != null ? String(v).trim().toLowerCase() : '';
        if (!s.includes(filterWorker.trim().toLowerCase())) return false;
      }
      if (filterTrailermode) {
        const v = row.trailermode;
        if (v == null || String(v).trim() !== filterTrailermode) return false;
      }
      if (filterActualFrom || filterActualTo) {
        const v = row.actual_start_time;
        if (v == null || v === '') return false;
        const str = String(v).trim();
        const datePart = str.match(/^(\d{4})-(\d{2})-(\d{2})/)?.[0] ?? '';
        if (!datePart) return false;
        const from = filterActualFrom ? filterActualFrom.slice(0, 10) : '';
        const to = filterActualTo ? filterActualTo.slice(0, 10) : '';
        if (from && datePart < from) return false;
        if (to && datePart > to) return false;
      }
      return true;
    });
  }, [rows, effectiveCustomer, filterTemplate, filterWinery, filterVineyardGroup, filterVineyard, filterTruckId, filterWorker, filterTrailermode, filterActualFrom, filterActualTo]);

  /** Find the time limit row that matches this job's delivery_winery, vineyard_group, and trailer type (tbl_vworkjobs = tbl_wineryminutes; customer/template already applied to timeLimitRows). Limit row with null/empty vineyardgroup matches any job vineyard_group. If no T or TT row exists for the job's trailermode, fall back to a TTT row (some wineries only have TTT). */
  const getMatchingLimitsRow = (job: Row): TimeLimitRow | null => {
    const winery = (job.delivery_winery ?? '').toString().trim();
    if (!winery) return null;
    const jobTT = (job.trailermode ?? job.trailertype ?? '').toString().trim();
    const jobVg = ((job as Record<string, unknown>).vineyard_group ?? '').toString().trim();
    const match = (r: TimeLimitRow) => {
      if ((r.Winery ?? '').toString().trim() !== winery) return false;
      const limitVg = (r.vineyardgroup ?? '').toString().trim();
      if (limitVg !== '' && limitVg !== jobVg) return false;
      return true;
    };
    const exact = timeLimitRows.find((r) => match(r) && (r.TT ?? '').toString().trim() === jobTT);
    if (exact) return exact;
    if (jobTT === 'T' || jobTT === 'TT') {
      const tttRow = timeLimitRows.find((r) => match(r) && (r.TT ?? '').toString().trim() === 'TTT');
      if (tttRow) return tttRow;
    }
    return null;
  };

  /** Get limit value from a limits row for a given key. Travel_ = ToVine + ToWine. */
  const getLimitFromRow = (lim: TimeLimitRow | null, key: string): number | null => {
    if (!lim) return null;
    if (key === '2') return lim.ToVineMins ?? null;
    if (key === '3' || key === 'in_vineyard') return lim.InVineMins ?? null;
    if (key === '4') return lim.ToWineMins ?? null;
    if (key === '5' || key === 'in_winery') return lim.InWineMins ?? null;
    if (key === 'travel') {
      const toV = lim.ToVineMins != null ? Number(lim.ToVineMins) : null;
      const toW = lim.ToWineMins != null ? Number(lim.ToWineMins) : null;
      if (toV == null && toW == null) return null;
      const sum = (toV ?? 0) + (toW ?? 0);
      return Number.isNaN(sum) ? null : sum;
    }
    if (key === 'total') return lim.TotalMins ?? null;
    return null;
  };

  /** True if value exceeds the limit from the given limits row (used for per-job coloring by delivery_winery). */
  const isOverLimitWithRow = (val: number | null, lim: TimeLimitRow | null, key: string): boolean => {
    const limit = getLimitFromRow(lim, key);
    if (val == null || limit == null) return false;
    return val > limit;
  };

  /** For step columns 3 and 5 we check both step key and in_vineyard/in_winery. */
  const isOverLimitStepWithRow = (val: number | null, lim: TimeLimitRow | null, stepNum: number): boolean => {
    if (stepNum === 3) return isOverLimitWithRow(val, lim, '3') || isOverLimitWithRow(val, lim, 'in_vineyard');
    if (stepNum === 5) return isOverLimitWithRow(val, lim, '5') || isOverLimitWithRow(val, lim, 'in_winery');
    return isOverLimitWithRow(val, lim, String(stepNum));
  };

  /** For Limits column: X if job total mins exceeds winery total limit, else -. */
  const limitsBreachedForRow = (job: Row): 'X' | '-' => {
    const lim = getMatchingLimitsRow(job);
    const totalLimit = getLimitFromRow(lim, 'total');
    const jobTotal = totalMins(job);
    if (totalLimit == null || jobTotal == null) return '-';
    return jobTotal > totalLimit ? 'X' : '-';
  };

  /** Rows with computed limits_breached for sorting and display. */
  const rowsWithLimits = useMemo((): (Row & { limits_breached: '-' | 'X' })[] =>
    filteredRows.map((r) => ({ ...r, limits_breached: limitsBreachedForRow(r) })), [filteredRows, timeLimitRows]);

  /** Get YYYY-MM-DD from row actual_start_time for By Day rollup. */
  const getRowDate = (row: Row): string | null => {
    const v = row.actual_start_time ?? row.step_1_actual_time ?? row.step_1_gps_completed_at ?? row.step_1_completed_at ?? '';
    if (v == null || v === '') return null;
    const s = String(v).trim();
    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    return m ? `${m[1]}-${m[2]}-${m[3]}` : null;
  };

  /** By Day: date range from min to max actual_start_time in filtered rows. */
  const dayRange = useMemo(() => {
    let minStr: string | null = null;
    let maxStr: string | null = null;
    for (const row of filteredRows) {
      const d = getRowDate(row);
      if (d) {
        if (minStr == null || d < minStr) minStr = d;
        if (maxStr == null || d > maxStr) maxStr = d;
      }
    }
    return { min: minStr, max: maxStr };
  }, [filteredRows]);

  type MinsQuad = { total: number; max: number; min: number; av: number };

  /** By Day: one row per day, or when splitByLimits three rows per day (Over Limit, Within Limit, Daily Total). */
  const rowsByDay = useMemo(() => {
    const { min, max } = dayRange;
    if (min == null || max == null || min > max) return [];
    const fromValues = (values: number[]): MinsQuad => {
      const valid = values.filter((v) => v != null && !Number.isNaN(v));
      const n = valid.length;
      const total = valid.reduce((a, b) => a + b, 0);
      return {
        total,
        max: n ? Math.max(...valid) : 0,
        min: n ? Math.min(...valid) : 0,
        av: n ? Math.round(total / n) : 0,
      };
    };
    type DayRow = {
      date: string; dateLabel: string; rowType?: 'Over Limit' | 'Within Limit' | 'Daily Total'; jobs: Row[];
      mins_2: MinsQuad; mins_3: MinsQuad; mins_4: MinsQuad; mins_5: MinsQuad; travel: MinsQuad; total: MinsQuad;
    };
    const out: DayRow[] = [];
    const [minY, minMo, minDay] = min.split('-').map(Number);
    const [maxY, maxMo, maxDay] = max.split('-').map(Number);
    const minT = new Date(minY, minMo - 1, minDay).getTime();
    const maxT = new Date(maxY, maxMo - 1, maxDay).getTime();
    const sourceRows = splitByLimits ? rowsWithLimits : filteredRows;
    for (let t = minT; t <= maxT; t += 86400000) {
      const d = new Date(t);
      const y = d.getFullYear();
      const mo = String(d.getMonth() + 1).padStart(2, '0');
      const day = String(d.getDate()).padStart(2, '0');
      const dateStr = `${y}-${mo}-${day}`;
      const dateLabel = `${day}/${mo}/${String(y).slice(2)}`;
      const dayJobs = sourceRows.filter((row) => getRowDate(row) === dateStr);
      if (splitByLimits) {
        const overJobs = dayJobs.filter((row) => (row as Row & { limits_breached?: string }).limits_breached === 'X');
        const withinJobs = dayJobs.filter((row) => (row as Row & { limits_breached?: string }).limits_breached === '-');
        for (const { label, jobs } of [
          { label: 'Over Limit' as const, jobs: overJobs },
          { label: 'Within Limit' as const, jobs: withinJobs },
          { label: 'Daily Total' as const, jobs: dayJobs },
        ]) {
          const m2vals = jobs.map((row) => minsBetween(row, 1, 2)).filter((v): v is number => v != null);
          const m3vals = jobs.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null);
          const m4vals = jobs.map((row) => minsBetween(row, 3, 4)).filter((v): v is number => v != null);
          const m5vals = jobs.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null);
          const travelVals = jobs.map((row) => travelMins(row)).filter((v): v is number => v != null);
          const totalVals = jobs.map((row) => totalMins(row)).filter((v): v is number => v != null);
          out.push({
            date: dateStr,
            dateLabel,
            rowType: label,
            jobs,
            mins_2: fromValues(m2vals),
            mins_3: fromValues(m3vals),
            mins_4: fromValues(m4vals),
            mins_5: fromValues(m5vals),
            travel: fromValues(travelVals),
            total: fromValues(totalVals),
          });
        }
      } else {
        const jobs = dayJobs;
        const m2vals = jobs.map((row) => minsBetween(row, 1, 2)).filter((v): v is number => v != null);
        const m3vals = jobs.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null);
        const m4vals = jobs.map((row) => minsBetween(row, 3, 4)).filter((v): v is number => v != null);
        const m5vals = jobs.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null);
        const travelVals = jobs.map((row) => travelMins(row)).filter((v): v is number => v != null);
        const totalVals = jobs.map((row) => totalMins(row)).filter((v): v is number => v != null);
        out.push({
          date: dateStr,
          dateLabel,
          jobs,
          mins_2: fromValues(m2vals),
          mins_3: fromValues(m3vals),
          mins_4: fromValues(m4vals),
          mins_5: fromValues(m5vals),
          travel: fromValues(travelVals),
          total: fromValues(totalVals),
        });
      }
    }
    return out;
  }, [filteredRows, rowsWithLimits, splitByLimits, dayRange]);

  const fromValuesToQuad = (values: number[]): MinsQuad => {
    const valid = values.filter((v) => v != null && !Number.isNaN(v));
    const n = valid.length;
    const total = valid.reduce((a, b) => a + b, 0);
    return {
      total,
      max: n ? Math.max(...valid) : 0,
      min: n ? Math.min(...valid) : 0,
      av: n ? Math.round(total / n) : 0,
    };
  };

  /** Season uses same job set as Daily / By Job: all filters (API + filteredRows). */
  const seasonRollup = useMemo((): {
    jobs: Row[];
    mins_2: MinsQuad; mins_3: MinsQuad; mins_4: MinsQuad; mins_5: MinsQuad; travel: MinsQuad; total: MinsQuad;
  } | null => {
    if (filteredRows.length === 0) return null;
    const m2vals = filteredRows.map((row) => minsBetween(row, 1, 2)).filter((v): v is number => v != null);
    const m3vals = filteredRows.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null);
    const m4vals = filteredRows.map((row) => minsBetween(row, 3, 4)).filter((v): v is number => v != null);
    const m5vals = filteredRows.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null);
    const travelVals = filteredRows.map((row) => travelMins(row)).filter((v): v is number => v != null);
    const totalVals = filteredRows.map((row) => totalMins(row)).filter((v): v is number => v != null);
    return {
      jobs: filteredRows,
      mins_2: fromValuesToQuad(m2vals),
      mins_3: fromValuesToQuad(m3vals),
      mins_4: fromValuesToQuad(m4vals),
      mins_5: fromValuesToQuad(m5vals),
      travel: fromValuesToQuad(travelVals),
      total: fromValuesToQuad(totalVals),
    };
  }, [filteredRows]);

  /** When splitByLimits: 3 rows (Over Limit, Within Limit, Season Total). Otherwise same as seasonRollup in single row. */
  type SeasonRollupRow = { rowType: 'Over Limit' | 'Within Limit' | 'Season Total'; jobs: Row[]; mins_2: MinsQuad; mins_3: MinsQuad; mins_4: MinsQuad; mins_5: MinsQuad; travel: MinsQuad; total: MinsQuad };
  const seasonRollupRows = useMemo((): SeasonRollupRow[] | null => {
    if (filteredRows.length === 0) return null;
    const withLimits = filteredRows.map((r) => ({ ...r, limits_breached: limitsBreachedForRow(r) }));
    const overJobs = withLimits.filter((r) => (r as Row & { limits_breached?: string }).limits_breached === 'X');
    const withinJobs = withLimits.filter((r) => (r as Row & { limits_breached?: string }).limits_breached === '-');
    const build = (jobs: Row[]) => ({
      jobs,
      mins_2: fromValuesToQuad(jobs.map((row) => minsBetween(row, 1, 2)).filter((v): v is number => v != null)),
      mins_3: fromValuesToQuad(jobs.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null)),
      mins_4: fromValuesToQuad(jobs.map((row) => minsBetween(row, 3, 4)).filter((v): v is number => v != null)),
      mins_5: fromValuesToQuad(jobs.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null)),
      travel: fromValuesToQuad(jobs.map((row) => travelMins(row)).filter((v): v is number => v != null)),
      total: fromValuesToQuad(jobs.map((row) => totalMins(row)).filter((v): v is number => v != null)),
    });
    if (splitByLimits) {
      return [
        { rowType: 'Over Limit', ...build(overJobs) },
        { rowType: 'Within Limit', ...build(withinJobs) },
        { rowType: 'Season Total', ...build(filteredRows) },
      ];
    }
    return seasonRollup ? [{ rowType: 'Season Total', ...seasonRollup }] : null;
  }, [filteredRows, seasonRollup, splitByLimits, timeLimitRows]);

  /** By Day footer: Total, Max, Min, Av. When split by limits: one set per type (Over, Within, Daily Total); Av = totalMins / totalJobs (not average of daily Av). */
  type ByDayFooterSet = { rowType?: 'Over Limit' | 'Within Limit' | 'Daily Total'; jobCount: number; stats: Record<string, MinsQuad> };
  const byDayFooterSets = useMemo((): ByDayFooterSet[] => {
    const keys = ['mins_2', 'mins_3', 'mins_4', 'mins_5', 'travel', 'total'] as const;
    const build = (rowsForFooter: typeof rowsByDay): ByDayFooterSet => {
      const jobCount = rowsForFooter.reduce((a, r) => a + r.jobs.length, 0);
      const stats: Record<string, MinsQuad> = {};
      for (const k of keys) {
        const quads = rowsForFooter.map((r) => r[k]);
        const sumTotal = quads.reduce((a, q) => a + q.total, 0);
        const n = rowsForFooter.length;
        stats[k] = {
          total: sumTotal,
          max: n ? Math.max(...quads.map((q) => q.max)) : 0,
          min: n ? Math.min(...quads.map((q) => q.min)) : 0,
          av: jobCount > 0 ? Math.round(sumTotal / jobCount) : 0,
        };
      }
      return { jobCount, stats };
    };
    if (splitByLimits) {
      const over = rowsByDay.filter((r) => (r as { rowType?: string }).rowType === 'Over Limit');
      const within = rowsByDay.filter((r) => (r as { rowType?: string }).rowType === 'Within Limit');
      const dailyTotal = rowsByDay.filter((r) => (r as { rowType?: string }).rowType === 'Daily Total');
      return [
        { rowType: 'Over Limit', ...build(over) },
        { rowType: 'Within Limit', ...build(within) },
        { rowType: 'Daily Total', ...build(dailyTotal) },
      ];
    }
    return [{ ...build(rowsByDay) }];
  }, [rowsByDay, splitByLimits]);

  /** Single set for non-split footer (first set). */
  const byDayStats = useMemo(() => byDayFooterSets[0]?.stats ?? ({} as Record<string, MinsQuad>), [byDayFooterSets]);

  const handleSort = (key: string) => {
    if (sortKey === key) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    else {
      setSortKey(key);
      setSortDir('asc');
    }
  };

  const setSortColumn = (idx: 0 | 1 | 2, value: string) => {
    const next: [string, string, string] = [...sortColumns];
    next[idx] = value;
    setSortColumns(next);
  };

  const saveSortColumns = (cols: [string, string, string]) => {
    const valid = cols.filter(Boolean);
    setSortSaveStatus('saving');
    fetch('/api/settings', {
      method: 'PUT',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: SORT_SETTING_TYPE,
        settingname: SORT_SETTING_NAME,
        settingvalue: JSON.stringify(valid),
      }),
    })
      .then(async (r) => {
        const body = await r.json().catch(() => ({}));
        if (r.ok) setSortSaveStatus('saved');
        else setSortSaveStatus('error');
      })
      .catch(() => setSortSaveStatus('error'))
      .finally(() => {
        setTimeout(() => setSortSaveStatus('idle'), 2000);
      });
  };

  /** Flat list of column keys for body/sort: step 1 = Time, Via; steps 2–5 = Mins, Time, Via */
  const stepColumns = useMemo(() => {
    const out: { key: string; label: string }[] = [];
    for (const n of STEP_NUMS) {
      if (n >= 2) out.push({ key: `mins_${n}`, label: 'Mins' });
      out.push({ key: `step_${n}_actual_time`, label: 'Time' });
      out.push({ key: `step_${n}_via`, label: 'Via' });
    }
    return out;
  }, []);

  const allColumns = useMemo(() => [...BY_JOB_LEAD_COLUMNS, ...stepColumns, { key: 'travel', label: 'Travel' }, { key: 'in_vineyard', label: 'In Vineyard' }, { key: 'in_winery', label: 'In Winery' }, { key: 'total', label: 'Total' }], [stepColumns]);
  const sortableKeys = useMemo(() => allColumns.map((c) => c.key), [allColumns]);

  useEffect(() => {
    if (sortColumnsInitialized.current || sortableKeys.length === 0) return;
    sortColumnsInitialized.current = true;
    const cols = new Set(sortableKeys);
    fetch(`/api/settings?${new URLSearchParams({ type: SORT_SETTING_TYPE, name: SORT_SETTING_NAME })}`, { cache: 'no-store' })
      .then((r) => r.json())
      .then((data) => {
        if (data?.settingvalue) {
          try {
            const arr = JSON.parse(data.settingvalue) as unknown[];
            if (Array.isArray(arr) && arr.every((x) => typeof x === 'string')) {
              const valid = (arr as string[]).filter((c) => cols.has(c));
              setSortColumns([valid[0] ?? '', valid[1] ?? '', valid[2] ?? '']);
              return;
            }
          } catch {
            /* ignore */
          }
        }
        setSortColumns(['', '', '']);
      })
      .catch(() => setSortColumns(['', '', '']));
  }, [sortableKeys]);

  const getThresholdNum = (key: string): number | null => {
    const s = minsThresholds[key]?.trim() ?? '';
    if (s === '') return null;
    const n = parseFloat(s);
    return Number.isNaN(n) ? null : n;
  };
  const isMinsOverThreshold = (val: number | null, key: string): boolean => {
    const limit = getThresholdNum(key);
    const numVal = val != null ? Number(val) : NaN;
    return !Number.isNaN(numVal) && limit != null && numVal > limit;
  };
  /** Threshold keys for step columns: 3 and 5 also check in_vineyard/in_winery so limits apply consistently. */
  const getThresholdKeysForStep = (stepNum: number): string[] => {
    if (stepNum === 3) return ['3', 'in_vineyard'];
    if (stepNum === 5) return ['5', 'in_winery'];
    return [String(stepNum)];
  };
  const isMinsOverThresholdForStepCol = (val: number | null, stepNum: number): boolean =>
    getThresholdKeysForStep(stepNum).some((k) => isMinsOverThreshold(val, k));
  /** Threshold key for season/daily column: mins_2 -> '2', travel -> 'travel', etc. */
  const thresholdKey = (col: string) => (col.startsWith('mins_') ? col.replace('mins_', '') : col);
  const limitDisplay = (key: string) => {
    const n = getThresholdNum(key);
    return n != null ? String(n) : '—';
  };
  const redIfOver = (val: number | null, key: string) =>
    isMinsOverThreshold(val, key) ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : '';
  /** Red if over any threshold for this step column (used for step 3 and 5 to match 2 and 4). */
  const redIfOverStepCol = (val: number | null, stepNum: number) =>
    isMinsOverThresholdForStepCol(val, stepNum) ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : '';

  const sortedRows = useMemo(() => {
    const out = [...rowsWithLimits];
    const [c1, c2, c3] = sortColumns;
    const dropdownCols = [c1, c2, c3].filter(Boolean);
    out.sort((a, b) => {
      if (sortKey) {
        const va = sortValueFor(a, sortKey);
        const vb = sortValueFor(b, sortKey);
        const c = compare(va, vb);
        return sortDir === 'asc' ? c : -c;
      }
      for (const col of dropdownCols) {
        const c = compare(sortValueFor(a, col), sortValueFor(b, col));
        if (c !== 0) return c;
      }
      return 0;
    });
    return out;
  }, [rowsWithLimits, sortKey, sortDir, sortColumns]);

  /** By Job table shows one page; season/daily use full sortedRows via filteredRows. */
  const sortedRowsPage = useMemo(() => {
    if (summaryTab !== 'by_job') return sortedRows;
    const start = jobsPage * jobsPageSize;
    return sortedRows.slice(start, start + jobsPageSize);
  }, [sortedRows, summaryTab, jobsPage, jobsPageSize]);

  useEffect(() => {
    const jid = pendingScrollToJobIdRef.current;
    if (!jid || summaryTab !== 'by_job') return;
    if (loading) return;
    const onPage = sortedRowsPage.some((r) => String(r.job_id ?? '').trim() === jid);
    if (!onPage) {
      pendingScrollToJobIdRef.current = null;
      return;
    }
    let cancelled = false;
    const run = () => {
      if (cancelled) return;
      const rowEl = document.querySelector(
        `[data-summary-focus-job="${CSS.escape(jid)}"]`,
      ) as HTMLElement | null;
      const scrollParent = byJobTableScrollRef.current;
      if (!rowEl) return;
      pendingScrollToJobIdRef.current = null;
      setFocusHighlightJobId(jid);
      /** Space below sticky multi-row thead inside the scroll box. */
      const stickyHeaderPad = 100;
      if (scrollParent) {
        const prect = scrollParent.getBoundingClientRect();
        const rrect = rowEl.getBoundingClientRect();
        const nextTop = scrollParent.scrollTop + (rrect.top - prect.top) - stickyHeaderPad;
        scrollParent.scrollTo({ top: Math.max(0, nextTop), behavior: 'smooth' });
      } else {
        rowEl.scrollIntoView({ block: 'start', behavior: 'smooth' });
      }
    };
    const id1 = window.requestAnimationFrame(() => {
      window.requestAnimationFrame(run);
    });
    return () => {
      cancelled = true;
      cancelAnimationFrame(id1);
    };
  }, [loading, summaryTab, sortedRowsPage]);

  useEffect(() => {
    if (!focusHighlightJobId) return;
    const t = window.setTimeout(() => setFocusHighlightJobId(null), 20000);
    return () => clearTimeout(t);
  }, [focusHighlightJobId]);

  /** Sum and average of mins columns over sorted (visible) rows. */
  const columnStats = useMemo(() => {
    const stats: Record<string, { sum: number; avg: number | null; count: number }> = {};
    for (const n of [2, 3, 4, 5]) {
      const values = sortedRows.map((r) => minsBetween(r, n - 1, n)).filter((v): v is number => v != null);
      const sum = values.reduce((a, b) => a + b, 0);
      const count = values.length;
      stats[`mins_${n}`] = { sum, avg: count ? Math.round(sum / count) : null, count };
    }
    const travelValues = sortedRows.map(travelMins).filter((v): v is number => v != null);
    const travelSum = travelValues.reduce((a, b) => a + b, 0);
    const travelCount = travelValues.length;
    stats.travel = { sum: travelSum, avg: travelCount ? Math.round(travelSum / travelCount) : null, count: travelCount };
    const inVineyardValues = sortedRows.map((r) => minsBetween(r, 2, 3)).filter((v): v is number => v != null);
    const inVineyardSum = inVineyardValues.reduce((a, b) => a + b, 0);
    const inVineyardCount = inVineyardValues.length;
    stats.in_vineyard = { sum: inVineyardSum, avg: inVineyardCount ? Math.round(inVineyardSum / inVineyardCount) : null, count: inVineyardCount };
    const inWineryValues = sortedRows.map((r) => minsBetween(r, 4, 5)).filter((v): v is number => v != null);
    const inWinerySum = inWineryValues.reduce((a, b) => a + b, 0);
    const inWineryCount = inWineryValues.length;
    stats.in_winery = { sum: inWinerySum, avg: inWineryCount ? Math.round(inWinerySum / inWineryCount) : null, count: inWineryCount };
    const totalValues = sortedRows.map(totalMins).filter((v): v is number => v != null);
    const totalSum = totalValues.reduce((a, b) => a + b, 0);
    const totalCount = totalValues.length;
    stats.total = { sum: totalSum, avg: totalCount ? Math.round(totalSum / totalCount) : null, count: totalCount };
    return stats;
  }, [sortedRows]);

  const dayClientView = summaryTab === 'by_day' && viewMode === 'client';
  const jobClientView = summaryTab === 'by_job' && viewMode === 'client';
  const seasonClientView = summaryTab === 'season' && viewMode === 'client';

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-4">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Summary</h1>
      </div>
      {loading && <p className="text-zinc-600">Loading…</p>}
      {error && (
        <div className="rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          <p className="font-medium">{error}</p>
        </div>
      )}
      {!loading && !error && (
        <>
          <div className="mb-4 flex flex-wrap items-end gap-4 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Customer</label>
              <div
                title="Set in sidebar (Client view)"
                className="rounded border border-zinc-200 bg-zinc-50 px-2 py-1.5 text-sm text-zinc-700 dark:border-zinc-700 dark:bg-zinc-800 dark:text-zinc-300"
              >
                {clientCustomer || '— All —'}
              </div>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Template</label>
              <select
                value={filterTemplate}
                onChange={(e) => setFilterTemplate(e.target.value)}
                disabled={!effectiveCustomer.trim()}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— All —</option>
                {distinctTemplates.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Winery</label>
              <select
                value={filterWinery}
                onChange={(e) => setFilterWinery(e.target.value)}
                disabled={!effectiveCustomer.trim()}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— All —</option>
                {distinctWineries.map((w) => (
                  <option key={w} value={w}>{w}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Vineyard group</label>
              <select
                value={filterVineyardGroup}
                onChange={(e) => setFilterVineyardGroup(e.target.value)}
                disabled={!effectiveCustomer.trim()}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— All —</option>
                {distinctVineyardGroups.map((g) => (
                  <option key={g} value={g}>{g}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Vineyard</label>
              <select
                value={filterVineyard}
                onChange={(e) => setFilterVineyard(e.target.value)}
                disabled={!effectiveCustomer.trim()}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— All —</option>
                {distinctVineyards.map((v) => (
                  <option key={v} value={v}>{v}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Actual start from</label>
              <input
                type="date"
                value={filterActualFrom}
                onChange={(e) => setFilterActualFrom(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Actual start to</label>
              <input
                type="date"
                value={filterActualTo}
                onChange={(e) => setFilterActualTo(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Truck</label>
              <select
                value={filterTruckId}
                onChange={(e) => setFilterTruckId(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— All —</option>
                {distinctTruckIds.map((id) => (
                  <option key={id} value={id}>{id}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Worker</label>
              <select
                value={filterWorker}
                onChange={(e) => setFilterWorker(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                title="Distinct workers in current result set (Customer, Template, Winery, Vineyard, Truck, Date)"
              >
                <option value="">— All —</option>
                {distinctWorkers.map((w) => (
                  <option key={w} value={w}>{w}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">TT</label>
              <select
                value={filterTrailermode}
                onChange={(e) => setFilterTrailermode(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                title="Filter by trailermode (T = truck only, T_T = truck + trailer)"
              >
                <option value="">— All —</option>
                {distinctTrailermodes.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Sort 1</label>
              <select
                value={sortColumns[0]}
                onChange={(e) => setSortColumn(0, e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— None —</option>
                {sortableKeys.map((k) => (
                  <option key={k} value={k}>{allColumns.find((c) => c.key === k)?.label ?? k}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Sort 2</label>
              <select
                value={sortColumns[1]}
                onChange={(e) => setSortColumn(1, e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— None —</option>
                {sortableKeys.map((k) => (
                  <option key={k} value={k}>{allColumns.find((c) => c.key === k)?.label ?? k}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Sort 3</label>
              <select
                value={sortColumns[2]}
                onChange={(e) => setSortColumn(2, e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— None —</option>
                {sortableKeys.map((k) => (
                  <option key={k} value={k}>{allColumns.find((c) => c.key === k)?.label ?? k}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500 invisible">Save</label>
              <button
                type="button"
                onClick={() => saveSortColumns(sortColumns)}
                disabled={sortSaveStatus === 'saving'}
                className="rounded bg-green-600 px-3 py-1.5 text-sm text-white hover:bg-green-700 disabled:opacity-70 dark:bg-green-700 dark:hover:bg-green-600"
              >
                {sortSaveStatus === 'saving' ? 'Saving…' : sortSaveStatus === 'saved' ? 'Saved ✓' : sortSaveStatus === 'error' ? 'Save failed' : 'Save sort config'}
              </button>
            </div>
          </div>
          {/* Time Limits: rows for selected customer (tbl_wineryminutes). Click a row to show its limits in the header white boxes (for info only). By Job table colours each job using limits from the row where Winery = job.delivery_winery and TT = job.trailermode (T = truck only, TT = truck + trailer). */}
          {effectiveCustomer.trim() && (
            <div className="mb-4 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
              <div className="mb-2 flex items-center justify-between gap-2">
                <h2 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">Time Limits</h2>
                <label className="flex cursor-pointer items-center gap-2 text-xs text-zinc-600 dark:text-zinc-400">
                  <input
                    type="checkbox"
                    checked={showLimitsTable}
                    onChange={(e) => setShowLimitsTable(e.target.checked)}
                    className="rounded border-zinc-300"
                  />
                  Show limits table
                </label>
              </div>
              {showLimitsTable && timeLimitRows.length === 0 ? (
                <p className="text-sm text-zinc-500 dark:text-zinc-400">No time limit rows for this customer.</p>
              ) : showLimitsTable && timeLimitRows.length > 0 ? (
                <>
                  <p className="mb-2 text-xs text-zinc-500 dark:text-zinc-400">Click a row to show its limits in the header boxes (for reference). Each job uses the row where Winery = delivery_winery, Vineyard group = vineyard_group, and TT = trailermode (T / TT). Columns align with By Job table.</p>
                  <div className="overflow-x-auto">
                    <table className="w-full min-w-[64rem] text-left text-sm">
                      <thead>
                        <tr className="border-b border-zinc-200 dark:border-zinc-700">
                          <th className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">Template</th>
                          <th className="border-r border-zinc-200 px-2 py-1.5 dark:border-zinc-700" />
                          <th className="border-r border-zinc-200 px-2 py-1.5 dark:border-zinc-700" />
                          <th className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">TT</th>
                          <th className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">Winery</th>
                          <th className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">Vineyard group</th>
                          <th className="border-r border-zinc-200 px-2 py-1.5 dark:border-zinc-700" />
                          <th colSpan={2} className="border-r border-zinc-200 px-2 py-1.5 dark:border-zinc-700" />
                          <th className={`border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[2]}`}>To Vine</th>
                          <th colSpan={2} className={`border-r border-zinc-200 dark:border-zinc-700 ${STEP_GROUP_BG[2]}`} />
                          <th className={`border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>In Vine</th>
                          <th colSpan={2} className={`border-r border-zinc-200 dark:border-zinc-700 ${STEP_GROUP_BG[3]}`} />
                          <th className={`border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[4]}`}>To Wine</th>
                          <th colSpan={2} className={`border-r border-zinc-200 dark:border-zinc-700 ${STEP_GROUP_BG[4]}`} />
                          <th className={`border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>In Wine</th>
                          <th colSpan={2} className={`border-r border-zinc-200 dark:border-zinc-700 ${STEP_GROUP_BG[5]}`} />
                          <th className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">Travel_</th>
                          <th className={`border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>In Vineyard</th>
                          <th className={`border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>In Winery</th>
                          <th className="px-2 py-1.5 font-medium text-zinc-600 dark:text-zinc-400">Total</th>
                        </tr>
                      </thead>
                      <tbody>
                        {timeLimitRows.map((r) => {
                          const toV = r.ToVineMins != null ? Number(r.ToVineMins) : null;
                          const toW = r.ToWineMins != null ? Number(r.ToWineMins) : null;
                          const travelVal = (toV != null || toW != null) ? (toV ?? 0) + (toW ?? 0) : null;
                          const isSelected = selectedTimeLimitRowId === r.id;
                          const emptyCell = (k: string) => <td key={k} className="border-r border-zinc-200 px-2 py-1.5 dark:border-zinc-700">&nbsp;</td>;
                          return (
                            <tr
                              key={r.id}
                              role="button"
                              tabIndex={0}
                              onClick={() => {
                                setSelectedTimeLimitRowId(r.id);
                                const toV = r.ToVineMins != null ? String(r.ToVineMins) : '';
                                const inV = r.InVineMins != null ? String(r.InVineMins) : '';
                                const toW = r.ToWineMins != null ? String(r.ToWineMins) : '';
                                const inW = r.InWineMins != null ? String(r.InWineMins) : '';
                                const travelStr = travelVal != null ? String(travelVal) : '';
                                const totalStr = r.TotalMins != null ? String(r.TotalMins) : '';
                                setMinsThresholds({
                                  '2': toV, '3': inV, '4': toW, '5': inW,
                                  travel: travelStr, in_vineyard: inV, in_winery: inW, total: totalStr,
                                });
                              }}
                              onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); (e.target as HTMLTableRowElement).click(); } }}
                              className={`border-b border-zinc-100 dark:border-zinc-800 cursor-pointer ${isSelected ? 'bg-sky-100 dark:bg-sky-900/40' : 'hover:bg-zinc-100 dark:hover:bg-zinc-800/50'}`}
                            >
                              <td className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">{formatCell(r.Template)}</td>
                              {emptyCell(`${r.id}-e1`)}
                              {emptyCell(`${r.id}-e2`)}
                              <td className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">{formatCell(r.TT)}</td>
                              <td className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">{formatCell(r.Winery)}</td>
                              <td className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">{formatCell(r.vineyardgroup)}</td>
                              {emptyCell(`${r.id}-e5`)}
                              <td colSpan={2} className="border-r border-zinc-200 px-2 py-1.5 dark:border-zinc-700">&nbsp;</td>
                              <td className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[2]}`}>{r.ToVineMins != null ? String(r.ToVineMins) : '—'}</td>
                              <td colSpan={2} className={`border-r border-zinc-200 px-2 py-1.5 dark:border-zinc-700 ${STEP_GROUP_BG[2]}`}>&nbsp;</td>
                              <td className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>{r.InVineMins != null ? String(r.InVineMins) : '—'}</td>
                              <td colSpan={2} className={`border-r border-zinc-200 px-2 py-1.5 dark:border-zinc-700 ${STEP_GROUP_BG[3]}`}>&nbsp;</td>
                              <td className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[4]}`}>{r.ToWineMins != null ? String(r.ToWineMins) : '—'}</td>
                              <td colSpan={2} className={`border-r border-zinc-200 px-2 py-1.5 dark:border-zinc-700 ${STEP_GROUP_BG[4]}`}>&nbsp;</td>
                              <td className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>{r.InWineMins != null ? String(r.InWineMins) : '—'}</td>
                              <td colSpan={2} className={`border-r border-zinc-200 px-2 py-1.5 dark:border-zinc-700 ${STEP_GROUP_BG[5]}`}>&nbsp;</td>
                              <td className="border-r border-zinc-200 px-2 py-1.5 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">{travelVal != null ? String(travelVal) : '—'}</td>
                              <td className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>{r.InVineMins != null ? String(r.InVineMins) : '—'}</td>
                              <td className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>{r.InWineMins != null ? String(r.InWineMins) : '—'}</td>
                              <td className="px-2 py-1.5 tabular-nums font-medium text-zinc-700 dark:text-zinc-300">{r.TotalMins != null ? String(r.TotalMins) : '—'}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </>
              ) : null}
            </div>
          )}
          <div className="mb-4 flex flex-wrap items-center gap-4 border-b border-zinc-200 dark:border-zinc-700">
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => setSummaryTab('season')}
                className={`rounded-t px-4 py-2 text-sm font-medium ${summaryTab === 'season' ? 'bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100' : 'bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750'}`}
              >
                Season Summary
              </button>
              <button
                type="button"
                onClick={() => {
                  const a = filterActualFrom.trim().slice(0, 10);
                  const b = filterActualTo.trim().slice(0, 10);
                  const singleDay = a.length > 0 && b.length > 0 && a === b;
                  if (singleDay) {
                    setFilterActualFrom('');
                    setFilterActualTo('');
                  }
                  setSummaryTab('by_day');
                }}
                className={`rounded-t px-4 py-2 text-sm font-medium ${summaryTab === 'by_day' ? 'bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100' : 'bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750'}`}
              >
                Daily Summary
              </button>
              <button
                type="button"
                onClick={() => setSummaryTab('by_job')}
                className={`rounded-t px-4 py-2 text-sm font-medium ${summaryTab === 'by_job' ? 'bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100' : 'bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750'}`}
              >
                By Job
              </button>
            </div>
            {(summaryTab === 'season' || summaryTab === 'by_day') && (
              <label className="flex cursor-pointer items-center gap-2 rounded border border-zinc-200 bg-white px-3 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-900">
                <input
                  type="checkbox"
                  checked={splitByLimits}
                  onChange={(e) => setSplitByLimits(e.target.checked)}
                  className="rounded border-zinc-300"
                />
                <span className="text-zinc-700 dark:text-zinc-300">Split by Limits</span>
              </label>
            )}
          </div>
          {summaryTab === 'season' && (
            <div className="max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
              <p className="mb-2 text-sm text-zinc-500">
                {seasonClientView
                  ? 'Season rollup for your account. Travel + In Vineyard + In Winery = Total mins.'
                  : 'Rollup of all jobs for the selected Customer and Template (same column sections as Daily Summary for audit). Use the Customer and Template dropdowns above.'}
              </p>
              <table className={`w-full text-left text-sm ${seasonClientView ? 'min-w-[24rem]' : 'min-w-[64rem]'}`}>
                <thead className="sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]">
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    <th className="border-r border-zinc-200 px-2 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-700">Limit (red if &gt;)</th>
                    <th className="border-r border-zinc-200 px-2 py-1 dark:border-zinc-700" />
                    {(!seasonClientView ? (['2', '3', '4', '5', 'travel', 'in_vineyard', 'in_winery', 'total'] as const) : (['travel', 'in_vineyard', 'in_winery', 'total'] as const)).map((key) =>
                      (seasonClientView ? (['Total', 'Av'] as const) : (['Total', 'Max', 'Min', 'Av'] as const)).map((sub) => (
                        <th key={`limit-${key}-${sub}`} className="whitespace-nowrap border-r border-zinc-200 px-2 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-700">
                          {sub === 'Av' ? limitDisplay(key) : '\u00A0'}
                        </th>
                      ))
                    )}
                  </tr>
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Season</th>
                    {splitByLimits && (
                      <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700">Type</th>
                    )}
                    <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700">Jobs</th>
                    {!seasonClientView && (
                      <>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[2]}`}>2. Travel To Vineyard</th>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[3]}`}>3. Time In Vineyard</th>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[4]}`}>4. Travel To Winery</th>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[5]}`}>5. Unloading Winery</th>
                      </>
                    )}
                    <th colSpan={seasonClientView ? 2 : 4} className="border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Travel</th>
                    <th colSpan={seasonClientView ? 2 : 4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[3]}`}>In Vineyard</th>
                    <th colSpan={seasonClientView ? 2 : 4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[5]}`}>In Winery</th>
                    <th colSpan={seasonClientView ? 2 : 4} className="border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Total</th>
                  </tr>
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    {(!seasonClientView ? ([2, 3, 4, 5, 'travel', 'in_vineyard', 'in_winery', 'total'] as const) : (['travel', 'in_vineyard', 'in_winery', 'total'] as const)).map((step, stepIdx) => {
                      const groupBg = !seasonClientView && stepIdx < 4 ? STEP_GROUP_BG[(stepIdx + 2) as 2 | 3 | 4 | 5] : (step === 'in_vineyard' ? STEP_GROUP_BG[3] : step === 'in_winery' ? STEP_GROUP_BG[5] : '');
                      return (seasonClientView ? (['Total', 'Av'] as const) : (['Total', 'Max', 'Min', 'Av'] as const)).map((sub) => (
                        <th
                          key={`${step}-${sub}`}
                          className={`whitespace-nowrap border-r border-zinc-200 px-2 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-700 ${groupBg} ${step === 'total' && sub === 'Av' ? 'border-r-0' : ''}`}
                        >
                          {sub}
                        </th>
                      ));
                    })}
                  </tr>
                </thead>
                <tbody>
                  {!seasonRollupRows || seasonRollupRows.length === 0 ? (
                    <tr>
                      <td colSpan={(seasonClientView ? 10 : 34) + (splitByLimits ? 1 : 0)} className="px-3 py-6 text-center text-zinc-500">
                        {effectiveCustomer || filterTemplate
                          ? 'No jobs for the selected Customer and Template.'
                          : 'Select Customer (and optionally Template) above to see season rollup.'}
                      </td>
                    </tr>
                  ) : (
                    seasonRollupRows.map((rollup) => {
                      const isSubTotal = splitByLimits && (rollup.rowType === 'Over Limit' || rollup.rowType === 'Within Limit');
                      const isSeasonTotal = splitByLimits && rollup.rowType === 'Season Total';
                      const seasonRowClass = [
                        'border-b border-zinc-100 dark:border-zinc-800',
                        isSubTotal ? 'bg-zinc-100 dark:bg-zinc-800/50' : isSeasonTotal ? 'border-t-2 border-zinc-300 dark:border-zinc-600 bg-white dark:bg-zinc-900 font-medium' : 'bg-zinc-50/50 dark:bg-zinc-800/30',
                      ].filter(Boolean).join(' ');
                      return (
                    <tr key={rollup.rowType} className={seasonRowClass}>
                      <td className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">
                        {effectiveCustomer && filterTemplate ? `${effectiveCustomer} / ${filterTemplate}` : effectiveCustomer || 'All'}
                      </td>
                      {splitByLimits && (
                        <td className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">{rollup.rowType}</td>
                      )}
                      <td className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                        {rollup.jobs.length}
                      </td>
                      {!seasonClientView && [rollup.mins_2, rollup.mins_3, rollup.mins_4, rollup.mins_5].map((q, i) =>
                        (['total', 'max', 'min', 'av'] as const).map((key) => {
                          const stepNum = i + 2;
                          const redClass = key === 'av' ? redIfOverStepCol(q[key], stepNum) : '';
                          return (
                            <td key={`${i}-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums ${redClass ? redClass : `${STEP_GROUP_BG[(i + 2) as 2 | 3 | 4 | 5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                              {q[key]}
                            </td>
                          );
                        })
                      )}
                      {(seasonClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                        const redClass = key === 'av' ? redIfOver(rollup.travel[key], 'travel') : '';
                        return (
                          <td key={`travel-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums ${redClass || 'text-zinc-600 dark:border-zinc-700 dark:text-zinc-400'}`}>
                            {rollup.travel[key]}
                          </td>
                        );
                      })}
                      {(seasonClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                        const redClass = key === 'av' ? redIfOver(rollup.mins_3[key], 'in_vineyard') : '';
                        return (
                          <td key={`in_vineyard-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums ${redClass ? redClass : `${STEP_GROUP_BG[3]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                            {rollup.mins_3[key]}
                          </td>
                        );
                      })}
                      {(seasonClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                        const redClass = key === 'av' ? redIfOver(rollup.mins_5[key], 'in_winery') : '';
                        return (
                          <td key={`in_winery-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums ${redClass ? redClass : `${STEP_GROUP_BG[5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                            {rollup.mins_5[key]}
                          </td>
                        );
                      })}
                      {(seasonClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                        const redClass = key === 'av' ? redIfOver(rollup.total[key], 'total') : '';
                        return (
                          <td key={`total-${key}`} className={`whitespace-nowrap border-zinc-200 px-2 py-2 tabular-nums font-medium ${redClass || 'text-zinc-700 dark:border-zinc-700 dark:text-zinc-300'}`}>
                            {rollup.total[key]}
                          </td>
                        );
                      })}
                    </tr>
                    ); })
                  )}
                </tbody>
              </table>
              {(() => {
                /** When split mode: chart for each of Over Limit, Within Limit, Season Total. Otherwise single chart from seasonRollup. */
                const chartSources = splitByLimits && seasonRollupRows && seasonRollupRows.length === 3
                  ? seasonRollupRows
                  : seasonRollup ? [{ rowType: 'Season Total' as const, ...seasonRollup }] : [];
                if (chartSources.length === 0) return null;
                const size = 200;
                const cx = size / 2;
                const cy = size / 2;
                const r = size / 2 - 4;
                return (
                  <div className="mt-6 flex flex-wrap items-start gap-8 border-t border-zinc-200 px-4 py-6 dark:border-zinc-700">
                    {chartSources.map((source) => {
                      const travel = source.travel.total;
                      const inVineyard = source.mins_3.total;
                      const inWinery = source.mins_5.total;
                      const totalMins = source.total.total;
                      const pieSum = travel + inVineyard + inWinery;
                      const barValues = [
                        { label: 'Total', value: totalMins, fill: 'rgb(59 130 246)' },
                        { label: 'Travel', value: travel, fill: 'rgb(34 197 94)' },
                        { label: 'In Vineyard', value: inVineyard, fill: 'rgb(234 179 8)' },
                        { label: 'In Winery', value: inWinery, fill: 'rgb(239 68 68)' },
                      ];
                      const barMax = Math.max(...barValues.map((b) => b.value), 1);
                      const pieSegments = pieSum > 0
                        ? [
                            { label: 'Travel', value: travel, start: 0, sweep: (travel / pieSum) * 360, fill: 'rgb(34 197 94)' },
                            { label: 'In Vineyard', value: inVineyard, start: (travel / pieSum) * 360, sweep: (inVineyard / pieSum) * 360, fill: 'rgb(234 179 8)' },
                            { label: 'In Winery', value: inWinery, start: ((travel + inVineyard) / pieSum) * 360, sweep: (inWinery / pieSum) * 360, fill: 'rgb(239 68 68)' },
                          ]
                        : [];
                      return (
                        <div key={source.rowType} className="flex flex-col items-center gap-2">
                          <span className="text-sm font-semibold text-zinc-800 dark:text-zinc-200">{source.rowType}</span>
                          <span className="text-xs text-zinc-500 dark:text-zinc-400">Minutes by phase (Travel + Vineyard + Winery = Total)</span>
                          <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} className="overflow-visible">
                            {pieSum <= 0 ? (
                              <circle cx={cx} cy={cy} r={r} fill="rgb(228 228 231)" stroke="rgb(161 161 170)" strokeWidth={1} />
                            ) : (
                              pieSegments.map((seg, i) => {
                                const startDeg = seg.start - 90;
                                const endDeg = startDeg + seg.sweep;
                                const x1 = cx + r * Math.cos((startDeg * Math.PI) / 180);
                                const y1 = cy + r * Math.sin((startDeg * Math.PI) / 180);
                                const x2 = cx + r * Math.cos((endDeg * Math.PI) / 180);
                                const y2 = cy + r * Math.sin((endDeg * Math.PI) / 180);
                                const large = seg.sweep > 180 ? 1 : 0;
                                const d = `M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2} Z`;
                                return <path key={i} d={d} fill={seg.fill} stroke="rgb(255 255 255)" strokeWidth={1} />;
                              })
                            )}
                            <circle cx={cx} cy={cy} r={r} fill="none" stroke="rgb(161 161 170)" strokeWidth={1} />
                          </svg>
                          <div className="flex flex-wrap justify-center gap-4 text-xs">
                            <span className="flex items-center gap-1.5"><span className="inline-block h-3 w-3 rounded-sm bg-green-500" /> Travel: {travel}</span>
                            <span className="flex items-center gap-1.5"><span className="inline-block h-3 w-3 rounded-sm bg-yellow-500" /> In Vineyard: {inVineyard}</span>
                            <span className="flex items-center gap-1.5"><span className="inline-block h-3 w-3 rounded-sm bg-red-500" /> In Winery: {inWinery}</span>
                            <span className="font-medium">Total: {totalMins}</span>
                          </div>
                          <span className="text-xs text-zinc-500 dark:text-zinc-400">Total, Travel, Vineyard, Winery (mins)</span>
                          <svg width={320} height={180} viewBox="0 0 320 180" className="overflow-visible">
                            <line x1={40} y1={140} x2={300} y2={140} stroke="rgb(161 161 170)" strokeWidth={1} />
                            <line x1={40} y1={20} x2={40} y2={140} stroke="rgb(161 161 170)" strokeWidth={1} />
                            {barValues.map((b, i) => {
                              const x = 50 + i * 68;
                              const h = barMax > 0 ? (b.value / barMax) * 100 : 0;
                              return (
                                <g key={b.label}>
                                  <rect x={x} y={140 - h} width={48} height={h} fill={b.fill} stroke="rgb(255 255 255)" strokeWidth={1} />
                                  <text x={x + 24} y={155} textAnchor="middle" className="fill-zinc-600 text-[10px] font-medium dark:fill-zinc-400">{b.label}</text>
                                  <text x={x + 24} y={135 - h} textAnchor="middle" className="fill-zinc-800 text-[10px] font-medium dark:fill-zinc-200">{b.value}</text>
                                </g>
                              );
                            })}
                          </svg>
                        </div>
                      );
                    })}
                  </div>
                );
              })()}
            </div>
          )}
          {summaryTab === 'by_day' && (
            <div className="max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
              <table className="w-full min-w-[64rem] text-left text-sm">
                <thead className="sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]">
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    <th className="border-r border-zinc-200 px-2 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-700">Limit (red if &gt;)</th>
                    {splitByLimits && <th className="border-r border-zinc-200 px-2 py-1 dark:border-zinc-700" />}
                    <th className="border-r border-zinc-200 px-2 py-1 dark:border-zinc-700" />
                    {(dayClientView ? (['travel', 'in_vineyard', 'in_winery', 'total'] as const) : (['2', '3', '4', '5', 'travel', 'in_vineyard', 'in_winery', 'total'] as const)).map((key) =>
                      (dayClientView ? (['Total', 'Av'] as const) : (['Total', 'Max', 'Min', 'Av'] as const)).map((sub) => (
                        <th key={`limit-${key}-${sub}`} className="whitespace-nowrap border-r border-zinc-200 px-2 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-700">
                          {sub === 'Av' ? limitDisplay(key) : '\u00A0'}
                        </th>
                      ))
                    )}
                  </tr>
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Date</th>
                    {splitByLimits && (
                      <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700">Type</th>
                    )}
                    <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700">Jobs</th>
                    {!dayClientView && (
                      <>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[2]}`}>2. Travel To Vineyard</th>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[3]}`}>3. Time In Vineyard</th>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[4]}`}>4. Travel To Winery</th>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[5]}`}>5. Unloading Winery</th>
                      </>
                    )}
                    <th colSpan={dayClientView ? 2 : 4} className="border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Travel</th>
                    <th colSpan={dayClientView ? 2 : 4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[3]}`}>In Vineyard</th>
                    <th colSpan={dayClientView ? 2 : 4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[5]}`}>In Winery</th>
                    <th colSpan={dayClientView ? 2 : 4} className="border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Total</th>
                  </tr>
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    {(!dayClientView ? [2, 3, 4, 5, 'travel', 'in_vineyard', 'in_winery', 'total'] : ['travel', 'in_vineyard', 'in_winery', 'total']).map((step, stepIdx) => {
                      const groupBg = !dayClientView
                        ? (stepIdx < 4 ? STEP_GROUP_BG[(stepIdx + 2) as 2 | 3 | 4 | 5] : stepIdx === 5 ? STEP_GROUP_BG[3] : stepIdx === 6 ? STEP_GROUP_BG[5] : '')
                        : (stepIdx === 1 ? STEP_GROUP_BG[3] : stepIdx === 2 ? STEP_GROUP_BG[5] : '');
                      const subCols = dayClientView ? (['Total', 'Av'] as const) : (['Total', 'Max', 'Min', 'Av'] as const);
                      return subCols.map((sub) => (
                        <th
                          key={`${step}-${sub}`}
                          className={`whitespace-nowrap border-r border-zinc-200 px-2 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-700 ${groupBg} ${step === 'total' && sub === 'Av' ? 'border-r-0' : ''}`}
                        >
                          {sub}
                        </th>
                      ));
                    })}
                  </tr>
                </thead>
                <tbody>
                  {rowsByDay.length === 0 ? (
                    <tr>
                      <td colSpan={(dayClientView ? 10 : 34) + (splitByLimits ? 1 : 0)} className="px-3 py-6 text-center text-zinc-500">
                        No days {filteredRows.length === 0 ? '(no jobs match filters)' : '(no job dates in range)'}.
                      </td>
                    </tr>
                  ) : (
                    rowsByDay.map((r, idx) => {
                      const rowType = (r as { rowType?: string }).rowType;
                      const isFirstRowOfDay = !splitByLimits || idx === 0 || (rowsByDay[idx - 1] as { date?: string }).date !== r.date;
                      const dailyRowClass = [
                        'border-b border-zinc-100 dark:border-zinc-800',
                        splitByLimits && isFirstRowOfDay ? 'border-t-2 border-zinc-300 dark:border-zinc-600' : '',
                        splitByLimits && (rowType === 'Over Limit' || rowType === 'Within Limit') ? 'bg-zinc-100 dark:bg-zinc-800/50' : '',
                        splitByLimits && rowType === 'Daily Total' ? 'bg-white dark:bg-zinc-900 font-medium' : '',
                        !splitByLimits ? 'hover:bg-zinc-50 dark:hover:bg-zinc-800/50' : (rowType === 'Daily Total' ? '' : 'hover:bg-zinc-200/50 dark:hover:bg-zinc-700/50'),
                      ].filter(Boolean).join(' ');
                      const dayTimeOutlier = isFirstRowOfDay ? dayRollupOutlierSeverity(r as RollupQuads) : null;
                      const dateCellFlag =
                        dayTimeOutlier === 'red'
                          ? SUMMARY_OUTLIER_RED
                          : dayTimeOutlier === 'yellow'
                            ? SUMMARY_OUTLIER_YELLOW
                            : '';
                      return (
                      <tr key={`${r.date}-${rowType ?? 'day'}`} className={dailyRowClass}>
                        <td
                          className={`whitespace-nowrap border-r border-zinc-200 px-3 py-2 dark:border-zinc-700${dateCellFlag ? ` ${dateCellFlag}` : ''}`}
                        >
                          {isFirstRowOfDay ? (
                            dayClientView ? (
                              <span className="font-medium text-zinc-700 dark:text-zinc-300">{r.dateLabel}</span>
                            ) : (
                              <button
                                type="button"
                                onClick={() => {
                                  setFilterActualFrom(r.date);
                                  setFilterActualTo(r.date);
                                  setSummaryTab('by_job');
                                }}
                                className={
                                  dateCellFlag
                                    ? 'font-medium text-inherit underline decoration-current hover:opacity-90'
                                    : 'font-medium text-blue-600 underline hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300'
                                }
                              >
                                {r.dateLabel}
                              </button>
                            )
                          ) : (
                            '\u00A0'
                          )}
                        </td>
                        {splitByLimits && (
                          <td className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">{rowType}</td>
                        )}
                        <td className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">{r.jobs.length}</td>
                        {!dayClientView && [r.mins_2, r.mins_3, r.mins_4, r.mins_5].map((q, i) =>
                          (['total', 'max', 'min', 'av'] as const).map((key) => {
                            const stepNum = i + 2;
                            const redClass = key === 'av' ? redIfOverStepCol(q[key], stepNum) : '';
                            return (
                              <td key={`${i}-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums ${redClass ? redClass : `${STEP_GROUP_BG[(i + 2) as 2 | 3 | 4 | 5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                                {q[key]}
                              </td>
                            );
                          })
                        )}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                          const redClass = key === 'av' ? redIfOver(r.travel[key], 'travel') : '';
                          return (
                            <td key={`travel-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums ${redClass || 'text-zinc-600 dark:border-zinc-700 dark:text-zinc-400'}`}>
                              {r.travel[key]}
                            </td>
                          );
                        })}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                          const redClass = key === 'av' ? redIfOver(r.mins_3[key], 'in_vineyard') : '';
                          return (
                            <td key={`in_vineyard-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums ${redClass ? redClass : `${STEP_GROUP_BG[3]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                              {r.mins_3[key]}
                            </td>
                          );
                        })}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                          const redClass = key === 'av' ? redIfOver(r.mins_5[key], 'in_winery') : '';
                          return (
                            <td key={`in_winery-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums ${redClass ? redClass : `${STEP_GROUP_BG[5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                              {r.mins_5[key]}
                            </td>
                          );
                        })}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                          const redClass = key === 'av' ? redIfOver(r.total[key], 'total') : '';
                          return (
                            <td key={`total-${key}`} className={`whitespace-nowrap border-zinc-200 px-2 py-2 tabular-nums font-medium ${redClass || 'text-zinc-700 dark:border-zinc-700 dark:text-zinc-300'}`}>
                              {r.total[key]}
                            </td>
                          );
                        })}
                      </tr>
                    ); })
                  )}
                </tbody>
                {rowsByDay.length > 0 && (
                  <tfoot className="border-t-2 border-zinc-300 bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800">
                    {byDayFooterSets.map((set) => {
                      const s = set.stats;
                      const subKeys = dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const);
                      const footOutlier = footerStatsOutlierSeverity(s, set.jobCount);
                      const footFlag =
                        footOutlier === 'red'
                          ? SUMMARY_OUTLIER_RED
                          : footOutlier === 'yellow'
                            ? SUMMARY_OUTLIER_YELLOW
                            : '';
                      return (
                        <tr key={set.rowType ?? 'all'} className="border-b border-zinc-200 dark:border-zinc-700">
                          <td
                            className={`border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400${footFlag ? ` ${footFlag}` : ''}`}
                          >
                            Total / Av
                          </td>
                          {splitByLimits && (
                            <td className="border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
                              {set.rowType}
                            </td>
                          )}
                          <td className="border-r border-zinc-200 px-3 py-1.5 tabular-nums text-xs text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                            {set.jobCount}
                          </td>
                          {!dayClientView && [s.mins_2, s.mins_3, s.mins_4, s.mins_5].map((quad, i) =>
                            subKeys.map((key) => {
                              const val = key === 'total' ? quad.total : key === 'max' ? quad.max : key === 'min' ? quad.min : quad.av;
                              const redClass = key === 'av' && val != null ? redIfOverStepCol(val, i + 2) : '';
                              return (
                                <td key={`${i}-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-xs ${redClass ? redClass : `${STEP_GROUP_BG[(i + 2) as 2 | 3 | 4 | 5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                                  {val ?? '—'}
                                </td>
                              );
                            })
                          )}
                          {subKeys.map((key) => {
                            const val = key === 'total' ? s.travel.total : key === 'max' ? s.travel.max : key === 'min' ? s.travel.min : s.travel.av;
                            const redClass = key === 'av' && val != null ? redIfOver(val, 'travel') : '';
                            return (
                              <td key={`travel-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-xs ${redClass || (val != null ? 'text-zinc-600 dark:border-zinc-700 dark:text-zinc-400' : 'text-zinc-600 dark:border-zinc-700 dark:text-zinc-400')}`}>
                                {val ?? '—'}
                              </td>
                            );
                          })}
                          {subKeys.map((key) => {
                            const val = key === 'total' ? s.mins_3.total : key === 'max' ? s.mins_3.max : key === 'min' ? s.mins_3.min : s.mins_3.av;
                            const redClass = key === 'av' && val != null ? redIfOver(val, 'in_vineyard') : '';
                            return (
                              <td key={`in_vineyard-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-xs ${redClass ? redClass : `${STEP_GROUP_BG[3]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                                {val ?? '—'}
                              </td>
                            );
                          })}
                          {subKeys.map((key) => {
                            const val = key === 'total' ? s.mins_5.total : key === 'max' ? s.mins_5.max : key === 'min' ? s.mins_5.min : s.mins_5.av;
                            const redClass = key === 'av' && val != null ? redIfOver(val, 'in_winery') : '';
                            return (
                              <td key={`in_winery-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-xs ${redClass ? redClass : `${STEP_GROUP_BG[5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                                {val ?? '—'}
                              </td>
                            );
                          })}
                          {subKeys.map((key) => {
                            const val = key === 'total' ? s.total.total : key === 'max' ? s.total.max : key === 'min' ? s.total.min : s.total.av;
                            const redClass = key === 'av' && val != null ? redIfOver(val, 'total') : '';
                            return (
                              <td key={`total-${key}`} className={`border-zinc-200 px-2 py-1.5 tabular-nums text-xs font-medium ${redClass || (val != null ? 'text-zinc-700 dark:border-zinc-700 dark:text-zinc-300' : 'text-zinc-700 dark:border-zinc-700 dark:text-zinc-300')}`}>
                                {val ?? '—'}
                              </td>
                            );
                          })}
                        </tr>
                      );
                    })}
                  </tfoot>
                )}
              </table>
            </div>
          )}
          {summaryTab === 'by_job' && (
          <div
            ref={byJobTableScrollRef}
            className="max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900"
          >
            <table className="w-full min-w-[64rem] text-left text-sm">
              <thead className="sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]">
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  <th colSpan={9} className="border-r border-zinc-200 px-2 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-700" title={timeLimitRows.length > 0 ? "Per job: red if value > limit from row where Winery = delivery_winery, Vineyard group = vineyard_group, and TT = trailermode (T / TT). Boxes below show selected Time Limits row for reference only." : undefined}>
                    Limit (red if &gt;){timeLimitRows.length > 0 ? ' (per winery)' : ''}
                  </th>
                  {!jobClientView && (
                    <>
                      {[2, 3, 4, 5].map((n) => {
                        const key = `mins_${n}`;
                        const st = columnStats[key];
                        const groupBg = STEP_GROUP_BG[n];
                        return (
                          <React.Fragment key={n}>
                            <th className={`border-r border-zinc-200 px-1 py-1 dark:border-zinc-700 ${groupBg}`}>
                              <div className="flex flex-col gap-0.5">
                                <input
                                  type="number"
                                  min={0}
                                  placeholder="—"
                                  value={minsThresholds[String(n)] ?? ''}
                                  onChange={(e) => setMinsThreshold(String(n), e.target.value)}
                                  onClick={(e) => e.stopPropagation()}
                                  className="w-12 rounded border border-zinc-300 bg-white px-1 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                                  title={`Step ${n} Mins: red if value > this`}
                                />
                                {st && st.count > 0 && (
                                  <span className="text-[10px] text-zinc-500 dark:text-zinc-400 tabular-nums">
                                    Σ {st.sum} · avg {st.avg ?? '—'}
                                  </span>
                                )}
                              </div>
                            </th>
                            <th colSpan={2} className={`border-r border-zinc-200 dark:border-zinc-700 ${groupBg}`} />
                          </React.Fragment>
                        );
                      })}
                    </>
                  )}
                  <th className="border-r border-zinc-200 px-1 py-1 dark:border-zinc-700">
                    <div className="flex flex-col gap-0.5">
                      <input
                        type="number"
                        min={0}
                        placeholder="—"
                        value={minsThresholds.travel ?? ''}
                        onChange={(e) => setMinsThreshold('travel', e.target.value)}
                        onClick={(e) => e.stopPropagation()}
                        className="w-12 rounded border border-zinc-300 bg-white px-1 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                        title="Travel: red if value > this"
                      />
                      {columnStats.travel?.count > 0 && (
                        <span className="text-[10px] text-zinc-500 dark:text-zinc-400 tabular-nums">
                          Σ {columnStats.travel.sum} · avg {columnStats.travel.avg ?? '—'}
                        </span>
                      )}
                    </div>
                  </th>
                  <th className="border-r border-zinc-200 px-1 py-1 dark:border-zinc-700">
                    <div className="flex flex-col gap-0.5">
                      <input
                        type="number"
                        min={0}
                        placeholder="—"
                        value={minsThresholds.in_vineyard ?? ''}
                        onChange={(e) => setMinsThreshold('in_vineyard', e.target.value)}
                        onClick={(e) => e.stopPropagation()}
                        className="w-12 rounded border border-zinc-300 bg-white px-1 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                        title="In Vineyard (step 3 mins): red if value > this"
                      />
                      {columnStats.in_vineyard?.count > 0 && (
                        <span className="text-[10px] text-zinc-500 dark:text-zinc-400 tabular-nums">
                          Σ {columnStats.in_vineyard.sum} · avg {columnStats.in_vineyard.avg ?? '—'}
                        </span>
                      )}
                    </div>
                  </th>
                  <th className="border-r border-zinc-200 px-1 py-1 dark:border-zinc-700">
                    <div className="flex flex-col gap-0.5">
                      <input
                        type="number"
                        min={0}
                        placeholder="—"
                        value={minsThresholds.in_winery ?? ''}
                        onChange={(e) => setMinsThreshold('in_winery', e.target.value)}
                        onClick={(e) => e.stopPropagation()}
                        className="w-12 rounded border border-zinc-300 bg-white px-1 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                        title="In Winery (step 5 mins): red if value > this"
                      />
                      {columnStats.in_winery?.count > 0 && (
                        <span className="text-[10px] text-zinc-500 dark:text-zinc-400 tabular-nums">
                          Σ {columnStats.in_winery.sum} · avg {columnStats.in_winery.avg ?? '—'}
                        </span>
                      )}
                    </div>
                  </th>
                  <th className="border-zinc-200 px-1 py-1 dark:border-zinc-700">
                    <div className="flex flex-col gap-0.5">
                      <input
                        type="number"
                        min={0}
                        placeholder="—"
                        value={minsThresholds.total ?? ''}
                        onChange={(e) => setMinsThreshold('total', e.target.value)}
                        onClick={(e) => e.stopPropagation()}
                        className="w-12 rounded border border-zinc-300 bg-white px-1 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                        title="Total: red if value > this"
                      />
                      {columnStats.total?.count > 0 && (
                        <span className="text-[10px] text-zinc-500 dark:text-zinc-400 tabular-nums">
                          Σ {columnStats.total.sum} · avg {columnStats.total.avg ?? '—'}
                        </span>
                      )}
                    </div>
                  </th>
                </tr>
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  {BY_JOB_LEAD_COLUMNS.map(({ key, label }) => (
                    <th
                      key={key}
                      rowSpan={jobClientView ? 1 : 2}
                      onClick={() => handleSort(key)}
                      className={`cursor-pointer select-none whitespace-nowrap border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === key ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                      title={key === 'limits_breached' ? 'X = total time exceeds winery total limit; click to sort' : 'Click to sort'}
                    >
                      {label} {sortKey === key ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                    </th>
                  ))}
                  {jobClientView && (
                    <th
                      onClick={() => handleSort('step_1_actual_time')}
                      className={`cursor-pointer select-none whitespace-nowrap border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === 'step_1_actual_time' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                    >
                      Start Time {sortKey === 'step_1_actual_time' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                    </th>
                  )}
                  {!jobClientView && (
                    <>
                      <th colSpan={2} className={`border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 dark:border-zinc-700 ${STEP_GROUP_BG[1]}`}>
                        {STEP_LABELS[1]}
                      </th>
                      {[2, 3, 4, 5].map((n) => (
                        <th key={n} colSpan={3} className={`border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 dark:border-zinc-700 ${STEP_GROUP_BG[n]}`}>
                          {STEP_LABELS[n]}
                        </th>
                      ))}
                    </>
                  )}
                  <th
                    rowSpan={2}
                    onClick={() => handleSort('travel')}
                    className={`cursor-pointer select-none whitespace-nowrap border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === 'travel' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                  >
                    Travel {sortKey === 'travel' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th
                    rowSpan={2}
                    onClick={() => handleSort('in_vineyard')}
                    className={`cursor-pointer select-none whitespace-nowrap border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === 'in_vineyard' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                  >
                    In Vineyard {sortKey === 'in_vineyard' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th
                    rowSpan={2}
                    onClick={() => handleSort('in_winery')}
                    className={`cursor-pointer select-none whitespace-nowrap border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === 'in_winery' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                  >
                    In Winery {sortKey === 'in_winery' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th
                    rowSpan={2}
                    onClick={() => handleSort('total')}
                    className={`cursor-pointer select-none whitespace-nowrap border-b border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === 'total' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                  >
                    Total {sortKey === 'total' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                </tr>
                {!jobClientView && (
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  <th
                    onClick={() => handleSort('step_1_actual_time')}
                    className={`cursor-pointer select-none whitespace-nowrap border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-700 ${STEP_GROUP_BG[1]} ${sortKey === 'step_1_actual_time' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                  >
                    Time {sortKey === 'step_1_actual_time' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th
                    onClick={() => handleSort('step_1_via')}
                    className={`cursor-pointer select-none whitespace-nowrap border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-700 ${STEP_GROUP_BG[1]} ${sortKey === 'step_1_via' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                  >
                    Via {sortKey === 'step_1_via' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  {[2, 3, 4, 5].map((n) => {
                    const groupBg = STEP_GROUP_BG[n];
                    return (
                      <React.Fragment key={n}>
                        <th
                          onClick={() => handleSort(`mins_${n}`)}
                          className={`cursor-pointer select-none whitespace-nowrap border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-700 ${groupBg} ${sortKey === `mins_${n}` ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                        >
                        Mins {sortKey === `mins_${n}` ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                        </th>
                        <th
                          onClick={() => handleSort(`step_${n}_actual_time`)}
                          className={`cursor-pointer select-none whitespace-nowrap border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-700 ${groupBg} ${sortKey === `step_${n}_actual_time` ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                        >
                          Time {sortKey === `step_${n}_actual_time` ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                        </th>
                        <th
                          onClick={() => handleSort(`step_${n}_via`)}
                          className={`cursor-pointer select-none whitespace-nowrap border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-700 ${groupBg} ${sortKey === `step_${n}_via` ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                        >
                          Via {sortKey === `step_${n}_via` ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                        </th>
                      </React.Fragment>
                    );
                  })}
                </tr>
                )}
              </thead>
              <tbody>
                {sortedRows.length === 0 ? (
                  <tr>
                    <td colSpan={jobClientView ? 12 : allColumns.length} className="px-3 py-6 text-center text-zinc-500">
                      No rows {filterActualFrom || filterActualTo || effectiveCustomer || filterTemplate || filterTruckId || filterWorker.trim() || filterTrailermode ? '(try relaxing filters)' : ''}.
                    </td>
                  </tr>
                ) : (
                  sortedRowsPage.map((row, i) => {
                    const isHistoryFocusRow =
                      focusHighlightJobId != null &&
                      String(focusHighlightJobId).trim() !== '' &&
                      String(row.job_id ?? '').trim() === String(focusHighlightJobId).trim();
                    const historyPlainLeadHighlight = (k: string) =>
                      isHistoryFocusRow &&
                      (k === 'Customer' || k === 'job_id' || k === 'worker')
                        ? ' bg-sky-100 dark:bg-sky-900/45'
                        : '';
                    const jobTimeOutlier = timeOutlierSeverity(jobRowMinuteValues(row));
                    return (
                    <tr
                      key={i}
                      data-summary-focus-job={
                        row.job_id != null && String(row.job_id).trim() !== ''
                          ? String(row.job_id).trim()
                          : undefined
                      }
                      className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50"
                    >
                      {BY_JOB_LEAD_COLUMNS.map(({ key }) => {
                        const jobIdOutlierBg =
                          key === 'job_id'
                            ? jobTimeOutlier === 'red'
                              ? ` ${SUMMARY_OUTLIER_RED}`
                              : jobTimeOutlier === 'yellow'
                                ? ` ${SUMMARY_OUTLIER_YELLOW}`
                                : historyPlainLeadHighlight(key)
                            : historyPlainLeadHighlight(key);
                        return (
                        <td
                          key={key}
                          className={`whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300${jobIdOutlierBg}${key === 'limits_breached' ? ' text-center font-medium tabular-nums ' + (row.limits_breached === 'X' ? 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-200' : 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-200') : ''}`}
                        >
                          {key === 'limits_breached' ? (
                            row.limits_breached ?? '-'
                          ) : key === 'job_id' && viewMode !== 'client' ? (
                            <Link
                              href={inspectUrlForRow(row)}
                              onClick={(e) => {
                                if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return;
                                const meta = inspectJumpMetaForRow(row);
                                if (meta) registerPendingInspectForHistory(meta);
                              }}
                              className={
                                jobTimeOutlier
                                  ? 'font-medium text-inherit underline decoration-current hover:opacity-90'
                                  : 'text-blue-600 underline hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300'
                              }
                            >
                              {formatCell(row[key])}
                            </Link>
                          ) : key === 'Customer' ? (
                            formatCell(row['Customer'] ?? row['customer'])
                          ) : (
                            formatCell(row[key])
                          )}
                        </td>
                      );
                      })}
                      {jobClientView && (
                        <td className="whitespace-nowrap px-3 py-2 text-zinc-600 dark:text-zinc-400">
                          {formatDateDDMM(row.actual_start_time ?? row.step_1_actual_time ?? row.step_1_gps_completed_at ?? row.step_1_completed_at)}
                        </td>
                      )}
                      {!jobClientView && (() => {
                        const limitsRow = getMatchingLimitsRow(row);
                        return STEP_NUMS.map((n) => {
                          const actualKey = `step_${n}_actual_time`;
                          const viaKey = `step_${n}_via`;
                          const actualVal = row[actualKey] ?? row[`step_${n}_gps_completed_at`] ?? row[`step_${n}_completed_at`];
                          const viaVal = row[viaKey];
                          const viaStr = formatCell(viaVal);
                          const isGps = viaStr.toLowerCase() === 'gps';
                          const isGpsStar = viaStr === 'GPS*' || viaStr.toLowerCase() === 'gps*';
                          const isVineFencePlus = viaStr.toLowerCase() === 'vinefence+';
                          const isOride = viaStr.toUpperCase() === 'ORIDE';
                          const mins = n >= 2 ? minsBetween(row, n - 1, n) : null;
                          const minsOver = n >= 2 && isOverLimitStepWithRow(mins, limitsRow, n);
                          const groupBg = STEP_GROUP_BG[n];
                          const viaDisplay = isVineFencePlus ? 'GPS+' : viaStr;
                          const viaGreen = isGps || isVineFencePlus || isGpsStar;
                          return (
                            <React.Fragment key={n}>
                              {n >= 2 && (
                                <td
                                  className={`whitespace-nowrap px-3 py-2 tabular-nums ${minsOver ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : `${groupBg} text-zinc-500 dark:text-zinc-400`}`}
                                >
                                  {mins != null ? String(mins) : '—'}
                                </td>
                              )}
                            <td className={`whitespace-nowrap px-3 py-2 text-zinc-600 dark:text-zinc-400 ${groupBg}`}>
                              {formatDateDDMM(actualVal)}
                            </td>
                            <td
                              className={`whitespace-nowrap px-3 py-2 font-medium ${isOride ? 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-200' : viaGreen ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-200' : `${groupBg} text-zinc-700 dark:text-zinc-300`}`}
                            >
                              {viaDisplay}
                            </td>
                          </React.Fragment>
                          );
                        });
                      })()}
                      {(function travelSummaryCells() {
                        const limitsRow = getMatchingLimitsRow(row);
                        const travel = travelMins(row);
                        const inVineyard = minsBetween(row, 2, 3);
                        const inWinery = minsBetween(row, 4, 5);
                        const total = totalMins(row);
                        return (
                          <React.Fragment>
                            <td
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isOverLimitWithRow(travel, limitsRow, 'travel') ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {travel != null ? String(travel) : '—'}
                            </td>
                            <td
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isOverLimitWithRow(inVineyard, limitsRow, 'in_vineyard') ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {inVineyard != null ? String(inVineyard) : '—'}
                            </td>
                            <td
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isOverLimitWithRow(inWinery, limitsRow, 'in_winery') ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {inWinery != null ? String(inWinery) : '—'}
                            </td>
                            <td
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isOverLimitWithRow(total, limitsRow, 'total') ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {total != null ? String(total) : '—'}
                            </td>
                          </React.Fragment>
                        );
                      })()}
                    </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
          )}
          <div className="mt-3 flex flex-wrap items-center gap-3 text-zinc-500">
            <span>
              {summaryTab === 'by_job'
                ? (() => {
                    const n = sortedRows.length;
                    const start = n === 0 ? 0 : jobsPage * jobsPageSize + 1;
                    const end = Math.min((jobsPage + 1) * jobsPageSize, n);
                    return n > jobsPageSize
                      ? `${start}–${end} of ${n} jobs`
                      : `${n} row${n !== 1 ? 's' : ''}`;
                  })()
                : `${rowsByDay.length} day${rowsByDay.length !== 1 ? 's' : ''} · ${filteredRows.length} job${filteredRows.length !== 1 ? 's' : ''}${filteredRows.length !== totalJobsFromApi ? ` (client filter ${filteredRows.length} vs API ${totalJobsFromApi})` : ''}`}
            </span>
            {summaryTab === 'by_job' && sortedRows.length > jobsPageSize && (
              <span className="flex items-center gap-2">
                <button
                  type="button"
                  disabled={jobsPage === 0 || loading}
                  onClick={() => setJobsPage((p) => Math.max(0, p - 1))}
                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800"
                >
                  Prev
                </button>
                <span className="text-xs">
                  Page {jobsPage + 1} of {Math.ceil(sortedRows.length / jobsPageSize)}
                </span>
                <button
                  type="button"
                  disabled={jobsPage >= Math.ceil(sortedRows.length / jobsPageSize) - 1 || loading}
                  onClick={() => setJobsPage((p) => p + 1)}
                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800"
                >
                  Next
                </button>
              </span>
            )}
          </div>
          {jobsQueryDebug && (summaryTab === 'season' || summaryTab === 'by_day') && (
            <details className="mt-4 rounded border border-zinc-200 bg-zinc-50 p-3 text-xs dark:border-zinc-600 dark:bg-zinc-900/50">
              <summary className="cursor-pointer font-medium text-zinc-700 dark:text-zinc-300">
                Debug: job count SQL (criteria for this summary)
              </summary>
              <pre className="mt-2 max-h-48 overflow-auto whitespace-pre-wrap break-all font-mono text-zinc-600 dark:text-zinc-400">
                {jobsQueryDebug.debugSqlLiteral}
              </pre>
              <p className="mt-2 font-mono text-[10px] text-zinc-500 dark:text-zinc-500">
                Params JSON: {JSON.stringify(jobsQueryDebug.debugSqlParams)}
              </p>
            </details>
          )}
        </>
      )}
    </div>
  );
}

export default function SummaryPage() {
  return (
    <Suspense
      fallback={
        <div className="flex min-h-[200px] items-center justify-center p-6 text-zinc-500 dark:text-zinc-400">
          Loading…
        </div>
      }
    >
      <SummaryPageInner />
    </Suspense>
  );
}

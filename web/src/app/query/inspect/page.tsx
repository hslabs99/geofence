'use client';

import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { formatColumnLabel, formatDateNZ, computeColumnWidths } from '@/lib/utils';
import { formatIntNz } from '@/lib/format-nz';
import { addMinutesToTimestampAsNZ, runFetchStepsForJobs } from '@/lib/fetch-steps';
import { buildInspectDerivedStepsExplanation } from '@/lib/inspect-derived-steps-explanation';
import { buildInspectGpsWindowForJob } from '@/lib/inspect-gps-window';
import StaffOnlyGate from '@/components/StaffOnlyGate';

type Row = Record<string, unknown>;

// Inspect uses its own column-order and sort settings (branch from Vwork for independent development)
const COLUMN_ORDER_TABLE = 'tbl_vworkjobs_inspect';
const SORT_SETTING_TYPE = 'System';
const SORT_SETTING_NAME = 'Inspectsort';

/**
 * Virtual columns G1..G5: green Y / red N flag for whether step_N_gps_completed_at has data.
 * Computed client-side; not sortable; not filterable. Replaces the wide truck_id column on Inspect.
 */
const GPS_STEP_VIRTUAL_COLUMNS = ['g1', 'g2', 'g3', 'g4', 'g5'] as const;
const GPS_STEP_VIRTUAL_COLUMN_SET = new Set<string>(GPS_STEP_VIRTUAL_COLUMNS);

/** API columns kept on row data (so right-pane logic still works) but never shown as a grid column. */
const HIDDEN_API_COLUMNS = new Set<string>(['truck_id']);

const PRIORITY_COLUMNS = [
  'job_id', 'planned_start_time', 'customer', 'template',
  'delivery_winery', 'vineyard_name', 'distance', 'loadsize', 'trailermode', 'worker',
  ...GPS_STEP_VIRTUAL_COLUMNS, 'truck_rego',
];

const INSPECT_PAGE_SIZE = 200;
/** Must match API cap on repeated `jobId=` (Data Audit → Inspect). */
const MAX_AUDIT_JOB_FILTER_IDS = 5000;
const AUDIT_JOBS_STORAGE_PREFIX = 'geodata_inspect_audit_jobs_';

function isIsoDateString(v: unknown): v is string {
  return typeof v === 'string' && /^\d{4}-\d{2}-\d{2}/.test(v);
}

/** Normalize timestamp to YYYY-MM-DD HH:mm:ss for comparison (e.g. step deltas). */
function normalizeForCompare(v: unknown): string | null {
  if (v == null || v === '') return null;
  const s = String(v).trim().replace(/\s+(?:GMT|UTC)[+-]\d{3,4}.*$/i, '').trim();
  if (!s) return null;
  const dmy = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2}):(\d{2})/);
  if (dmy) {
    const yy = dmy[3].length === 2 ? `20${dmy[3]}` : dmy[3];
    return `${yy}-${dmy[2].padStart(2, '0')}-${dmy[1].padStart(2, '0')} ${dmy[4].padStart(2, '0')}:${dmy[5]}:${dmy[6]}`;
  }
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2})?:?(\d{2})?:?(\d{2})?/);
  if (iso) {
    const h = iso[4]?.padStart(2, '0') ?? '00';
    const m = iso[5]?.padStart(2, '0') ?? '00';
    const sec = iso[6]?.padStart(2, '0') ?? '00';
    return `${iso[1]}-${iso[2]}-${iso[3]} ${h}:${m}:${sec}`;
  }
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(s)) return s.slice(0, 19);
  return null;
}

/** Minutes from prev to curr (curr - prev). Inspect UI only; returns null if either missing. */
function minutesBetweenInspect(prev: unknown, curr: unknown): number | null {
  const a = normalizeForCompare(prev);
  const b = normalizeForCompare(curr);
  if (!a || !b) return null;
  const ma = a.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  const mb = b.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  if (!ma || !mb) return null;
  const msA = Date.UTC(Number(ma[1]), Number(ma[2]) - 1, Number(ma[3]), Number(ma[4]), Number(ma[5]), Number(ma[6]));
  const msB = Date.UTC(Number(mb[1]), Number(mb[2]) - 1, Number(mb[3]), Number(mb[4]), Number(mb[5]), Number(mb[6]));
  return Math.round((msB - msA) / 60000);
}

/** Tokens appended to tbl_vworkjobs.calcnotes (suffix “:”, space-separated). Longest match first in regex. */
const CALCNOTE_KNOWN_TAG_RE =
  /(VineFenceV\+\([^)]+\):|VineFenceV\+:|VineFence\+:|Step3windback:|GPS\*:|VineSR1:)/g;

const CALCNOTE_TAG_HINTS: Record<string, string> = {
  'Step3windback:':
    'Cleanup: GPS step 4 (winery ENTER) is trusted; VWork step 3 was after that time. Step 3 actual was set by winding back from step 4 using morning travel (step 2 − step 1), or if that would not stay after step 2, the midpoint between step 2 and step 4.',
  'VineFence+:':
    'Steps+ used a buffered vineyard fence: step 2 and/or 3 had no raw ENTER/EXIT in the job window, so times were taken from GPS points inside an expanded polygon buffer.',
  'VineFenceV+:':
    'Steps+ widened only arrive vineyard: polygon ENTER/EXIT existed; buffered fence pulled step 2 more than 5 minutes earlier than polygon ENTER (queue at gate). Shorter deltas are ignored. Step 3 uses polygon EXIT verbatim.',
  'GPS*:':
    'Same-vineyard re-entry smoothing: the driver briefly left and re-entered the vineyard fence set with no other fence ENTER/EXIT between; step 3 uses the last EXIT in the chain (up to a few loops).',
  'VineSR1:':
    'Bankhouse South rule: when South vineyard polygons did not yield both step 2 and 3, times were taken from the mapped Bankhouse fence set instead.',
};

function calcnoteHintForSeg(seg: string): string | undefined {
  const s = seg.trim();
  const direct = CALCNOTE_TAG_HINTS[s as keyof typeof CALCNOTE_TAG_HINTS];
  if (direct) return direct;
  if (/^VineFenceV\+\([^)]+\):$/.test(s)) {
    return CALCNOTE_TAG_HINTS['VineFenceV+:'];
  }
  return undefined;
}

function readInspectStepGps(row: Row | null | undefined, n: number): string | null {
  if (!row) return null;
  const v =
    row[`step_${n}_gps_completed_at`] ??
    row[`Step_${n}_GPS_completed_at`] ??
    row[`Step_${n}_GPS_Completed_At`];
  if (v == null || v === '') return null;
  const s = String(v).trim();
  return s === '' ? null : s;
}

function readInspectStepVia(row: Row | null | undefined, n: number): string {
  if (!row) return '';
  const v = row[`step_${n}_via`] ?? row[`Step_${n}_Via`];
  if (v == null || v === '') return '';
  return String(v).trim();
}

/** Extra lines when tags say VineFence+ but Step 2/3 GPS columns are empty — explains Via vs Step_N_GPS_completed_at. */
function calcnoteVineFenceGpsGapHint(row: Row | null | undefined, tag: string): string {
  if (!row) return '';
  const t = tag.trim();
  if (t !== 'VineFence+:' && t !== 'VineFenceV+:' && !/^VineFenceV\+\([^)]+\):$/.test(t)) return '';
  const v2 = readInspectStepVia(row, 2);
  const v3 = readInspectStepVia(row, 3);
  const vineFenceVia = v2.includes('VineFence') || v3.includes('VineFence');
  if (!vineFenceVia) return '';
  const g2 = readInspectStepGps(row, 2);
  const g3 = readInspectStepGps(row, 3);
  if (g2 && g3) return '';
  return `\n\n— GPS columns vs Via —\nCalcNotes records that Steps+ ran (buffered vineyard). step_2_via / step_3_via can still show VineFence+ while step_2_gps_completed_at / step_3_gps_completed_at are empty if the merged GPS-layer times were cleared by guardrails after merge (e.g. vs job end / ordering) or did not pass Steps+ filters—final step times may then come from VWork. “GPS Step Data” only shows step_N_gps_completed_at from the DB, not Via alone.`;
}

/** Hover or keyboard focus shows an explanation overlay for each known calcnote token. */
function CalcNotesRichText({ text, row }: { text: string; row?: Row | null }) {
  const trimmed = text.trim();
  if (!trimmed) {
    return <span className="text-sm text-zinc-500 dark:text-zinc-400">—</span>;
  }
  const segments = trimmed.split(CALCNOTE_KNOWN_TAG_RE).filter((s) => s.length > 0);
  return (
    <span className="inline-flex flex-wrap items-baseline gap-x-0 text-sm text-zinc-800 dark:text-zinc-200">
      {segments.map((seg, i) => {
        const hint = calcnoteHintForSeg(seg);
        if (hint != null) {
          const fullHint = hint + calcnoteVineFenceGpsGapHint(row ?? null, seg);
          return (
            <span
              key={`cn-${i}-${seg}`}
              tabIndex={0}
              className="group relative inline cursor-help rounded-sm border-b border-dotted border-zinc-400 outline-none focus-visible:ring-2 focus-visible:ring-blue-500/60 dark:border-zinc-500 dark:focus-visible:ring-amber-500/50"
            >
              {seg}
              <span
                role="tooltip"
                className="pointer-events-none absolute left-0 top-full z-[60] mt-1.5 hidden max-h-[min(40vh,16rem)] w-[min(22rem,calc(100vw-2rem))] overflow-y-auto whitespace-pre-wrap rounded-md border border-zinc-200 bg-white px-2.5 py-2 text-left text-xs font-normal leading-snug text-zinc-700 shadow-lg group-hover:block group-focus-visible:block dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-200"
              >
                {fullHint}
              </span>
            </span>
          );
        }
        return (
          <span key={`cn-t-${i}`} className="whitespace-pre-wrap">
            {seg}
          </span>
        );
      })}
    </span>
  );
}

function getDefaultColumnOrder(apiColumns: string[]): string[] {
  const priority = PRIORITY_COLUMNS.filter((c) => apiColumns.includes(c));
  const rest = apiColumns.filter((c) => !PRIORITY_COLUMNS.includes(c));
  return [...priority, ...rest];
}

function mergeColumnOrder(apiColumns: string[], saved: string[] | null): string[] {
  const defaultOrder = getDefaultColumnOrder(apiColumns);
  if (!saved || saved.length === 0) return defaultOrder;
  const set = new Set(apiColumns);
  const ordered = saved.filter((c) => set.has(c));
  for (const c of apiColumns) if (!ordered.includes(c)) ordered.push(c);
  return ordered;
}

/** Keep Distance / Load Size / trailermode (TT) immediately after Vineyard when present (Inspect default). */
function normalizeInspectColumnOrder(ordered: string[]): string[] {
  const mapped = ordered.map((c) => (c === 'trailertype' ? 'trailermode' : c));
  const deduped: string[] = [];
  for (const c of mapped) {
    if (c === 'trailermode' && deduped.includes('trailermode')) continue;
    deduped.push(c);
  }
  if (!deduped.includes('vineyard_name')) return deduped;
  const dist = 'distance';
  const ls = 'loadsize';
  const tm = 'trailermode';

  const insert = [dist, ls, tm].filter((c) => deduped.includes(c));
  if (insert.length === 0) return deduped;

  const rest = deduped.filter((c) => c !== dist && c !== ls && c !== tm);
  const vi = rest.indexOf('vineyard_name');
  if (vi === -1) return deduped;
  rest.splice(vi + 1, 0, ...insert);
  return rest;
}

function InspectContent() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const jobIdParam = searchParams.get('jobId');
  const locateJobIdParam = searchParams.get('locateJobId');
  const paramToLocate = locateJobIdParam ?? jobIdParam;
  const [rows, setRows] = useState<Row[]>([]);
  /** False until first jobs list fetch finishes (success or error). */
  const [hasCompletedInitialFetch, setHasCompletedInitialFetch] = useState(false);
  /** True while refetching after filters/sort/page (keeps grid visible). */
  const [listRefreshing, setListRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [errorDetail, setErrorDetail] = useState<{ hint?: string; code?: string } | null>(null);
  const [sortColumns, setSortColumns] = useState<[string, string, string]>(['', '', '']);
  /** When the jobs list is empty, keep the last non-empty row shape so filters/sort/columns stay usable. */
  const lastNonEmptyInspectColumnKeysRef = useRef<string[]>([]);
  /** Column-date filter for `actual_end_time` only (datetime-local → API columnDate*). */
  const [filterActualEndFrom, setFilterActualEndFrom] = useState<string>('');
  const [filterActualEndTo, setFilterActualEndTo] = useState<string>('');
  const [filterCustomer, setFilterCustomer] = useState<string>('');
  const [filterTemplate, setFilterTemplate] = useState<string>('');
  const [filterTruckId, setFilterTruckId] = useState<string>('');
  const [filterWorker, setFilterWorker] = useState<string>('');
  const [filterTrailermode, setFilterTrailermode] = useState<string>('');
  const [filterWinery, setFilterWinery] = useState<string>('');
  const [filterVineyard, setFilterVineyard] = useState<string>('');
  const [filterLoadsize, setFilterLoadsize] = useState<string>('');
  /** Job id find (Enter): jump to page containing this job; list is not narrowed (same as URL locateJobId). */
  const [findJobIdDraft, setFindJobIdDraft] = useState<string>('');
  const [findJobIdTarget, setFindJobIdTarget] = useState<string>('');
  const [filterPlannedFrom, setFilterPlannedFrom] = useState<string>('');
  const [filterPlannedTo, setFilterPlannedTo] = useState<string>('');
  const [filterActualFrom, setFilterActualFrom] = useState<string>('');
  const [filterActualTo, setFilterActualTo] = useState<string>('');
  const [jobsPage, setJobsPage] = useState(0);
  const [totalJobsFromApi, setTotalJobsFromApi] = useState(0);
  const [lastApiQueryString, setLastApiQueryString] = useState('');
  const [apiDebugFromResponse, setApiDebugFromResponse] = useState<Record<string, unknown> | null>(null);
  const [locateJobNotFound, setLocateJobNotFound] = useState(false);
  const [truckIdsOptions, setTruckIdsOptions] = useState<string[]>([]);
  const [trailermodeOptions, setTrailermodeOptions] = useState<string[]>([]);
  const [wineryOptions, setWineryOptions] = useState<string[]>([]);
  const [vineyardOptions, setVineyardOptions] = useState<string[]>([]);
  const [loadsizeOptions, setLoadsizeOptions] = useState<string[]>([]);
  const [customersOptions, setCustomersOptions] = useState<string[]>([]);
  const [templateOptions, setTemplateOptions] = useState<string[]>([]);
  const [workerOptions, setWorkerOptions] = useState<string[]>([]);
  /** Data Audit pivot → Inspect: job IDs only (other form filters ignored for the list). */
  const [auditJobFilterIds, setAuditJobFilterIds] = useState<string[] | null>(null);
  const auditUrlHandledRef = useRef<string | null>(null);
  const skipJobsPageFetchRef = useRef(false);
  const jobsListFetchedOnceRef = useRef(false);
  const prevFilterKeyRef = useRef('');
  const locateResolveDoneRef = useRef(false);
  const userClearedLocateRef = useRef(false);
  const [columnOrder, setColumnOrder] = useState<string[]>([]);
  const [hiddenColumns, setHiddenColumns] = useState<Set<string>>(new Set());
  const [showColumnConfig, setShowColumnConfig] = useState(false);
  /** Row counts, pagination summary, full API query string + debug — hidden by default (saves space). */
  const [showListInfoApiDebug, setShowListInfoApiDebug] = useState(false);
  const columnOrderInitialized = useRef(false);
  const [selectedRowIndex, setSelectedRowIndex] = useState(0);
  const selectedRowRef = useRef<HTMLTableRowElement>(null);
  const [gpsMappings, setGpsMappings] = useState<{ type: string; vwname: string; gpsname: string }[]>([]);
  const [gpsMappingsLoading, setGpsMappingsLoading] = useState(false);
  const [trackingRows, setTrackingRows] = useState<Row[]>([]);
  const [trackingSql, setTrackingSql] = useState<string>('');
  const [trackingLoading, setTrackingLoading] = useState(false);
  const [trackingSortCol, setTrackingSortCol] = useState<string>('position_time_nz');
  const [trackingSortDir, setTrackingSortDir] = useState<'asc' | 'desc'>('asc');
  const [trackingPage, setTrackingPage] = useState(1);
  const [trackingPageSize, setTrackingPageSize] = useState(200);
  const [trackingTotal, setTrackingTotal] = useState(0);
  const [trackingTableView, setTrackingTableView] = useState<'raw' | 'entry_exit'>('entry_exit');
  /** When set, GPS grid uses this window instead of job start/end (e.g. from clicking a step time). */
  const [gpsWindowOverride, setGpsWindowOverride] = useState<{ positionAfter: string; positionBefore: string } | null>(null);
  const [startLessMinutes, setStartLessMinutes] = useState(10);
  const [endPlusMinutes, setEndPlusMinutes] = useState(60);
  /** Display-only: extra minutes before/after the Settings window for GPS data (does not affect step fetching). */
  const [displayExpandBefore, setDisplayExpandBefore] = useState(0);
  const [displayExpandAfter, setDisplayExpandAfter] = useState(0);
  /** Full JSON from last /api/tracking/derived-steps?writeBack=1 (initialPass + stepsPlusReport + debug + steps). */
  const [derivedStepsLogPayload, setDerivedStepsLogPayload] = useState<Record<string, unknown> | null>(null);
  const [derivedStepsLogTab, setDerivedStepsLogTab] = useState<'explanation' | 'json'>('explanation');
  const [showMappingSection, setShowMappingSection] = useState(false);
  const [refetchStepsRunning, setRefetchStepsRunning] = useState(false);
  const [retagAndRefetchRunning, setRetagAndRefetchRunning] = useState(false);
  const [trackingRefreshKey, setTrackingRefreshKey] = useState(0);
  /** Inspect-only: Steps+ buffer containment per tbl_tracking.id (current page). */
  const [fencePlusById, setFencePlusById] = useState<Record<string, string | null>>({});
  const [fencePlusMeta, setFencePlusMeta] = useState<{ bufferMeters: number; fenceNames: string[] } | null>(null);
  const [fencePlusLoading, setFencePlusLoading] = useState(false);
  const [fencePlusErr, setFencePlusErr] = useState<string | null>(null);
  const [pinJobModalOpen, setPinJobModalOpen] = useState(false);
  const [pinJobNoteDraft, setPinJobNoteDraft] = useState('');
  const [pinJobSaving, setPinJobSaving] = useState(false);
  const [pinJobError, setPinJobError] = useState<string | null>(null);
  const pinJobNoteRef = useRef<HTMLTextAreaElement>(null);
  /** Manual overrides (oride) per step + comment; synced from selectedRow when it changes. */
  const [stepOverrides, setStepOverrides] = useState<{ step1oride: string; step2oride: string; step3oride: string; step4oride: string; step5oride: string }>({
    step1oride: '', step2oride: '', step3oride: '', step4oride: '', step5oride: '',
  });
  const [steporidecomment, setSteporidecomment] = useState('');
  /** excluded=1: omit from Summary rollups; still listed on By Job. */
  const [excludedFromSummaries, setExcludedFromSummaries] = useState(false);
  const [excludednotes, setExcludednotes] = useState('');
  const [saveOverridesStatus, setSaveOverridesStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const apiColumns = useMemo(() => {
    let keys: string[];
    if (rows.length > 0) {
      keys = Object.keys(rows[0]);
      lastNonEmptyInspectColumnKeysRef.current = keys;
    } else {
      keys = lastNonEmptyInspectColumnKeysRef.current;
    }
    // CRITICAL: return [] when we have no real columns yet. Otherwise the
    // saved-column-order load effect fires on initial mount with only the
    // synthetic G1..G5 columns; mergeColumnOrder then filters the user's
    // saved order down to just those five and locks it in via the
    // columnOrderInitialized ref. Real columns appear later but get appended
    // at the end, the user's saved order is effectively lost on refresh, and
    // the next save persists that scrambled order.
    if (keys.length === 0) return [];
    // Drop columns we never display (truck_id, *_raw plumbing, _total window-fn artifact) and
    // append the synthetic G1..G5 GPS-presence flags so the column-config / sort / order machinery
    // treats them as first-class columns.
    //
    // *_raw is critical: the /api/vworkjobs response only deletes a `<col>_raw` key when its value
    // is a string; null timestamps leave the *_raw key on the row, so the visible key set drifts
    // depending on which row is rows[0]. That made `inspectSortInitKey` change on every refetch
    // and re-applied the saved sort each time, clobbering the user's Sort 1/2/3 picks.
    const filtered = keys.filter(
      (k) =>
        !HIDDEN_API_COLUMNS.has(k) &&
        !GPS_STEP_VIRTUAL_COLUMN_SET.has(k) &&
        k !== '_total' &&
        !k.endsWith('_raw'),
    );
    return [...filtered, ...GPS_STEP_VIRTUAL_COLUMNS];
  }, [rows]);

  /** Filters only (not sort) — changing sort does not reset page; sort is applied in API. */
  const inspectFilterOnlyKey = useMemo(
    () =>
      JSON.stringify({
        filterCustomer,
        filterTemplate,
        filterTruckId,
        filterWorker,
        filterTrailermode,
        filterWinery,
        filterVineyard,
        filterLoadsize,
        filterPlannedFrom,
        filterPlannedTo,
        filterActualFrom,
        filterActualTo,
        filterActualEndFrom,
        filterActualEndTo,
        auditJobIdsKey:
          auditJobFilterIds && auditJobFilterIds.length > 0 ? auditJobFilterIds.join('\u0001') : '',
      }),
    [
      filterCustomer,
      filterTemplate,
      filterTruckId,
      filterWorker,
      filterTrailermode,
      filterWinery,
      filterVineyard,
      filterLoadsize,
      filterPlannedFrom,
      filterPlannedTo,
      filterActualFrom,
      filterActualTo,
      filterActualEndFrom,
      filterActualEndTo,
      auditJobFilterIds,
    ],
  );

  const inspectDataKey = useMemo(
    () =>
      JSON.stringify({
        filterOnly: inspectFilterOnlyKey,
        sortColumns: [...sortColumns] as [string, string, string],
      }),
    [inspectFilterOnlyKey, sortColumns],
  );

  const locateTarget = useMemo(
    () => findJobIdTarget.trim() || String(paramToLocate ?? '').trim(),
    [findJobIdTarget, paramToLocate],
  );

  useEffect(() => {
    if (prevFilterKeyRef.current !== '' && prevFilterKeyRef.current !== inspectFilterOnlyKey) {
      setJobsPage(0);
    }
    prevFilterKeyRef.current = inspectFilterOnlyKey;
  }, [inspectFilterOnlyKey]);

  useEffect(() => {
    locateResolveDoneRef.current = false;
  }, [locateTarget]);

  useEffect(() => {
    const ref = searchParams.get('auditJobIdsRef')?.trim();
    if (!ref) return;
    if (auditUrlHandledRef.current === ref) return;
    auditUrlHandledRef.current = ref;
    try {
      const raw = localStorage.getItem(`${AUDIT_JOBS_STORAGE_PREFIX}${ref}`);
      if (raw) {
        const parsed = JSON.parse(raw) as { ids?: unknown };
        const ids = Array.isArray(parsed.ids)
          ? parsed.ids.map((x) => String(x).trim()).filter(Boolean)
          : [];
        const cap = ids.slice(0, MAX_AUDIT_JOB_FILTER_IDS);
        if (cap.length > 0) setAuditJobFilterIds(cap);
      }
    } catch {
      /* ignore */
    }
    try {
      localStorage.removeItem(`${AUDIT_JOBS_STORAGE_PREFIX}${ref}`);
    } catch {
      /* ignore */
    }
    const next = new URLSearchParams(searchParams.toString());
    next.delete('auditJobIdsRef');
    const q = next.toString();
    router.replace(q ? `${pathname}?${q}` : pathname, { scroll: false });
  }, [searchParams, pathname, router]);

  useEffect(() => {
    fetch('/api/vworkjobs/customers')
      .then((r) => r.json())
      .then((d) => setCustomersOptions(Array.isArray(d?.customers) ? d.customers : []))
      .catch(() => setCustomersOptions([]));
  }, []);

  useEffect(() => {
    const c = filterCustomer.trim();
    const q = c ? `?customer=${encodeURIComponent(c)}` : '';
    const ac = new AbortController();
    fetch(`/api/vworkjobs/filter-options${q}`, { signal: ac.signal })
      .then((r) => r.json())
      .then((d) => {
        setTruckIdsOptions(Array.isArray(d?.truckIds) ? d.truckIds : []);
        setTrailermodeOptions(Array.isArray(d?.trailermodes) ? d.trailermodes : []);
        setWineryOptions(Array.isArray(d?.deliveryWineries) ? d.deliveryWineries : []);
        setVineyardOptions(Array.isArray(d?.vineyardNames) ? d.vineyardNames : []);
        setLoadsizeOptions(Array.isArray(d?.loadSizes) ? d.loadSizes : []);
        setWorkerOptions(Array.isArray(d?.workers) ? d.workers : []);
      })
      .catch((err) => {
        if ((err as Error).name === 'AbortError') return;
        setTruckIdsOptions([]);
        setTrailermodeOptions([]);
        setWineryOptions([]);
        setVineyardOptions([]);
        setLoadsizeOptions([]);
        setWorkerOptions([]);
      });
    return () => ac.abort();
  }, [filterCustomer]);

  useEffect(() => {
    const c = filterCustomer.trim();
    if (!c) {
      setTemplateOptions([]);
      return;
    }
    const ac = new AbortController();
    fetch(`/api/vworkjobs/templates?customer=${encodeURIComponent(c)}`, { signal: ac.signal })
      .then((r) => r.json())
      .then((d) => setTemplateOptions(Array.isArray(d?.templates) ? d.templates : []))
      .catch((err) => {
        if ((err as Error).name === 'AbortError') return;
        setTemplateOptions([]);
      });
    return () => ac.abort();
  }, [filterCustomer]);

  useEffect(() => {
    if (apiColumns.length === 0 || columnOrderInitialized.current) return;
    columnOrderInitialized.current = true;
    const cols = apiColumns;
    const table = new URLSearchParams({ table: COLUMN_ORDER_TABLE }).toString();
    fetch(`/api/column-order?${table}`, { cache: 'no-store' })
      .then((r) => r.json())
      .then((data) => {
        const saved = Array.isArray(data?.columnOrder) && data.columnOrder.every((x: unknown) => typeof x === 'string')
          ? (data.columnOrder as string[])
          : null;
        // Saved orders are user-chosen — apply verbatim (modulo merging in any new API columns).
        // Only normalize the default order (when nothing has been saved yet) so distance/loadsize/trailermode
        // sit beside vineyard_name out of the box.
        const merged = saved && saved.length > 0
          ? mergeColumnOrder(cols, saved)
          : normalizeInspectColumnOrder(getDefaultColumnOrder(cols));
        setColumnOrder(merged);
        const hidden = Array.isArray(data?.hiddenColumns) && data.hiddenColumns.every((x: unknown) => typeof x === 'string')
          ? new Set(data.hiddenColumns as string[])
          : new Set<string>();
        setHiddenColumns(hidden);
      })
      .catch(() => setColumnOrder(normalizeInspectColumnOrder(getDefaultColumnOrder(cols))));
  }, [apiColumns.length, apiColumns]);

  /** Latched once data first arrives; prevents the saved-sort init effect from ever re-firing
   *  and clobbering the user's dropdown / saved-load state on later refetches. */
  const sortInitDoneRef = useRef(false);

  useEffect(() => {
    if (apiColumns.length === 0) return;
    if (sortInitDoneRef.current) return;
    sortInitDoneRef.current = true;

    const locate = locateJobIdParam?.trim();
    if (locate && apiColumns.includes('actual_start_time')) {
      setSortColumns(['actual_start_time', '', '']);
      return;
    }
    const ac = new AbortController();
    const q = new URLSearchParams({ type: SORT_SETTING_TYPE, name: SORT_SETTING_NAME }).toString();
    fetch(`/api/settings?${q}`, { cache: 'no-store', signal: ac.signal })
      .then((r) => r.json())
      .then((data) => {
        if (ac.signal.aborted) return;
        if (data?.settingvalue) {
          try {
            const arr = JSON.parse(data.settingvalue) as unknown[];
            if (Array.isArray(arr) && arr.every((x) => typeof x === 'string')) {
              const cols = new Set(apiColumns);
              const valid = arr.filter((c): c is string => typeof c === 'string' && cols.has(c));
              setSortColumns([valid[0] ?? '', valid[1] ?? '', valid[2] ?? '']);
              return;
            }
          } catch {}
        }
        // No usable saved value — leave state empty so appendInspectSortParams falls back to actual_start_time.
      })
      .catch(() => {
        if (ac.signal.aborted) return;
      });
    return () => ac.abort();
    // Only `apiColumns.length` and `locateJobIdParam` — the body itself guards with the
    // ref so it runs exactly once per mount (or once after a locate URL change cleared the ref).
  }, [apiColumns.length, locateJobIdParam, apiColumns]);

  /** When the locate URL param actually changes (Recent jobs / Summary link), allow sort to be re-initialized. */
  useEffect(() => {
    sortInitDoneRef.current = false;
  }, [locateJobIdParam]);

  useEffect(() => {
    const type = 'System';
    Promise.all([
      fetch(`/api/settings?${new URLSearchParams({ type, name: 'InspectStartLess' })}`, { cache: 'no-store' }).then((r) => r.json()),
      fetch(`/api/settings?${new URLSearchParams({ type, name: 'InspectEndPlus' })}`, { cache: 'no-store' }).then((r) => r.json()),
    ])
      .then(([data1, data2]) => {
        const v1 = data1?.settingvalue;
        if (v1 != null && v1 !== '') {
          const n = parseInt(String(v1), 10);
          if (!Number.isNaN(n) && n >= 0) setStartLessMinutes(Math.min(1440, n));
        }
        const v2 = data2?.settingvalue;
        if (v2 != null && v2 !== '') {
          const n = parseInt(String(v2), 10);
          if (!Number.isNaN(n) && n >= 0) setEndPlusMinutes(Math.min(1440, n));
        }
      })
      .catch(() => {});
  }, []);

  const allColumns = useMemo(() => {
    if (apiColumns.length === 0) return [];
    const order =
      columnOrder.length > 0 ? columnOrder : getDefaultColumnOrder(apiColumns);
    const set = new Set(apiColumns);
    const ordered = order.filter((c) => set.has(c));
    for (const c of apiColumns) if (!ordered.includes(c)) ordered.push(c);
    return ordered;
  }, [apiColumns, columnOrder]);

  const columns = useMemo(
    () => allColumns.filter((c) => !hiddenColumns.has(c)),
    [allColumns, hiddenColumns],
  );

  /** Sort dropdowns offer real DB columns only — virtual G1..G5 are computed client-side and not sortable. */
  const sortableColumns = useMemo(
    () => allColumns.filter((c) => !GPS_STEP_VIRTUAL_COLUMN_SET.has(c)),
    [allColumns],
  );

  /** Save status for column order/visibility — mirrors the sort Save button so UI feedback is consistent. */
  const [columnSaveStatus, setColumnSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const columnSaveResetTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const saveColumnConfig = useCallback((order: string[], hidden: Set<string>) => {
    const orderToSave = order.length ? order : allColumns;
    if (!orderToSave.length) return;
    const payload = {
      table: COLUMN_ORDER_TABLE,
      columnOrder: orderToSave,
      hiddenColumns: Array.from(hidden),
    };
    if (columnSaveResetTimer.current) {
      clearTimeout(columnSaveResetTimer.current);
      columnSaveResetTimer.current = null;
    }
    setColumnSaveStatus('saving');
    fetch('/api/column-order', {
      method: 'PUT',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
      .then(async (r) => {
        const body = await r.json().catch(() => ({}));
        if (r.ok) {
          setColumnSaveStatus('saved');
        } else {
          const details = { status: r.status, statusText: r.statusText, url: r.url, payload, responseBody: body };
          console.error('[Inspect save columns] FAILED:', JSON.stringify(details, null, 2));
          setColumnSaveStatus('error');
        }
      })
      .catch((err) => {
        console.error('[Inspect save columns] FETCH ERROR:', err);
        setColumnSaveStatus('error');
      })
      .finally(() => {
        columnSaveResetTimer.current = setTimeout(() => setColumnSaveStatus('idle'), 2000);
      });
  }, [allColumns]);

  const toggleColumnVisibility = (col: string) => {
    const next = new Set(hiddenColumns);
    if (next.has(col)) next.delete(col);
    else next.add(col);
    setHiddenColumns(next);
    saveColumnConfig(columnOrder, next);
  };

  const showAllColumns = () => {
    setHiddenColumns(new Set());
    saveColumnConfig(columnOrder, new Set());
  };

  const resetColumnConfig = () => {
    const defaultOrder = normalizeInspectColumnOrder(getDefaultColumnOrder(apiColumns));
    setColumnOrder(defaultOrder);
    setHiddenColumns(new Set());
    saveColumnConfig(defaultOrder, new Set());
    setShowColumnConfig(false);
  };

  const [dragCol, setDragCol] = useState<string | null>(null);
  const [dropTargetCol, setDropTargetCol] = useState<string | null>(null);

  const handleColumnDragStart = (col: string) => setDragCol(col);
  const handleColumnDragOver = (e: React.DragEvent, col: string) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    if (dragCol && dragCol !== col) setDropTargetCol(col);
  };
  const handleColumnDragLeave = () => setDropTargetCol(null);
  const handleColumnDrop = (e: React.DragEvent, targetCol: string) => {
    e.preventDefault();
    setDropTargetCol(null);
    setDragCol(null);
    if (!dragCol || dragCol === targetCol) return;
    const fromIdx = allColumns.indexOf(dragCol);
    const toIdx = allColumns.indexOf(targetCol);
    if (fromIdx === -1 || toIdx === -1) return;
    const next = [...allColumns];
    next.splice(fromIdx, 1);
    next.splice(toIdx, 0, dragCol);
    setColumnOrder(next);
    saveColumnConfig(next, hiddenColumns);
  };
  const handleColumnDragEnd = () => {
    setDragCol(null);
    setDropTargetCol(null);
  };

  const loadGpsMappings = useCallback(async () => {
    setGpsMappingsLoading(true);
    try {
      const r = await fetch(`/api/gpsmappings?_=${Date.now()}`, { cache: 'no-store' });
      const data = (await r.json().catch(() => ({}))) as { rows?: { type?: string; vwname?: string; gpsname?: string }[] };
      setGpsMappings(Array.isArray(data?.rows) ? (data.rows as { type: string; vwname: string; gpsname: string }[]) : []);
    } catch {
      setGpsMappings([]);
    } finally {
      setGpsMappingsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadGpsMappings();
  }, [loadGpsMappings]);

  const appendInspectSortParams = useCallback((p: URLSearchParams) => {
    const c1 = sortColumns[0]?.trim() || 'actual_start_time';
    p.set('sortColumn', c1);
    const c2 = sortColumns[1]?.trim();
    if (c2) p.set('sortColumn2', c2);
    const c3 = sortColumns[2]?.trim();
    if (c3) p.set('sortColumn3', c3);
    p.set('sortDir', 'asc');
  }, [sortColumns]);

  const buildInspectApiParams = useCallback(
    (opts: { resolveJobId: string | null }) => {
      if (auditJobFilterIds && auditJobFilterIds.length > 0) {
        const p = new URLSearchParams();
        p.set('limit', String(INSPECT_PAGE_SIZE));
        p.set('offset', String(jobsPage * INSPECT_PAGE_SIZE));
        appendInspectSortParams(p);
        const cap = auditJobFilterIds.slice(0, MAX_AUDIT_JOB_FILTER_IDS);
        for (const id of cap) {
          const t = id.trim();
          if (t) p.append('jobId', t);
        }
        return p;
      }
      const p = new URLSearchParams();
      p.set('limit', String(INSPECT_PAGE_SIZE));
      p.set('offset', String(jobsPage * INSPECT_PAGE_SIZE));
      if (opts.resolveJobId) p.set('resolvePageForJob', opts.resolveJobId);
      appendInspectSortParams(p);
      if (filterCustomer.trim()) p.set('customer', filterCustomer.trim());
      if (filterTemplate.trim()) p.set('template', filterTemplate.trim());
      if (filterTruckId.trim()) p.set('truck_id', filterTruckId.trim());
      if (filterWorker.trim()) p.set('device', filterWorker.trim());
      if (filterTrailermode.trim()) p.set('trailermode', filterTrailermode.trim());
      if (filterWinery.trim()) p.set('winery', filterWinery.trim());
      if (filterVineyard.trim()) p.set('vineyard', filterVineyard.trim());
      if (filterLoadsize.trim()) p.set('loadsize', filterLoadsize.trim());
      if (filterPlannedFrom?.trim() && /^\d{4}-\d{2}-\d{2}/.test(filterPlannedFrom)) p.set('plannedDateFrom', filterPlannedFrom.slice(0, 10));
      if (filterPlannedTo?.trim() && /^\d{4}-\d{2}-\d{2}/.test(filterPlannedTo)) p.set('plannedDateTo', filterPlannedTo.slice(0, 10));
      if (filterActualFrom?.trim() && /^\d{4}-\d{2}-\d{2}/.test(filterActualFrom)) p.set('dateFrom', filterActualFrom.slice(0, 10));
      if (filterActualTo?.trim() && /^\d{4}-\d{2}-\d{2}/.test(filterActualTo)) p.set('dateTo', filterActualTo.slice(0, 10));
      const hasActualEndColFilter = !!(filterActualEndFrom?.trim() || filterActualEndTo?.trim());
      if (hasActualEndColFilter) {
        p.set('columnDateCol', 'actual_end_time');
        const df = filterActualEndFrom?.trim().slice(0, 10);
        const dt = filterActualEndTo?.trim().slice(0, 10);
        if (df && /^\d{4}-\d{2}-\d{2}$/.test(df)) p.set('columnDateFrom', df);
        if (dt && /^\d{4}-\d{2}-\d{2}$/.test(dt)) p.set('columnDateTo', dt);
      }
      const fromUrl = searchParams.get('actualFrom') ?? '';
      const toUrl = searchParams.get('actualTo') ?? '';
      const hasLocate = !!(searchParams.get('locateJobId') ?? searchParams.get('jobId'));
      if (
        hasLocate &&
        fromUrl &&
        toUrl &&
        /^\d{4}-\d{2}-\d{2}$/.test(fromUrl) &&
        /^\d{4}-\d{2}-\d{2}$/.test(toUrl) &&
        !hasActualEndColFilter &&
        !filterActualFrom?.trim() &&
        !filterActualTo?.trim()
      ) {
        p.set('dateFrom', fromUrl);
        p.set('dateTo', toUrl);
      }
      return p;
    },
    [
      jobsPage,
      sortColumns,
      filterCustomer,
      filterTemplate,
      filterTruckId,
      filterWorker,
      filterTrailermode,
      filterWinery,
      filterVineyard,
      filterLoadsize,
      filterPlannedFrom,
      filterPlannedTo,
      filterActualFrom,
      filterActualTo,
      filterActualEndFrom,
      filterActualEndTo,
      searchParams,
      auditJobFilterIds,
      appendInspectSortParams,
    ],
  );

  /** Merge one job from DB into the grid — no full list reload (avoids loading flash / re-scroll). */
  const refreshJobRowFromApi = useCallback(async (jobId: unknown) => {
    const id = String(jobId ?? '').trim();
    if (!id) return;
    const p = new URLSearchParams();
    p.set('limit', '1');
    p.set('offset', '0');
    appendInspectSortParams(p);
    p.set('jobIdExact', id);
    try {
      const res = await fetch(`/api/vworkjobs?${p.toString()}`, { cache: 'no-store' });
      const data = await res.json();
      if (!res.ok) throw new Error(typeof data?.error === 'string' ? data.error : res.statusText);
      const fresh = (data.rows ?? [])[0] as Row | undefined;
      if (!fresh) return;
      const jid = String(fresh.job_id ?? '').trim();
      setRows((prev) => {
        let hit = false;
        const next = prev.map((r) => {
          if (String(r.job_id ?? '').trim() === jid) {
            hit = true;
            return fresh;
          }
          return r;
        });
        return hit ? next : prev;
      });
    } catch (e) {
      console.error('[Inspect refresh job row]', e);
    }
  }, [appendInspectSortParams]);

  useEffect(() => {
    if (skipJobsPageFetchRef.current) {
      skipJobsPageFetchRef.current = false;
      return;
    }
    let cancelled = false;
    const want = locateTarget.trim();
    const auditActive = !!(auditJobFilterIds && auditJobFilterIds.length > 0);
    const useResolve =
      !auditActive && !!want && !userClearedLocateRef.current && !locateResolveDoneRef.current;
    const params = buildInspectApiParams({ resolveJobId: useResolve ? want : null });
    const qs = params.toString();
    if (jobsListFetchedOnceRef.current) setListRefreshing(true);
    fetch(`/api/vworkjobs?${qs}`)
      .then(async (res) => {
        const data = await res.json();
        if (!res.ok) {
          setErrorDetail({ hint: data?.hint, code: data?.code });
          throw new Error(data?.error ?? res.statusText);
        }
        return data;
      })
      .then((data) => {
        if (cancelled) return;
        setRows(data.rows ?? []);
        setTotalJobsFromApi(typeof data.total === 'number' ? data.total : (data.rows ?? []).length);
        setLastApiQueryString(qs);
        setApiDebugFromResponse((data.debug as Record<string, unknown>) ?? null);
        setLocateJobNotFound(!!data.jobNotFound);
        setError(null);
        setErrorDetail(null);
        if (typeof data.resolvePage === 'number') {
          skipJobsPageFetchRef.current = true;
          locateResolveDoneRef.current = true;
          setJobsPage(data.resolvePage);
        }
      })
      .catch((e) => {
        if (!cancelled) setError(e.message);
      })
      .finally(() => {
        if (!cancelled) {
          jobsListFetchedOnceRef.current = true;
          setHasCompletedInitialFetch(true);
          setListRefreshing(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [inspectDataKey, jobsPage, buildInspectApiParams, locateTarget, auditJobFilterIds]);

  useEffect(() => {
    setFilterTemplate('');
    setFilterWinery('');
    setFilterVineyard('');
    setFilterLoadsize('');
    setFilterWorker('');
  }, [filterCustomer]);

  const clearAllFilters = useCallback(() => {
    setFilterCustomer('');
    setFilterTemplate('');
    setFilterTruckId('');
    setFilterWorker('');
    setFilterTrailermode('');
    setFilterWinery('');
    setFilterVineyard('');
    setFilterLoadsize('');
    setFindJobIdDraft('');
    setFindJobIdTarget('');
    setFilterPlannedFrom('');
    setFilterPlannedTo('');
    setFilterActualFrom('');
    setFilterActualTo('');
    setFilterActualEndFrom('');
    setFilterActualEndTo('');
    setAuditJobFilterIds(null);
    setJobsPage(0);
  }, []);

  /** Row order matches /api/vworkjobs ORDER BY (sortColumn, sortColumn2, sortColumn3). */
  const sortedRows = rows;

  const selectedRow = sortedRows[Math.min(selectedRowIndex, Math.max(0, sortedRows.length - 1))] ?? sortedRows[0] ?? null;

  /** Refetch steps for the selected job only (force: ignores steps_fetched). Same runFetchStepsForJobs as API GPS Import. */
  const refetchStepsForSelectedJob = useCallback(async () => {
    if (!selectedRow) return;
    setRefetchStepsRunning(true);
    setDerivedStepsLogPayload(null);
    try {
      const result = await runFetchStepsForJobs({
        jobs: [selectedRow],
        startLessMinutes,
        endPlusMinutes,
      });
      if (result.log.some((e) => e.status === 'ok')) {
        await refreshJobRowFromApi(selectedRow.job_id);
        setTrackingRefreshKey((k) => k + 1);
      }
      if (result.lastResult != null && typeof result.lastResult === 'object') {
        setDerivedStepsLogPayload({ ...(result.lastResult as Record<string, unknown>), _inspectSingleJob: true });
        setDerivedStepsLogTab('explanation');
      }
    } finally {
      setRefetchStepsRunning(false);
    }
  }, [selectedRow, startLessMinutes, endPlusMinutes, refreshJobRowFromApi]);

  /** URL params: with locateJobId (Recent jobs / Summary job link), only truck — clear other persisted filters and all date filters so the job is not hidden. */
  useEffect(() => {
    const truck = searchParams.get('truckId') ?? '';
    const worker = searchParams.get('worker') ?? '';
    const actualFrom = searchParams.get('actualFrom') ?? '';
    const actualTo = searchParams.get('actualTo') ?? '';
    const jobId = searchParams.get('jobId') ?? '';
    const locateJobId = searchParams.get('locateJobId') ?? '';
    if (locateJobId) {
      setFilterCustomer('');
      setFilterTemplate('');
      setFilterTrailermode('');
      setFilterWinery('');
      setFilterVineyard('');
      setFilterLoadsize('');
      setFilterWorker('');
      setFindJobIdDraft('');
      setFindJobIdTarget('');
      setFilterPlannedFrom('');
      setFilterPlannedTo('');
      setFilterActualFrom('');
      setFilterActualTo('');
      setFilterActualEndFrom('');
      setFilterActualEndTo('');
      // Prefer worker filter when provided (audit links), otherwise allow truck filter (recent jobs).
      if (worker && worker.trim()) {
        setFilterTruckId('');
        setFilterWorker(worker);
      } else {
        setFilterWorker('');
        setFilterTruckId(truck);
      }
    } else {
      if (truck) setFilterTruckId(truck);
      if (worker) setFilterWorker(worker);
      if (actualFrom) setFilterActualFrom(actualFrom);
      if (actualTo) setFilterActualTo(actualTo);
      if (jobId && !locateJobId) {
        setFindJobIdDraft(jobId);
      }
    }
  }, [searchParams]);

  /** Add this job to sidebar "Recent jobs" history when we landed with locateJobId/jobId or used Find. */
  useEffect(() => {
    const jobId = locateTarget.trim();
    if (!jobId) return;
    fetch('/api/inspect-history', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_id: jobId }),
    }).catch(() => {});
  }, [locateTarget]);

  /** When user manually clicks a different row (not the located job), stop re-applying locate on sortedRows changes. */
  useEffect(() => {
    if (!locateTarget) userClearedLocateRef.current = false;
  }, [locateTarget]);

  /** Hyperlink arrival: apply filters first (URL effect above), then after data + sort are ready, locate the job. Re-run whenever sortedRows changes so when sort/column config loads we re-locate; stop once user has selected a different row. */
  useEffect(() => {
    if (!locateTarget || !hasCompletedInitialFetch || sortedRows.length === 0 || userClearedLocateRef.current) return;
    const want = locateTarget.trim();
    const idx = sortedRows.findIndex((r) => String(r.job_id ?? '').trim() === want);
    if (idx >= 0) {
      setSelectedRowIndex(idx);
    }
  }, [locateTarget, hasCompletedInitialFetch, sortedRows]);

  /** Scroll to the located/found job row once it's rendered (URL locateJobId/jobId or Find box after Enter). */
  useEffect(() => {
    if (!locateTarget || selectedRowIndex < 0) return;
    const selectedRowForScroll = sortedRows[selectedRowIndex];
    if (!selectedRowForScroll || String(selectedRowForScroll.job_id ?? '').trim() !== locateTarget.trim()) return;
    const raf = requestAnimationFrame(() => {
      selectedRowRef.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    });
    return () => cancelAnimationFrame(raf);
  }, [locateTarget, selectedRowIndex, sortedRows]);

  /** Fixed Inspect UI labels (do not use raw VWork step_N_name here). */
  const STEP_ROWS = [
    {
      completedKey: 'step_1_completed_at',
      actualTimeKey: 'step_1_actual_time',
      displayLabel: 'Step 1: Start Job (Depart Winery)',
    },
    {
      completedKey: 'step_2_completed_at',
      actualTimeKey: 'step_2_actual_time',
      displayLabel: 'Step 2: Arrive Vineyard',
    },
    {
      completedKey: 'step_3_completed_at',
      actualTimeKey: 'step_3_actual_time',
      displayLabel: 'Step 3: Depart Vineyard',
    },
    {
      completedKey: 'step_4_completed_at',
      actualTimeKey: 'step_4_actual_time',
      displayLabel: 'Step 4: Arrive Winery',
    },
    {
      completedKey: 'step_5_completed_at',
      actualTimeKey: 'step_5_actual_time',
      displayLabel: 'Step 5: Complete Job',
    },
  ] as const;

  /** Sync override state from selected job (from API/DB). */
  useEffect(() => {
    if (!selectedRow) return;
    const raw = (key: string) => {
      const v = selectedRow[key] ?? (selectedRow as Row)[key];
      return v != null && v !== '' ? String(v).trim() : '';
    };
    setStepOverrides({
      step1oride: raw('step1oride'),
      step2oride: raw('step2oride'),
      step3oride: raw('step3oride'),
      step4oride: raw('step4oride'),
      step5oride: raw('step5oride'),
    });
    setSteporidecomment(raw('steporidecomment'));
    const ex = selectedRow.excluded;
    setExcludedFromSummaries(Number(selectedRow.excluded) === 1);
    const en = selectedRow.excludednotes;
    setExcludednotes(en != null && String(en) !== '' ? String(en).slice(0, 250) : '');
  }, [selectedRow?.job_id]);

  const vineyardName = selectedRow ? String(selectedRow.vineyard_name ?? '') : '';
  const deliveryWinery = selectedRow ? String(selectedRow.delivery_winery ?? '') : '';
  const truckId = selectedRow ? String(selectedRow.truck_id ?? '') : '';
  /** Device for tbl_tracking: worker (device_name) not truck_id — GPS records are keyed by worker. */
  const deviceForTracking = selectedRow
    ? String((selectedRow.worker ?? selectedRow.truck_id) ?? '').trim()
    : '';
  const plannedStartTime = selectedRow ? String(selectedRow.planned_start_time ?? '') : '';
  const actualStartTime = selectedRow ? String(selectedRow.actual_start_time ?? '') : '';
  const actualEndTime = selectedRow
    ? String(selectedRow.actual_end_time ?? selectedRow.gps_end_time ?? '')
    : '';

  const vineyardTrim = (vineyardName ?? '').trim();
  const wineryTrim = (deliveryWinery ?? '').trim();

  /** All rows in tbl_gpsmappings that match this job's vineyard or winery (vwname or gpsname match; trimmed). */
  const relevantMappings = useMemo(() => {
    const out: { type: string; vwname: string; gpsname: string }[] = [];
    for (const m of gpsMappings) {
      const vw = (m.vwname ?? '').trim();
      const gps = (m.gpsname ?? '').trim();
      if (m.type === 'Vineyard' && vineyardTrim && (vw === vineyardTrim || gps === vineyardTrim)) {
        out.push(m);
      } else if (m.type === 'Winery' && wineryTrim && (vw === wineryTrim || gps === wineryTrim)) {
        out.push(m);
      }
    }
    return out;
  }, [gpsMappings, vineyardTrim, wineryTrim]);

  /** All fence names for SQL IN: every vwname and gpsname from relevant mappings, plus job vineyard/winery when no mapping. */
  const fenceNamesForInClause = useMemo(() => {
    const set = new Set<string>();
    for (const m of relevantMappings) {
      const vw = (m.vwname ?? '').trim();
      const gps = (m.gpsname ?? '').trim();
      if (vw) set.add(vw);
      if (gps) set.add(gps);
    }
    if (vineyardTrim && !relevantMappings.some((m) => m.type === 'Vineyard')) set.add(vineyardTrim);
    if (wineryTrim && !relevantMappings.some((m) => m.type === 'Winery')) set.add(wineryTrim);
    return [...set].filter(Boolean);
  }, [relevantMappings, vineyardTrim, wineryTrim]);

  /** GPS grid: tint rows whose `fence_name` matches mapped vineyard (green) or winery (blue) names. */
  const inspectGpsFenceLcSets = useMemo(() => {
    const vineyard = new Set<string>();
    const winery = new Set<string>();
    for (const m of relevantMappings) {
      const t = (m.type ?? '').trim();
      const vw = (m.vwname ?? '').trim();
      const gps = (m.gpsname ?? '').trim();
      if (t === 'Vineyard') {
        if (vw) vineyard.add(vw.toLowerCase());
        if (gps) vineyard.add(gps.toLowerCase());
      } else if (t === 'Winery') {
        if (vw) winery.add(vw.toLowerCase());
        if (gps) winery.add(gps.toLowerCase());
      }
    }
    if (vineyardTrim && !relevantMappings.some((m) => (m.type ?? '').trim() === 'Vineyard')) {
      vineyard.add(vineyardTrim.toLowerCase());
    }
    if (wineryTrim && !relevantMappings.some((m) => (m.type ?? '').trim() === 'Winery')) {
      winery.add(wineryTrim.toLowerCase());
    }
    return { vineyard, winery } as const;
  }, [relevantMappings, vineyardTrim, wineryTrim]);

  const gpsTrackingFenceRowClass = useCallback(
    (row: Row) => {
      const raw = row.fence_name ?? (row as { Fence_Name?: unknown }).Fence_Name;
      if (raw == null || raw === '') return '';
      const lc = String(raw).trim().toLowerCase();
      if (!lc) return '';
      if (inspectGpsFenceLcSets.vineyard.has(lc)) {
        return 'bg-green-50/95 hover:bg-green-100/95 dark:bg-green-950/45 dark:hover:bg-green-900/50';
      }
      if (inspectGpsFenceLcSets.winery.has(lc)) {
        return 'bg-blue-50/95 hover:bg-blue-100/95 dark:bg-blue-950/45 dark:hover:bg-blue-900/50';
      }
      return '';
    },
    [inspectGpsFenceLcSets],
  );

  /** Extract YYYY-MM-DD from job's actual_start_time (or similar timestamp) for tagging. */
  const jobDateForTagging = useMemo(() => {
    const raw = selectedRow?.actual_start_time ?? selectedRow?.actual_end_time ?? selectedRow?.planned_start_time;
    if (raw == null || raw === '') return '';
    const s = String(raw).trim();
    const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;
    const dmy = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
    if (dmy) {
      const yy = dmy[3].length === 2 ? `20${dmy[3]}` : dmy[3];
      return `${yy}-${dmy[2].padStart(2, '0')}-${dmy[1].padStart(2, '0')}`;
    }
    return '';
  }, [selectedRow]);

  /** Retag from actual_start_time − 2h to actual_start_time + 24h (then refetch steps). Ensures we cover the job window, not just midnight-to-midnight of start day. */
  const retagDayAndRefetchSteps = useCallback(async () => {
    if (!selectedRow || !deviceForTracking || !actualStartTime?.trim()) return;
    const startNorm = actualStartTime.trim();
    const windowStart = addMinutesToTimestampAsNZ(startNorm, -120); // 2 hours before job start
    const windowEnd = addMinutesToTimestampAsNZ(startNorm, 24 * 60); // 24 hours after job start
    const tagDateFrom = windowStart.slice(0, 10); // YYYY-MM-DD
    const tagDateTo = windowEnd.slice(0, 10);
    setRetagAndRefetchRunning(true);
    setDerivedStepsLogPayload(null);
    try {
      const tagRes = await fetch('/api/admin/tagging/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dateFrom: tagDateFrom,
          dateTo: tagDateTo,
          deviceNames: [deviceForTracking],
          graceSeconds: 300,
          bufferHours: 1,
        }),
      });
      if (!tagRes.ok) {
        const err = await tagRes.text();
        throw new Error(err || `Tagging ${tagRes.status}`);
      }
      // Consume the stream to completion (same as Tagging page) so the server runs full tagging; do not cancel.
      if (tagRes.body) {
        const reader = tagRes.body.getReader();
        while (true) {
          const { done } = await reader.read();
          if (done) break;
        }
      }
      const result = await runFetchStepsForJobs({
        jobs: [selectedRow],
        startLessMinutes,
        endPlusMinutes,
      });
      if (result.log.some((e) => e.status === 'ok')) {
        await refreshJobRowFromApi(selectedRow.job_id);
      }
      setTrackingRefreshKey((k) => k + 1);
      if (result.lastResult != null && typeof result.lastResult === 'object') {
        setDerivedStepsLogPayload({ ...(result.lastResult as Record<string, unknown>), _inspectSingleJob: true });
        setDerivedStepsLogTab('explanation');
      }
    } finally {
      setRetagAndRefetchRunning(false);
    }
  }, [selectedRow, deviceForTracking, actualStartTime, startLessMinutes, endPlusMinutes, refreshJobRowFromApi]);

  const submitPinJobToRecent = useCallback(async () => {
    const jobId = selectedRow?.job_id != null && selectedRow.job_id !== '' ? String(selectedRow.job_id).trim() : '';
    if (!jobId) return;
    setPinJobSaving(true);
    setPinJobError(null);
    try {
      const r = await fetch('/api/inspect-history', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_id: jobId, note: pinJobNoteDraft.trim() }),
      });
      const data = (await r.json().catch(() => ({}))) as { error?: string };
      if (!r.ok) {
        setPinJobError(typeof data?.error === 'string' ? data.error : `Failed (${r.status})`);
        return;
      }
      setPinJobModalOpen(false);
      setPinJobNoteDraft('');
      window.dispatchEvent(new Event('geodata-inspect-history-changed'));
    } finally {
      setPinJobSaving(false);
    }
  }, [selectedRow, pinJobNoteDraft]);

  const submitUnpinJobFromRecent = useCallback(async () => {
    const jobId = selectedRow?.job_id != null && selectedRow.job_id !== '' ? String(selectedRow.job_id).trim() : '';
    if (!jobId) return;
    setPinJobSaving(true);
    setPinJobError(null);
    try {
      const r = await fetch('/api/inspect-history', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_id: jobId, note: null }),
      });
      const data = (await r.json().catch(() => ({}))) as { error?: string };
      if (!r.ok) {
        setPinJobError(typeof data?.error === 'string' ? data.error : `Failed (${r.status})`);
        return;
      }
      setPinJobModalOpen(false);
      setPinJobNoteDraft('');
      window.dispatchEvent(new Event('geodata-inspect-history-changed'));
    } finally {
      setPinJobSaving(false);
    }
  }, [selectedRow]);

  useEffect(() => {
    if (!pinJobModalOpen) return;
    const id = requestAnimationFrame(() => pinJobNoteRef.current?.focus());
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        setPinJobModalOpen(false);
        setPinJobError(null);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => {
      cancelAnimationFrame(id);
      window.removeEventListener('keydown', onKey);
    };
  }, [pinJobModalOpen]);

  useEffect(() => {
    if (!deviceForTracking || !actualStartTime.trim()) {
      setTrackingRows([]);
      setTrackingSql('');
      setTrackingTotal(0);
      return;
    }
    const abort = new AbortController();
    setTrackingLoading(true);
    const offset = (trackingPage - 1) * trackingPageSize;
    const useOverride = gpsWindowOverride != null;
    let positionAfter: string;
    let positionBefore: string | null;
    if (useOverride) {
      positionAfter = gpsWindowOverride.positionAfter;
      positionBefore = gpsWindowOverride.positionBefore;
    } else {
      const win = buildInspectGpsWindowForJob(selectedRow as Record<string, unknown>, {
        startLessMinutes,
        endPlusMinutes,
        displayExpandBefore,
        displayExpandAfter,
      });
      if (win.error) {
        setTrackingRows([]);
        setTrackingSql('');
        setTrackingTotal(0);
        setTrackingLoading(false);
        return () => abort.abort();
      }
      positionAfter = win.positionAfter;
      positionBefore = win.positionBefore;
    }
    const params = new URLSearchParams({
      device: deviceForTracking,
      positionAfter,
      limit: String(trackingPageSize),
      offset: String(offset),
    });
    if (positionBefore) {
      params.set('positionBefore', positionBefore);
    }
    // Display: do not filter by job/mapping geofences — show all device movement in the time window. Steps logic (refetch, derived-steps) still uses job + mappings and is unchanged.
    const effectiveView = useOverride ? 'raw' : trackingTableView;
    if (effectiveView === 'entry_exit') {
      params.set('geofenceFilter', 'entry_or_exit');
    }
    fetch(`/api/tracking?${params}`, { signal: abort.signal })
      .then(async (r) => {
        const data = await r.json().catch(() => ({}));
        if (r.ok && data != null && Array.isArray(data.rows)) {
          setTrackingRows(data.rows);
          setTrackingSql(data.sqlCopyPaste ?? data.sql ?? '');
          setTrackingTotal(typeof data.total === 'number' ? data.total : 0);
        }
      })
      .catch((err) => {
        if (err?.name === 'AbortError') return;
        // Don't clear existing data on network error so we don't wipe the table
      })
      .finally(() => setTrackingLoading(false));
    return () => abort.abort();
  }, [
    deviceForTracking,
    actualStartTime,
    actualEndTime,
    trackingPage,
    trackingPageSize,
    trackingTableView,
    startLessMinutes,
    endPlusMinutes,
    displayExpandBefore,
    displayExpandAfter,
    trackingRefreshKey,
    gpsWindowOverride,
    selectedRow,
  ]);

  useEffect(() => {
    setTrackingPage(1);
  }, [
    deviceForTracking,
    actualStartTime,
    actualEndTime,
    trackingTableView,
    startLessMinutes,
    endPlusMinutes,
    displayExpandBefore,
    displayExpandAfter,
    gpsWindowOverride,
  ]);

  /** Clear step-time filter when job changes so we don't keep a stale window. */
  useEffect(() => {
    setGpsWindowOverride(null);
  }, [actualStartTime, actualEndTime]);

  /** Focus GPS grid on step time: RAW view, window = stepTime −2 min to job end +60 min. */
  const focusGpsOnStepTime = useCallback((stepTime: unknown) => {
    const normalized = normalizeForCompare(stepTime);
    if (!normalized || !actualEndTime?.trim()) return;
    setGpsWindowOverride({
      positionAfter: addMinutesToTimestampAsNZ(normalized, -2),
      positionBefore: addMinutesToTimestampAsNZ(actualEndTime.trim(), endPlusMinutes),
    });
    setTrackingTableView('raw');
    setTrackingPage(1);
    setTrackingRefreshKey((k) => k + 1);
  }, [actualEndTime, endPlusMinutes]);

  /** SQL WHERE fragment (text only, for later use): fence_name IN ('mapA','mapB',...) including direct (original) names when no mapping. */
  const mappingFenceNameInClause = useMemo(() => {
    if (fenceNamesForInClause.length === 0) return '';
    const escaped = fenceNamesForInClause.map((n) => `'${String(n).replace(/'/g, "''")}'`);
    return `fence_name IN (${escaped.join(', ')})`;
  }, [fenceNamesForInClause]);

  /** One row per mapping (or one per type when no mapping — direct). Multiple Vineyard/Winery mappings each get a row. */
  const mappingTableRows = useMemo(() => {
    const vineyardMappings = relevantMappings.filter((m) => m.type === 'Vineyard');
    const wineryMappings = relevantMappings.filter((m) => m.type === 'Winery');
    const rows: { type: string; original: string; result: string; toUse: string }[] = [];
    if (vineyardMappings.length) {
      vineyardMappings.forEach((m) => {
        const gps = (m.gpsname ?? '').trim();
        rows.push({ type: 'Vineyard', original: vineyardTrim || '—', result: gps || '—', toUse: gps || vineyardTrim || '—' });
      });
    } else if (vineyardTrim) {
      rows.push({ type: 'Vineyard', original: vineyardTrim, result: '—', toUse: vineyardTrim });
    }
    if (wineryMappings.length) {
      wineryMappings.forEach((m) => {
        const gps = (m.gpsname ?? '').trim();
        rows.push({ type: 'Winery', original: wineryTrim || '—', result: gps || '—', toUse: gps || wineryTrim || '—' });
      });
    } else if (wineryTrim) {
      rows.push({ type: 'Winery', original: wineryTrim, result: '—', toUse: wineryTrim });
    }
    return rows;
  }, [relevantMappings, vineyardTrim, wineryTrim]);

  /**
   * One row per Winery mapping (no dedupe). `pairWinery` matches tbl_distances.delivery_winery (job vWork name when set).
   */
  const inspectPairMatrixWineryColumns = useMemo(() => {
    const wRows = mappingTableRows.filter((r) => r.type === 'Winery');
    if (wRows.length === 0) {
      if (!wineryTrim) return [] as { id: string; header: string; pairWinery: string }[];
      return [{ id: 'winery-direct', header: wineryTrim, pairWinery: wineryTrim.trim() }];
    }
    return wRows.map((row, i) => {
      const orig = row.original !== '—' ? row.original.trim() : '';
      const res = row.result !== '—' ? row.result.trim() : '';
      const toUse = row.toUse !== '—' ? row.toUse.trim() : '';
      const header =
        res && toUse && res !== toUse
          ? `${res} → ${toUse}`
          : res || toUse || orig || `Winery ${i + 1}`;
      const pairWinery = (wineryTrim || toUse || res || orig).trim();
      return { id: `winery-row-${i}`, header, pairWinery };
    });
  }, [mappingTableRows, wineryTrim]);

  /**
   * One column per Vineyard row in Mapping (no dedupe — duplicate GPS targets stay separate columns).
   * `pairVineyard` is the string matched against tbl_distances.vineyard_name (job vWork name when set).
   */
  const inspectPairMatrixVineyardColumns = useMemo(() => {
    const vRows = mappingTableRows.filter((r) => r.type === 'Vineyard');
    if (vRows.length === 0) {
      if (!vineyardTrim) return [] as { id: string; header: string; pairVineyard: string }[];
      return [{ id: 'vineyard-direct', header: vineyardTrim, pairVineyard: vineyardTrim.trim() }];
    }
    return vRows.map((row, i) => {
      const orig = row.original !== '—' ? row.original.trim() : '';
      const res = row.result !== '—' ? row.result.trim() : '';
      const toUse = row.toUse !== '—' ? row.toUse.trim() : '';
      const header =
        res && toUse && res !== toUse
          ? `${res} → ${toUse}`
          : res || toUse || orig || `Vineyard ${i + 1}`;
      const pairVineyard = (vineyardTrim || toUse || res || orig).trim();
      return { id: `vineyard-col-${i}`, header, pairVineyard };
    });
  }, [mappingTableRows, vineyardTrim]);

  const inspectPairMatrixVineyardPairKeys = useMemo(
    () => [...new Set(inspectPairMatrixVineyardColumns.map((c) => c.pairVineyard).filter(Boolean))],
    [inspectPairMatrixVineyardColumns],
  );

  const inspectPairMatrixWineryPairKeys = useMemo(
    () => [...new Set(inspectPairMatrixWineryColumns.map((r) => r.pairWinery).filter(Boolean))],
    [inspectPairMatrixWineryColumns],
  );

  const [pairDistMatrixByKey, setPairDistMatrixByKey] = useState<
    Record<string, { distance_m: number | null; duration_min: number | null; manual_override: boolean }>
  >({});
  const [pairDistMatrixLoading, setPairDistMatrixLoading] = useState(false);
  const [pairDistMatrixErr, setPairDistMatrixErr] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedRow || inspectPairMatrixWineryPairKeys.length === 0 || inspectPairMatrixVineyardPairKeys.length === 0) {
      setPairDistMatrixByKey({});
      setPairDistMatrixErr(null);
      setPairDistMatrixLoading(false);
      return;
    }
    const pairs = inspectPairMatrixWineryPairKeys.flatMap((w) =>
      inspectPairMatrixVineyardPairKeys.map((v) => ({ winery: w, vineyard: v })),
    );
    if (pairs.length > 400) {
      setPairDistMatrixErr('Too many winery×vineyard combinations (max 400).');
      setPairDistMatrixByKey({});
      setPairDistMatrixLoading(false);
      return;
    }
    const ac = new AbortController();
    setPairDistMatrixLoading(true);
    setPairDistMatrixErr(null);
    fetch('/api/inspect/pair-distances-matrix', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pairs }),
      signal: ac.signal,
    })
      .then(async (r) => {
        const data = (await r.json().catch(() => ({}))) as {
          error?: string;
          pairs?: Array<{
            winery?: string;
            vineyard?: string;
            distance_m?: unknown;
            duration_min?: unknown;
            manual_override?: unknown;
          }>;
        };
        if (!r.ok) {
          throw new Error(typeof data?.error === 'string' ? data.error : r.statusText);
        }
        const list = Array.isArray(data.pairs) ? data.pairs : [];
        const next: Record<string, { distance_m: number | null; duration_min: number | null; manual_override: boolean }> =
          {};
        for (const p of list) {
          if (!p || typeof p !== 'object') continue;
          const w = typeof p.winery === 'string' ? p.winery : '';
          const v = typeof p.vineyard === 'string' ? p.vineyard : '';
          if (!w || !v) continue;
          const dm = p.distance_m;
          const dur = p.duration_min;
          next[`${w}\x1f${v}`] = {
            distance_m:
              dm == null || dm === ''
                ? null
                : typeof dm === 'number'
                  ? dm
                  : Number(dm),
            duration_min:
              dur == null || dur === ''
                ? null
                : typeof dur === 'number'
                  ? dur
                  : Number(dur),
            manual_override: p.manual_override === true || p.manual_override === 1,
          };
        }
        setPairDistMatrixByKey(next);
      })
      .catch((e: unknown) => {
        if (e instanceof Error && e.name === 'AbortError') return;
        setPairDistMatrixErr(e instanceof Error ? e.message : String(e));
        setPairDistMatrixByKey({});
      })
      .finally(() => setPairDistMatrixLoading(false));
    return () => ac.abort();
  }, [selectedRow?.job_id, inspectPairMatrixWineryPairKeys, inspectPairMatrixVineyardPairKeys]);

  const [sortSaveStatus, setSortSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const saveSortColumns = (cols: [string, string, string]) => {
    const valid = cols.filter(Boolean);
    const payload = {
      type: SORT_SETTING_TYPE,
      settingname: SORT_SETTING_NAME,
      settingvalue: JSON.stringify(valid),
    };
    setSortSaveStatus('saving');
    fetch('/api/settings', {
      method: 'PUT',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
      .then(async (r) => {
        const body = await r.json().catch(() => ({}));
        if (r.ok) {
          setSortSaveStatus('saved');
        } else {
          const details = { status: r.status, statusText: r.statusText, url: r.url, payload, responseBody: body };
          console.error('[Inspect save sort] FAILED:', JSON.stringify(details, null, 2));
          setSortSaveStatus('error');
        }
      })
      .catch((err) => {
        console.error('[Inspect save sort] FETCH ERROR:', err);
        setSortSaveStatus('error');
      })
      .finally(() => { setTimeout(() => setSortSaveStatus('idle'), 2000); });
  };

  const setSortColumn = (idx: 0 | 1 | 2, value: string) => {
    const next: [string, string, string] = [...sortColumns];
    next[idx] = value;
    setSortColumns(next);
    setJobsPage(0);
  };

  const formatCell = useCallback((v: unknown): string => {
    if (v == null || v === '') return '—';
    if (isIsoDateString(v)) return formatDateNZ(v);
    return String(v);
  }, []);

  const formatDistanceCell = useCallback((v: unknown): string => {
    if (v == null || v === '') return '—';
    const n = typeof v === 'number' ? v : Number(v);
    if (!Number.isFinite(n)) return '—';
    return n.toFixed(2);
  }, []);

  /** GPS table: format enter_time, outer_time as dd/mm/yy hh:mm:ss (same as vwork grid), no time adjustment. Lat/lon as number. */
  const formatGpsCell = useCallback((v: unknown): string => {
    if (v == null || v === '') return '—';
    if (isIsoDateString(v)) return formatDateNZ(String(v));
    if (typeof v === 'number' && !Number.isNaN(v)) return String(v);
    return String(v);
  }, []);

  const TRACKING_DATE_COLS = new Set(['position_time', 'position_time_nz']);
  /** GPS grid columns; `fence_plus` is filled client-side after “Tag Fence+ rows” (not from tbl_tracking). */
  const TRACKING_GRID_COLUMNS = ['device_name', 'fence_name', 'geofence_id', 'fence_plus', 'geofence_type', 'position_time_nz', 'position_time', 'lat', 'lon'] as const;

  const trackingGridColLabel = useCallback((col: string) => {
    if (col === 'fence_plus') return 'Fence+';
    return formatColumnLabel(col);
  }, []);

  useEffect(() => {
    setFencePlusById({});
    setFencePlusMeta(null);
    setFencePlusErr(null);
  }, [trackingRows]);

  /** Haversine distance in meters between two WGS84 (lat, lon) points. Returns null if either point is invalid. */
  const haversineMeters = useCallback((lat1: number, lon1: number, lat2: number, lon2: number): number | null => {
    if (!Number.isFinite(lat1) || !Number.isFinite(lon1) || !Number.isFinite(lat2) || !Number.isFinite(lon2)) return null;
    const R = 6371000;
    const dLat = ((lat2 - lat1) * Math.PI) / 180;
    const dLon = ((lon2 - lon1) * Math.PI) / 180;
    const a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
  }, []);
  const sortedTrackingRows = useMemo(() => {
    const out = [...trackingRows];
    const compareTracking = (a: unknown, b: unknown, col: string): number => {
      const va = a == null ? '' : a;
      const vb = b == null ? '' : b;
      if (va === vb) return 0;
      if (TRACKING_DATE_COLS.has(col) && isIsoDateString(va) && isIsoDateString(vb)) return String(va).localeCompare(String(vb));
      if (typeof va === 'number' && typeof vb === 'number') return va - vb;
      if (typeof va === 'string' && typeof vb === 'string') {
        const na = Number(va);
        const nb = Number(vb);
        if (!Number.isNaN(na) && !Number.isNaN(nb)) return na - nb;
        return va.localeCompare(vb);
      }
      return String(va).localeCompare(String(vb));
    };
    out.sort((r1, r2) => {
      let c = 0;
      if (trackingSortCol === 'fence_plus') {
        const id1 = r1.id != null ? String(r1.id) : '';
        const id2 = r2.id != null ? String(r2.id) : '';
        const fp1 = fencePlusById[id1] ?? '';
        const fp2 = fencePlusById[id2] ?? '';
        c = String(fp1).localeCompare(String(fp2));
      } else {
        c = compareTracking(r1[trackingSortCol], r2[trackingSortCol], trackingSortCol);
      }
      return trackingSortDir === 'asc' ? c : -c;
    });
    return out;
  }, [trackingRows, trackingSortCol, trackingSortDir, fencePlusById]);

  const runTagFencePlusRows = useCallback(async () => {
    if (!deviceForTracking || !vineyardTrim) {
      setFencePlusErr('Need worker (device) and job vineyard_name.');
      return;
    }
    const ids = sortedTrackingRows
      .map((r) => {
        const raw = r.id ?? (r as { ID?: unknown }).ID;
        if (raw == null) return NaN;
        const n = typeof raw === 'number' ? raw : parseInt(String(raw), 10);
        return Number.isFinite(n) ? n : NaN;
      })
      .filter((n) => Number.isFinite(n) && n > 0);
    if (ids.length === 0) {
      setFencePlusErr('No tbl_tracking.id on loaded rows — refresh GPS data.');
      return;
    }
    setFencePlusLoading(true);
    setFencePlusErr(null);
    try {
      const res = await fetch('/api/inspect/fence-plus-tags', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          device: deviceForTracking,
          vineyardName: vineyardTrim,
          trackingIds: ids,
        }),
      });
      const data = (await res.json()) as {
        error?: string;
        tags?: Record<string, string | null>;
        bufferMeters?: number;
        fenceNames?: string[];
      };
      if (!res.ok) {
        setFencePlusErr(data.error ?? res.statusText);
        return;
      }
      if (data.tags && typeof data.tags === 'object') {
        setFencePlusById(data.tags);
      }
      if (data.bufferMeters != null && Array.isArray(data.fenceNames)) {
        setFencePlusMeta({ bufferMeters: data.bufferMeters, fenceNames: data.fenceNames });
      }
    } catch (e) {
      setFencePlusErr(e instanceof Error ? e.message : String(e));
    } finally {
      setFencePlusLoading(false);
    }
  }, [deviceForTracking, vineyardTrim, sortedTrackingRows]);

  const columnWidths = useMemo(
    () => computeColumnWidths(columns, sortedRows, formatCell, formatColumnLabel),
    [columns, sortedRows],
  );

  /** Per-column filter controls aligned above each data column (filter row in thead). */
  const renderColumnFilter = (col: string) => {
    const fi =
      'w-full min-w-0 rounded border border-zinc-300 bg-white px-1 py-0.5 text-[10px] dark:border-zinc-600 dark:bg-zinc-800';

    switch (col) {
      case 'job_id':
        return (
          <input
            type="text"
            value={findJobIdDraft}
            onChange={(e) => setFindJobIdDraft(e.target.value)}
            onKeyDown={(e) => {
              if (e.key !== 'Enter') return;
              e.preventDefault();
              setFindJobIdTarget(findJobIdDraft.trim());
              userClearedLocateRef.current = false;
            }}
            list="filter-job-id-list"
            placeholder="Find id · Enter"
            className={fi}
            title="Type job id and Enter to jump to that row; the list stays full (same sort/filters), only the page and selection change"
          />
        );
      case 'customer':
      case 'Customer':
        return (
          <select value={filterCustomer} onChange={(e) => setFilterCustomer(e.target.value)} className={fi} title="Customer">
            <option value="">All</option>
            {customersOptions.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        );
      case 'template':
        return (
          <select
            value={filterTemplate}
            onChange={(e) => setFilterTemplate(e.target.value)}
            className={fi}
            disabled={!filterCustomer}
            title="Template"
          >
            <option value="">All</option>
            {templateOptions.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        );
      case 'delivery_winery':
        return (
          <select value={filterWinery} onChange={(e) => setFilterWinery(e.target.value)} className={fi} title="delivery_winery">
            <option value="">All</option>
            {wineryOptions.map((w) => (
              <option key={w} value={w}>
                {w}
              </option>
            ))}
          </select>
        );
      case 'vineyard_name':
        return (
          <select value={filterVineyard} onChange={(e) => setFilterVineyard(e.target.value)} className={fi} title="vineyard_name">
            <option value="">All</option>
            {vineyardOptions.map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
          </select>
        );
      case 'loadsize':
        return (
          <select value={filterLoadsize} onChange={(e) => setFilterLoadsize(e.target.value)} className={fi} title="loadsize">
            <option value="">All</option>
            {loadsizeOptions.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        );
      case 'truck_id':
        return (
          <select value={filterTruckId} onChange={(e) => setFilterTruckId(e.target.value)} className={fi} title="Truck">
            <option value="">All</option>
            {truckIdsOptions.map((id) => (
              <option key={id} value={id}>
                {id}
              </option>
            ))}
          </select>
        );
      case 'worker':
        return (
          <select value={filterWorker} onChange={(e) => setFilterWorker(e.target.value)} className={fi} title="worker (device)">
            <option value="">All</option>
            {workerOptions.map((w) => (
              <option key={w} value={w}>
                {w}
              </option>
            ))}
          </select>
        );
      case 'trailermode':
        return (
          <select value={filterTrailermode} onChange={(e) => setFilterTrailermode(e.target.value)} className={fi} title="trailermode">
            <option value="">All</option>
            {trailermodeOptions.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        );
      case 'planned_start_time':
        return (
          <div className="flex min-w-[6.5rem] flex-col gap-0.5">
            <input
              type="date"
              value={filterPlannedFrom}
              onChange={(e) => setFilterPlannedFrom(e.target.value)}
              className={fi}
              title="Planned from"
            />
            <input type="date" value={filterPlannedTo} onChange={(e) => setFilterPlannedTo(e.target.value)} className={fi} title="Planned to" />
          </div>
        );
      case 'actual_start_time':
        return (
          <div className="flex min-w-[6.5rem] flex-col gap-0.5">
            <input
              type="date"
              value={filterActualFrom}
              onChange={(e) => setFilterActualFrom(e.target.value)}
              className={fi}
              title="Actual start from"
            />
            <input type="date" value={filterActualTo} onChange={(e) => setFilterActualTo(e.target.value)} className={fi} title="Actual start to" />
          </div>
        );
      case 'actual_end_time':
        return (
          <div className="flex min-w-[6.5rem] flex-col gap-0.5">
            <input
              type="datetime-local"
              value={filterActualEndFrom}
              onChange={(e) => setFilterActualEndFrom(e.target.value)}
              className={fi}
              title="actual_end_time from"
            />
            <input
              type="datetime-local"
              value={filterActualEndTo}
              onChange={(e) => setFilterActualEndTo(e.target.value)}
              className={fi}
              title="actual_end_time to"
            />
          </div>
        );
      default:
        return <span className="block min-h-[1.25rem]" aria-hidden />;
    }
  };

  return (
    <>
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-4 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Inspect Data Vwork&gt;GPS</h1>
      <p className="mb-4 text-sm text-zinc-500">Jobs grid (branched from Vwork — will evolve into merged jobs + GPS view)</p>
      {auditJobFilterIds && auditJobFilterIds.length > 0 && (
        <div className="mb-4 flex flex-wrap items-center gap-2 rounded-lg border border-sky-200 bg-sky-50 px-3 py-2 text-sm dark:border-sky-800 dark:bg-sky-950/50">
          <span className="text-sky-900 dark:text-sky-100">
            Data Audit: listing <strong>{formatIntNz(auditJobFilterIds.length)}</strong> job
            {auditJobFilterIds.length !== 1 ? 's' : ''} from the pivot. Other filters below do not apply to this list until
            cleared.
          </span>
          <button
            type="button"
            onClick={() => {
              setAuditJobFilterIds(null);
              setJobsPage(0);
            }}
            className="rounded border border-sky-300 bg-white px-2 py-1 text-xs font-medium text-sky-900 hover:bg-sky-100 dark:border-sky-700 dark:bg-sky-900 dark:text-sky-100 dark:hover:bg-sky-800"
          >
            Clear audit filter
          </button>
        </div>
      )}
      {!hasCompletedInitialFetch && !error && <p className="text-zinc-600">Loading…</p>}
      {error && (
        <div className="rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          <p className="font-medium">{error}</p>
          {errorDetail?.hint && <p className="mt-2 text-sm opacity-90">{errorDetail.hint}</p>}
          {errorDetail?.code && <p className="mt-1 text-xs">Code: {errorDetail.code}</p>}
        </div>
      )}
      {hasCompletedInitialFetch && !error && (
        <>
          <div className="mb-2 flex flex-wrap items-center gap-2 rounded-lg border border-zinc-200 bg-white px-3 py-2 dark:border-zinc-700 dark:bg-zinc-900">
            <div className="flex flex-wrap items-center gap-x-1.5 gap-y-1">
              <span className="text-[10px] font-medium uppercase tracking-wide text-zinc-400">Sort</span>
              <span className="text-[10px] text-zinc-400">1</span>
              <select
                value={sortColumns[0]}
                onChange={(e) => setSortColumn(0, e.target.value)}
                className="max-w-[8.5rem] rounded border border-zinc-200 bg-white px-1 py-0.5 text-[11px] text-zinc-600 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300"
                title="Primary sort (SQL ORDER BY)"
              >
                <option value="">—</option>
                {sortableColumns.map((c) => (
                  <option key={c} value={c}>
                    {formatColumnLabel(c)}
                  </option>
                ))}
              </select>
              <span className="text-[10px] text-zinc-400">2</span>
              <select
                value={sortColumns[1]}
                onChange={(e) => setSortColumn(1, e.target.value)}
                className="max-w-[8.5rem] rounded border border-zinc-200 bg-white px-1 py-0.5 text-[11px] text-zinc-600 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300"
                title="Secondary sort (SQL)"
              >
                <option value="">—</option>
                {sortableColumns.map((c) => (
                  <option key={c} value={c}>
                    {formatColumnLabel(c)}
                  </option>
                ))}
              </select>
              <span className="text-[10px] text-zinc-400">3</span>
              <select
                value={sortColumns[2]}
                onChange={(e) => setSortColumn(2, e.target.value)}
                className="max-w-[8.5rem] rounded border border-zinc-200 bg-white px-1 py-0.5 text-[11px] text-zinc-600 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300"
                title="Tertiary sort (SQL)"
              >
                <option value="">—</option>
                {sortableColumns.map((c) => (
                  <option key={c} value={c}>
                    {formatColumnLabel(c)}
                  </option>
                ))}
              </select>
              <button
                type="button"
                onClick={() => saveSortColumns(sortColumns)}
                disabled={sortSaveStatus === 'saving'}
                title="Save Sort 1/2/3 picks (this button only saves the SORT — see Save Cols for column order/visibility)"
                className="ml-1 rounded border border-zinc-200 bg-transparent px-1.5 py-0.5 text-[11px] text-zinc-500 hover:bg-zinc-100 disabled:opacity-60 dark:border-zinc-600 dark:hover:bg-zinc-800"
              >
                {sortSaveStatus === 'saving' ? '…' : sortSaveStatus === 'saved' ? 'Saved' : sortSaveStatus === 'error' ? 'Error' : 'Save Sort'}
              </button>
              <button
                type="button"
                onClick={() => saveColumnConfig(allColumns, hiddenColumns)}
                disabled={columnSaveStatus === 'saving' || allColumns.length === 0}
                title="Save the current column ORDER and SHOW/HIDE state. Drag-reorder also auto-saves; this is the explicit button."
                className="rounded border border-zinc-200 bg-transparent px-1.5 py-0.5 text-[11px] text-zinc-500 hover:bg-zinc-100 disabled:opacity-60 dark:border-zinc-600 dark:hover:bg-zinc-800"
              >
                {columnSaveStatus === 'saving' ? '…' : columnSaveStatus === 'saved' ? 'Saved' : columnSaveStatus === 'error' ? 'Error' : 'Save Cols'}
              </button>
              <div className="relative">
                <button
                  type="button"
                  onClick={() => setShowColumnConfig((v) => !v)}
                  className="rounded border border-zinc-200 bg-transparent px-1.5 py-0.5 text-[11px] text-zinc-500 hover:bg-zinc-100 dark:border-zinc-600 dark:hover:bg-zinc-800"
                >
                  Columns
                </button>
                {showColumnConfig && (
                  <>
                    <div
                      className="fixed inset-0 z-40"
                      onClick={() => setShowColumnConfig(false)}
                      aria-hidden="true"
                    />
                    <div className="absolute left-0 top-full z-50 mt-1 min-w-[260px] rounded-lg border border-zinc-200 bg-white p-3 shadow-lg dark:border-zinc-700 dark:bg-zinc-900">
                      <div className="mb-2 flex items-center justify-between gap-2">
                        <span className="text-xs font-medium text-zinc-500">Show/hide columns</span>
                        <div className="flex items-center gap-2">
                          <button
                            type="button"
                            onClick={() => saveColumnConfig(allColumns, hiddenColumns)}
                            disabled={columnSaveStatus === 'saving' || allColumns.length === 0}
                            className="rounded border border-zinc-200 bg-transparent px-1.5 py-0.5 text-[11px] text-zinc-600 hover:bg-zinc-100 disabled:opacity-60 dark:border-zinc-600 dark:hover:bg-zinc-800"
                            title="Save the current column ORDER and SHOW/HIDE state"
                          >
                            {columnSaveStatus === 'saving' ? '…' : columnSaveStatus === 'saved' ? 'Saved' : columnSaveStatus === 'error' ? 'Error' : 'Save'}
                          </button>
                          <button type="button" onClick={showAllColumns} className="text-xs text-blue-600 hover:underline dark:text-blue-400">
                            Show all
                          </button>
                          <button type="button" onClick={resetColumnConfig} className="text-xs text-zinc-500 hover:underline">
                            Reset
                          </button>
                        </div>
                      </div>
                      <div className="max-h-64 space-y-1 overflow-y-auto">
                        {allColumns.map((col) => (
                          <label key={col} className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 hover:bg-zinc-100 dark:hover:bg-zinc-800">
                            <input
                              type="checkbox"
                              checked={!hiddenColumns.has(col)}
                              onChange={() => toggleColumnVisibility(col)}
                              className="rounded"
                            />
                            <span className="text-sm">
                              {GPS_STEP_VIRTUAL_COLUMN_SET.has(col)
                                ? `${col.toUpperCase()} — step_${col.slice(1)}_gps_completed_at present (Y/N)`
                                : formatColumnLabel(col)}
                            </span>
                          </label>
                        ))}
                      </div>
                      <p className="mt-2 text-xs text-zinc-500">
                        {columns.length} of {allColumns.length} visible · drag headers to reorder
                      </p>
                    </div>
                  </>
                )}
              </div>
            </div>
            {listRefreshing && (
              <span className="text-[10px] text-zinc-400" aria-live="polite">
                Updating…
              </span>
            )}
          </div>
          <div className="mb-1 flex justify-end">
            <button
              type="button"
              onClick={clearAllFilters}
              className="rounded border border-zinc-300 bg-zinc-50 px-2 py-0.5 text-[11px] text-zinc-600 hover:bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
            >
              Clear all filters
            </button>
          </div>
          <datalist id="filter-job-id-list" />
          <div
            className={`max-h-[28rem] overflow-auto rounded-lg border border-zinc-200 bg-white transition-opacity dark:border-zinc-700 dark:bg-zinc-900 ${listRefreshing ? 'opacity-75' : ''}`}
          >
            <table className="w-max table-fixed text-left text-sm">
              <colgroup>
                {columns.map((col) => (
                  <col key={col} style={{ width: columnWidths[col], minWidth: columnWidths[col] }} />
                ))}
              </colgroup>
              <thead>
                <tr className="border-b border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-900/95">
                  {columns.map((col) => (
                    <th key={`f-${col}`} className="align-top px-1.5 py-1.5 font-normal">
                      {renderColumnFilter(col)}
                    </th>
                  ))}
                </tr>
                <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                  {columns.map((col) => (
                    <th
                      key={col}
                      draggable
                      onDragStart={() => handleColumnDragStart(col)}
                      onDragOver={(e) => handleColumnDragOver(e, col)}
                      onDragLeave={handleColumnDragLeave}
                      onDrop={(e) => handleColumnDrop(e, col)}
                      onDragEnd={handleColumnDragEnd}
                      className={`cursor-grab select-none whitespace-nowrap bg-zinc-100 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700 ${dropTargetCol === col ? 'bg-blue-200 dark:bg-blue-800' : ''} ${dragCol === col ? 'opacity-60' : ''}`}
                      title="Drag to reorder"
                    >
                      {col === 'trailermode'
                        ? 'TT'
                        : col === 'loadsize'
                            ? 'Load Size'
                            : col === 'distance'
                                ? 'Distance'
                            : GPS_STEP_VIRTUAL_COLUMN_SET.has(col)
                                ? col.toUpperCase()
                            : formatColumnLabel(col)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedRows.length === 0 ? (
                  <tr>
                    <td
                      colSpan={Math.max(1, columns.length)}
                      className="px-3 py-10 text-center text-sm text-zinc-500 dark:text-zinc-400"
                    >
                      {listRefreshing ? 'Updating…' : 'No rows match the current filters. Adjust filters above or clear them.'}
                    </td>
                  </tr>
                ) : (
                  sortedRows.map((row, i) => (
                  <tr
                    key={i}
                    ref={i === selectedRowIndex ? selectedRowRef : undefined}
                    onClick={() => {
                      setSelectedRowIndex(i);
                      const jobId = String(row.job_id ?? '').trim();
                      if (locateTarget && jobId !== locateTarget.trim()) {
                        userClearedLocateRef.current = true;
                      }
                    }}
                    className={`cursor-pointer border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50 ${selectedRowIndex === i ? 'bg-blue-100 dark:bg-blue-900/40' : ''}`}
                  >
                    {columns.map((col) => {
                      if (GPS_STEP_VIRTUAL_COLUMN_SET.has(col)) {
                        const stepN = parseInt(col.slice(1), 10);
                        const has = readInspectStepGps(row, stepN) !== null;
                        return (
                          <td
                            key={col}
                            className="whitespace-nowrap px-1.5 py-2 text-center"
                            title={`step_${stepN}_gps_completed_at ${has ? 'has data' : 'is empty'}`}
                          >
                            <span
                              className={`inline-flex h-5 w-5 items-center justify-center rounded text-[11px] font-bold ${
                                has
                                  ? 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300'
                                  : 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300'
                              }`}
                            >
                              {has ? 'Y' : 'N'}
                            </span>
                          </td>
                        );
                      }
                      return (
                        <td key={col} className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">
                          {col === 'distance' ? formatDistanceCell(row[col]) : formatCell(row[col])}
                        </td>
                      );
                    })}
                  </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
          <div className="mt-3 flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0 flex-1 space-y-2">
              <button
                type="button"
                onClick={() => setShowListInfoApiDebug((v) => !v)}
                className="text-left text-xs text-zinc-500 underline decoration-zinc-400/70 underline-offset-2 hover:text-zinc-700 dark:text-zinc-400 dark:hover:text-zinc-200"
                aria-expanded={showListInfoApiDebug}
              >
                {showListInfoApiDebug ? 'Hide' : 'Show'} list info & API query
              </button>
              {showListInfoApiDebug && (
                <>
                  <p className="text-sm text-zinc-500 dark:text-zinc-400">
                    {sortedRows.length} row{sortedRows.length !== 1 ? 's' : ''} on this page · {totalJobsFromApi} total matching filters · page {jobsPage + 1} of{' '}
                    {Math.max(1, Math.ceil(totalJobsFromApi / INSPECT_PAGE_SIZE) || 1)} · click row to select
                  </p>
                  <p className="font-mono text-[11px] leading-snug text-zinc-400 break-all dark:text-zinc-500">
                    API query: /api/vworkjobs?{lastApiQueryString || '—'}
                    {apiDebugFromResponse != null && <> · debug: {JSON.stringify(apiDebugFromResponse)}</>}
                  </p>
                </>
              )}
            </div>
            {totalJobsFromApi > INSPECT_PAGE_SIZE && (
              <div className="flex shrink-0 items-center gap-2">
                <button
                  type="button"
                  disabled={jobsPage <= 0 || listRefreshing || !hasCompletedInitialFetch}
                  onClick={() => setJobsPage((p) => Math.max(0, p - 1))}
                  className="rounded border border-zinc-300 px-2 py-1 text-xs disabled:opacity-50 dark:border-zinc-600"
                >
                  Previous
                </button>
                <button
                  type="button"
                  disabled={listRefreshing || !hasCompletedInitialFetch || jobsPage >= Math.max(0, Math.ceil(totalJobsFromApi / INSPECT_PAGE_SIZE) - 1)}
                  onClick={() => setJobsPage((p) => p + 1)}
                  className="rounded border border-zinc-300 px-2 py-1 text-xs disabled:opacity-50 dark:border-zinc-600"
                >
                  Next
                </button>
              </div>
            )}
          </div>
          {locateJobNotFound && locateTarget.trim() !== '' && (
            <p className="mt-2 text-sm text-amber-700 dark:text-amber-300">
              Job ID {locateTarget.trim()} was not found for the current API filters
              {showListInfoApiDebug ? ' (see query below).' : ' (use “Show list info & API query” for the request URL).'} Adjust filters or check the job exists for this truck.
            </p>
          )}
          <section className="mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            {selectedRow && (
              <div className="mb-3">
                <span
                  className="text-xs font-medium text-zinc-500 dark:text-zinc-400"
                  title="Hover each dotted tag (e.g. VineFence+:) for what it means. If Via shows VineFence+ but GPS Step Data is empty, the tag tooltip explains why."
                >
                  CalcNotes:{' '}
                </span>
                <CalcNotesRichText
                  text={String(selectedRow.calcnotes ?? selectedRow.calc_notes ?? selectedRow.CalcNotes ?? '')}
                  row={selectedRow}
                />
              </div>
            )}
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
              <div className="flex min-w-0 max-w-full flex-wrap items-baseline gap-x-2 gap-y-1">
                <h2 className="shrink-0 text-sm font-semibold text-zinc-700 dark:text-zinc-300">
                  Step details
                </h2>
                {selectedRow && (
                  <span
                    className="min-w-0 truncate text-sm font-normal text-zinc-600 dark:text-zinc-400"
                    title={`${deliveryWinery} - ${vineyardName}`}
                  >
                    {deliveryWinery}
                    {' - '}
                    {vineyardName}
                  </span>
                )}
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  onClick={() => {
                    setPinJobError(null);
                    setPinJobNoteDraft('');
                    setPinJobModalOpen(true);
                  }}
                  disabled={!selectedRow}
                  className="rounded border border-zinc-300 bg-white px-3 py-1.5 text-sm font-medium text-zinc-800 shadow-sm hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700 dark:disabled:opacity-50"
                  title="Pin this job in Query → Recent jobs until it is explicitly unpinned."
                >
                  Pin job
                </button>
                <button
                  type="button"
                  onClick={() => void retagDayAndRefetchSteps()}
                  disabled={!selectedRow || !deviceForTracking || !actualStartTime?.trim() || retagAndRefetchRunning || refetchStepsRunning}
                  className="rounded bg-amber-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50 dark:bg-amber-700 dark:hover:bg-amber-600 dark:disabled:opacity-50"
                  title="Run ENTER/EXIT tagging from actual_start_time −2h to +24h for this device, then refetch GPS steps."
                >
                  {retagAndRefetchRunning ? 'Retag + refetch…' : 'Retag day + refetch steps'}
                </button>
                <button
                  type="button"
                  onClick={() => void refetchStepsForSelectedJob()}
                  disabled={!selectedRow || !actualStartTime?.trim() || refetchStepsRunning || retagAndRefetchRunning}
                  className="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600 dark:disabled:opacity-50"
                  title="Refetch GPS steps for this job (force: runs even if steps_fetched=true). Same logic as API GPS Import Step 4."
                >
                  {refetchStepsRunning ? 'Refetching…' : 'Refetch steps (this job)'}
                </button>
              </div>
            </div>
            <table className="w-full min-w-[78rem] text-left text-sm table-fixed">
              <colgroup>
                {/* Step name must fit longest label without overflowing into VWork completed; GPS step needs room for NZ datetime + optional "N min" without overlapping GPS row */}
                <col style={{ width: '19rem' }} />
                <col style={{ width: '11rem' }} />
                <col style={{ width: '10.5rem' }} />
                <col style={{ width: '6.5rem' }} />
                <col style={{ width: '14rem' }} />
                <col style={{ width: '12rem' }} />
                <col style={{ width: '5rem' }} />
              </colgroup>
              <thead>
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  <th rowSpan={2} className="align-bottom px-2 py-1.5 font-medium text-zinc-500">
                    Step Name
                  </th>
                  <th className="px-2 py-1.5 text-left font-medium text-zinc-500">VWork Data</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">GPS Step Data</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">GPS Row</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Manual Overrides</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Final Step Data</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Details</th>
                </tr>
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Step Completed</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Step Completed</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">—</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Step Completed</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Step Completed</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">—</th>
                </tr>
              </thead>
              <tbody>
                {STEP_ROWS.map(({ completedKey, actualTimeKey, displayLabel }, i) => {
                  const n = i + 1;
                  const orideKey = `step${n}oride` as keyof typeof stepOverrides;
                  const gpsCompletedKey = `step_${n}_gps_completed_at` as keyof Row;
                  const gpsCompletedKeyAlt = `Step_${n}_GPS_completed_at` as keyof Row;
                  const gpsIdKey = `step${n}_gps_id` as keyof Row;
                  const gpsIdKeyAlt = `Step${n}_gps_id` as keyof Row;
                  const gpsStepValue = selectedRow?.[gpsCompletedKey] ?? selectedRow?.[gpsCompletedKeyAlt];
                  const gpsStepDisplay = gpsStepValue != null && gpsStepValue !== '' ? formatDateNZ(String(gpsStepValue)) : '—';
                  const gpsRowId = selectedRow?.[gpsIdKey] ?? selectedRow?.[gpsIdKeyAlt];
                  const gpsRowDisplay = gpsRowId != null && gpsRowId !== '' ? String(gpsRowId) : '—';
                  const vworkValue = selectedRow?.[completedKey];
                  const step1CarryFromLastJobRaw =
                    n === 1
                      ? (selectedRow as unknown as { step1oridefromlastjob?: unknown; Step1oridefromlastjob?: unknown })
                          ?.step1oridefromlastjob ??
                        (selectedRow as unknown as { step1oridefromlastjob?: unknown; Step1oridefromlastjob?: unknown })
                          ?.Step1oridefromlastjob
                      : null;
                  const step1CarryFromLastJob =
                    step1CarryFromLastJobRaw != null && String(step1CarryFromLastJobRaw).trim() !== ''
                      ? String(step1CarryFromLastJobRaw).trim()
                      : '';
                  const orideValue = stepOverrides[orideKey] ?? '';
                  /** Final = oride if set, else DB actual time (merge from steps), else VWork completed. */
                  const finalValue: unknown = orideValue
                    ? orideValue
                    : (selectedRow?.[actualTimeKey] ?? selectedRow?.[completedKey]);
                  const finalDisplay = finalValue != null && finalValue !== '' ? formatCell(finalValue) : '—';
                  const vworkClickable = selectedRow && (selectedRow[completedKey] != null && selectedRow[completedKey] !== '');
                  const gpsClickable = gpsStepValue != null && gpsStepValue !== '';
                  const finalClickable = finalValue != null && finalValue !== '';
                  const clickableClass = 'cursor-pointer underline decoration-dotted hover:bg-zinc-100 dark:hover:bg-zinc-800';
                  /** Previous step values for minutes (only when n >= 2). */
                  const vworkPrev = n >= 2 ? (selectedRow?.[`step_${n - 1}_completed_at`] ?? selectedRow?.[`Step_${n - 1}_completed_at`]) : null;
                  const gpsPrev = n >= 2 ? (selectedRow?.[`step_${n - 1}_gps_completed_at`] ?? selectedRow?.[`Step_${n - 1}_GPS_completed_at`]) : null;
                  const oridePrev = n >= 2 ? (stepOverrides[`step${n - 1}oride` as keyof typeof stepOverrides] ?? '') : '';
                  const prevFinalVal = n >= 2 ? (oridePrev || (selectedRow?.[`step_${n - 1}_actual_time`] ?? selectedRow?.[`step_${n - 1}_completed_at`] ?? selectedRow?.[`Step_${n - 1}_actual_time`] ?? selectedRow?.[`Step_${n - 1}_completed_at`])) : null;
                  const vworkMins = n >= 2 ? minutesBetweenInspect(vworkPrev, vworkValue) : null;
                  const gpsMins = n >= 2 ? minutesBetweenInspect(gpsPrev, gpsStepValue) : null;
                  const orideMins = n >= 2 ? minutesBetweenInspect(oridePrev || null, orideValue || null) : null;
                  const finalMins = n >= 2 ? minutesBetweenInspect(prevFinalVal, finalValue) : null;
                  /** To datetime-local value YYYY-MM-DDTHH:mm from timestamp string. */
                  const toDatetimeLocal = (v: unknown): string => {
                    if (v == null || v === '') return '';
                    const s = String(v).trim().replace(/\s+(?:GMT|UTC)[+-]\d{3,4}.*$/i, '').trim();
                    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2})?:?(\d{2})?/);
                    if (!m) return '';
                    return `${m[1]}-${m[2]}-${m[3]}T${(m[4] ?? '00').padStart(2, '0')}:${(m[5] ?? '00').padStart(2, '0')}`;
                  };
                  const fromDatetimeLocal = (v: string): string => (v ? `${v.replace('T', ' ')}:00` : '');
                  const vworkEmpty = !selectedRow || selectedRow[completedKey] == null || selectedRow[completedKey] === '';
                  const gpsStepEmpty = gpsStepDisplay === '—';
                  return (
                  <tr key={i} className="border-b border-zinc-100 dark:border-zinc-800">
                    <td className="max-w-0 overflow-hidden text-ellipsis whitespace-nowrap px-2 py-1.5 text-zinc-700 dark:text-zinc-300" title={selectedRow ? displayLabel : undefined}>
                      {selectedRow ? displayLabel : '—'}
                    </td>
                    <td
                      className={`whitespace-nowrap px-2 py-1.5 ${vworkEmpty ? 'text-zinc-400 dark:text-zinc-500' : 'text-black dark:text-zinc-50'} ${vworkClickable ? clickableClass : ''}`}
                      onClick={vworkClickable ? () => focusGpsOnStepTime(selectedRow![completedKey]) : undefined}
                      title={vworkClickable ? 'Show GPS raw around this time (step −2 min to job end +60)' : undefined}
                    >
                      <div className="flex flex-col gap-0.5">
                        {n === 1 && step1CarryFromLastJob ? (
                          <button
                            type="button"
                            className="w-fit text-left text-xs font-medium text-purple-800 underline decoration-dotted hover:bg-zinc-100 dark:text-purple-300 dark:hover:bg-zinc-800"
                            onClick={(e) => {
                              e.stopPropagation();
                              focusGpsOnStepTime(step1CarryFromLastJob);
                            }}
                            title="System carry-forward: Step 1 from previous job end (click to view GPS around this time)"
                          >
                            Carry fwd: {formatCell(step1CarryFromLastJob)}
                          </button>
                        ) : null}
                        <span className="inline-flex items-baseline gap-1.5">
                          {selectedRow ? formatCell(selectedRow[completedKey]) : '—'}
                          {vworkMins != null && (
                            <span
                              className={`text-xs tabular-nums ${vworkEmpty ? 'text-zinc-400 dark:text-zinc-500' : 'text-zinc-600 dark:text-zinc-400'}`}
                            >
                              {vworkMins} min
                            </span>
                          )}
                        </span>
                      </div>
                    </td>
                    <td
                      className={`overflow-hidden whitespace-nowrap px-2 py-1.5 ${gpsStepEmpty ? 'text-zinc-400 dark:text-zinc-500' : 'font-medium text-blue-800 dark:text-blue-300'} ${gpsClickable ? clickableClass : ''}`}
                      onClick={gpsClickable ? () => focusGpsOnStepTime(gpsStepValue) : undefined}
                      title={gpsClickable ? 'Show GPS raw around this time (step −2 min to job end +60)' : undefined}
                    >
                      <span className="inline-flex items-baseline gap-1.5">
                        {gpsStepDisplay}
                        {gpsMins != null && (
                          <span className={`text-xs tabular-nums ${gpsStepEmpty ? 'text-zinc-400 dark:text-zinc-500' : 'font-medium text-blue-700 dark:text-blue-400'}`}>{gpsMins} min</span>
                        )}
                      </span>
                    </td>
                    <td className="whitespace-nowrap px-2 py-1.5 font-mono text-xs text-zinc-600 dark:text-zinc-400" title={gpsRowId != null && gpsRowId !== '' ? `tbl_tracking.id = ${gpsRowId}` : undefined}>{gpsRowDisplay}</td>
                    <td className={`whitespace-nowrap px-2 py-1.5 ${orideValue ? 'text-red-800 dark:text-red-300' : 'text-zinc-400 dark:text-zinc-500'}`}>
                      <div className="flex flex-wrap items-center gap-1">
                        <div className="flex items-center gap-1.5">
                          <input
                            type="datetime-local"
                            value={orideValue ? toDatetimeLocal(orideValue) : ''}
                            onChange={(e) => setStepOverrides((prev) => ({ ...prev, [orideKey]: fromDatetimeLocal(e.target.value) }))}
                            className={`w-[10.8rem] rounded border px-1.5 py-0.5 text-xs ${orideValue ? 'border-red-400 bg-white font-semibold text-red-900 dark:border-red-500 dark:bg-zinc-900 dark:text-red-200' : 'border-zinc-200 bg-zinc-50 text-zinc-400 placeholder:text-zinc-400 dark:border-zinc-600 dark:bg-zinc-800/50 dark:text-zinc-500 dark:placeholder:text-zinc-500'}`}
                            title="Manual override for this step"
                          />
                          {orideMins != null && (
                            <span className={`text-xs tabular-nums whitespace-nowrap ${orideValue ? 'text-red-800 dark:text-red-300' : 'text-zinc-400 dark:text-zinc-500'}`}>{orideMins} min</span>
                          )}
                        </div>
                        <span className="flex gap-0.5">
                          <button
                            type="button"
                            onClick={() => setStepOverrides((prev) => ({ ...prev, [orideKey]: vworkValue != null && vworkValue !== '' ? String(vworkValue).trim().slice(0, 19) : '' }))}
                            className="rounded border border-zinc-300 bg-zinc-100 px-1.5 py-0.5 text-xs font-medium text-zinc-700 hover:bg-zinc-200 dark:border-zinc-600 dark:bg-zinc-700 dark:text-zinc-300 dark:hover:bg-zinc-600"
                            title="Copy from Vwork"
                          >
                            V
                          </button>
                          <button
                            type="button"
                            onClick={() => setStepOverrides((prev) => ({ ...prev, [orideKey]: gpsStepValue != null && gpsStepValue !== '' ? String(gpsStepValue).trim().slice(0, 19) : '' }))}
                            className="rounded border border-zinc-300 bg-zinc-100 px-1.5 py-0.5 text-xs font-medium text-zinc-700 hover:bg-zinc-200 dark:border-zinc-600 dark:bg-zinc-700 dark:text-zinc-300 dark:hover:bg-zinc-600"
                            title="Copy from GPS"
                          >
                            G
                          </button>
                          <button
                            type="button"
                            onClick={() => setStepOverrides((prev) => ({ ...prev, [orideKey]: '' }))}
                            className="rounded border border-zinc-300 bg-zinc-100 px-1.5 py-0.5 text-xs font-medium text-zinc-700 hover:bg-zinc-200 dark:border-zinc-600 dark:bg-zinc-700 dark:text-zinc-300 dark:hover:bg-zinc-600"
                            title="Clear override"
                          >
                            X
                          </button>
                        </span>
                      </div>
                    </td>
                    <td
                      className={`whitespace-nowrap px-2 py-1.5 ${finalValue != null && finalValue !== '' ? 'font-semibold text-green-900 dark:text-green-400' : 'text-zinc-400 dark:text-zinc-500'} ${finalClickable ? clickableClass : ''}`}
                      onClick={finalClickable ? () => focusGpsOnStepTime(finalValue) : undefined}
                      title={finalClickable ? 'Show GPS raw around this time (step −2 min to job end +60)' : undefined}
                    >
                      <span className="inline-flex items-baseline gap-1.5">
                        {finalDisplay}
                        {finalMins != null && (
                          <span className={`text-xs tabular-nums ${finalValue != null && finalValue !== '' ? 'text-green-800 dark:text-green-500' : 'text-zinc-400 dark:text-zinc-500'}`}>{finalMins} min</span>
                        )}
                      </span>
                    </td>
                    <td className="whitespace-nowrap px-2 py-1.5 text-zinc-400 dark:text-zinc-500">—</td>
                  </tr>
                );})}
              </tbody>
            </table>
            <div className="mt-4 flex flex-col gap-4 lg:flex-row lg:items-start lg:gap-6">
              <div className="min-w-0 flex-1 space-y-2">
                <div className="flex flex-wrap items-baseline gap-2">
                  <h3 className="text-xs font-semibold uppercase tracking-wide text-zinc-600 dark:text-zinc-400">
                    Pair distances (tbl_distances)
                  </h3>
                  <span className="text-xs text-zinc-500 dark:text-zinc-500">
                    Rows: one per winery mapping · Columns: one per vineyard mapping. Lookups use job vWork winery /
                    vineyard when set (same keys as tbl_distances / Admin Distances). Effective km / min; manual overrides
                    apply.
                  </span>
                </div>
                {pairDistMatrixErr && (
                  <p className="text-sm text-amber-700 dark:text-amber-300">{pairDistMatrixErr}</p>
                )}
                {inspectPairMatrixWineryColumns.length > 0 && inspectPairMatrixVineyardColumns.length > 0 ? (
                  <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
                    <table className="min-w-[16rem] border-collapse text-left text-xs">
                      <thead>
                        <tr className="border-b border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-800/80">
                          <th className="sticky left-0 z-10 border-r border-zinc-200 bg-zinc-50 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:bg-zinc-800 dark:text-zinc-400">
                            Winery \ Vineyard
                          </th>
                          {inspectPairMatrixVineyardColumns.map((col) => (
                            <th
                              key={col.id}
                              className="max-w-[10rem] px-2 py-1.5 font-medium text-zinc-600 dark:text-zinc-400"
                              title={`${col.header}${col.pairVineyard !== col.header ? ` · pair: ${col.pairVineyard}` : ''}`}
                            >
                              <span className="line-clamp-3 break-words">{col.header}</span>
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {pairDistMatrixLoading && Object.keys(pairDistMatrixByKey).length === 0 ? (
                          <tr>
                            <td
                              colSpan={inspectPairMatrixVineyardColumns.length + 1}
                              className="px-2 py-3 text-zinc-500 dark:text-zinc-400"
                            >
                              Loading distances…
                            </td>
                          </tr>
                        ) : (
                          inspectPairMatrixWineryColumns.map((wrow) => (
                            <tr key={wrow.id} className="border-b border-zinc-100 dark:border-zinc-800">
                              <th
                                className="sticky left-0 z-10 max-w-[11rem] border-r border-zinc-200 bg-white px-2 py-1.5 text-left font-medium text-zinc-800 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-200"
                                title={`${wrow.header}${wrow.pairWinery !== wrow.header ? ` · pair: ${wrow.pairWinery}` : ''}`}
                              >
                                <span className="line-clamp-4 break-words">{wrow.header}</span>
                              </th>
                              {inspectPairMatrixVineyardColumns.map((col) => {
                                const w = wrow.pairWinery;
                                const v = col.pairVineyard;
                                const cell = pairDistMatrixByKey[`${w}\x1f${v}`];
                                const dm = cell?.distance_m;
                                const dur = cell?.duration_min;
                                const hasData =
                                  cell != null &&
                                  ((dm != null && Number.isFinite(dm)) || (dur != null && Number.isFinite(dur)));
                                const km =
                                  dm != null && Number.isFinite(dm) ? (dm / 1000).toFixed(1) : null;
                                const minStr =
                                  dur != null && Number.isFinite(dur)
                                    ? dur % 1 === 0
                                      ? `${Math.round(dur)} min`
                                      : `${dur.toFixed(1)} min`
                                    : null;
                                const jobPair =
                                  wineryTrim &&
                                  vineyardTrim &&
                                  w.trim().toLowerCase() === wineryTrim.toLowerCase() &&
                                  v.trim().toLowerCase() === vineyardTrim.toLowerCase();
                                return (
                                  <td
                                    key={`${wrow.id}-${col.id}`}
                                    className={`max-w-[9rem] px-2 py-1.5 align-top leading-snug ${
                                      jobPair
                                        ? 'bg-amber-50 font-medium text-zinc-900 dark:bg-amber-950/40 dark:text-zinc-100'
                                        : hasData
                                          ? 'text-zinc-800 dark:text-zinc-200'
                                          : 'text-zinc-400 dark:text-zinc-500'
                                    }`}
                                    title={
                                      cell?.manual_override
                                        ? 'Includes tbl_distances_manual override'
                                        : undefined
                                    }
                                  >
                                    {!hasData ? (
                                      '—'
                                    ) : (
                                      <span className="flex flex-col gap-0.5">
                                        {km != null && <span>{km} km</span>}
                                        {minStr != null && <span className="tabular-nums">{minStr}</span>}
                                        {cell?.manual_override ? (
                                          <span className="text-[10px] font-normal text-zinc-500 dark:text-zinc-400">
                                            manual
                                          </span>
                                        ) : null}
                                      </span>
                                    )}
                                  </td>
                                );
                              })}
                            </tr>
                          ))
                        )}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="text-sm text-zinc-500 dark:text-zinc-400">
                    Select a job with winery and vineyard names to show pair distances (aligned with Mapping candidates).
                  </p>
                )}
              </div>
              <div className="w-full shrink-0 space-y-3 lg:w-[min(22rem,100%)]">
                <div className="flex flex-col gap-2 rounded border border-zinc-200 bg-zinc-50/80 px-2 py-2 dark:border-zinc-600 dark:bg-zinc-800/40">
                  <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-800 dark:text-zinc-200">
                    <input
                      type="checkbox"
                      checked={excludedFromSummaries}
                      onChange={(e) => setExcludedFromSummaries(e.target.checked)}
                      className="rounded border-zinc-300 dark:border-zinc-600"
                    />
                    <span>Exclude from summaries</span>
                  </label>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-zinc-500">Exclude notes (optional)</label>
                    <textarea
                      value={excludednotes}
                      onChange={(e) => setExcludednotes(e.target.value.slice(0, 250))}
                      placeholder="Why this job is excluded from daily/season rollups…"
                      rows={2}
                      maxLength={250}
                      className="w-full min-w-[12rem] rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                    />
                  </div>
                </div>
                <div className="flex items-end gap-2">
                  <div className="min-w-0 flex-1">
                    <label className="mb-1 block text-xs font-medium text-zinc-500">Comment (what you changed)</label>
                    <textarea
                      value={steporidecomment}
                      onChange={(e) => setSteporidecomment(e.target.value)}
                      placeholder="Optional note about manual overrides…"
                      rows={2}
                      className="w-full min-w-[12rem] rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                    />
                  </div>
                  <button
                    type="button"
                    disabled={!selectedRow?.job_id || saveOverridesStatus === 'saving'}
                    onClick={async () => {
                      if (!selectedRow?.job_id) return;
                      setSaveOverridesStatus('saving');
                      try {
                        const res = await fetch('/api/vworkjobs/step-overrides', {
                          method: 'POST',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify({
                            job_id: String(selectedRow.job_id),
                            step1oride: stepOverrides.step1oride || null,
                            step2oride: stepOverrides.step2oride || null,
                            step3oride: stepOverrides.step3oride || null,
                            step4oride: stepOverrides.step4oride || null,
                            step5oride: stepOverrides.step5oride || null,
                            steporidecomment: steporidecomment || null,
                            excluded: excludedFromSummaries ? 1 : 0,
                            excludednotes: excludednotes.trim() ? excludednotes.trim().slice(0, 250) : null,
                          }),
                        });
                        const data = await res.json().catch(() => ({}));
                        if (!res.ok) {
                          throw new Error(
                            typeof data?.error === 'string' ? data.error : res.statusText,
                          );
                        }
                        const s = data?.saved as Record<string, unknown> | undefined;
                        if (s && typeof s === 'object') {
                          const str = (k: string) =>
                            s[k] != null && String(s[k]).trim() !== '' ? String(s[k]).trim().slice(0, 19) : '';
                          setStepOverrides({
                            step1oride: str('step1oride'),
                            step2oride: str('step2oride'),
                            step3oride: str('step3oride'),
                            step4oride: str('step4oride'),
                            step5oride: str('step5oride'),
                          });
                          const c = s.steporidecomment;
                          setSteporidecomment(c != null && String(c).trim() !== '' ? String(c) : '');
                          setExcludedFromSummaries(Number(s.excluded) === 1);
                          const en = s.excludednotes;
                          setExcludednotes(en != null && String(en) !== '' ? String(en).slice(0, 250) : '');
                        }
                        await refreshJobRowFromApi(selectedRow.job_id);
                        setSaveOverridesStatus('saved');
                      } catch (e) {
                        setSaveOverridesStatus('error');
                        console.error('[Save step overrides]', e);
                      } finally {
                        setTimeout(() => setSaveOverridesStatus('idle'), 2000);
                      }
                    }}
                    className="shrink-0 rounded bg-green-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-green-700 disabled:opacity-50 dark:bg-green-700 dark:hover:bg-green-600"
                  >
                    {saveOverridesStatus === 'saving' ? 'Saving…' : saveOverridesStatus === 'saved' ? 'Saved ✓' : saveOverridesStatus === 'error' ? 'Save failed' : 'Save'}
                  </button>
                </div>
              </div>
            </div>
            <div className="mt-4 rounded border border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-800/50">
              <div className="flex flex-wrap items-center justify-between gap-2 border-b border-zinc-200 px-3 py-2 dark:border-zinc-700">
                <span className="text-sm font-medium text-zinc-700 dark:text-zinc-300">Steps debug</span>
                {derivedStepsLogPayload != null && (
                  <div className="flex rounded border border-zinc-300 bg-white text-xs dark:border-zinc-600 dark:bg-zinc-900">
                    <button
                      type="button"
                      onClick={() => setDerivedStepsLogTab('explanation')}
                      className={`px-2.5 py-1 font-medium ${
                        derivedStepsLogTab === 'explanation'
                          ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100'
                          : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
                      }`}
                    >
                      Explanation
                    </button>
                    <button
                      type="button"
                      onClick={() => setDerivedStepsLogTab('json')}
                      className={`px-2.5 py-1 font-medium ${
                        derivedStepsLogTab === 'json'
                          ? 'bg-zinc-200 text-zinc-900 dark:bg-zinc-700 dark:text-zinc-100'
                          : 'text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800'
                      }`}
                    >
                      JSON
                    </button>
                  </div>
                )}
              </div>
              {derivedStepsLogPayload == null ? (
                <p className="px-3 py-4 text-sm text-zinc-500 dark:text-zinc-400">
                  Refetch steps to see log.
                </p>
              ) : derivedStepsLogTab === 'json' ? (
                <pre className="max-h-[480px] overflow-auto whitespace-pre-wrap break-all px-3 py-3 font-mono text-xs text-zinc-800 dark:text-zinc-200">
                  {JSON.stringify(derivedStepsLogPayload, null, 2)}
                </pre>
              ) : (
                <div className="max-h-[480px] divide-y divide-zinc-200 overflow-auto px-2 py-1 text-sm dark:divide-zinc-700">
                  {buildInspectDerivedStepsExplanation(derivedStepsLogPayload).map((item) =>
                    item.detail != null && item.detail.length > 0 ? (
                      <details key={item.id} className="group px-1 py-0.5">
                        <summary className="cursor-pointer list-none py-2 pl-1 text-sm font-medium text-zinc-900 marker:content-none dark:text-zinc-100 [&::-webkit-details-marker]:hidden">
                          <span className="inline align-middle text-zinc-400 dark:text-zinc-500">▸ </span>
                          {item.headline}
                        </summary>
                        <ul className="mb-2 ml-4 list-disc space-y-1.5 border-l border-zinc-200 pl-3 text-xs leading-relaxed text-zinc-600 dark:border-zinc-600 dark:text-zinc-400">
                          {item.detail.map((line, i) => (
                            <li key={i}>{line}</li>
                          ))}
                        </ul>
                      </details>
                    ) : (
                      <div key={item.id} className="py-2 pl-1 text-sm font-medium text-zinc-800 dark:text-zinc-200">
                        {item.headline}
                      </div>
                    )
                  )}
                </div>
              )}
            </div>
          </section>
          <section className="mt-6 rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
            <button
              type="button"
              onClick={() => setShowMappingSection((v) => !v)}
              className="flex w-full items-center justify-between px-4 py-3 text-left text-sm font-medium text-zinc-700 dark:text-zinc-300"
            >
              <span>Mapping</span>
              <span>{showMappingSection ? '▼ Hide' : '▶ Show'}</span>
            </button>
            {showMappingSection && (
              <div className="border-t border-zinc-200 p-4 dark:border-zinc-700">
                <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
                  <p className="text-xs text-zinc-500 dark:text-zinc-400">
                    Mappings load once when Inspect opens; use Refetch after edits in Admin → GPS mappings.
                  </p>
                  <button
                    type="button"
                    onClick={() => void loadGpsMappings()}
                    disabled={gpsMappingsLoading}
                    className="shrink-0 rounded border border-zinc-300 bg-white px-3 py-1.5 text-sm font-medium text-zinc-800 shadow-sm hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:hover:bg-zinc-700 dark:disabled:opacity-50"
                    title="Reload tbl_gpsmappings from the server (bypasses browser cache)."
                  >
                    {gpsMappingsLoading ? 'Loading…' : 'Refetch mappings'}
                  </button>
                </div>
                <table className="min-w-[20rem] text-left text-sm">
                  <thead>
                    <tr className="border-b border-zinc-200 dark:border-zinc-700">
                      <th className="px-2 py-1.5 font-medium text-zinc-500"></th>
                      <th className="px-2 py-1.5 font-medium text-zinc-500">Original</th>
                      <th className="px-2 py-1.5 font-medium text-zinc-500">Result from Mapping</th>
                      <th className="px-2 py-1.5 font-medium text-zinc-500">To Use</th>
                    </tr>
                  </thead>
                  <tbody>
                    {mappingTableRows.map((row, i) => (
                      <tr key={`${row.type}-${i}`} className="border-b border-zinc-100 dark:border-zinc-800">
                        <td className="px-2 py-1.5 font-medium text-zinc-600 dark:text-zinc-400">{row.type}</td>
                        <td className="px-2 py-1.5 text-zinc-700 dark:text-zinc-300">{row.original}</td>
                        <td className="px-2 py-1.5 text-zinc-700 dark:text-zinc-300">{row.result}</td>
                        <td className="px-2 py-1.5 font-medium text-zinc-800 dark:text-zinc-200">{row.toUse}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {mappingFenceNameInClause && (
                  <p className="mt-3 text-xs text-zinc-500">
                    For later (tbl_tracking / fence filter): <code className="select-all rounded bg-zinc-100 px-1 py-0.5 dark:bg-zinc-800">{mappingFenceNameInClause}</code>
                  </p>
                )}
              </div>
            )}
          </section>
          <section className="mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <div className="mb-3 flex flex-wrap items-center gap-4">
              <h2 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">tbl_tracking</h2>
              {gpsWindowOverride ? (
                <>
                  <span className="text-sm text-amber-600 dark:text-amber-400">
                    Filtered: step time −2 min → job end +{endPlusMinutes} min (RAW)
                  </span>
                  <button
                    type="button"
                    onClick={() => { setGpsWindowOverride(null); setTrackingRefreshKey((k) => k + 1); }}
                    className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm text-zinc-700 hover:bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                  >
                    Clear filters
                  </button>
                </>
              ) : (
                <>
                  <span className="text-sm text-zinc-500">
                    Window: start −{startLessMinutes + displayExpandBefore} min, end +{endPlusMinutes + displayExpandAfter} min
                    {(displayExpandBefore > 0 || displayExpandAfter > 0) && (
                      <span className="ml-1 text-zinc-400">(base from Settings + display expand; steps unchanged)</span>
                    )}
                  </span>
                  <div className="flex flex-wrap items-center gap-3">
                    <label className="flex items-center gap-2 text-sm text-zinc-600 dark:text-zinc-400">
                      <span>Expand display before:</span>
                      <input
                        type="number"
                        min={0}
                        max={1440}
                        value={displayExpandBefore}
                        onChange={(e) => {
                          const v = parseInt(e.target.value, 10);
                          setDisplayExpandBefore(Number.isNaN(v) || v < 0 ? 0 : Math.min(1440, v));
                        }}
                        className="w-16 rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                        title="Extra minutes before job start for GPS display only. Click Refresh to apply."
                      />
                      <span className="text-zinc-500">min</span>
                    </label>
                    <label className="flex items-center gap-2 text-sm text-zinc-600 dark:text-zinc-400">
                      <span>Expand display after:</span>
                      <input
                        type="number"
                        min={0}
                        max={1440}
                        value={displayExpandAfter}
                        onChange={(e) => {
                          const v = parseInt(e.target.value, 10);
                          setDisplayExpandAfter(Number.isNaN(v) || v < 0 ? 0 : Math.min(1440, v));
                        }}
                        className="w-16 rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                        title="Extra minutes after job end for GPS display only. Click Refresh to apply."
                      />
                      <span className="text-zinc-500">min</span>
                    </label>
                    <button
                      type="button"
                      onClick={() => {
                        setTrackingTableView('raw');
                        setTrackingPage(1);
                        setTrackingRefreshKey((k) => k + 1);
                      }}
                      disabled={!deviceForTracking || !actualStartTime.trim() || trackingLoading}
                      className="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600 dark:disabled:opacity-50"
                      title="Refetch GPS data with current expand window (raw view)"
                    >
                      Refresh
                    </button>
                  </div>
                </>
              )}
            </div>
            {!gpsWindowOverride && (
            <p className="mb-2 text-sm text-zinc-500">
              position_time_nz &gt; actual_start_time − {startLessMinutes + displayExpandBefore} min AND position_time_nz &lt; actual_end_time + {endPlusMinutes + displayExpandAfter} min
            </p>
            )}
            {trackingLoading && <p className="text-sm text-zinc-500">Loading…</p>}
            {!trackingLoading && !deviceForTracking && <p className="text-sm text-zinc-500">Select a job with worker (device for GPS) to load tracking data.</p>}
            {!trackingLoading && deviceForTracking && !actualStartTime.trim() && <p className="text-sm text-zinc-500">Job has no actual_start_time.</p>}
            {!trackingLoading && deviceForTracking && actualStartTime.trim() && (
              <>
                <div className="mb-2 flex flex-wrap items-center gap-4">
                  <span className="text-sm text-zinc-500">View</span>
                  <div className="flex gap-1 rounded bg-zinc-100 p-0.5 dark:bg-zinc-800">
                    <button
                      type="button"
                      onClick={() => setTrackingTableView('raw')}
                      className={`rounded px-3 py-1.5 text-sm font-medium ${trackingTableView === 'raw' ? 'bg-white text-zinc-900 shadow dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100'}`}
                    >
                      Raw
                    </button>
                    <button
                      type="button"
                      onClick={() => setTrackingTableView('entry_exit')}
                      className={`rounded px-3 py-1.5 text-sm font-medium ${trackingTableView === 'entry_exit' ? 'bg-white text-zinc-900 shadow dark:bg-zinc-700 dark:text-zinc-100' : 'text-zinc-600 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100'}`}
                    >
                      Entry/Exit
                    </button>
                  </div>
                  {trackingRows.length === 0 && trackingTotal === 0 && (
                    <span className="text-sm text-zinc-500">No rows in this view. Switch to Raw to see unfiltered tracking data.</span>
                  )}
                </div>
            {trackingRows.length === 0 && trackingTotal === 0 && (
              <p className="mb-2 text-sm text-zinc-500">No tracking rows for device={deviceForTracking} in window (actual_start − {startLessMinutes + displayExpandBefore} min to actual_end + {endPlusMinutes + displayExpandAfter} min).</p>
            )}
                {(trackingRows.length > 0 || trackingTotal > 0) && (
                  <>
                    <div className="mb-2 flex flex-wrap items-center gap-4">
                      <span className="text-sm text-zinc-500">
                        Showing {(trackingPage - 1) * trackingPageSize + 1}–{(trackingPage - 1) * trackingPageSize + trackingRows.length} of {trackingTotal}
                      </span>
                      <label className="flex items-center gap-2 text-sm">
                        <span className="text-zinc-500">Per page</span>
                        <select
                          value={trackingPageSize}
                          onChange={(e) => { setTrackingPageSize(Number(e.target.value)); setTrackingPage(1); }}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                        >
                          {[25, 50, 100, 200].map((n) => (
                            <option key={n} value={n}>{n}</option>
                          ))}
                        </select>
                      </label>
                      <span className="flex items-center gap-1 text-sm">
                        <button
                          type="button"
                          disabled={trackingPage <= 1 || trackingLoading}
                          onClick={() => setTrackingPage((p) => Math.max(1, p - 1))}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800"
                        >
                          Prev
                        </button>
                        <span className="px-2 text-zinc-600 dark:text-zinc-400">Page {trackingPage} of {Math.max(1, Math.ceil(trackingTotal / trackingPageSize))}</span>
                        <button
                          type="button"
                          disabled={trackingPage >= Math.ceil(trackingTotal / trackingPageSize) || trackingLoading}
                          onClick={() => setTrackingPage((p) => p + 1)}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800"
                        >
                          Next
                        </button>
                      </span>
                      <label className="flex items-center gap-2 text-sm">
                        <span className="text-zinc-500">Sort by</span>
                        <select
                          value={trackingSortCol}
                          onChange={(e) => setTrackingSortCol(e.target.value)}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                        >
                          {TRACKING_GRID_COLUMNS.map((col) => (
                            <option key={col} value={col}>{trackingGridColLabel(col)}</option>
                          ))}
                        </select>
                        <select
                          value={trackingSortDir}
                          onChange={(e) => setTrackingSortDir(e.target.value as 'asc' | 'desc')}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                        >
                          <option value="asc">Ascending</option>
                          <option value="desc">Descending</option>
                        </select>
                      </label>
                      <button
                        type="button"
                        onClick={() => void runTagFencePlusRows()}
                        disabled={
                          fencePlusLoading ||
                          !deviceForTracking ||
                          !vineyardTrim ||
                          sortedTrackingRows.length === 0
                        }
                        className="rounded border border-violet-300 bg-violet-50 px-2 py-1 text-sm font-medium text-violet-900 hover:bg-violet-100 disabled:opacity-50 dark:border-violet-600 dark:bg-violet-950/40 dark:text-violet-100 dark:hover:bg-violet-900/50"
                        title="Per-row: job vineyard Steps+ buffer (Settings buffer m). No duration filter. Does not change step logic."
                      >
                        {fencePlusLoading ? 'Tagging…' : 'Tag Fence+ rows'}
                      </button>
                    </div>
                    {fencePlusErr != null && fencePlusErr !== '' && (
                      <p className="mb-2 text-sm text-red-600 dark:text-red-400">{fencePlusErr}</p>
                    )}
                    {fencePlusMeta != null && (
                      <p className="mb-2 text-xs text-zinc-500">
                        Fence+ uses buffer {fencePlusMeta.bufferMeters} m; fence names: {fencePlusMeta.fenceNames.join(', ')}
                      </p>
                    )}
                    <div className="max-h-[1200px] min-h-[600px] overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
                      <table className="min-w-full text-left text-sm">
                        <thead className="sticky top-0 z-10 bg-zinc-100 dark:bg-zinc-800">
                          <tr>
                            {TRACKING_GRID_COLUMNS.map((col) => (
                              <th
                                key={col}
                                className="whitespace-nowrap px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300"
                                title={
                                  col === 'fence_plus'
                                    ? 'Point inside ST_Buffer(job vineyard fence(s), Steps+ buffer m) — same geometry as Steps+; per point, no min duration.'
                                    : undefined
                                }
                              >
                                {trackingGridColLabel(col)}
                              </th>
                            ))}
                            <th className="whitespace-nowrap px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">
                              Distance (m)
                            </th>
                            <th className="whitespace-nowrap px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">
                              Map
                            </th>
                            <th
                              className="whitespace-nowrap px-2 py-1.5 text-right font-medium text-zinc-700 dark:text-zinc-300"
                              title="tbl_tracking primary key"
                            >
                              tbl_tracking.id
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {sortedTrackingRows.map((row, i) => {
                            const lat = row.lat != null ? Number(row.lat) : NaN;
                            const lon = row.lon != null ? Number(row.lon) : NaN;
                            const hasCoords = !Number.isNaN(lat) && !Number.isNaN(lon);
                            const prev = i > 0 ? sortedTrackingRows[i - 1] : null;
                            const prevLat = prev && prev.lat != null ? Number(prev.lat) : NaN;
                            const prevLon = prev && prev.lon != null ? Number(prev.lon) : NaN;
                            const distanceM = prev && Number.isFinite(prevLat) && Number.isFinite(prevLon) && hasCoords
                              ? haversineMeters(prevLat, prevLon, lat, lon)
                              : null;
                            const rowKey = row.id != null && String(row.id).trim() !== '' ? `t-${String(row.id)}` : `i-${i}`;
                            return (
                            <tr key={rowKey} className={`border-t border-zinc-100 dark:border-zinc-800 ${gpsTrackingFenceRowClass(row)}`}>
                              {TRACKING_GRID_COLUMNS.map((col) => {
                                if (col === 'fence_plus') {
                                  const rid = row.id != null ? String(row.id) : '';
                                  const fp = rid !== '' ? fencePlusById[rid] : null;
                                  return (
                                    <td
                                      key={col}
                                      className="max-w-[16rem] truncate px-2 py-1.5 text-zinc-600 dark:text-zinc-400"
                                      title={fp != null && fp !== '' ? fp : undefined}
                                    >
                                      {fp != null && fp !== '' ? fp : '—'}
                                    </td>
                                  );
                                }
                                return (
                                  <td key={col} className="whitespace-nowrap px-2 py-1.5 text-zinc-600 dark:text-zinc-400">
                                    {formatGpsCell((row as Record<string, unknown>)[col])}
                                  </td>
                                );
                              })}
                              <td className="whitespace-nowrap px-2 py-1.5 text-zinc-600 dark:text-zinc-400 tabular-nums">
                                {distanceM != null ? `${Math.round(distanceM)}` : '—'}
                              </td>
                              <td className="whitespace-nowrap px-2 py-1.5">
                                {hasCoords ? (
                                  <a
                                    href={`https://www.google.com/maps?q=${lat},${lon}`}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="text-blue-600 underline hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
                                  >
                                    View
                                  </a>
                                ) : (
                                  <span className="text-zinc-400">—</span>
                                )}
                              </td>
                              <td className="whitespace-nowrap px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:text-zinc-400">
                                {row.id != null && String(row.id).trim() !== ''
                                  ? String(row.id)
                                  : (row as { ID?: unknown }).ID != null && String((row as { ID: unknown }).ID).trim() !== ''
                                    ? String((row as { ID: unknown }).ID)
                                    : '—'}
                              </td>
                            </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                    <div className="mt-2 flex flex-wrap items-center gap-4">
                      <span className="text-sm text-zinc-500">
                        Showing {(trackingPage - 1) * trackingPageSize + 1}–{(trackingPage - 1) * trackingPageSize + trackingRows.length} of {trackingTotal}
                      </span>
                      <label className="flex items-center gap-2 text-sm">
                        <span className="text-zinc-500">Per page</span>
                        <select
                          value={trackingPageSize}
                          onChange={(e) => { setTrackingPageSize(Number(e.target.value)); setTrackingPage(1); }}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                        >
                          {[25, 50, 100, 200].map((n) => (
                            <option key={n} value={n}>{n}</option>
                          ))}
                        </select>
                      </label>
                      <span className="flex items-center gap-1 text-sm">
                        <button
                          type="button"
                          disabled={trackingPage <= 1 || trackingLoading}
                          onClick={() => setTrackingPage((p) => Math.max(1, p - 1))}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800"
                        >
                          Prev
                        </button>
                        <span className="px-2 text-zinc-600 dark:text-zinc-400">Page {trackingPage} of {Math.max(1, Math.ceil(trackingTotal / trackingPageSize))}</span>
                        <button
                          type="button"
                          disabled={trackingPage >= Math.ceil(trackingTotal / trackingPageSize) || trackingLoading}
                          onClick={() => setTrackingPage((p) => p + 1)}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800"
                        >
                          Next
                        </button>
                      </span>
                      <label className="flex items-center gap-2 text-sm">
                        <span className="text-zinc-500">Sort by</span>
                        <select
                          value={trackingSortCol}
                          onChange={(e) => setTrackingSortCol(e.target.value)}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                        >
                          {TRACKING_GRID_COLUMNS.map((col) => (
                            <option key={col} value={col}>{trackingGridColLabel(col)}</option>
                          ))}
                        </select>
                        <select
                          value={trackingSortDir}
                          onChange={(e) => setTrackingSortDir(e.target.value as 'asc' | 'desc')}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                        >
                          <option value="asc">Ascending</option>
                          <option value="desc">Descending</option>
                        </select>
                      </label>
                    </div>
                  </>
                )}
            {deviceForTracking && actualStartTime.trim() && (
              <pre className="mt-3 select-all rounded border border-zinc-200 bg-zinc-50 p-3 font-mono text-xs text-zinc-800 dark:border-zinc-600 dark:bg-zinc-800/50 dark:text-zinc-200" title="Copy and paste to run in your SQL editor (window in NZ for position_time_nz)">
                {trackingSql || (() => {
                const totalStart = startLessMinutes + displayExpandBefore;
                const totalEnd = endPlusMinutes + displayExpandAfter;
                const after = addMinutesToTimestampAsNZ(actualStartTime.trim(), -totalStart);
                const before = actualEndTime.trim() ? addMinutesToTimestampAsNZ(actualEndTime.trim(), totalEnd) : null;
                const esc = (s: string) => `'${String(s).replace(/'/g, "''")}'`;
                let where = `t.device_name=${esc(deviceForTracking)} AND t.position_time_nz>${esc(after)}`;
                if (before) where += ` AND t.position_time_nz<${esc(before)}`;
                return `SELECT t.id, t.device_name, t.geofence_id, g.fence_name, t.geofence_type, t.position_time_nz, t.position_time, t.lat, t.lon FROM tbl_tracking t LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id WHERE ${where} ORDER BY t.position_time_nz ASC LIMIT 500`;
              })()}
              </pre>
            )}
              </>
            )}
          </section>
        </>
      )}
    </div>
    {pinJobModalOpen &&
      typeof document !== 'undefined' &&
      createPortal(
        <div
          className="fixed inset-0 z-[10100] flex items-center justify-center bg-black/40 p-4"
          role="dialog"
          aria-modal="true"
          aria-labelledby="pin-job-title"
          onClick={() => {
            if (!pinJobSaving) {
              setPinJobModalOpen(false);
              setPinJobError(null);
            }
          }}
        >
          <div
            className="w-full max-w-md rounded-lg border border-zinc-200 bg-white p-4 shadow-xl dark:border-zinc-600 dark:bg-zinc-900"
            onClick={(e) => e.stopPropagation()}
          >
            <h3 id="pin-job-title" className="text-base font-semibold text-zinc-900 dark:text-zinc-100">
              Pin job
            </h3>
            <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
              Adds a bookmark under <span className="font-medium text-zinc-800 dark:text-zinc-200">Query → Recent jobs → Pinned</span>. Pins stay in the list when you open the job again; each pin keeps its own note. Use Unpin to remove every pin for this job.
            </p>
            {selectedRow?.job_id != null && selectedRow.job_id !== '' ? (
              <p className="mt-2 font-mono text-xs text-zinc-500 dark:text-zinc-400">Job {String(selectedRow.job_id)}</p>
            ) : null}
            <label className="mt-3 block text-xs font-medium text-zinc-600 dark:text-zinc-300" htmlFor="pin-job-note">
              Note (optional, max 500 characters)
            </label>
            <textarea
              id="pin-job-note"
              ref={pinJobNoteRef}
              value={pinJobNoteDraft}
              onChange={(e) => setPinJobNoteDraft(e.target.value)}
              rows={4}
              maxLength={500}
              placeholder="e.g. Check step 4 vs GPS after rule change…"
              className="mt-1 w-full resize-y rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm text-zinc-900 placeholder:text-zinc-400 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100 dark:placeholder:text-zinc-500"
            />
            <p className="mt-0.5 text-right text-[10px] text-zinc-400">{pinJobNoteDraft.length}/500</p>
            {pinJobError ? <p className="mt-2 text-sm text-red-600 dark:text-red-400">{pinJobError}</p> : null}
            <div className="mt-4 flex flex-wrap justify-end gap-2">
              <button
                type="button"
                disabled={pinJobSaving || !selectedRow}
                onClick={() => void submitUnpinJobFromRecent()}
                className="mr-auto rounded border border-amber-300 bg-white px-3 py-1.5 text-sm font-medium text-amber-700 hover:bg-amber-50 disabled:opacity-50 dark:border-amber-700 dark:bg-zinc-900 dark:text-amber-200 dark:hover:bg-amber-950/30"
                title="Remove all pinned bookmarks for this job from the sidebar."
              >
                Unpin
              </button>
              <button
                type="button"
                disabled={pinJobSaving}
                onClick={() => {
                  setPinJobModalOpen(false);
                  setPinJobError(null);
                }}
                className="rounded border border-zinc-300 bg-white px-3 py-1.5 text-sm font-medium text-zinc-700 hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-200 dark:hover:bg-zinc-700"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={pinJobSaving || !selectedRow}
                onClick={() => void submitPinJobToRecent()}
                className="rounded bg-amber-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50 dark:bg-amber-700 dark:hover:bg-amber-600"
              >
                {pinJobSaving ? 'Saving…' : 'Pin to list'}
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}
    </>
  );
}

export default function InspectPage() {
  return (
    <StaffOnlyGate>
      <Suspense fallback={<div className="flex min-h-[200px] items-center justify-center p-6 text-zinc-500">Loading…</div>}>
        <InspectContent />
      </Suspense>
    </StaffOnlyGate>
  );
}

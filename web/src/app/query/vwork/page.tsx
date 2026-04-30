'use client';

import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import { formatColumnLabel, formatDateNZ, computeColumnWidths } from '@/lib/utils';

type Row = Record<string, unknown>;

type DbHealth = { ok: boolean; error?: string; hint?: string; code?: string | null } | null;

const TABLE_NAME = 'tbl_vworkjobs';
const SORT_SETTING_TYPE = 'System';
const SORT_SETTING_NAME = 'VWsort';

const PRIORITY_COLUMNS = ['job_id', 'planned_start_time', 'truck_id', 'truck_rego', 'step4to5'];

const DATE_COLUMNS = new Set([
  'planned_start_time', 'actual_start_time', 'gps_start_time', 'gps_end_time',
  'step_1_completed_at', 'step_1_safe', 'step_2_completed_at', 'step_3_completed_at', 'step_4_completed_at',
]);

/** Server-side date filter columns supported by /api/vworkjobs (columnDateCol / columnDateFrom / columnDateTo). */
const API_COLUMN_DATE_COLS = new Set(['planned_start_time', 'actual_start_time', 'actual_end_time']);

function isIsoDateString(v: unknown): v is string {
  return typeof v === 'string' && /^\d{4}-\d{2}-\d{2}/.test(v);
}

function compare(a: unknown, b: unknown, col: string): number {
  const va = a == null ? '' : a;
  const vb = b == null ? '' : b;
  if (va === vb) return 0;
  if (DATE_COLUMNS.has(col) && isIsoDateString(va) && isIsoDateString(vb)) {
    return va.localeCompare(vb);
  }
  if (typeof va === 'number' && typeof vb === 'number') return va - vb;
  if (typeof va === 'string' && typeof vb === 'string') {
    const na = Number(va);
    const nb = Number(vb);
    if (!Number.isNaN(na) && !Number.isNaN(nb)) return na - nb;
    return va.localeCompare(vb);
  }
  return String(va).localeCompare(String(vb));
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

function VworkPageContent() {
  const searchParams = useSearchParams();
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(false);
  const [dataLoadedOnce, setDataLoadedOnce] = useState(false);
  const jobFetchGenRef = useRef(0);
  const [customerOptions, setCustomerOptions] = useState<string[]>([]);
  const [templateOptions, setTemplateOptions] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [errorDetail, setErrorDetail] = useState<{ hint?: string; code?: string } | null>(null);
  const [dbHealth, setDbHealth] = useState<DbHealth>(null);
  const [debugInfo, setDebugInfo] = useState<Record<string, unknown> | null>(null);
  const [sortColumns, setSortColumns] = useState<[string, string, string]>(['', '', '']);
  const sortColumnsInitialized = useRef(false);
  const [dateFilterCol, setDateFilterCol] = useState<string>('');
  const [dateFrom, setDateFrom] = useState<string>('');
  const [dateTo, setDateTo] = useState<string>('');
  const [filterCustomer, setFilterCustomer] = useState<string>('');
  const [filterTemplate, setFilterTemplate] = useState<string>('');
  /** API: step4to5=0 | step4to5=1 */
  const [filterStep4to5, setFilterStep4to5] = useState<'0' | '1' | ''>('');
  /** Step4→5 blocked preset from Data Checks deep link */
  const [blockedView, setBlockedView] = useState<'normal' | 'rerun' | 'ordering' | ''>('');
  const [columnOrder, setColumnOrder] = useState<string[]>([]);
  const [hiddenColumns, setHiddenColumns] = useState<Set<string>>(new Set());
  const [showColumnConfig, setShowColumnConfig] = useState(false);
  const columnOrderInitialized = useRef(false);

  const apiColumns = useMemo(
    () => (rows.length > 0 ? Object.keys(rows[0]) : []),
    [rows],
  );

  useEffect(() => {
    if (apiColumns.length === 0 || columnOrderInitialized.current) return;
    columnOrderInitialized.current = true;
    const cols = apiColumns;
    const table = new URLSearchParams({ table: TABLE_NAME }).toString();
    fetch(`/api/column-order?${table}`, { cache: 'no-store' })
      .then((r) => r.json())
      .then((data) => {
        const saved = Array.isArray(data?.columnOrder) && data.columnOrder.every((x: unknown) => typeof x === 'string')
          ? (data.columnOrder as string[])
          : null;
        setColumnOrder(mergeColumnOrder(cols, saved));
        const hidden = Array.isArray(data?.hiddenColumns) && data.hiddenColumns.every((x: unknown) => typeof x === 'string')
          ? new Set(data.hiddenColumns as string[])
          : new Set<string>();
        setHiddenColumns(hidden);
      })
      .catch(() => setColumnOrder(getDefaultColumnOrder(cols)));
  }, [apiColumns.length, apiColumns]);

  useEffect(() => {
    if (apiColumns.length === 0 || sortColumnsInitialized.current) return;
    sortColumnsInitialized.current = true;
    const q = new URLSearchParams({ type: SORT_SETTING_TYPE, name: SORT_SETTING_NAME }).toString();
    fetch(`/api/settings?${q}`, { cache: 'no-store' })
      .then((r) => r.json())
      .then((data) => {
        if (data?.settingvalue) {
          try {
            const arr = JSON.parse(data.settingvalue) as unknown[];
            if (Array.isArray(arr) && arr.every((x) => typeof x === 'string')) {
              const cols = new Set(apiColumns);
              const valid = arr.filter((c): c is string => typeof c === 'string' && cols.has(c));
              setSortColumns([
                valid[0] ?? '',
                valid[1] ?? '',
                valid[2] ?? '',
              ]);
              return;
            }
          } catch {}
        }
        // Legacy: no sort in tbl_settings — apply no sort
        setSortColumns(['', '', '']);
      })
      .catch(() => setSortColumns(['', '', '']));
  }, [apiColumns.length, apiColumns]);

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

  const saveColumnConfig = (order: string[], hidden: Set<string>) => {
    const orderToSave = order.length ? order : allColumns;
    if (!orderToSave.length) return;
    fetch('/api/column-order', {
      method: 'PUT',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        table: TABLE_NAME,
        columnOrder: orderToSave,
        hiddenColumns: Array.from(hidden),
      }),
    }).catch(() => {});
  };

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
    const defaultOrder = getDefaultColumnOrder(apiColumns);
    setColumnOrder(defaultOrder);
    setHiddenColumns(new Set());
    saveColumnConfig(defaultOrder, new Set());
    setShowColumnConfig(false);
  };

  /** Date filters sent to API only support a subset of columns; always offer those before first load. */
  const dateColumns = useMemo(() => {
    const fromLoaded = columns.filter((c) => DATE_COLUMNS.has(c) && API_COLUMN_DATE_COLS.has(c));
    if (fromLoaded.length > 0) return fromLoaded;
    return ['planned_start_time', 'actual_start_time', 'actual_end_time'];
  }, [columns]);

  useEffect(() => {
    fetch('/api/vworkjobs/customers', { cache: 'no-store' })
      .then((r) => r.json())
      .then((d) => setCustomerOptions(Array.isArray(d?.customers) ? d.customers : []))
      .catch(() => setCustomerOptions([]));
  }, []);

  useEffect(() => {
    const c = filterCustomer.trim();
    if (!c) {
      setTemplateOptions([]);
      return;
    }
    fetch(`/api/vworkjobs/templates?customer=${encodeURIComponent(c)}`, { cache: 'no-store' })
      .then((r) => r.json())
      .then((d) => setTemplateOptions(Array.isArray(d?.templates) ? d.templates : []))
      .catch(() => setTemplateOptions([]));
  }, [filterCustomer]);

  const canLoadVworkJobs = useMemo(() => {
    if (!filterCustomer.trim()) return false;
    if (filterTemplate.trim()) return true;
    if (
      dateFilterCol &&
      API_COLUMN_DATE_COLS.has(dateFilterCol) &&
      (dateFrom.trim().slice(0, 10) !== '' || dateTo.trim().slice(0, 10) !== '')
    ) {
      return true;
    }
    return false;
  }, [filterCustomer, filterTemplate, dateFilterCol, dateFrom, dateTo]);

  useEffect(() => {
    jobFetchGenRef.current += 1;
    setLoading(false);
    setRows([]);
    setError(null);
    setDataLoadedOnce(false);
  }, [filterCustomer, filterTemplate, dateFilterCol, dateFrom, dateTo, filterStep4to5, blockedView]);

  const buildVworkJobsParams = useCallback((): URLSearchParams | null => {
    if (!canLoadVworkJobs) return null;
    const params = new URLSearchParams();
    params.set('customer', filterCustomer.trim());
    if (filterTemplate.trim()) params.set('template', filterTemplate.trim());
    if (filterStep4to5 === '0' || filterStep4to5 === '1') params.set('step4to5', filterStep4to5);
    if (blockedView === 'normal' || blockedView === 'rerun' || blockedView === 'ordering')
      params.set('blockedView', blockedView);
    if (dateFilterCol && API_COLUMN_DATE_COLS.has(dateFilterCol)) {
      const df = dateFrom.trim().slice(0, 10);
      const dt = dateTo.trim().slice(0, 10);
      if (df || dt) {
        params.set('columnDateCol', dateFilterCol);
        if (df) params.set('columnDateFrom', df);
        if (dt) params.set('columnDateTo', dt);
      }
    }
    return params;
  }, [
    canLoadVworkJobs,
    filterCustomer,
    filterTemplate,
    filterStep4to5,
    blockedView,
    dateFilterCol,
    dateFrom,
    dateTo,
  ]);

  const runVworkJobsFetch = useCallback((params: URLSearchParams) => {
    const gen = ++jobFetchGenRef.current;
    setLoading(true);
    fetch(`/api/vworkjobs?${params}`, { cache: 'no-store' })
      .then(async (res) => {
        const data = await res.json();
        if (!res.ok) {
          setErrorDetail({ hint: data?.hint, code: data?.code });
          throw new Error(data?.error ?? res.statusText);
        }
        return data;
      })
      .then((data) => {
        if (gen !== jobFetchGenRef.current) return;
        setRows(data.rows ?? []);
        setError(null);
        setErrorDetail(null);
        setDataLoadedOnce(true);
      })
      .catch((e) => {
        if (gen !== jobFetchGenRef.current) return;
        setError(e.message);
        setDataLoadedOnce(true);
      })
      .finally(() => {
        if (gen === jobFetchGenRef.current) setLoading(false);
      });
  }, []);

  const loadVworkJobs = useCallback(() => {
    const params = buildVworkJobsParams();
    if (!params) return;
    runVworkJobsFetch(params);
  }, [buildVworkJobsParams, runVworkJobsFetch]);

  const urlHydratedRef = useRef(false);
  useEffect(() => {
    if (urlHydratedRef.current) return;
    urlHydratedRef.current = true;
    const c = searchParams.get('customer')?.trim() ?? '';
    const t = searchParams.get('template')?.trim() ?? '';
    if (c) setFilterCustomer(c);
    if (t) setFilterTemplate(t);
    const s45 = searchParams.get('step4to5')?.trim();
    if (s45 === '0' || s45 === '1') setFilterStep4to5(s45);
    const bv = searchParams.get('blockedView')?.trim().toLowerCase();
    if (bv === 'normal' || bv === 'rerun' || bv === 'ordering') setBlockedView(bv as 'normal' | 'rerun' | 'ordering');
    const auto = searchParams.get('autoLoad') === '1';
    if (!auto || !c || !t) return;
    const params = new URLSearchParams();
    params.set('customer', c);
    params.set('template', t);
    if (s45 === '0' || s45 === '1') params.set('step4to5', s45);
    if (bv === 'normal' || bv === 'rerun' || bv === 'ordering') params.set('blockedView', bv);
    runVworkJobsFetch(params);
  }, [searchParams, runVworkJobsFetch]);

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

  const filteredRows = useMemo(() => {
    return rows.filter((row) => {
      if (filterCustomer) {
        const v = row.Customer ?? row.customer;
        if (v == null || String(v).trim() !== filterCustomer) return false;
      }
      if (filterTemplate) {
        const v = row.template;
        if (v == null || String(v).trim() !== filterTemplate) return false;
      }
      if (dateFilterCol && (dateFrom || dateTo)) {
        const v = row[dateFilterCol];
        if (v == null || v === '') return false;
        const str = String(v);
        if (!/^\d{4}-\d{2}-\d{2}/.test(str)) return false;
        const datePart = str.match(/^(\d{4})-(\d{2})-(\d{2})/)?.[0] ?? '';
        if (!datePart) return false;
        if (dateFrom && datePart < dateFrom.slice(0, 10)) return false;
        if (dateTo && datePart > dateTo.slice(0, 10)) return false;
      }
      return true;
    });
  }, [rows, filterCustomer, filterTemplate, dateFilterCol, dateFrom, dateTo]);

  const sortedRows = useMemo(() => {
    const [c1, c2, c3] = sortColumns;
    const cols = [c1, c2, c3].filter(Boolean);
    if (cols.length === 0) return filteredRows;
    const out = [...filteredRows];
    out.sort((a, b) => {
      for (const col of cols) {
        const c = compare(a[col], b[col], col);
        if (c !== 0) return c;
      }
      return 0;
    });
    return out;
  }, [filteredRows, sortColumns]);

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
          console.error('[Vwork save sort] FAILED:', JSON.stringify(details, null, 2));
          setSortSaveStatus('error');
        }
      })
      .catch((err) => {
        console.error('[Vwork save sort] FETCH ERROR:', err);
        setSortSaveStatus('error');
      })
      .finally(() => { setTimeout(() => setSortSaveStatus('idle'), 2000); });
  };

  const setSortColumn = (idx: 0 | 1 | 2, value: string) => {
    const next: [string, string, string] = [...sortColumns];
    next[idx] = value;
    setSortColumns(next);
  };

  const formatCell = useCallback((v: unknown): string => {
    if (v == null || v === '') return '—';
    if (isIsoDateString(v)) return formatDateNZ(v);
    return String(v);
  }, []);

  const columnWidths = useMemo(
    () => computeColumnWidths(columns, sortedRows, formatCell, formatColumnLabel),
    [columns, sortedRows],
  );

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-4 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">tbl_vworkjobs</h1>
      {error && (
        <div className="mb-4 rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          <p className="font-medium">{error}</p>
          {errorDetail?.hint && <p className="mt-2 text-sm opacity-90">{errorDetail.hint}</p>}
          {errorDetail?.code && <p className="mt-1 text-xs">Code: {errorDetail.code}</p>}
        </div>
      )}
      {loading && <p className="mb-2 text-zinc-600 dark:text-zinc-400">Loading jobs…</p>}
      <div className="mb-4 flex flex-wrap items-end gap-4 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
        <div>
          <label className="mb-1 block text-xs font-medium text-zinc-500">Customer</label>
          <select
            value={filterCustomer}
            onChange={(e) => {
              setFilterCustomer(e.target.value);
              setFilterTemplate('');
            }}
            className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          >
            <option value="">— Select —</option>
            {customerOptions.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-zinc-500">Template</label>
          <select
            value={filterTemplate}
            onChange={(e) => setFilterTemplate(e.target.value)}
            className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            disabled={!filterCustomer.trim()}
          >
            <option value="">— All —</option>
            {templateOptions.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-zinc-500">step4to5</label>
          <select
            value={filterStep4to5}
            onChange={(e) => setFilterStep4to5((e.target.value === '0' || e.target.value === '1' ? e.target.value : '') as '' | '0' | '1')}
            className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            disabled={!filterCustomer.trim()}
            title="Step4→5 migration flag on tbl_vworkjobs"
          >
            <option value="">— All —</option>
            <option value="0">Not migrated (0)</option>
            <option value="1">Migrated (1)</option>
          </select>
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-zinc-500">Date column</label>
          <select
            value={dateFilterCol}
            onChange={(e) => setDateFilterCol(e.target.value)}
            className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          >
            <option value="">— None —</option>
            {dateColumns.map((c) => (
              <option key={c} value={c}>{formatColumnLabel(c)}</option>
            ))}
          </select>
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-zinc-500">From</label>
          <input
            type="datetime-local"
            value={dateFrom}
            onChange={(e) => setDateFrom(e.target.value)}
            className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-zinc-500">To</label>
          <input
            type="datetime-local"
            value={dateTo}
            onChange={(e) => setDateTo(e.target.value)}
            className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-zinc-500">Jobs</label>
          <button
            type="button"
            onClick={() => loadVworkJobs()}
            disabled={!canLoadVworkJobs || loading}
            className="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600"
          >
            {loading ? 'Loading…' : 'Load'}
          </button>
        </div>
      </div>
      {blockedView && (
        <div className="mb-4 flex flex-wrap items-center justify-between gap-2 rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-950 dark:border-amber-700 dark:bg-amber-950/40 dark:text-amber-100">
          <span>
            <strong>Blocked filter</strong> (Step4→5){' '}
            {blockedView === 'normal'
              ? '— step4to5 = 0 but step_4 is not Job Completed, or step_5 name is Job Completed, or step_5_completed_at is set (normal Fix skips these).'
              : blockedView === 'rerun'
                ? '— step4to5 = 1 but not the done layout (step_4 Arrive Winery + step_5 Job Completed).'
                : '— done layout with step_4 already strictly before step_5 (Order fix skips these).'}
          </span>
          <button
            type="button"
            onClick={() => setBlockedView('')}
            className="shrink-0 rounded border border-amber-600 px-2 py-1 text-xs font-medium hover:bg-amber-100 dark:border-amber-500 dark:hover:bg-amber-900/50"
          >
            Clear blocked filter
          </button>
        </div>
      )}
      {!dataLoadedOnce && !loading && (
        <p className="mb-4 text-sm text-zinc-500 dark:text-zinc-400">
          Select a customer, then choose a template or set a date column (planned/actual start or end) with from/to, then click Load. Nothing is fetched until you load.
          Optional: filter by <code className="rounded bg-zinc-200 px-1 text-xs dark:bg-zinc-700">step4to5</code>. Open blocked rows from Data Checks → Step4→5 via the metrics table.
        </p>
      )}
      {dataLoadedOnce && rows.length === 0 && !loading && !error && (
        <p className="mb-4 text-zinc-600 dark:text-zinc-400">No rows match.</p>
      )}
      {rows.length > 0 && (
        <>
          <div className="mb-4 flex flex-wrap items-end gap-4 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Sort 1</label>
              <select
                value={sortColumns[0]}
                onChange={(e) => setSortColumn(0, e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— None —</option>
                {allColumns.map((c) => <option key={c} value={c}>{formatColumnLabel(c)}</option>)}
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
                {allColumns.map((c) => <option key={c} value={c}>{formatColumnLabel(c)}</option>)}
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
                {allColumns.map((c) => <option key={c} value={c}>{formatColumnLabel(c)}</option>)}
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
            <div className="relative">
              <button
                type="button"
                onClick={() => setShowColumnConfig((v) => !v)}
                className="rounded bg-zinc-200 px-3 py-1.5 text-sm hover:bg-zinc-300 dark:bg-zinc-700 dark:hover:bg-zinc-600"
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
                  <div className="absolute left-0 top-full z-50 mt-1 min-w-[220px] rounded-lg border border-zinc-200 bg-white p-3 shadow-lg dark:border-zinc-700 dark:bg-zinc-900">
                    <div className="mb-2 flex items-center justify-between gap-2">
                      <span className="text-xs font-medium text-zinc-500">Show/hide columns</span>
                      <div className="flex gap-2">
                        <button type="button" onClick={showAllColumns} className="text-xs text-blue-600 hover:underline dark:text-blue-400">
                          Show all
                        </button>
                        <button type="button" onClick={resetColumnConfig} className="text-xs text-zinc-500 hover:underline">
                          Reset
                        </button>
                      </div>
                    </div>
                    <div className="max-h-64 overflow-y-auto space-y-1">
                      {allColumns.map((col) => (
                        <label key={col} className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 hover:bg-zinc-100 dark:hover:bg-zinc-800">
                          <input
                            type="checkbox"
                            checked={!hiddenColumns.has(col)}
                            onChange={() => toggleColumnVisibility(col)}
                            className="rounded"
                          />
                          <span className="text-sm">{formatColumnLabel(col)}</span>
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
          <div className="max-h-[56rem] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
            <table className="w-max table-fixed text-left text-sm">
              <colgroup>
                {columns.map((col) => (
                  <col key={col} style={{ width: columnWidths[col], minWidth: columnWidths[col] }} />
                ))}
              </colgroup>
              <thead className="sticky top-0 z-10 bg-zinc-100 dark:bg-zinc-800">
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
                      title="Drag to reorder · Use Sort dropdowns above"
                    >
                      {formatColumnLabel(col)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row, i) => (
                  <tr key={i} className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50">
                    {columns.map((col) => (
                      <td key={col} className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">
                        {formatCell(row[col])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="mt-3 text-zinc-500 dark:text-zinc-400">
            {sortedRows.length} row{sortedRows.length !== 1 ? 's' : ''}
            {(filterCustomer || filterTemplate || dateFilterCol) && rows.length !== sortedRows.length
              ? ` (client filter ${sortedRows.length} of ${rows.length} loaded)`
              : ''}{' '}
            · loaded with current API filters
          </p>
        </>
      )}
      <section className="mt-8 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-2 text-sm font-semibold text-zinc-700 dark:text-zinc-300">Debug</h2>
        <div className="flex flex-wrap gap-2">
          <button type="button" onClick={() => { setDbHealth(null); fetch('/api/health/db').then((r) => r.json()).then(setDbHealth); }}
            className="rounded bg-zinc-200 px-3 py-1.5 text-sm hover:bg-zinc-300 dark:bg-zinc-700 dark:hover:bg-zinc-600">Check DB connection</button>
          <button type="button" onClick={() => { setDebugInfo(null); fetch('/api/debug').then((r) => r.json()).then(setDebugInfo); }}
            className="rounded bg-zinc-200 px-3 py-1.5 text-sm hover:bg-zinc-300 dark:bg-zinc-700 dark:hover:bg-zinc-600">Show env</button>
        </div>
        {dbHealth !== null && <pre className="mt-2 overflow-auto rounded bg-zinc-100 p-2 text-xs dark:bg-zinc-800">{JSON.stringify(dbHealth, null, 2)}</pre>}
        {debugInfo !== null && <pre className="mt-2 overflow-auto rounded bg-zinc-100 p-2 text-xs dark:bg-zinc-800">{JSON.stringify(debugInfo, null, 2)}</pre>}
      </section>
    </div>
  );
}

export default function VworkPage() {
  return (
    <Suspense fallback={<div className="w-full min-w-0 p-6 text-zinc-500 dark:text-zinc-400">Loading…</div>}>
      <VworkPageContent />
    </Suspense>
  );
}

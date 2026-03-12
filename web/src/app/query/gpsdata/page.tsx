'use client';

import Link from 'next/link';
import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import { formatColumnLabel, formatDateNZ, computeColumnWidths } from '@/lib/utils';

type Row = Record<string, unknown>;

type DbHealth = { ok: boolean; error?: string; hint?: string; code?: string | null } | null;

const TABLE_NAME = 'tbl_tracking';
const SORT_SETTING_TYPE = 'System';
const SORT_SETTING_NAME = 'TrackingSort';

const PRIORITY_COLUMNS = ['device_name', 'imei', 'position_time', 'position_time_nz', 'geofence_id', 'fence_name', 'geofence_type', 'apirow'];

const DATE_COLUMNS = new Set(['position_time', 'position_time_nz']);

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

function GpsDataContent() {
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [errorDetail, setErrorDetail] = useState<{ hint?: string; code?: string } | null>(null);
  const [dbHealth, setDbHealth] = useState<DbHealth>(null);
  const [debugInfo, setDebugInfo] = useState<Record<string, unknown> | null>(null);
  const [sortColumns, setSortColumns] = useState<[string, string, string]>(['', '', '']);
  const sortColumnsInitialized = useRef(false);
  const [dateFilterCol, setDateFilterCol] = useState<string>('');
  const [dateFrom, setDateFrom] = useState<string>('');
  const [dateTo, setDateTo] = useState<string>('');
  const searchParams = useSearchParams();
  const [deviceNameFilter, setDeviceNameFilter] = useState<string>('');
  const [fenceNameFilter, setFenceNameFilter] = useState<string>('');
  const [geofenceIdFilter, setGeofenceIdFilter] = useState<string>(() => {
    const id = searchParams.get('geofenceId');
    return (id != null && id !== '') ? id.trim() : '';
  }); // from URL or filter bar; sent to API
  const [geofenceTypeFilter, setGeofenceTypeFilter] = useState<string>(''); // '' = All, 'ENTER', 'EXIT', 'entry_or_exit' = Enter or Exit
  const [headerSortColumn, setHeaderSortColumn] = useState<string | null>(null);
  const [headerSortDir, setHeaderSortDir] = useState<'asc' | 'desc'>('asc');
  const [columnOrder, setColumnOrder] = useState<string[]>([]);
  const [hiddenColumns, setHiddenColumns] = useState<Set<string>>(new Set());
  const [showColumnConfig, setShowColumnConfig] = useState(false);
  const columnOrderInitialized = useRef(false);
  const [totalRows, setTotalRows] = useState<number>(0);
  const [offset, setOffset] = useState(0);
  const [apiOrderBy, setApiOrderBy] = useState<'position_time_nz' | 'position_time'>('position_time_nz');
  const [apiOrderDir, setApiOrderDir] = useState<'asc' | 'desc'>('asc');

  const apiColumns = useMemo(
    () => (rows.length > 0 ? Object.keys(rows[0]) : []),
    [rows],
  );

  useEffect(() => {
    const id = searchParams.get('geofenceId');
    setGeofenceIdFilter((prev) => {
      const next = (id != null && id !== '') ? id.trim() : '';
      return next !== prev ? next : prev;
    });
  }, [searchParams]);

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

  const dateColumns = useMemo(() => columns.filter((c) => DATE_COLUMNS.has(c)), [columns]);

  const uniqueDeviceNames = useMemo(() => {
    const set = new Set<string>();
    rows.forEach((row) => {
      const v = row.device_name;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    });
    return Array.from(set).sort();
  }, [rows]);

  const uniqueFenceNames = useMemo(() => {
    const set = new Set<string>();
    rows.forEach((row) => {
      const v = row.fence_name;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    });
    return Array.from(set).sort();
  }, [rows]);

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

  const handleHeaderSort = (col: string) => {
    setHeaderSortColumn((prev) => {
      if (prev === col) {
        setHeaderSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
        return prev;
      }
      setHeaderSortDir('asc');
      return col;
    });
  };

  const fetchPage = useCallback((off: number) => {
    setLoading(true);
    const params = new URLSearchParams({
      limit: '500',
      offset: String(off),
      orderBy: apiOrderBy,
      orderDir: apiOrderDir,
    });
    if (deviceNameFilter.trim()) params.set('device', deviceNameFilter.trim());
    if (geofenceIdFilter.trim()) params.set('geofenceId', geofenceIdFilter.trim());
    if (geofenceTypeFilter) params.set('geofenceType', geofenceTypeFilter);
    fetch(`/api/gpsdata?${params}`)
      .then(async (res) => {
        const data = await res.json();
        if (!res.ok) {
          setErrorDetail({ hint: data?.hint, code: data?.code });
          throw new Error(data?.error ?? res.statusText);
        }
        return data;
      })
      .then((data) => {
        setRows(data.rows ?? []);
        setTotalRows(Number(data.total) ?? 0);
        setOffset(off);
        setError(null);
        setErrorDetail(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [apiOrderBy, apiOrderDir, deviceNameFilter, geofenceIdFilter, geofenceTypeFilter]);

  useEffect(() => {
    fetchPage(0);
  }, [fetchPage]);

  const goToPrevPage = () => {
    const next = Math.max(0, offset - 500);
    if (next !== offset) fetchPage(next);
  };
  const goToNextPage = () => {
    if (offset + 500 < totalRows) fetchPage(offset + 500);
  };

  const filteredRows = useMemo(() => {
    let out = rows;
    if (dateFilterCol && (dateFrom || dateTo)) {
      out = out.filter((row) => {
        const v = row[dateFilterCol];
        if (v == null || v === '') return false;
        const str = String(v);
        if (!/^\d{4}-\d{2}-\d{2}/.test(str)) return false;
        const datePart = str.match(/^(\d{4})-(\d{2})-(\d{2})/)?.[0] ?? '';
        if (!datePart) return false;
        if (dateFrom && datePart < dateFrom.slice(0, 10)) return false;
        if (dateTo && datePart > dateTo.slice(0, 10)) return false;
        return true;
      });
    }
    if (deviceNameFilter) {
      out = out.filter((row) => String(row.device_name ?? '').trim() === deviceNameFilter);
    }
    if (fenceNameFilter) {
      out = out.filter((row) => String(row.fence_name ?? '').trim() === fenceNameFilter);
    }
    // geofence type is applied server-side (API) so we get 500 matching rows per page
    return out;
  }, [rows, dateFilterCol, dateFrom, dateTo, deviceNameFilter, fenceNameFilter]);

  const sortedRows = useMemo(() => {
    const out = [...filteredRows];
    const [c1, c2, c3] = sortColumns;
    const dropdownCols = [c1, c2, c3].filter(Boolean);
    out.sort((a, b) => {
      if (headerSortColumn) {
        const c = compare(a[headerSortColumn], b[headerSortColumn], headerSortColumn);
        if (c !== 0) return headerSortDir === 'desc' ? -c : c;
      }
      for (const col of dropdownCols) {
        const c = compare(a[col], b[col], col);
        if (c !== 0) return c;
      }
      return 0;
    });
    return out;
  }, [filteredRows, sortColumns, headerSortColumn, headerSortDir]);

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
          console.error('[GPS save sort] FAILED:', JSON.stringify(details, null, 2));
          setSortSaveStatus('error');
        }
      })
      .catch((err) => {
        console.error('[GPS save sort] FETCH ERROR:', err);
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
      <h1 className="mb-4 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">GPS Tracking</h1>
      <p className="mb-4 text-sm text-zinc-500 dark:text-zinc-400">
        Inspect tbl_tracking in detail (device_name, imei, position times, geofence, apirow). Not linked to any job — for raw tracking inspection.
      </p>
      {loading && <p className="text-zinc-600">Loading…</p>}
      {error && (
        <div className="rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          <p className="font-medium">{error}</p>
          {errorDetail?.hint && <p className="mt-2 text-sm opacity-90">{errorDetail.hint}</p>}
          {errorDetail?.code && <p className="mt-1 text-xs">Code: {errorDetail.code}</p>}
        </div>
      )}
      {!loading && !error && rows.length === 0 && <p className="text-zinc-600">No rows.</p>}
      {!loading && !error && rows.length > 0 && (
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
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Date column</label>
              <select
                value={dateFilterCol}
                onChange={(e) => setDateFilterCol(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— None —</option>
                {dateColumns.map((c) => <option key={c} value={c}>{formatColumnLabel(c)}</option>)}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">From</label>
              <input type="datetime-local" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800" />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">To</label>
              <input type="datetime-local" value={dateTo} onChange={(e) => setDateTo(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800" />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Device name</label>
              <select
                value={deviceNameFilter}
                onChange={(e) => setDeviceNameFilter(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— All —</option>
                {uniqueDeviceNames.map((name) => (
                  <option key={name} value={name}>{name}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Fence name</label>
              <select
                value={fenceNameFilter}
                onChange={(e) => setFenceNameFilter(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— All —</option>
                {uniqueFenceNames.map((name) => (
                  <option key={name} value={name}>{name}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Fence ID</label>
              <input
                type="text"
                value={geofenceIdFilter}
                onChange={(e) => setGeofenceIdFilter(e.target.value)}
                placeholder="— All —"
                className="w-24 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                title="Filter by geofence_id (e.g. from GPS Mappings DataCount link)"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Geofence type</label>
              <select
                value={geofenceTypeFilter}
                onChange={(e) => setGeofenceTypeFilter(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">All</option>
                <option value="ENTER">Enter</option>
                <option value="EXIT">Exit</option>
                <option value="entry_or_exit">Enter or Exit</option>
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Server order</label>
              <select
                value={`${apiOrderBy}:${apiOrderDir}`}
                onChange={(e) => {
                  const v = e.target.value;
                  const [col, dir] = v.split(':') as [string, string];
                  setApiOrderBy(col as 'position_time_nz' | 'position_time');
                  setApiOrderDir(dir as 'asc' | 'desc');
                }}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                title="Raw column order for pagination — no timezone conversion"
              >
                <option value="position_time_nz:asc">Earliest first (position_time_nz)</option>
                <option value="position_time_nz:desc">Latest first (position_time_nz)</option>
                <option value="position_time:asc">Earliest first (position_time)</option>
                <option value="position_time:desc">Latest first (position_time)</option>
              </select>
            </div>
          </div>
          <div className="max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
            <table className="w-max table-fixed text-left text-sm">
              <colgroup>
                {columns.map((col) => (
                  <col key={col} style={{ width: columnWidths[col], minWidth: columnWidths[col] }} />
                ))}
              </colgroup>
              <thead className="sticky top-0 z-10 bg-zinc-100 dark:bg-zinc-800 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:shadow-[0_1px_0_0_rgba(255,255,255,0.1)]">
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  {columns.map((col) => (
                    <th
                      key={col}
                      draggable
                      onDragStart={() => handleColumnDragStart(col)}
                      onDragOver={(e) => handleColumnDragOver(e, col)}
                      onDragLeave={handleColumnDragLeave}
                      onDrop={(e) => handleColumnDrop(e, col)}
                      onDragEnd={handleColumnDragEnd}
                      onClick={() => handleHeaderSort(col)}
                      className={`cursor-grab select-none whitespace-nowrap px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:text-zinc-100 dark:hover:bg-zinc-700 ${dropTargetCol === col ? 'bg-blue-200 dark:bg-blue-800' : ''} ${dragCol === col ? 'opacity-60' : ''}`}
                      title="Drag to reorder · Click to sort by this column"
                    >
                      <span className="inline-flex items-center gap-1">
                        {formatColumnLabel(col)}
                        {headerSortColumn === col && (
                          <span className="text-zinc-500" aria-hidden>
                            {headerSortDir === 'asc' ? '↑' : '↓'}
                          </span>
                        )}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row, i) => (
                  <tr key={i} className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50">
                    {columns.map((col) => (
                      <td
                        key={col}
                        className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300"
                        title={col === 'fence_name' && row.geofence_id != null ? `geofence_id: ${row.geofence_id}` : undefined}
                      >
                        {formatCell(row[col])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="mt-3 flex flex-wrap items-center gap-4">
            <p className="text-zinc-500">
              {sortedRows.length} row{sortedRows.length !== 1 ? 's' : ''}
              {(dateFilterCol || fenceNameFilter) && ` (filtered from ${rows.length} on this page)`}
              {sortedRows.length > 0 && (() => {
                const times = sortedRows.map((r) => r.position_time_nz ?? r.position_time).filter(Boolean) as string[];
                if (times.length === 0) return null;
                const sorted = [...times].sort();
                const minT = sorted[0].toString().slice(0, 19).replace('T', ' ');
                const maxT = sorted[sorted.length - 1].toString().slice(0, 19).replace('T', ' ');
                return ` · This page range: ${minT} → ${maxT}`;
              })()}
            </p>
            <div className="flex items-center gap-2">
              <span className="text-sm text-zinc-600 dark:text-zinc-400">
                Rows {totalRows === 0 ? '0' : `${offset + 1}–${Math.min(offset + rows.length, totalRows)}`} of {totalRows}
              </span>
              <button
                type="button"
                onClick={goToPrevPage}
                disabled={offset === 0 || loading}
                className="rounded border border-zinc-400 bg-white px-3 py-1.5 text-sm font-medium text-zinc-700 hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-500 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
              >
                Previous
              </button>
              <button
                type="button"
                onClick={goToNextPage}
                disabled={offset + 500 >= totalRows || loading}
                className="rounded border border-zinc-400 bg-white px-3 py-1.5 text-sm font-medium text-zinc-700 hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-500 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
              >
                Next
              </button>
            </div>
          </div>
          <p className="mt-1 text-xs text-zinc-400 dark:text-zinc-500">
            Pagination uses raw column order (no timezone). Column order and sort (Sort 1–3) are saved when you click &quot;Save sort config&quot;.
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

export default function GpsDataPage() {
  return (
    <Suspense fallback={<div className="flex min-h-[200px] items-center justify-center p-6 text-zinc-500">Loading…</div>}>
      <GpsDataContent />
    </Suspense>
  );
}

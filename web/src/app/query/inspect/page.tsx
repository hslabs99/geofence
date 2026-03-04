'use client';

import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import { formatColumnLabel, formatDateNZ, computeColumnWidths } from '@/lib/utils';
import { addMinutesToTimestamp, runFetchStepsForJobs } from '@/lib/fetch-steps';

type Row = Record<string, unknown>;

// Inspect uses its own column-order and sort settings (branch from Vwork for independent development)
const COLUMN_ORDER_TABLE = 'tbl_vworkjobs_inspect';
const SORT_SETTING_TYPE = 'System';
const SORT_SETTING_NAME = 'Inspectsort';

const PRIORITY_COLUMNS = ['job_id', 'planned_start_time', 'worker', 'truck_id', 'truck_rego'];

const DATE_COLUMNS = new Set([
  'planned_start_time', 'actual_start_time', 'actual_end_time', 'gps_start_time', 'gps_end_time',
  'step_1_completed_at', 'step_2_completed_at', 'step_3_completed_at', 'step_4_completed_at',
]);

function isIsoDateString(v: unknown): v is string {
  return typeof v === 'string' && /^\d{4}-\d{2}-\d{2}/.test(v);
}

/** Normalize timestamp to YYYY-MM-DD HH:mm:ss for comparison (step 5 rule: only use GPS when < VWork). */
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

function InspectContent() {
  const searchParams = useSearchParams();
  const jobIdParam = searchParams.get('jobId');
  const locateJobIdParam = searchParams.get('locateJobId');
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [errorDetail, setErrorDetail] = useState<{ hint?: string; code?: string } | null>(null);
  const [sortColumns, setSortColumns] = useState<[string, string, string]>(['', '', '']);
  const sortColumnsInitialized = useRef(false);
  const [dateFilterCol, setDateFilterCol] = useState<string>('');
  const [dateFrom, setDateFrom] = useState<string>('');
  const [dateTo, setDateTo] = useState<string>('');
  const [filterTruckId, setFilterTruckId] = useState<string>('');
  const [filterJobId, setFilterJobId] = useState<string>('');
  const [filterPlannedFrom, setFilterPlannedFrom] = useState<string>('');
  const [filterPlannedTo, setFilterPlannedTo] = useState<string>('');
  const [filterActualFrom, setFilterActualFrom] = useState<string>('');
  const [filterActualTo, setFilterActualTo] = useState<string>('');
  const [columnOrder, setColumnOrder] = useState<string[]>([]);
  const [hiddenColumns, setHiddenColumns] = useState<Set<string>>(new Set());
  const [showColumnConfig, setShowColumnConfig] = useState(false);
  const columnOrderInitialized = useRef(false);
  const [selectedRowIndex, setSelectedRowIndex] = useState(0);
  const selectedRowRef = useRef<HTMLTableRowElement>(null);
  const [gpsMappings, setGpsMappings] = useState<{ type: string; vwname: string; gpsname: string }[]>([]);
  const [trackingRows, setTrackingRows] = useState<Row[]>([]);
  const [trackingSql, setTrackingSql] = useState<string>('');
  const [trackingLoading, setTrackingLoading] = useState(false);
  const [trackingSortCol, setTrackingSortCol] = useState<string>('position_time_nz');
  const [trackingSortDir, setTrackingSortDir] = useState<'asc' | 'desc'>('asc');
  const [trackingPage, setTrackingPage] = useState(1);
  const [trackingPageSize, setTrackingPageSize] = useState(50);
  const [trackingTotal, setTrackingTotal] = useState(0);
  const [trackingTableView, setTrackingTableView] = useState<'raw' | 'entry_exit'>('entry_exit');
  const [startLessMinutes, setStartLessMinutes] = useState(10);
  const [endPlusMinutes, setEndPlusMinutes] = useState(60);
  const [derivedStepsDebug, setDerivedStepsDebug] = useState<Record<string, unknown> | null>(null);
  const [showDerivedStepsDebug, setShowDerivedStepsDebug] = useState(false);
  const [refetchStepsRunning, setRefetchStepsRunning] = useState(false);
  const apiColumns = useMemo(
    () => (rows.length > 0 ? Object.keys(rows[0]) : []),
    [rows],
  );

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
        setSortColumns(['', '', '']);
      })
      .catch(() => setSortColumns(['', '', '']));
  }, [apiColumns.length, apiColumns]);

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

  const saveColumnConfig = (order: string[], hidden: Set<string>) => {
    const orderToSave = order.length ? order : allColumns;
    if (!orderToSave.length) return;
    fetch('/api/column-order', {
      method: 'PUT',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        table: COLUMN_ORDER_TABLE,
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

  useEffect(() => {
    fetch('/api/gpsmappings')
      .then((r) => r.json())
      .then((data) => setGpsMappings(data?.rows ?? []))
      .catch(() => setGpsMappings([]));
  }, []);

  useEffect(() => {
    fetch('/api/vworkjobs')
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
        setError(null);
        setErrorDetail(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const refetchVworkJobs = useCallback((): Promise<void> => {
    return fetch('/api/vworkjobs')
      .then((res) => res.json().then((data) => ({ ok: res.ok, data })))
      .then(({ ok, data }) => {
        if (ok && data?.rows) setRows(data.rows);
      })
      .catch(() => {});
  }, []);

  const distinctTruckIds = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      const v = row.truck_id;
      if (v != null && v !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows]);

  const distinctJobIds = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      const v = row.job_id;
      if (v != null && v !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows]);

  const filteredRows = useMemo(() => {
    return rows.filter((row) => {
      if (filterTruckId) {
        const v = row.truck_id;
        if (v == null || String(v).trim() !== filterTruckId) return false;
      }
      if (filterJobId.trim()) {
        const v = row.job_id;
        const s = v != null ? String(v).trim().toLowerCase() : '';
        const q = filterJobId.trim().toLowerCase();
        if (!s.includes(q)) return false;
      }
      if (filterPlannedFrom || filterPlannedTo) {
        const v = row.planned_start_time;
        if (v == null || v === '') return false;
        const str = String(v).trim();
        const datePart = str.match(/^(\d{4})-(\d{2})-(\d{2})/)?.[0] ?? '';
        if (!datePart) return false;
        const from = filterPlannedFrom ? filterPlannedFrom.slice(0, 10) : '';
        const to = filterPlannedTo ? filterPlannedTo.slice(0, 10) : '';
        if (from && datePart < from) return false;
        if (to && datePart > to) return false;
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
      if (dateFilterCol && (dateFrom || dateTo)) {
        const v = row[dateFilterCol];
        if (v == null || v === '') return false;
        const str = String(v).trim();
        const datePart = str.match(/^(\d{4})-(\d{2})-(\d{2})/)?.[0] ?? '';
        if (!datePart) return false;
        if (dateFrom && datePart < dateFrom.slice(0, 10)) return false;
        if (dateTo && datePart > dateTo.slice(0, 10)) return false;
      }
      return true;
    });
  }, [rows, filterTruckId, filterJobId, filterPlannedFrom, filterPlannedTo, filterActualFrom, filterActualTo, dateFilterCol, dateFrom, dateTo]);

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

  const selectedRow = sortedRows[Math.min(selectedRowIndex, Math.max(0, sortedRows.length - 1))] ?? sortedRows[0] ?? null;

  /** Refetch steps for the selected job only (force: ignores steps_fetched). Same runFetchStepsForJobs as API GPS Import. */
  const refetchStepsForSelectedJob = useCallback(async () => {
    if (!selectedRow) return;
    setRefetchStepsRunning(true);
    setDerivedStepsDebug(null);
    try {
      const result = await runFetchStepsForJobs({
        jobs: [selectedRow],
        startLessMinutes,
        endPlusMinutes,
      });
      await refetchVworkJobs();
      if (result.lastResult != null && typeof result.lastResult === 'object') {
        const d = (result.lastResult as { debug?: Record<string, unknown> }).debug;
        setDerivedStepsDebug(d != null ? { ...d, _singleJob: true } : { _singleJob: true, fullResponse: result.lastResult });
        setShowDerivedStepsDebug(true);
      }
    } finally {
      setRefetchStepsRunning(false);
    }
  }, [selectedRow, startLessMinutes, endPlusMinutes, refetchVworkJobs]);

  /** Apply URL params (from Summary link): truck, actual from/to. If locateJobId, do not set job filter so list shows jobs before/after. */
  useEffect(() => {
    const truck = searchParams.get('truckId') ?? '';
    const actualFrom = searchParams.get('actualFrom') ?? '';
    const actualTo = searchParams.get('actualTo') ?? '';
    const jobId = searchParams.get('jobId') ?? '';
    const locateJobId = searchParams.get('locateJobId') ?? '';
    if (truck) setFilterTruckId(truck);
    if (actualFrom) setFilterActualFrom(actualFrom);
    if (actualTo) setFilterActualTo(actualTo);
    if (jobId && !locateJobId) setFilterJobId(jobId);
  }, [searchParams]);

  const paramToLocate = locateJobIdParam ?? jobIdParam;
  useEffect(() => {
    if (!paramToLocate || sortedRows.length === 0) return;
    const idx = sortedRows.findIndex((r) => String(r.job_id ?? '') === paramToLocate.trim());
    if (idx >= 0) setSelectedRowIndex(idx);
  }, [paramToLocate, sortedRows]);

  useEffect(() => {
    if (!locateJobIdParam || selectedRowIndex < 0) return;
    selectedRowRef.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }, [locateJobIdParam, selectedRowIndex]);

  const STEP_ROWS = [
    { nameKey: 'step_1_name', completedKey: 'step_1_completed_at', actualTimeKey: 'step_1_actual_time' },
    { nameKey: 'step_2_name', completedKey: 'step_2_completed_at', actualTimeKey: 'step_2_actual_time' },
    { nameKey: 'step_3_name', completedKey: 'step_3_completed_at', actualTimeKey: 'step_3_actual_time' },
    { nameKey: 'step_4_name', completedKey: 'step_4_completed_at', actualTimeKey: 'step_4_actual_time' },
    { nameKey: 'step_5_name', completedKey: 'step_5_completed_at', actualTimeKey: 'step_5_actual_time' },
  ] as const;

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

  useEffect(() => {
    if (!deviceForTracking || !actualStartTime.trim()) {
      setTrackingRows([]);
      setTrackingSql('');
      setTrackingTotal(0);
      return;
    }
    setTrackingLoading(true);
    const offset = (trackingPage - 1) * trackingPageSize;
    const positionAfter = addMinutesToTimestamp(actualStartTime.trim(), -startLessMinutes);
    const params = new URLSearchParams({
      device: deviceForTracking,
      positionAfter,
      limit: String(trackingPageSize),
      offset: String(offset),
    });
    if (actualEndTime.trim()) {
      params.set('positionBefore', addMinutesToTimestamp(actualEndTime.trim(), endPlusMinutes));
    }
    if (trackingTableView === 'entry_exit') {
      params.set('geofenceFilter', 'entry_or_exit');
    }
    fetch(`/api/tracking?${params}`)
      .then((r) => r.json())
      .then((data) => {
        setTrackingRows(data?.rows ?? []);
        setTrackingSql(data?.sql ?? '');
        setTrackingTotal(typeof data?.total === 'number' ? data.total : 0);
      })
      .catch(() => {
        setTrackingRows([]);
        setTrackingSql('');
        setTrackingTotal(0);
      })
      .finally(() => setTrackingLoading(false));
  }, [deviceForTracking, actualStartTime, actualEndTime, trackingPage, trackingPageSize, trackingTableView, startLessMinutes, endPlusMinutes]);

  useEffect(() => {
    setTrackingPage(1);
  }, [deviceForTracking, actualStartTime, actualEndTime, trackingTableView, startLessMinutes, endPlusMinutes]);

  const relevantMappings = useMemo(() => {
    const out: { type: string; vwname: string; gpsname: string }[] = [];
    for (const m of gpsMappings) {
      const vw = (m.vwname ?? '').trim();
      const gps = (m.gpsname ?? '').trim();
      if (m.type === 'Vineyard' && vineyardName && (vw === vineyardName || gps === vineyardName)) {
        out.push(m);
      } else if ((m.type === 'Winery') && deliveryWinery && (vw === deliveryWinery || gps === deliveryWinery)) {
        out.push(m);
      }
    }
    return out;
  }, [gpsMappings, vineyardName, deliveryWinery]);

  /** SQL WHERE fragment (text only, for later use): fence_name IN ('mapA','mapB',...) from all relevant mappings. */
  const mappingFenceNameInClause = useMemo(() => {
    const names = relevantMappings.map((m) => (m.gpsname ?? '').trim()).filter(Boolean);
    if (names.length === 0) return '';
    const escaped = names.map((n) => `'${String(n).replace(/'/g, "''")}'`);
    return `fence_name IN (${escaped.join(', ')})`;
  }, [relevantMappings]);

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
  };

  const formatCell = useCallback((v: unknown): string => {
    if (v == null || v === '') return '—';
    if (isIsoDateString(v)) return formatDateNZ(v);
    return String(v);
  }, []);

  /** GPS table: format enter_time, outer_time as dd/mm/yy hh:mm:ss (same as vwork grid), no time adjustment. */
  const formatGpsCell = useCallback((v: unknown): string => {
    if (v == null || v === '') return '—';
    if (isIsoDateString(v)) return formatDateNZ(String(v));
    return String(v);
  }, []);

  const TRACKING_DATE_COLS = new Set(['position_time', 'position_time_nz']);
  const TRACKING_DISPLAY_COLUMNS = ['device_name', 'fence_name', 'geofence_type', 'position_time_nz', 'position_time'] as const;
  const sortedTrackingRows = useMemo(() => {
    const cols = trackingSortCol ? [trackingSortCol] : [];
    if (cols.length === 0) return trackingRows;
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
      const c = compareTracking(r1[trackingSortCol], r2[trackingSortCol], trackingSortCol);
      return trackingSortDir === 'asc' ? c : -c;
    });
    return out;
  }, [trackingRows, trackingSortCol, trackingSortDir]);

  const columnWidths = useMemo(
    () => computeColumnWidths(columns, sortedRows, formatCell, formatColumnLabel),
    [columns, sortedRows],
  );

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-4 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Inspect Data Vwork&gt;GPS</h1>
      <p className="mb-4 text-sm text-zinc-500">Jobs grid (branched from Vwork — will evolve into merged jobs + GPS view)</p>
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
          </div>
          <datalist id="filter-job-id-list">
            {distinctJobIds.map((id) => (
              <option key={id} value={id} />
            ))}
          </datalist>
          <div className="max-h-[28rem] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
            <table className="w-max table-fixed text-left text-sm">
              <colgroup>
                {columns.map((col) => (
                  <col key={col} style={{ width: columnWidths[col], minWidth: columnWidths[col] }} />
                ))}
              </colgroup>
              <thead className="sticky top-0 z-10">
                <tr className="border-b border-zinc-200 bg-zinc-50 dark:border-zinc-800 dark:bg-zinc-900/50">
                  {columns.map((col) => (
                    <th key={`filter-${col}`} className="px-1 py-1.5 align-bottom">
                      {col === 'truck_id' && (
                        <select
                          value={filterTruckId}
                          onChange={(e) => setFilterTruckId(e.target.value)}
                          onClick={(e) => e.stopPropagation()}
                          className="w-full max-w-full rounded border border-zinc-300 bg-white px-1.5 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                          title="Filter by Truck ID"
                        >
                          <option value="">— All —</option>
                          {distinctTruckIds.map((id) => (
                            <option key={id} value={id}>{id}</option>
                          ))}
                        </select>
                      )}
                      {col === 'job_id' && (
                        <input
                          type="text"
                          value={filterJobId}
                          onChange={(e) => setFilterJobId(e.target.value)}
                          onClick={(e) => e.stopPropagation()}
                          list="filter-job-id-list"
                          placeholder="Filter…"
                          className="w-full min-w-0 rounded border border-zinc-300 bg-white px-1.5 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                          title="Filter by Job ID (autofill)"
                        />
                      )}
                      {col === 'planned_start_time' && (
                        <div className="flex flex-col gap-0.5">
                          <input
                            type="date"
                            value={filterPlannedFrom}
                            onChange={(e) => setFilterPlannedFrom(e.target.value)}
                            onClick={(e) => e.stopPropagation()}
                            placeholder="From"
                            className="w-full rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                            title="Planned start from"
                          />
                          <input
                            type="date"
                            value={filterPlannedTo}
                            onChange={(e) => setFilterPlannedTo(e.target.value)}
                            onClick={(e) => e.stopPropagation()}
                            placeholder="To"
                            className="w-full rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                            title="Planned start to"
                          />
                        </div>
                      )}
                      {col === 'actual_start_time' && (
                        <div className="flex flex-col gap-0.5">
                          <input
                            type="date"
                            value={filterActualFrom}
                            onChange={(e) => setFilterActualFrom(e.target.value)}
                            onClick={(e) => e.stopPropagation()}
                            placeholder="From"
                            className="w-full rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                            title="Actual start from"
                          />
                          <input
                            type="date"
                            value={filterActualTo}
                            onChange={(e) => setFilterActualTo(e.target.value)}
                            onClick={(e) => e.stopPropagation()}
                            placeholder="To"
                            className="w-full rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                            title="Actual start to"
                          />
                        </div>
                      )}
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
                      title="Drag to reorder · Use Sort dropdowns above"
                    >
                      {formatColumnLabel(col)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row, i) => (
                  <tr
                    key={i}
                    ref={i === selectedRowIndex ? selectedRowRef : undefined}
                    onClick={() => setSelectedRowIndex(i)}
                    className={`cursor-pointer border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50 ${selectedRowIndex === i ? 'bg-blue-100 dark:bg-blue-900/40' : ''}`}
                  >
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
          <p className="mt-3 text-zinc-500">
            {sortedRows.length} row{sortedRows.length !== 1 ? 's' : ''}
            {(filterTruckId || filterJobId.trim() || filterPlannedFrom || filterPlannedTo || filterActualFrom || filterActualTo || dateFilterCol) && ` (filtered from ${rows.length})`} · max 500 from API · click row to select
          </p>
          <section className="mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
              <h2 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">Step details</h2>
              <button
                type="button"
                onClick={() => void refetchStepsForSelectedJob()}
                disabled={!selectedRow || !actualStartTime?.trim() || refetchStepsRunning}
                className="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50 dark:bg-blue-700 dark:hover:bg-blue-600 dark:disabled:opacity-50"
                title="Refetch GPS steps for this job (force: runs even if steps_fetched=true). Same logic as API GPS Import Step 4."
              >
                {refetchStepsRunning ? 'Refetching…' : 'Refetch steps (this job)'}
              </button>
            </div>
            <table className="w-full min-w-[48rem] text-left text-sm table-fixed">
              <colgroup>
                <col style={{ width: '14rem' }} /><col style={{ width: '12rem' }} /><col style={{ width: '6rem' }} /><col style={{ width: '6rem' }} /><col style={{ width: '12rem' }} /><col style={{ width: '5rem' }} />
              </colgroup>
              <thead>
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  <th colSpan={2} className="px-2 py-1.5 font-medium text-zinc-500 text-center">
                    Vwork Data
                  </th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">GPS Step Data</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">GPS Row</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Final Step Data</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Details</th>
                </tr>
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Step Name</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">Step Completed</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">—</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">—</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">—</th>
                  <th className="px-2 py-1.5 font-medium text-zinc-500">—</th>
                </tr>
              </thead>
              <tbody>
                {STEP_ROWS.map(({ nameKey, completedKey, actualTimeKey }, i) => {
                  const n = i + 1;
                  const gpsCompletedKey = `step_${n}_gps_completed_at` as keyof Row;
                  const gpsCompletedKeyAlt = `Step_${n}_GPS_completed_at` as keyof Row;
                  const gpsIdKey = `step${n}_gps_id` as keyof Row;
                  const gpsIdKeyAlt = `Step${n}_gps_id` as keyof Row;
                  const gpsStepValue = selectedRow?.[gpsCompletedKey] ?? selectedRow?.[gpsCompletedKeyAlt];
                  const gpsStepDisplay = gpsStepValue != null && gpsStepValue !== '' ? formatDateNZ(String(gpsStepValue)) : '—';
                  const gpsRowId = selectedRow?.[gpsIdKey] ?? selectedRow?.[gpsIdKeyAlt];
                  const gpsRowDisplay = gpsRowId != null && gpsRowId !== '' ? String(gpsRowId) : '—';
                  let finalValue: unknown = selectedRow?.[actualTimeKey] ?? selectedRow?.[completedKey];
                  if (n === 5 && selectedRow) {
                    const vworkStep5 = selectedRow.step_5_completed_at ?? selectedRow.Step_5_completed_at;
                    if (gpsStepValue != null && gpsStepValue !== '' && vworkStep5 != null && vworkStep5 !== '') {
                      const gpsNorm = normalizeForCompare(gpsStepValue);
                      const vworkNorm = normalizeForCompare(vworkStep5);
                      if (gpsNorm != null && vworkNorm != null && gpsNorm >= vworkNorm) {
                        finalValue = vworkStep5;
                      }
                    }
                  }
                  const finalDisplay = finalValue != null && finalValue !== '' ? formatCell(finalValue) : '—';
                  return (
                  <tr key={i} className="border-b border-zinc-100 dark:border-zinc-800">
                    <td className="whitespace-nowrap px-2 py-1.5 text-zinc-700 dark:text-zinc-300">
                      {selectedRow ? formatCell(selectedRow[nameKey]) : '—'}
                    </td>
                    <td className="whitespace-nowrap px-2 py-1.5 text-zinc-700 dark:text-zinc-300">
                      {selectedRow ? formatCell(selectedRow[completedKey]) : '—'}
                    </td>
                    <td className="whitespace-nowrap px-2 py-1.5 text-zinc-400 dark:text-zinc-500">{gpsStepDisplay}</td>
                    <td className="whitespace-nowrap px-2 py-1.5 text-zinc-400 dark:text-zinc-500 font-mono text-xs" title={gpsRowId != null && gpsRowId !== '' ? `tbl_tracking.id = ${gpsRowId}` : undefined}>{gpsRowDisplay}</td>
                    <td className="whitespace-nowrap px-2 py-1.5 text-zinc-700 dark:text-zinc-300">
                      {finalDisplay}
                    </td>
                    <td className="whitespace-nowrap px-2 py-1.5 text-zinc-400 dark:text-zinc-500">—</td>
                  </tr>
                );})}
              </tbody>
            </table>
            <div className="mt-4 rounded border border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-800/50">
              <div className="border-b border-zinc-200 px-3 py-2 text-sm font-medium text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                Log (fence IDs, mappings, tracking lookups)
              </div>
              {derivedStepsDebug == null ? (
                <p className="px-3 py-4 text-sm text-zinc-500 dark:text-zinc-400">
                  Use &quot;Refetch steps (this job)&quot; to re-run GPS steps for the selected job (force: runs even if already fetched). For batch by date, use Admin → API GPS Import → Step 4.
                </p>
              ) : (
                <>
                  <button
                    type="button"
                    onClick={() => setShowDerivedStepsDebug((v) => !v)}
                    className="flex w-full items-center justify-between px-3 py-1.5 text-left text-xs font-medium text-zinc-600 dark:text-zinc-400"
                  >
                    <span>{showDerivedStepsDebug ? 'Hide' : 'Show'} full log</span>
                    <span>{showDerivedStepsDebug ? '▼' : '▶'}</span>
                  </button>
                  {showDerivedStepsDebug && (
                    <pre className="max-h-[420px] overflow-auto whitespace-pre-wrap break-all px-3 py-2 font-mono text-xs text-zinc-800 dark:text-zinc-200">
                      {JSON.stringify(derivedStepsDebug, null, 2)}
                    </pre>
                  )}
                </>
              )}
            </div>
          </section>
          <section className="mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
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
                    <tr className="border-b border-zinc-100 dark:border-zinc-800">
                      <td className="px-2 py-1.5 font-medium text-zinc-600 dark:text-zinc-400">Vineyard</td>
                      <td className="px-2 py-1.5 text-zinc-700 dark:text-zinc-300">{vineyardName || '—'}</td>
                      <td className="px-2 py-1.5 text-zinc-700 dark:text-zinc-300">
                        {relevantMappings.find((m) => m.type === 'Vineyard')?.gpsname || '—'}
                      </td>
                      <td className="px-2 py-1.5 font-medium text-zinc-800 dark:text-zinc-200">
                        {relevantMappings.find((m) => m.type === 'Vineyard')?.gpsname || vineyardName || '—'}
                      </td>
                    </tr>
                    <tr className="border-b border-zinc-100 dark:border-zinc-800">
                      <td className="px-2 py-1.5 font-medium text-zinc-600 dark:text-zinc-400">Winery</td>
                      <td className="px-2 py-1.5 text-zinc-700 dark:text-zinc-300">{deliveryWinery || '—'}</td>
                      <td className="px-2 py-1.5 text-zinc-700 dark:text-zinc-300">
                        {relevantMappings.find((m) => m.type === 'Winery')?.gpsname || '—'}
                      </td>
                      <td className="px-2 py-1.5 font-medium text-zinc-800 dark:text-zinc-200">
                        {relevantMappings.find((m) => m.type === 'Winery')?.gpsname || deliveryWinery || '—'}
                      </td>
                    </tr>
                  </tbody>
                </table>
            {mappingFenceNameInClause && (
              <p className="mt-3 text-xs text-zinc-500">
                For later (tbl_tracking / fence filter): <code className="select-all rounded bg-zinc-100 px-1 py-0.5 dark:bg-zinc-800">{mappingFenceNameInClause}</code>
              </p>
            )}
          </section>
          <section className="mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <div className="mb-3 flex flex-wrap items-center gap-4">
              <h2 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">tbl_tracking</h2>
              <span className="text-sm text-zinc-500">
                Window: start −{startLessMinutes} min, end +{endPlusMinutes} min (Admin → Settings)
              </span>
            </div>
            <p className="mb-2 text-sm text-zinc-500">
              position_time_nz &gt; actual_start_time − {startLessMinutes} min AND position_time_nz &lt; actual_end_time + {endPlusMinutes} min
            </p>
            {trackingLoading && <p className="text-sm text-zinc-500">Loading…</p>}
            {!trackingLoading && !deviceForTracking && <p className="text-sm text-zinc-500">Select a job with worker (device for GPS) to load tracking data.</p>}
            {!trackingLoading && deviceForTracking && !actualStartTime.trim() && <p className="text-sm text-zinc-500">Job has no actual_start_time.</p>}
            {!trackingLoading && deviceForTracking && actualStartTime.trim() && trackingRows.length === 0 && (
              <p className="text-sm text-zinc-500">No tracking rows for device={deviceForTracking} in window (actual_start − {startLessMinutes} min to actual_end + {endPlusMinutes} min).</p>
            )}
                {!trackingLoading && (trackingRows.length > 0 || trackingTotal > 0) && (
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
                          {TRACKING_DISPLAY_COLUMNS.map((col) => (
                            <option key={col} value={col}>{formatColumnLabel(col)}</option>
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
                    <div className="max-h-[1200px] min-h-[600px] overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
                      <table className="min-w-full text-left text-sm">
                        <thead className="sticky top-0 z-10 bg-zinc-100 dark:bg-zinc-800">
                          <tr>
                            {TRACKING_DISPLAY_COLUMNS.map((col) => (
                              <th key={col} className="whitespace-nowrap px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">
                                {formatColumnLabel(col)}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {sortedTrackingRows.map((row, i) => (
                            <tr key={i} className="border-t border-zinc-100 dark:border-zinc-800">
                              {TRACKING_DISPLAY_COLUMNS.map((col) => (
                                <td key={col} className="whitespace-nowrap px-2 py-1.5 text-zinc-600 dark:text-zinc-400">
                                  {formatGpsCell(row[col])}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
            {deviceForTracking && actualStartTime.trim() && (
              <pre className="mt-3 select-all rounded border border-zinc-200 bg-zinc-50 p-3 font-mono text-xs text-zinc-800 dark:border-zinc-600 dark:bg-zinc-800/50 dark:text-zinc-200" title="Copy and paste to run in your SQL editor">
                {trackingSql || (() => {
                const after = addMinutesToTimestamp(actualStartTime.trim(), -startLessMinutes);
                const before = actualEndTime.trim() ? addMinutesToTimestamp(actualEndTime.trim(), endPlusMinutes) : null;
                let where = `t.device_name='${String(deviceForTracking).replace(/'/g, "''")}' AND t.position_time_nz>'${after.replace(/'/g, "''")}'`;
                if (before) where += ` AND t.position_time_nz<'${before.replace(/'/g, "''")}'`;
                return `SELECT t.id, t.device_name, g.fence_name, t.geofence_type, t.position_time_nz, t.position_time FROM tbl_tracking t LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id WHERE ${where} ORDER BY t.position_time_nz LIMIT 500`;
              })()}
              </pre>
            )}
          </section>
        </>
      )}
    </div>
  );
}

export default function InspectPage() {
  return (
    <Suspense fallback={<div className="flex min-h-[200px] items-center justify-center p-6 text-zinc-500">Loading…</div>}>
      <InspectContent />
    </Suspense>
  );
}

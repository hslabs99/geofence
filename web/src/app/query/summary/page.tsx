'use client';

import React, { useEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import { useViewMode } from '@/contexts/ViewModeContext';

type Row = Record<string, unknown>;

const SORT_SETTING_TYPE = 'System';
const SORT_SETTING_NAME = 'SummarySort';

const LEAD_COLUMNS = [
  { key: 'Customer', label: 'Customer' },
  { key: 'job_id', label: 'Job ID' },
  { key: 'worker', label: 'Worker' },
  { key: 'delivery_winery', label: 'Winery' },
  { key: 'vineyard_name', label: 'Vineyard' },
] as const;

const STEP_NUMS = [1, 2, 3, 4, 5] as const;

const STEP_LABELS: Record<number, string> = {
  1: '1. Start',
  2: '2. Arrive',
  3: '3. Leave',
  4: '4. Winery',
  5: '5. End',
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

function compare(a: unknown, b: unknown): number {
  const va = a == null ? '' : a;
  const vb = b == null ? '' : b;
  if (va === vb) return 0;
  if (typeof va === 'number' && typeof vb === 'number') return va - vb;
  return String(va).localeCompare(String(vb));
}

/** Resolve sort value for a row by key (handles mins_*, travel, total, Customer). */
function sortValueFor(row: Row, key: string): unknown {
  const minsMatch = key.match(/^mins_(\d+)$/);
  if (minsMatch) return minsBetween(row, parseInt(minsMatch[1], 10) - 1, parseInt(minsMatch[1], 10));
  if (key === 'travel') return travelMins(row);
  if (key === 'in_vineyard') return minsBetween(row, 2, 3);
  if (key === 'in_winery') return minsBetween(row, 4, 5);
  if (key === 'total') return totalMins(row);
  if (key === 'Customer') return row['Customer'] ?? row['customer'];
  return row[key];
}

/** Build Inspect URL to locate this job in the list (date range ±1 day, truck) so jobs before/after are visible. */
function inspectUrlForRow(row: Row): string {
  const jobId = row.job_id != null ? String(row.job_id).trim() : '';
  const truckId = row.truck_id != null ? String(row.truck_id).trim() : '';
  const actualStartRaw =
    row.actual_start_time ?? row.step_1_actual_time ?? row.step_1_gps_completed_at ?? row.step_1_completed_at ?? '';
  const actualStart = actualStartRaw ? String(actualStartRaw).trim() : '';
  const params = new URLSearchParams();
  if (jobId) params.set('locateJobId', jobId);
  if (truckId) params.set('truckId', truckId);
  if (actualStart) {
    const d = new Date(actualStart.includes('T') ? actualStart : actualStart.replace(' ', 'T'));
    if (!Number.isNaN(d.getTime())) {
      const pad = (n: number) => String(n).padStart(2, '0');
      const from = new Date(d);
      from.setDate(from.getDate() - 1);
      params.set('actualFrom', `${from.getFullYear()}-${pad(from.getMonth() + 1)}-${pad(from.getDate())}`);
      const to = new Date(d);
      to.setDate(to.getDate() + 1);
      params.set('actualTo', `${to.getFullYear()}-${pad(to.getMonth() + 1)}-${pad(to.getDate())}`);
    }
  }
  return `/query/inspect?${params.toString()}`;
}

export default function SummaryPage() {
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filterActualFrom, setFilterActualFrom] = useState('');
  const [filterActualTo, setFilterActualTo] = useState('');
  const [filterCustomer, setFilterCustomer] = useState('');
  const [filterTemplate, setFilterTemplate] = useState('');
  const [filterTruckId, setFilterTruckId] = useState('');
  const [filterWorker, setFilterWorker] = useState('');
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [sortColumns, setSortColumns] = useState<[string, string, string]>(['', '', '']);
  const sortColumnsInitialized = useRef(false);
  const [sortSaveStatus, setSortSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const { viewMode, clientCustomer } = useViewMode();
  /** In client view, customer filter is locked to menu value; otherwise use local filter. */
  const effectiveCustomer = viewMode === 'client' ? clientCustomer : filterCustomer;
  const [summaryTab, setSummaryTab] = useState<'by_job' | 'by_day'>('by_job');
  const [minsThresholds, setMinsThresholds] = useState<Record<string, string>>({
    '2': '', '3': '', '4': '', '5': '', travel: '', in_vineyard: '', in_winery: '', total: '',
  });
  const setMinsThreshold = (key: string, value: string) => {
    setMinsThresholds((prev) => ({ ...prev, [key]: value }));
  };

  useEffect(() => {
    fetch('/api/vworkjobs')
      .then(async (res) => {
        const data = await res.json();
        if (!res.ok) throw new Error(data?.error ?? res.statusText);
        return data;
      })
      .then((data) => {
        setRows(data.rows ?? []);
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const distinctTruckIds = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      const v = row.truck_id;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows]);

  const distinctWorkers = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      const v = row.worker;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows]);

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

  /** Clear template when customer changes so we don't keep a template from another customer. */
  useEffect(() => {
    setFilterTemplate('');
  }, [effectiveCustomer]);

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
      if (filterTruckId) {
        const v = row.truck_id;
        if (v == null || String(v).trim() !== filterTruckId) return false;
      }
      if (filterWorker.trim()) {
        const v = row.worker;
        const s = v != null ? String(v).trim().toLowerCase() : '';
        if (!s.includes(filterWorker.trim().toLowerCase())) return false;
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
  }, [rows, effectiveCustomer, filterTemplate, filterTruckId, filterWorker, filterActualFrom, filterActualTo]);

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

  /** By Day: one row per day; each mins column has Total, Max, Min, Av for that day's jobs. */
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
    const out: {
      date: string; dateLabel: string; jobs: Row[];
      mins_2: MinsQuad; mins_3: MinsQuad; mins_4: MinsQuad; mins_5: MinsQuad; travel: MinsQuad; total: MinsQuad;
    }[] = [];
    const [minY, minMo, minDay] = min.split('-').map(Number);
    const [maxY, maxMo, maxDay] = max.split('-').map(Number);
    const minT = new Date(minY, minMo - 1, minDay).getTime();
    const maxT = new Date(maxY, maxMo - 1, maxDay).getTime();
    for (let t = minT; t <= maxT; t += 86400000) {
      const d = new Date(t);
      const y = d.getFullYear();
      const mo = String(d.getMonth() + 1).padStart(2, '0');
      const day = String(d.getDate()).padStart(2, '0');
      const dateStr = `${y}-${mo}-${day}`;
      const dateLabel = `${day}/${mo}/${String(y).slice(2)}`;
      const jobs = filteredRows.filter((row) => getRowDate(row) === dateStr);
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
    return out;
  }, [filteredRows, dayRange]);

  /** By Day footer: Total, Max, Min, Av across all days (from each day's .total, .max, .min, .av). */
  const byDayStats = useMemo(() => {
    const keys = ['mins_2', 'mins_3', 'mins_4', 'mins_5', 'travel', 'total'] as const;
    const stats: Record<string, MinsQuad> = {};
    for (const k of keys) {
      const quads = rowsByDay.map((r) => r[k]);
      const sumTotal = quads.reduce((a, q) => a + q.total, 0);
      const sumAv = quads.reduce((a, q) => a + q.av, 0);
      const n = rowsByDay.length;
      stats[k] = {
        total: sumTotal,
        max: n ? Math.max(...quads.map((q) => q.max)) : 0,
        min: n ? Math.min(...quads.map((q) => q.min)) : 0,
        av: n ? Math.round(sumAv / n) : 0,
      };
    }
    return stats;
  }, [rowsByDay]);

  const sortedRows = useMemo(() => {
    const out = [...filteredRows];
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
  }, [filteredRows, sortKey, sortDir, sortColumns]);

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

  const allColumns = useMemo(() => [...LEAD_COLUMNS, ...stepColumns, { key: 'travel', label: 'Travel' }, { key: 'in_vineyard', label: 'In Vineyard' }, { key: 'in_winery', label: 'In Winery' }, { key: 'total', label: 'Total' }], [stepColumns]);
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
    const n = parseInt(s, 10);
    return Number.isNaN(n) ? null : n;
  };
  const isMinsOverThreshold = (val: number | null, key: string): boolean => {
    const limit = getThresholdNum(key);
    return val != null && limit != null && val > limit;
  };

  const dayClientView = summaryTab === 'by_day' && viewMode === 'client';
  const jobClientView = summaryTab === 'by_job' && viewMode === 'client';

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-4">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Summary</h1>
        <p className="mt-1 text-sm text-zinc-500">Job list with single source of truth for step times (Step 1–5) and source (GPS or VW).</p>
      </div>
      {loading && <p className="text-zinc-600">Loading…</p>}
      {error && (
        <div className="rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          <p className="font-medium">{error}</p>
        </div>
      )}
      {!loading && !error && (
        <>
          <div className="mb-4 flex gap-2 border-b border-zinc-200 dark:border-zinc-700">
            <button
              type="button"
              onClick={() => setSummaryTab('by_day')}
              className={`rounded-t px-4 py-2 text-sm font-medium ${summaryTab === 'by_day' ? 'bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100' : 'bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750'}`}
            >
              Summary
            </button>
            <button
              type="button"
              onClick={() => setSummaryTab('by_job')}
              className={`rounded-t px-4 py-2 text-sm font-medium ${summaryTab === 'by_job' ? 'bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100' : 'bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750'}`}
            >
              By Job
            </button>
          </div>
          <div className="mb-4 flex flex-wrap items-end gap-4 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
            <div>
              <label className="mb-1 block text-xs font-medium text-zinc-500">Customer</label>
              <select
                value={viewMode === 'client' ? clientCustomer : filterCustomer}
                onChange={(e) => setFilterCustomer(e.target.value)}
                disabled={viewMode === 'client'}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
              >
                <option value="">— All —</option>
                {distinctCustomers.map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
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
              <input
                type="text"
                placeholder="Filter by worker"
                value={filterWorker}
                onChange={(e) => setFilterWorker(e.target.value)}
                className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
              />
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
          {summaryTab === 'by_day' && (
            <div className="max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
              <table className="w-full min-w-[64rem] text-left text-sm">
                <thead className="sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]">
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Date</th>
                    <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700">Jobs</th>
                    {!dayClientView && (
                      <>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[2]}`}>2. TO Vineyard Mins</th>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[3]}`}>3. In Vineyard Mins</th>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[4]}`}>4. TO Winery Mins</th>
                        <th colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[5]}`}>5. End Mins</th>
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
                      <td colSpan={dayClientView ? 10 : 34} className="px-3 py-6 text-center text-zinc-500">
                        No days {filteredRows.length === 0 ? '(no jobs match filters)' : '(no job dates in range)'}.
                      </td>
                    </tr>
                  ) : (
                    rowsByDay.map((r) => (
                      <tr key={r.date} className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50">
                        <td className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 dark:border-zinc-700">
                          <button
                            type="button"
                            onClick={() => {
                              setFilterActualFrom(r.date);
                              setFilterActualTo(r.date);
                              setSummaryTab('by_job');
                            }}
                            className="font-medium text-blue-600 underline hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
                          >
                            {r.dateLabel}
                          </button>
                        </td>
                        <td className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">{r.jobs.length}</td>
                        {!dayClientView && [r.mins_2, r.mins_3, r.mins_4, r.mins_5].map((q, i) =>
                          (['total', 'max', 'min', 'av'] as const).map((key) => (
                            <td key={`${i}-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[(i + 2) as 2 | 3 | 4 | 5]}`}>
                              {q[key]}
                            </td>
                          ))
                        )}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => (
                          <td key={`travel-${key}`} className="whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
                            {r.travel[key]}
                          </td>
                        ))}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => (
                          <td key={`in_vineyard-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>
                            {r.mins_3[key]}
                          </td>
                        ))}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => (
                          <td key={`in_winery-${key}`} className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>
                            {r.mins_5[key]}
                          </td>
                        ))}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => (
                          <td key={`total-${key}`} className="whitespace-nowrap border-zinc-200 px-2 py-2 tabular-nums font-medium text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                            {r.total[key]}
                          </td>
                        ))}
                      </tr>
                    ))
                  )}
                </tbody>
                {rowsByDay.length > 0 && (
                  <tfoot className="border-t-2 border-zinc-300 bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800">
                    {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((metric) => (
                      <tr key={metric} className="border-b border-zinc-200 dark:border-zinc-700">
                        <td className="border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
                          {metric === 'total' ? 'Total:' : 'Av:'}
                        </td>
                        <td className="border-r border-zinc-200 px-3 py-1.5 tabular-nums text-xs text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                          {metric === 'total' ? rowsByDay.reduce((a, r) => a + r.jobs.length, 0) : '—'}
                        </td>
                        {!dayClientView && [byDayStats.mins_2, byDayStats.mins_3, byDayStats.mins_4, byDayStats.mins_5].map((s, i) =>
                          (['total', 'max', 'min', 'av'] as const).map((key) => (
                            <td key={`${i}-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-xs text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[(i + 2) as 2 | 3 | 4 | 5]}`}>
                              {key === metric ? (metric === 'total' ? s.total : metric === 'max' ? s.max : metric === 'min' ? s.min : s.av) : '—'}
                            </td>
                          ))
                        )}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => (
                          <td key={`travel-${key}`} className="border-r border-zinc-200 px-2 py-1.5 tabular-nums text-xs text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
                            {key === metric ? (metric === 'total' ? byDayStats.travel.total : byDayStats.travel.av) : '—'}
                          </td>
                        ))}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => (
                          <td key={`in_vineyard-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-xs text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>
                            {key === metric ? (metric === 'total' ? byDayStats.mins_3.total : byDayStats.mins_3.av) : '—'}
                          </td>
                        ))}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => (
                          <td key={`in_winery-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 tabular-nums text-xs text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>
                            {key === metric ? (metric === 'total' ? byDayStats.mins_5.total : byDayStats.mins_5.av) : '—'}
                          </td>
                        ))}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => (
                          <td key={`total-${key}`} className="border-zinc-200 px-2 py-1.5 tabular-nums text-xs font-medium text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                            {key === metric ? (metric === 'total' ? byDayStats.total.total : byDayStats.total.av) : '—'}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tfoot>
                )}
              </table>
            </div>
          )}
          {summaryTab === 'by_job' && (
          <div className="max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
            <table className="w-full min-w-[64rem] text-left text-sm">
              <thead className="sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]">
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  <th colSpan={5} className="border-r border-zinc-200 px-2 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-700">
                    Limit (red if &gt;)
                  </th>
                  {!jobClientView && (
                    <>
                      <th colSpan={2} className={`border-r border-zinc-200 dark:border-zinc-700 ${STEP_GROUP_BG[1]}`} />
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
                  {LEAD_COLUMNS.map(({ key, label }) => (
                    <th
                      key={key}
                      rowSpan={jobClientView ? 1 : 2}
                      onClick={() => handleSort(key)}
                      className={`cursor-pointer select-none whitespace-nowrap border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === key ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                      title="Click to sort"
                    >
                      {label} {sortKey === key ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                    </th>
                  ))}
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
                    <td colSpan={jobClientView ? 9 : allColumns.length} className="px-3 py-6 text-center text-zinc-500">
                      No rows {filterActualFrom || filterActualTo || effectiveCustomer || filterTemplate || filterTruckId || filterWorker.trim() ? '(try relaxing filters)' : ''}.
                    </td>
                  </tr>
                ) : (
                  sortedRows.map((row, i) => (
                    <tr key={i} className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50">
                      {LEAD_COLUMNS.map(({ key }) => (
                        <td key={key} className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">
                          {key === 'job_id' ? (
                            <Link
                              href={inspectUrlForRow(row)}
                              className="text-blue-600 underline hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
                            >
                              {formatCell(row[key])}
                            </Link>
                          ) : key === 'Customer' ? (
                            formatCell(row['Customer'] ?? row['customer'])
                          ) : (
                            formatCell(row[key])
                          )}
                        </td>
                      ))}
                      {!jobClientView && STEP_NUMS.map((n) => {
                        const actualKey = `step_${n}_actual_time`;
                        const viaKey = `step_${n}_via`;
                        const actualVal = row[actualKey] ?? row[`step_${n}_gps_completed_at`] ?? row[`step_${n}_completed_at`];
                        const viaVal = row[viaKey];
                        const viaStr = formatCell(viaVal);
                        const isGps = viaStr.toLowerCase() === 'gps';
                        const mins = n >= 2 ? minsBetween(row, n - 1, n) : null;
                        const minsOver = n >= 2 && isMinsOverThreshold(mins, String(n));
                        const groupBg = STEP_GROUP_BG[n];
                        return (
                          <React.Fragment key={n}>
                            {n >= 2 && (
                              <td
                                className={`whitespace-nowrap px-3 py-2 tabular-nums ${groupBg} ${minsOver ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : 'text-zinc-500 dark:text-zinc-400'}`}
                              >
                                {mins != null ? String(mins) : '—'}
                              </td>
                            )}
                            <td className={`whitespace-nowrap px-3 py-2 text-zinc-600 dark:text-zinc-400 ${groupBg}`}>
                              {formatDateDDMM(actualVal)}
                            </td>
                            <td
                              className={`whitespace-nowrap px-3 py-2 font-medium ${groupBg} ${isGps ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-200' : 'text-zinc-700 dark:text-zinc-300'}`}
                            >
                              {viaStr}
                            </td>
                          </React.Fragment>
                        );
                      })}
                      {(function travelSummaryCells() {
                        const travel = travelMins(row);
                        const inVineyard = minsBetween(row, 2, 3);
                        const inWinery = minsBetween(row, 4, 5);
                        const total = totalMins(row);
                        return (
                          <React.Fragment>
                            <td
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isMinsOverThreshold(travel, 'travel') ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {travel != null ? String(travel) : '—'}
                            </td>
                            <td
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isMinsOverThreshold(inVineyard, 'in_vineyard') ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {inVineyard != null ? String(inVineyard) : '—'}
                            </td>
                            <td
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isMinsOverThreshold(inWinery, 'in_winery') ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {inWinery != null ? String(inWinery) : '—'}
                            </td>
                            <td
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isMinsOverThreshold(total, 'total') ? 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {total != null ? String(total) : '—'}
                            </td>
                          </React.Fragment>
                        );
                      })()}
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
          )}
          <p className="mt-3 text-zinc-500">
            {summaryTab === 'by_job'
              ? `${sortedRows.length} row${sortedRows.length !== 1 ? 's' : ''}${(filterActualFrom || filterActualTo || filterTruckId || filterWorker.trim()) ? ` (filtered from ${rows.length})` : ''} · max 500 from API`
              : `${rowsByDay.length} day${rowsByDay.length !== 1 ? 's' : ''} · ${filteredRows.length} job${filteredRows.length !== 1 ? 's' : ''}${(filterActualFrom || filterActualTo || filterTruckId || filterWorker.trim()) ? ` (filtered)` : ''}`}
          </p>
        </>
      )}
    </div>
  );
}

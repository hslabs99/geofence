'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { useViewMode } from '@/contexts/ViewModeContext';
import {
  buildDistancesReportRows,
  jobHasReportableDistanceKm,
  type DistancesReportFormat,
} from '@/lib/distances-report-rows';
import { formatIntNz, formatKmNz2 } from '@/lib/format-nz';

const THEAD_TH = '[&_th]:align-middle [&_th]:text-center';

type FilterOptions = {
  templates: string[];
  deliveryWineries: string[];
  vineyardNames: string[];
  truckIds: string[];
  workers: string[];
  trailermodes: string[];
};

const emptyOpts: FilterOptions = {
  templates: [],
  deliveryWineries: [],
  vineyardNames: [],
  truckIds: [],
  workers: [],
  trailermodes: [],
};

export default function DistancesPage() {
  const { viewMode, clientCustomer, clientCustomerLocked } = useViewMode();
  const isClient = viewMode === 'client';

  const [customers, setCustomers] = useState<string[]>([]);
  const [filterCustomer, setFilterCustomer] = useState('');
  const [filterTemplate, setFilterTemplate] = useState('');
  const [filterWinery, setFilterWinery] = useState('');
  const [filterVineyard, setFilterVineyard] = useState('');
  const [filterTruck, setFilterTruck] = useState('');
  const [filterWorker, setFilterWorker] = useState('');
  const [filterTrailermode, setFilterTrailermode] = useState('');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');

  const [opts, setOpts] = useState<FilterOptions>(emptyOpts);
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [total, setTotal] = useState(0);
  const [truncated, setTruncated] = useState(false);
  const [maxRows, setMaxRows] = useState(25_000);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [debug, setDebug] = useState<Record<string, unknown> | null>(null);
  const [reportFormat, setReportFormat] = useState<DistancesReportFormat>('customer_template_winery_truck');

  useEffect(() => {
    fetch('/api/vworkjobs/customers', { cache: 'no-store' })
      .then((r) => r.json())
      .then((d) => setCustomers(Array.isArray(d?.customers) ? (d.customers as string[]) : []))
      .catch(() => setCustomers([]));
  }, []);

  const effectiveCustomer = isClient ? clientCustomer.trim() : filterCustomer.trim();

  useEffect(() => {
    const url =
      effectiveCustomer !== ''
        ? `/api/vworkjobs/filter-options?customer=${encodeURIComponent(effectiveCustomer)}`
        : '/api/vworkjobs/filter-options';
    let cancelled = false;
    fetch(url, { cache: 'no-store' })
      .then((r) => r.json())
      .then((d) => {
        if (cancelled || d?.error) return;
        setOpts({
          templates: Array.isArray(d?.templates) ? d.templates.map((x: unknown) => String(x)) : [],
          deliveryWineries: Array.isArray(d?.deliveryWineries)
            ? d.deliveryWineries.map((x: unknown) => String(x))
            : [],
          vineyardNames: Array.isArray(d?.vineyardNames) ? d.vineyardNames.map((x: unknown) => String(x)) : [],
          truckIds: Array.isArray(d?.truckIds) ? d.truckIds.map((x: unknown) => String(x)) : [],
          workers: Array.isArray(d?.workers) ? d.workers.map((x: unknown) => String(x)) : [],
          trailermodes: Array.isArray(d?.trailermodes) ? d.trailermodes.map((x: unknown) => String(x)) : [],
        });
      })
      .catch(() => {
        if (!cancelled) setOpts(emptyOpts);
      });
    return () => {
      cancelled = true;
    };
  }, [effectiveCustomer]);

  const buildParams = useCallback(() => {
    const params = new URLSearchParams();
    if (effectiveCustomer) params.set('customer', effectiveCustomer);
    if (filterTemplate.trim()) params.set('template', filterTemplate.trim());
    if (filterWinery.trim()) params.set('winery', filterWinery.trim());
    if (filterVineyard.trim()) params.set('vineyard', filterVineyard.trim());
    if (filterTruck.trim()) params.set('truck_id', filterTruck.trim());
    if (filterWorker.trim()) params.set('device', filterWorker.trim());
    if (filterTrailermode.trim()) params.set('trailermode', filterTrailermode.trim());
    if (dateFrom.trim()) params.set('dateFrom', dateFrom.trim().slice(0, 10));
    if (dateTo.trim()) params.set('dateTo', dateTo.trim().slice(0, 10));
    return params;
  }, [
    effectiveCustomer,
    filterTemplate,
    filterWinery,
    filterVineyard,
    filterTruck,
    filterWorker,
    filterTrailermode,
    dateFrom,
    dateTo,
  ]);

  useEffect(() => {
    if (isClient && !clientCustomer.trim()) {
      setRows([]);
      setTotal(0);
      setTruncated(false);
      setDebug(null);
      setLoading(false);
      setError(null);
      return;
    }
    setLoading(true);
    setError(null);
    const params = buildParams();
    fetch(`/api/report/distances?${params}`, { cache: 'no-store' })
      .then(async (r) => {
        const d = await r.json();
        if (!r.ok) throw new Error(d?.error ?? r.statusText);
        return d;
      })
      .then((d) => {
        setRows(Array.isArray(d?.rows) ? (d.rows as Record<string, unknown>[]) : []);
        setTotal(typeof d?.total === 'number' ? d.total : 0);
        setTruncated(d?.truncated === true);
        setMaxRows(typeof d?.maxRows === 'number' ? d.maxRows : 25_000);
        setDebug(typeof d?.debug === 'object' && d.debug ? (d.debug as Record<string, unknown>) : null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  }, [isClient, clientCustomer, buildParams]);

  const reportRows = useMemo(
    () => buildDistancesReportRows(rows, reportFormat),
    [rows, reportFormat],
  );
  const isCompactFormat = reportFormat === 'customer_template';
  const tableColSpan = isCompactFormat ? 4 : 6;
  const loadedWithKmCount = useMemo(() => rows.filter(jobHasReportableDistanceKm).length, [rows]);
  const showDebug = viewMode !== 'client';

  const selectCls =
    'w-full max-w-[14rem] rounded border border-zinc-300 bg-white px-2 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800';

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-4 flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Distances</h1>
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
            Round-trip kilometres from <code className="rounded bg-zinc-100 px-1 text-xs dark:bg-zinc-800">tbl_vworkjobs.distance</code> only — no minutes.
            Filters are optional (Admin/Super: all customers when customer is empty). Rows and subtotals include jobs with{' '}
            <strong className="font-medium">distance &gt; 0</strong> only; <code className="rounded bg-zinc-100 px-1 text-xs dark:bg-zinc-800">excluded = 1</code> is
            omitted. <strong className="font-medium">Format · Customer · Template (compact)</strong> rolls km up to each template (no Winery/Truck columns or per-job lines).
          </p>
        </div>
        <div
          className="flex shrink-0 flex-col gap-1 rounded-md border border-zinc-200 bg-zinc-50 px-3 py-2 dark:border-zinc-600 dark:bg-zinc-900/70"
          onMouseDown={(e) => e.stopPropagation()}
        >
          <label htmlFor="distances-report-format" className="text-[11px] font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
            Format
          </label>
          <select
            id="distances-report-format"
            value={reportFormat}
            onChange={(e) => setReportFormat(e.target.value as DistancesReportFormat)}
            aria-label="Report format"
            title="Rollup layout (like Season Summary layout)"
            className="w-full min-w-[12rem] max-w-[22rem] rounded border border-zinc-300 bg-white py-1.5 pl-2 pr-8 text-xs font-normal text-zinc-800 shadow-sm dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-100"
          >
            <option value="customer_template_winery_truck">Customer · Template · Winery · Truck</option>
            <option value="customer_template">Customer · Template (compact)</option>
          </select>
        </div>
      </div>

      {error && (
        <div className="mb-4 rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>
      )}

      {truncated && (
        <div className="mb-4 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-950 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-100">
          Showing the first {formatIntNz(maxRows)} jobs matching filters ({formatIntNz(total)} total). Subtotals reflect{' '}
          <strong className="font-semibold">loaded rows only</strong> — narrow filters to include the full set.
        </div>
      )}

      <div className="mb-4 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">Filters</h2>
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Customer</label>
            {isClient ? (
              <div
                title="Set in sidebar"
                className="min-w-[10rem] rounded border border-zinc-200 bg-zinc-50 px-2 py-1 text-xs text-zinc-700 dark:border-zinc-700 dark:bg-zinc-800 dark:text-zinc-300"
              >
                {clientCustomer || '—'} {clientCustomerLocked ? '(locked)' : ''}
              </div>
            ) : (
              <select
                value={filterCustomer}
                onChange={(e) => {
                  setFilterCustomer(e.target.value);
                  setFilterTemplate('');
                  setFilterWinery('');
                  setFilterVineyard('');
                  setFilterTruck('');
                  setFilterWorker('');
                  setFilterTrailermode('');
                }}
                className={selectCls}
              >
                <option value="">All customers</option>
                {customers.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            )}
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Template</label>
            <select value={filterTemplate} onChange={(e) => setFilterTemplate(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {opts.templates.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Winery</label>
            <select value={filterWinery} onChange={(e) => setFilterWinery(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {opts.deliveryWineries.map((w) => (
                <option key={w} value={w}>
                  {w}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Vineyard</label>
            <select value={filterVineyard} onChange={(e) => setFilterVineyard(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {opts.vineyardNames.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Truck</label>
            <select value={filterTruck} onChange={(e) => setFilterTruck(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {opts.truckIds.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Worker</label>
            <select value={filterWorker} onChange={(e) => setFilterWorker(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {opts.workers.map((w) => (
                <option key={w} value={w}>
                  {w}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">TT</label>
            <select value={filterTrailermode} onChange={(e) => setFilterTrailermode(e.target.value)} className={selectCls}>
              <option value="">All</option>
              {opts.trailermodes.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Start from</label>
            <input
              type="date"
              value={dateFrom}
              onChange={(e) => setDateFrom(e.target.value)}
              className={selectCls}
            />
          </div>
          <div>
            <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Start to</label>
            <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} className={selectCls} />
          </div>
        </div>
      </div>

      {isClient && !clientCustomer.trim() ? (
        <p className="text-sm text-zinc-500 dark:text-zinc-400">Select a customer in the sidebar to load distances.</p>
      ) : loading ? (
        <p className="text-zinc-600 dark:text-zinc-400">Loading…</p>
      ) : (
        <div className="max-h-[72vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
          <table
            className={`w-full text-left text-xs ${isCompactFormat ? 'min-w-[26rem]' : 'min-w-[42rem]'}`}
          >
            <thead
              className={`${THEAD_TH} sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]`}
            >
              <tr className="border-b border-zinc-200 dark:border-zinc-700">
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Customer
                </th>
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Template
                </th>
                {!isCompactFormat && (
                  <>
                    <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                      Winery
                    </th>
                    <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                      Truck
                    </th>
                  </>
                )}
                <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                  Jobs
                </th>
                <th className="px-2 py-2 text-xs font-medium text-zinc-500 dark:text-zinc-400">Distance (km)</th>
              </tr>
            </thead>
            <tbody>
              {reportRows.length === 0 ? (
                <tr>
                  <td colSpan={tableColSpan} className="px-3 py-6 text-center text-zinc-500 dark:text-zinc-400">
                    {rows.length === 0
                      ? 'No jobs match the current filters.'
                      : loadedWithKmCount === 0
                        ? 'No jobs with distance &gt; 0 (missing, zero, or excluded).'
                        : 'Nothing to show.'}
                  </td>
                </tr>
              ) : (
                reportRows.map((r, i) => {
                  if (isCompactFormat) {
                    if (r.kind === 'template_total') {
                      return (
                        <tr key={`tt-${i}`} className="border-b border-zinc-100 dark:border-zinc-800">
                          <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
                            {r.customer}
                          </td>
                          <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
                            {r.template}
                          </td>
                          <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                            {formatIntNz(r.jobCount)}
                          </td>
                          <td className="whitespace-nowrap px-2 py-1.5 text-right tabular-nums text-zinc-700 dark:text-zinc-300">
                            {formatKmNz2(r.sumKm)}
                          </td>
                        </tr>
                      );
                    }
                    if (r.kind === 'subtotal' && (r.scope === 'customer' || r.scope === 'grand')) {
                      const scopeClass =
                        r.scope === 'grand'
                          ? 'border-t-2 border-zinc-400 bg-zinc-100 font-semibold dark:border-zinc-500 dark:bg-zinc-800/80'
                          : 'border-t border-zinc-300 bg-zinc-50 font-medium dark:border-zinc-600 dark:bg-zinc-800/50';
                      return (
                        <tr key={`s-${i}-${r.scope}`} className={`border-b border-zinc-100 dark:border-zinc-800 ${scopeClass}`}>
                          <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                            {r.label}
                          </td>
                          <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                            —
                          </td>
                          <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                            {formatIntNz(r.jobCount)}
                          </td>
                          <td className="whitespace-nowrap px-2 py-1.5 text-right tabular-nums text-zinc-800 dark:text-zinc-200">
                            {formatKmNz2(r.sumKm)}
                          </td>
                        </tr>
                      );
                    }
                    return null;
                  }

                  if (r.kind === 'template_total') return null;
                  if (r.kind === 'detail') {
                    return (
                      <tr key={`d-${i}`} className="border-b border-zinc-100 dark:border-zinc-800">
                        <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
                          {r.customer}
                        </td>
                        <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
                          {r.template}
                        </td>
                        <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
                          {r.winery}
                        </td>
                        <td className="min-w-[8rem] whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
                          {r.truck}
                        </td>
                        <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                          1
                        </td>
                        <td className="whitespace-nowrap px-2 py-1.5 text-right tabular-nums text-zinc-700 dark:text-zinc-300">
                          {formatKmNz2(r.distanceKm)}
                        </td>
                      </tr>
                    );
                  }
                  const scopeClass =
                    r.scope === 'grand'
                      ? 'border-t-2 border-zinc-400 bg-zinc-100 font-semibold dark:border-zinc-500 dark:bg-zinc-800/80'
                      : r.scope === 'customer'
                        ? 'border-t border-zinc-300 bg-zinc-50 font-medium dark:border-zinc-600 dark:bg-zinc-800/50'
                        : r.scope === 'template'
                          ? 'bg-zinc-50/80 dark:bg-zinc-800/35'
                          : 'bg-zinc-50/50 dark:bg-zinc-800/25';
                  const c = r.scope === 'grand' || r.scope === 'customer' ? r.label : r.customer;
                  const t =
                    r.scope === 'grand' || r.scope === 'customer'
                      ? '—'
                      : r.scope === 'template'
                        ? r.label
                        : r.template;
                  const w =
                    r.scope === 'grand' || r.scope === 'customer' || r.scope === 'template'
                      ? '—'
                      : r.scope === 'winery'
                        ? r.label
                        : r.winery;
                  const tr =
                    r.scope === 'truck'
                      ? r.label
                      : r.scope === 'grand' ||
                          r.scope === 'customer' ||
                          r.scope === 'template' ||
                          r.scope === 'winery'
                        ? '—'
                        : r.truck ?? '—';
                  return (
                    <tr key={`s-${i}-${r.scope}`} className={`border-b border-zinc-100 dark:border-zinc-800 ${scopeClass}`}>
                      <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                        {c}
                      </td>
                      <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                        {t}
                      </td>
                      <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                        {w}
                      </td>
                      <td className="min-w-[8rem] border-r border-zinc-200 px-2 py-1.5 text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
                        {tr}
                      </td>
                      <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                        {formatIntNz(r.jobCount)}
                      </td>
                      <td className="whitespace-nowrap px-2 py-1.5 text-right tabular-nums text-zinc-800 dark:text-zinc-200">
                        {formatKmNz2(r.sumKm)}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      )}

      <p className="mt-3 text-xs text-zinc-500 dark:text-zinc-400">
        {loading
          ? '…'
          : `API total: ${formatIntNz(total)} job${total !== 1 ? 's' : ''} · Loaded: ${formatIntNz(rows.length)} · With km &gt; 0: ${formatIntNz(loadedWithKmCount)} · Table rows: ${formatIntNz(reportRows.length)}`}
      </p>

      {showDebug && debug && Object.keys(debug).length > 0 && (
        <details className="mt-4 rounded border border-zinc-200 bg-zinc-50 p-3 text-xs dark:border-zinc-600 dark:bg-zinc-900/50">
          <summary className="cursor-pointer font-medium text-zinc-700 dark:text-zinc-300">Filter debug</summary>
          <pre className="mt-2 max-h-40 overflow-auto font-mono text-zinc-600 dark:text-zinc-400">
            {JSON.stringify(debug, null, 2)}
          </pre>
        </details>
      )}
    </div>
  );
}

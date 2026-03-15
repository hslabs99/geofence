'use client';

import { useCallback, useEffect, useState } from 'react';

type LogRow = {
  logid: number | string;
  logdatetime: string | null;
  logtype: string | null;
  logcat1: string | null;
  logcat2: string | null;
  logdetails: string | null;
};

const SORT_OPTIONS = [
  { value: 'logid', label: 'ID' },
  { value: 'logdatetime', label: 'Time' },
] as const;

const LIMIT_OPTIONS = [50, 100, 200, 500, 1000];

function formatLogTime(s: string | null | undefined): string {
  if (!s) return '—';
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? String(s) : d.toISOString().replace('T', ' ').slice(0, 19);
}

function parseDetails(s: string | null | undefined): unknown {
  if (!s || typeof s !== 'string') return null;
  try {
    return JSON.parse(s) as unknown;
  } catch {
    return s;
  }
}

export default function LogsPage() {
  const [logs, setLogs] = useState<LogRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [type, setType] = useState('');
  const [cat1, setCat1] = useState('');
  const [cat2, setCat2] = useState('');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [sort, setSort] = useState<string>('logid');
  const [dir, setDir] = useState<'asc' | 'desc'>('desc');
  const [limit, setLimit] = useState(200);
  const [expandedId, setExpandedId] = useState<string | number | null>(null);

  const loadLogs = useCallback(() => {
    setLoading(true);
    setError(null);
    const params = new URLSearchParams();
    params.set('limit', String(limit));
    if (type.trim()) params.set('type', type.trim());
    if (cat1.trim()) params.set('cat1', cat1.trim());
    if (cat2.trim()) params.set('cat2', cat2.trim());
    if (dateFrom.trim()) params.set('dateFrom', dateFrom.trim());
    if (dateTo.trim()) params.set('dateTo', dateTo.trim());
    params.set('sort', sort);
    params.set('dir', dir);
    fetch(`/api/logs?${params}`)
      .then((r) => r.json())
      .then((data) => {
        if (data.error) throw new Error(data.error);
        setLogs(data.logs ?? []);
      })
      .catch((e) => {
        setError(e instanceof Error ? e.message : String(e));
        setLogs([]);
      })
      .finally(() => setLoading(false));
  }, [type, cat1, cat2, dateFrom, dateTo, sort, dir, limit]);

  useEffect(() => {
    loadLogs();
  }, [loadLogs]);

  const toggleSort = (col: string) => {
    if (sort === col) {
      setDir((d) => (d === 'desc' ? 'asc' : 'desc'));
    } else {
      setSort(col);
      setDir('desc');
    }
  };

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-2 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Logs</h1>
      <p className="mb-6 text-sm text-zinc-500 dark:text-zinc-400">
        View and filter <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">tbl_logs</code>. Auto endpoints write logtype=auto, logcat1=endpoint name, logcat2=stage.
      </p>

      <section className="mb-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">Filters &amp; sort</h2>
        <div className="flex flex-wrap items-end gap-4">
          <div>
            <label className="mb-1 block text-xs font-medium text-zinc-600 dark:text-zinc-400">Type (logtype)</label>
            <input
              type="text"
              value={type}
              onChange={(e) => setType(e.target.value)}
              placeholder="e.g. auto, Import, APIfetch"
              className="w-40 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-zinc-600 dark:text-zinc-400">Cat1 (e.g. endpoint)</label>
            <input
              type="text"
              value={cat1}
              onChange={(e) => setCat1(e.target.value)}
              placeholder="e.g. gpsimport, vwimport"
              className="w-40 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-zinc-600 dark:text-zinc-400">Cat2 (e.g. stage)</label>
            <input
              type="text"
              value={cat2}
              onChange={(e) => setCat2(e.target.value)}
              placeholder="e.g. start, done, error"
              className="w-40 rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-zinc-600 dark:text-zinc-400">Date from</label>
            <input
              type="date"
              value={dateFrom}
              onChange={(e) => setDateFrom(e.target.value)}
              className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-zinc-600 dark:text-zinc-400">Date to</label>
            <input
              type="date"
              value={dateTo}
              onChange={(e) => setDateTo(e.target.value)}
              className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-zinc-600 dark:text-zinc-400">Sort</label>
            <select
              value={sort}
              onChange={(e) => setSort(e.target.value)}
              className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
            >
              {SORT_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-zinc-600 dark:text-zinc-400">Direction</label>
            <select
              value={dir}
              onChange={(e) => setDir(e.target.value as 'asc' | 'desc')}
              className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
            >
              <option value="desc">Newest first</option>
              <option value="asc">Oldest first</option>
            </select>
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-zinc-600 dark:text-zinc-400">Limit</label>
            <select
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
            >
              {LIMIT_OPTIONS.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </div>
          <button
            type="button"
            onClick={loadLogs}
            className="rounded bg-zinc-700 px-4 py-1.5 text-sm font-medium text-white hover:bg-zinc-600 dark:bg-zinc-600 dark:hover:bg-zinc-500"
          >
            Apply &amp; refresh
          </button>
        </div>
      </section>

      {error && (
        <p className="mb-4 text-sm text-red-600 dark:text-red-400">{error}</p>
      )}

      <div className="overflow-x-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
        <table className="min-w-full border-collapse text-left text-sm">
          <thead>
            <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
              <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">
                <button
                  type="button"
                  onClick={() => toggleSort('logid')}
                  className="hover:underline"
                >
                  ID {sort === 'logid' ? (dir === 'desc' ? '↓' : '↑') : ''}
                </button>
              </th>
              <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">
                <button
                  type="button"
                  onClick={() => toggleSort('logdatetime')}
                  className="hover:underline"
                >
                  Time {sort === 'logdatetime' ? (dir === 'desc' ? '↓' : '↑') : ''}
                </button>
              </th>
              <th className="whitespace-nowrap px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Type</th>
              <th className="whitespace-nowrap px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Cat1</th>
              <th className="whitespace-nowrap px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Cat2</th>
              <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Details</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={6} className="px-3 py-8 text-center text-zinc-500">
                  Loading…
                </td>
              </tr>
            ) : logs.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-3 py-8 text-center text-zinc-500">
                  No logs match the filters.
                </td>
              </tr>
            ) : (
              logs.map((log) => {
                const id = log.logid;
                const details = parseDetails(log.logdetails);
                const isExpanded = expandedId === id;
                const detailsStr = typeof details === 'object' && details !== null
                  ? JSON.stringify(details, null, 2)
                  : String(log.logdetails ?? '');
                return (
                  <tr
                    key={String(id)}
                    className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50"
                  >
                    <td className="whitespace-nowrap px-3 py-2 font-mono text-zinc-600 dark:text-zinc-400">
                      {String(id)}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-zinc-600 dark:text-zinc-400">
                      {formatLogTime(log.logdatetime)}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 font-medium text-zinc-800 dark:text-zinc-200">
                      {log.logtype ?? '—'}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">
                      {log.logcat1 ?? '—'}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">
                      {log.logcat2 ?? '—'}
                    </td>
                    <td className="max-w-md px-3 py-2">
                      {detailsStr ? (
                        <>
                          <button
                            type="button"
                            onClick={() => setExpandedId(isExpanded ? null : id)}
                            className="text-left text-xs font-medium text-blue-600 hover:underline dark:text-blue-400"
                          >
                            {isExpanded ? 'Collapse' : 'Expand'}
                          </button>
                          {isExpanded ? (
                            <pre className="mt-1 max-h-64 overflow-auto rounded bg-zinc-100 p-2 text-xs dark:bg-zinc-800">
                              {detailsStr}
                            </pre>
                          ) : (
                            <span className="truncate text-xs text-zinc-600 dark:text-zinc-400" title={detailsStr}>
                              {detailsStr.length > 80 ? `${detailsStr.slice(0, 80)}…` : detailsStr}
                            </span>
                          )}
                        </>
                      ) : (
                        '—'
                      )}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

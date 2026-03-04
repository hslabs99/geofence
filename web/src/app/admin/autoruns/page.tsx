'use client';

import { useEffect, useState } from 'react';
import { formatDateNZ } from '@/lib/utils';

type LogEntry = {
  logid: string;
  logdatetime: string;
  logtype: string;
  logcat1?: string | null;
  logcat2?: string | null;
  logdetails?: string | null;
};

type ResultItem = { file: string; status: string; rows?: number; error?: string };

type RunResult = { ok: boolean; results: ResultItem[] } | { error: string };

export default function AutoRunsPage() {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [logsLoading, setLogsLoading] = useState(true);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [vworkResult, setVworkResult] = useState<RunResult | null>(null);
  const [geoResult, setGeoResult] = useState<RunResult | null>(null);
  const [trackResult, setTrackResult] = useState<RunResult | null>(null);
  const [vworkRunning, setVworkRunning] = useState(false);
  const [geoRunning, setGeoRunning] = useState(false);
  const [trackRunning, setTrackRunning] = useState(false);
  const [logFilter, setLogFilter] = useState<string>('Import');
  const [importDebug, setImportDebug] = useState<Record<string, unknown> | null>(null);

  const loadLogs = () => {
    setLogsLoading(true);
    const params = new URLSearchParams({ limit: '200' });
    if (logFilter) params.set('type', logFilter);
    fetch(`/api/logs?${params}`)
      .then((r) => r.json())
      .then((data) => {
        if (data.error) throw new Error(data.error);
        setLogs(data.logs ?? []);
        setLogsError(null);
      })
      .catch((e) => setLogsError(e.message))
      .finally(() => setLogsLoading(false));
  };

  useEffect(() => {
    loadLogs();
    const id = setInterval(loadLogs, 10000);
    return () => clearInterval(id);
  }, [logFilter]);

  const runVwork = () => {
    setVworkRunning(true);
    setVworkResult(null);
    fetch('/api/import/run-vwork', { method: 'POST' })
      .then(async (r) => {
        const data = await r.json().catch(() => ({}));
        if (!r.ok) {
          setVworkResult({ error: data.error ?? `HTTP ${r.status}` });
        } else {
          setVworkResult(data);
        }
        loadLogs();
      })
      .catch((e) => setVworkResult({ error: e.message ?? 'Network error' }))
      .finally(() => setVworkRunning(false));
  };

  const runGeo = () => {
    setGeoRunning(true);
    setGeoResult(null);
    fetch('/api/import/run-geo', { method: 'POST' })
      .then(async (r) => {
        const data = await r.json().catch(() => ({}));
        if (!r.ok) {
          setGeoResult({ error: data.error ?? `HTTP ${r.status}` });
        } else {
          setGeoResult(data);
        }
        loadLogs();
      })
      .catch((e) => setGeoResult({ error: e.message ?? 'Network error' }))
      .finally(() => setGeoRunning(false));
  };

  const runTracking = () => {
    setTrackRunning(true);
    setTrackResult(null);
    fetch('/api/import/run-tracking', { method: 'POST' })
      .then(async (r) => {
        const data = await r.json().catch(() => ({}));
        if (!r.ok) {
          setTrackResult({ error: data.error ?? `HTTP ${r.status}` });
        } else {
          setTrackResult(data);
        }
        loadLogs();
      })
      .catch((e) => setTrackResult({ error: e.message ?? 'Network error' }))
      .finally(() => setTrackRunning(false));
  };

  const parseDetails = (d: string | null | undefined): Record<string, unknown> => {
    if (!d) return {};
    try {
      return JSON.parse(d) as Record<string, unknown>;
    } catch {
      return { raw: d };
    }
  };

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-6 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">AutoRuns</h1>

      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-zinc-800 dark:text-zinc-200">Run imports</h2>
        <div className="flex flex-wrap gap-4">
          <button
            type="button"
            onClick={() => {
              setImportDebug(null);
              fetch('/api/import/debug')
                .then((r) => r.json())
                .then(setImportDebug);
            }}
            className="rounded bg-zinc-600 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700"
          >
            Debug import service
          </button>
          <button
            type="button"
            onClick={runVwork}
            disabled={vworkRunning}
            className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          >
            {vworkRunning ? 'Running…' : 'Run Vwork import'}
          </button>
          <button
            type="button"
            onClick={runGeo}
            disabled={geoRunning}
            className="rounded bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
          >
            {geoRunning ? 'Running…' : 'Run GPS import'}
          </button>
          <button
            type="button"
            onClick={runTracking}
            disabled={trackRunning}
            className="rounded bg-violet-600 px-4 py-2 text-sm font-medium text-white hover:bg-violet-700 disabled:opacity-50"
          >
            {trackRunning ? 'Running…' : 'GPS Tracking'}
          </button>
        </div>

        {importDebug && (
          <div className="mt-4 rounded-lg border border-amber-200 bg-amber-50 p-4 dark:border-amber-800 dark:bg-amber-950/30">
            <h3 className="mb-2 text-sm font-semibold text-amber-800 dark:text-amber-200">Import service debug</h3>
            <pre className="overflow-auto rounded bg-white p-3 text-xs dark:bg-zinc-900">
              {JSON.stringify(importDebug, null, 2)}
            </pre>
          </div>
        )}
        <div className="mt-4 grid gap-6 md:grid-cols-3">
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <h3 className="mb-2 text-sm font-semibold text-zinc-700 dark:text-zinc-300">Vwork result</h3>
            {vworkResult === null ? (
              <p className="text-sm text-zinc-500">Not run yet.</p>
            ) : 'error' in vworkResult ? (
              <p className="text-sm text-red-600 dark:text-red-400">{vworkResult.error}</p>
            ) : (
              <pre className="overflow-auto rounded bg-zinc-100 p-3 text-xs dark:bg-zinc-800">
                {JSON.stringify(vworkResult.results, null, 2)}
              </pre>
            )}
          </div>
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <h3 className="mb-2 text-sm font-semibold text-zinc-700 dark:text-zinc-300">GPS result</h3>
            {geoResult === null ? (
              <p className="text-sm text-zinc-500">Not run yet.</p>
            ) : 'error' in geoResult ? (
              <p className="text-sm text-red-600 dark:text-red-400">{geoResult.error}</p>
            ) : Array.isArray(geoResult.results) && geoResult.results.length > 0 ? (
              <>
                <p className="mb-2 text-sm text-zinc-700 dark:text-zinc-300">
                  {geoResult.results.length} file(s) processed,{' '}
                  {geoResult.results.reduce((sum: number, r: { rows?: number }) => sum + (r.rows ?? 0), 0)} rows
                </p>
                <pre className="overflow-auto rounded bg-zinc-100 p-3 text-xs dark:bg-zinc-800">
                  {JSON.stringify(geoResult.results, null, 2)}
                </pre>
              </>
            ) : (
              <pre className="overflow-auto rounded bg-zinc-100 p-3 text-xs dark:bg-zinc-800">
                {JSON.stringify(geoResult.results ?? geoResult, null, 2)}
              </pre>
            )}
          </div>
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <h3 className="mb-2 text-sm font-semibold text-zinc-700 dark:text-zinc-300">GPS Tracking result</h3>
            {trackResult === null ? (
              <p className="text-sm text-zinc-500">Not run yet.</p>
            ) : 'error' in trackResult ? (
              <p className="text-sm text-red-600 dark:text-red-400">{trackResult.error}</p>
            ) : Array.isArray(trackResult.results) && trackResult.results.length > 0 ? (
              <>
                <p className="mb-2 text-sm text-zinc-700 dark:text-zinc-300">
                  {trackResult.results.length} file(s) processed,{' '}
                  {trackResult.results.reduce((sum: number, r: { rows?: number }) => sum + (r.rows ?? 0), 0)} rows
                </p>
                <pre className="overflow-auto rounded bg-zinc-100 p-3 text-xs dark:bg-zinc-800">
                  {JSON.stringify(trackResult.results, null, 2)}
                </pre>
              </>
            ) : (
              <pre className="overflow-auto rounded bg-zinc-100 p-3 text-xs dark:bg-zinc-800">
                {JSON.stringify(trackResult.results ?? trackResult, null, 2)}
              </pre>
            )}
          </div>
        </div>
      </section>

      <section>
        <div className="mb-3 flex items-center justify-between">
          <h2 className="text-lg font-medium text-zinc-800 dark:text-zinc-200">Import logs</h2>
          <div className="flex items-center gap-2">
            <label className="text-sm text-zinc-500">Type</label>
            <select
              value={logFilter}
              onChange={(e) => setLogFilter(e.target.value)}
              className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            >
              <option value="">All</option>
              <option value="Import">Import</option>
            </select>
            <button type="button" onClick={loadLogs} className="rounded bg-zinc-200 px-2 py-1 text-sm hover:bg-zinc-300 dark:bg-zinc-700 dark:hover:bg-zinc-600">
              Refresh
            </button>
          </div>
        </div>
        {logsError && (
          <p className="mb-2 text-sm text-red-600 dark:text-red-400">{logsError}</p>
        )}
        <div className="overflow-x-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
          <table className="min-w-full text-left text-sm">
            <thead>
              <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                <th className="whitespace-nowrap px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Time</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Type</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Cat1</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">File</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Details</th>
              </tr>
            </thead>
            <tbody>
              {logsLoading ? (
                <tr>
                  <td colSpan={5} className="px-3 py-4 text-center text-zinc-500">Loading…</td>
                </tr>
              ) : logs.length === 0 ? (
                <tr>
                  <td colSpan={5} className="px-3 py-4 text-center text-zinc-500">No logs.</td>
                </tr>
              ) : (
                logs.map((log) => {
                  const details = parseDetails(log.logdetails);
                  return (
                    <tr key={log.logid} className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50">
                      <td className="whitespace-nowrap px-3 py-2 text-zinc-600 dark:text-zinc-400">
                        {log.logdatetime ? formatDateNZ(String(log.logdatetime)) : '—'}
                      </td>
                      <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{log.logtype}</td>
                      <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{log.logcat1 ?? '—'}</td>
                      <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{log.logcat2 ?? '—'}</td>
                      <td className="px-3 py-2 text-zinc-700 dark:text-zinc-300">
                        {Object.keys(details).length > 0 ? (
                          <pre className="text-xs">{JSON.stringify(details)}</pre>
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
      </section>
    </div>
  );
}

'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
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

type FixKey = 'winery' | 'vineyard' | 'vineyardGroup' | 'driver' | 'trailer';

type BatchStepRow = {
  step: string;
  logcat2: string;
  ok: boolean;
  result?: Record<string, unknown>;
  error?: string;
  logid?: string;
  logdatetime?: string;
};

type BatchAllFixesResult = {
  ok: boolean;
  steps: BatchStepRow[];
};

const FIX_ACTIONS: { key: FixKey; label: string; path: string }[] = [
  { key: 'winery', label: 'Winery name fixes', path: '/api/admin/data-checks/wine-mapp/run-fixes' },
  { key: 'vineyard', label: 'Vineyard name fixes', path: '/api/admin/data-checks/vine-mapp/run-fixes' },
  { key: 'vineyardGroup', label: 'Vineyard mappings run', path: '/api/admin/data-checks/update-vineyard-group' },
  { key: 'driver', label: 'Driver name fixes', path: '/api/admin/data-checks/driver-mapp/run-fixes' },
  { key: 'trailer', label: 'Set trailer type', path: '/api/admin/data-checks/set-trailer-type' },
];

export default function AutoRunsPage() {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [logsLoading, setLogsLoading] = useState(true);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [vworkResult, setVworkResult] = useState<RunResult | null>(null);
  const [vworkRunning, setVworkRunning] = useState(false);
  const [logFilter, setLogFilter] = useState<string>('Import');
  const [importDebug, setImportDebug] = useState<Record<string, unknown> | null>(null);
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const [lastVworkRunAt, setLastVworkRunAt] = useState<string | null>(null);
  const [lastVworkRunLoading, setLastVworkRunLoading] = useState(true);
  const [fixRunning, setFixRunning] = useState<FixKey | null>(null);
  const [fixResult, setFixResult] = useState<{ label: string; data: unknown } | null>(null);
  const [fixError, setFixError] = useState<string | null>(null);
  const [allFixesRunning, setAllFixesRunning] = useState(false);
  const [batchFixesResult, setBatchFixesResult] = useState<BatchAllFixesResult | null>(null);
  const [batchFixesError, setBatchFixesError] = useState<string | null>(null);

  const loadLogs = (typeOverride?: string) => {
    setLogsLoading(true);
    const params = new URLSearchParams({ limit: '200' });
    const type = typeOverride !== undefined ? typeOverride : logFilter;
    if (type) params.set('type', type);
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

  const loadLastVworkRun = useCallback(() => {
    setLastVworkRunLoading(true);
    const params = new URLSearchParams({
      type: 'AutoRun',
      cat1: 'vwork-import',
      limit: '1',
      sort: 'logdatetime',
      dir: 'desc',
    });
    fetch(`/api/logs?${params}`)
      .then((r) => r.json())
      .then((data) => {
        if (data.error) throw new Error(data.error);
        const list = data.logs as LogEntry[] | undefined;
        const first = list?.[0];
        setLastVworkRunAt(first?.logdatetime ? String(first.logdatetime) : null);
      })
      .catch(() => setLastVworkRunAt(null))
      .finally(() => setLastVworkRunLoading(false));
  }, []);

  useEffect(() => {
    loadLogs();
    const id = setInterval(loadLogs, 10000);
    return () => clearInterval(id);
  }, [logFilter]);

  useEffect(() => {
    loadLastVworkRun();
  }, [loadLastVworkRun]);

  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', onDown);
    return () => document.removeEventListener('mousedown', onDown);
  }, [menuOpen]);

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
          loadLastVworkRun();
        }
        loadLogs();
      })
      .catch((e) => setVworkResult({ error: e.message ?? 'Network error' }))
      .finally(() => setVworkRunning(false));
  };

  const runFix = (action: (typeof FIX_ACTIONS)[number]) => {
    setFixRunning(action.key);
    setFixError(null);
    setFixResult(null);
    setBatchFixesResult(null);
    setBatchFixesError(null);
    fetch(action.path, { method: 'POST' })
      .then(async (r) => {
        const data = await r.json().catch(() => ({}));
        if (!r.ok) {
          setFixError(typeof data.error === 'string' ? data.error : `HTTP ${r.status}`);
        } else {
          setFixResult({ label: action.label, data });
        }
      })
      .catch((e) => setFixError(e.message ?? 'Network error'))
      .finally(() => setFixRunning(null));
  };

  const runAllFixes = () => {
    setAllFixesRunning(true);
    setBatchFixesResult(null);
    setBatchFixesError(null);
    setFixResult(null);
    setFixError(null);
    fetch('/api/admin/data-checks/run-all-vwork-fixes', { method: 'POST' })
      .then(async (r) => {
        const data = (await r.json().catch(() => ({}))) as BatchAllFixesResult & { error?: string };
        if (!r.ok) {
          setBatchFixesError(typeof data.error === 'string' ? data.error : `HTTP ${r.status}`);
          return;
        }
        if (!data.steps || !Array.isArray(data.steps)) {
          setBatchFixesError('Unexpected response');
          return;
        }
        setBatchFixesResult({ ok: data.ok, steps: data.steps });
        setLogFilter('AutoRun');
        loadLogs('AutoRun');
      })
      .catch((e) => setBatchFixesError(e.message ?? 'Network error'))
      .finally(() => setAllFixesRunning(false));
  };

  const parseDetails = (d: string | null | undefined): Record<string, unknown> => {
    if (!d) return {};
    try {
      return JSON.parse(d) as Record<string, unknown>;
    } catch {
      return { raw: d };
    }
  };

  const lastRunLabel =
    lastVworkRunLoading ? 'Loading…' : lastVworkRunAt ? formatDateNZ(lastVworkRunAt) : 'Never';

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-6 flex items-start justify-between gap-4">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">AutoRuns</h1>
        <div className="relative shrink-0" ref={menuRef}>
          <button
            type="button"
            aria-label="Page menu"
            aria-expanded={menuOpen}
            onClick={() => setMenuOpen((o) => !o)}
            className="rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm font-medium text-zinc-700 shadow-sm hover:bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-200 dark:hover:bg-zinc-700"
          >
            ⋯
          </button>
          {menuOpen && (
            <div className="absolute right-0 z-20 mt-1 min-w-[12rem] rounded-lg border border-zinc-200 bg-white py-1 shadow-lg dark:border-zinc-600 dark:bg-zinc-900">
              <button
                type="button"
                className="block w-full px-4 py-2 text-left text-sm text-zinc-800 hover:bg-zinc-100 dark:text-zinc-200 dark:hover:bg-zinc-800"
                onClick={() => {
                  setMenuOpen(false);
                  setImportDebug(null);
                  fetch('/api/import/debug')
                    .then((r) => r.json())
                    .then(setImportDebug);
                }}
              >
                Debug import service
              </button>
            </div>
          )}
        </div>
      </div>

      <section className="mb-8">
        <h2 className="mb-3 text-lg font-medium text-zinc-800 dark:text-zinc-200">Run imports</h2>
        <div className="flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={runVwork}
            disabled={vworkRunning}
            className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          >
            {vworkRunning ? 'Running…' : 'Run Vwork import'}
          </button>
          <span className="text-sm text-zinc-500 dark:text-zinc-400">
            Last run: <span className="text-zinc-700 dark:text-zinc-300">{lastRunLabel}</span>
          </span>
        </div>

        <p className="mt-3 text-sm text-zinc-500 dark:text-zinc-400">
          Vwork table updates (same actions as Admin → Data checks). These read mapping tables and update{' '}
          <code className="rounded bg-zinc-100 px-1 text-xs dark:bg-zinc-800">tbl_vworkjobs</code>.
        </p>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          {FIX_ACTIONS.map((a) => (
            <button
              key={a.key}
              type="button"
              onClick={() => runFix(a)}
              disabled={fixRunning !== null || allFixesRunning}
              className="rounded border border-zinc-300 bg-white px-3 py-1.5 text-sm font-medium text-zinc-800 hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-200 dark:hover:bg-zinc-800"
            >
              {fixRunning === a.key ? 'Running…' : a.label}
            </button>
          ))}
          <button
            type="button"
            onClick={runAllFixes}
            disabled={fixRunning !== null || allFixesRunning}
            className="rounded bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
          >
            {allFixesRunning ? 'Running all…' : 'Run all fixes'}
          </button>
        </div>
        <p className="mt-2 text-xs text-zinc-500 dark:text-zinc-400">
          Run all fixes executes the five steps in order and writes one <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">tbl_logs</code> row per step (
          <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">AutoRun</code> /{' '}
          <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">vwork-fixes-batch</code>). The log table below switches to AutoRun when finished.
        </p>

        {fixError && (
          <p className="mt-3 text-sm text-red-600 dark:text-red-400">{fixError}</p>
        )}
        {fixResult && (
          <div className="mt-3 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <h3 className="mb-2 text-sm font-semibold text-zinc-700 dark:text-zinc-300">{fixResult.label}</h3>
            <pre className="overflow-auto rounded bg-zinc-100 p-3 text-xs dark:bg-zinc-800">
              {JSON.stringify(fixResult.data, null, 2)}
            </pre>
          </div>
        )}

        {batchFixesError && (
          <p className="mt-3 text-sm text-red-600 dark:text-red-400">{batchFixesError}</p>
        )}
        {batchFixesResult && (
          <div className="mt-3 rounded-lg border border-indigo-200 bg-indigo-50/50 p-4 dark:border-indigo-900 dark:bg-indigo-950/20">
            <h3 className="mb-2 text-sm font-semibold text-indigo-900 dark:text-indigo-200">
              Run all fixes — {batchFixesResult.ok ? 'all steps completed' : 'one or more steps failed'}
            </h3>
            <div className="mb-3 overflow-x-auto rounded border border-indigo-100 bg-white dark:border-indigo-900 dark:bg-zinc-900">
              <table className="min-w-full text-left text-xs">
                <thead>
                  <tr className="border-b border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-800">
                    <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">Step</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">Status</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">Log time</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">logid</th>
                    <th className="px-2 py-1.5 font-medium text-zinc-700 dark:text-zinc-300">logcat2</th>
                  </tr>
                </thead>
                <tbody>
                  {batchFixesResult.steps.map((s) => (
                    <tr key={`${s.logcat2}-${s.logid ?? s.step}`} className="border-b border-zinc-100 dark:border-zinc-800">
                      <td className="px-2 py-1.5 text-zinc-800 dark:text-zinc-200">{s.step}</td>
                      <td className="px-2 py-1.5">
                        {s.ok ? (
                          <span className="text-emerald-700 dark:text-emerald-400">OK</span>
                        ) : (
                          <span className="text-red-600 dark:text-red-400" title={s.error}>
                            Failed
                          </span>
                        )}
                      </td>
                      <td className="whitespace-nowrap px-2 py-1.5 text-zinc-600 dark:text-zinc-400">
                        {s.logdatetime ? formatDateNZ(s.logdatetime) : '—'}
                      </td>
                      <td className="px-2 py-1.5 font-mono text-zinc-600 dark:text-zinc-400">{s.logid ?? '—'}</td>
                      <td className="px-2 py-1.5 font-mono text-zinc-500 dark:text-zinc-500">{s.logcat2}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {batchFixesResult.steps.some((s) => !s.ok && s.error) && (
              <ul className="mb-3 list-inside list-disc text-sm text-red-700 dark:text-red-400">
                {batchFixesResult.steps
                  .filter((s) => !s.ok && s.error)
                  .map((s) => (
                    <li key={s.logcat2}>
                      <strong>{s.step}:</strong> {s.error}
                    </li>
                  ))}
              </ul>
            )}
            <p className="mb-1 text-xs font-medium text-zinc-600 dark:text-zinc-400">Full response (API + log row ids)</p>
            <pre className="max-h-80 overflow-auto rounded bg-zinc-100 p-3 text-xs dark:bg-zinc-800">
              {JSON.stringify(batchFixesResult, null, 2)}
            </pre>
          </div>
        )}

        {importDebug && (
          <div className="mt-4 rounded-lg border border-amber-200 bg-amber-50 p-4 dark:border-amber-800 dark:bg-amber-950/30">
            <div className="mb-2 flex items-center justify-between gap-2">
              <h3 className="text-sm font-semibold text-amber-800 dark:text-amber-200">Import service debug</h3>
              <button
                type="button"
                onClick={() => setImportDebug(null)}
                className="text-xs text-amber-800 underline dark:text-amber-200"
              >
                Dismiss
              </button>
            </div>
            <pre className="overflow-auto rounded bg-white p-3 text-xs dark:bg-zinc-900">
              {JSON.stringify(importDebug, null, 2)}
            </pre>
          </div>
        )}

        <div className="mt-6">
          <div className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <h3 className="mb-2 text-sm font-semibold text-zinc-700 dark:text-zinc-300">Vwork import result</h3>
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
              <option value="AutoRun">AutoRun</option>
            </select>
            <button type="button" onClick={() => loadLogs()} className="rounded bg-zinc-200 px-2 py-1 text-sm hover:bg-zinc-300 dark:bg-zinc-700 dark:hover:bg-zinc-600">
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

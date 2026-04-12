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

type ResultItem = {
  file: string;
  status: string;
  rows?: number;
  error?: string;
  omittedDbColumns?: string[];
  unmappedCsvHeaders?: string[];
  mappingTargetsMissingFromTable?: string[];
  rowFailureCount?: number;
  rowFailureSamples?: { row: number; error: string }[];
};

type RunResult = { ok: boolean; results: ResultItem[] } | { error: string };

type FixKey = 'winery' | 'vineyard' | 'vineyardGroup' | 'driver' | 'trailer' | 'loadsizeTm' | 'step4to5';

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

/** Must match `VWORK_FIX_BATCH_STEPS` order in `@/lib/vwork-fixes-batch` (labels + logcat2 for UI only). */
const BATCH_STEP_META = [
  { label: 'Winery name fixes', logcat2: 'winery-name-fixes' },
  { label: 'Vineyard name fixes', logcat2: 'vineyard-name-fixes' },
  { label: 'Vineyard mappings run', logcat2: 'vineyard-group-mapping' },
  { label: 'Driver name fixes', logcat2: 'driver-name-fixes' },
  { label: 'Set trailer type', logcat2: 'set-trailer-type' },
  { label: 'Trailermode from load size', logcat2: 'set-trailermode-from-loadsize' },
  { label: 'Step4→5 fix (all eligible jobs)', logcat2: 'step4to5-normal-all' },
] as const;

type BatchRunUiStep = {
  index: number;
  label: string;
  logcat2: string;
  phase: 'pending' | 'running' | 'done';
  outcome?: BatchStepRow;
};

function summarizeBatchStep(row: BatchStepRow): string {
  if (!row.ok) return row.error ?? 'Failed';
  const r = row.result;
  if (!r) return 'OK';
  switch (row.logcat2) {
    case 'winery-name-fixes':
    case 'vineyard-name-fixes':
    case 'driver-name-fixes':
      return `Updated ${Number(r.totalUpdated ?? 0)} row(s)`;
    case 'vineyard-group-mapping':
      return `Set NA: ${String(r.setToNa ?? '—')}, matched groups: ${String(r.matched ?? '—')}, jobs in table: ${String(r.totalRows ?? '—')}`;
    case 'set-trailer-type':
      return `TT: ${String(r.updatedTT ?? '—')}, T: ${String(r.updatedT ?? '—')} (total ${String(r.totalUpdated ?? '—')})`;
    case 'set-trailermode-from-loadsize':
      return `Threshold ${String(r.threshold ?? '—')}: TT ${String(r.updatedTT ?? '—')}, T ${String(r.updatedT ?? '—')} (total ${String(r.totalUpdated ?? '—')})`;
    case 'step4to5-normal-all':
      return `Updated ${Number(r.updated ?? 0)} job(s)`;
    default:
      return 'OK';
  }
}

const FIX_ACTIONS: { key: FixKey; label: string; path: string }[] = [
  { key: 'winery', label: 'Winery name fixes', path: '/api/admin/data-checks/wine-mapp/run-fixes' },
  { key: 'vineyard', label: 'Vineyard name fixes', path: '/api/admin/data-checks/vine-mapp/run-fixes' },
  { key: 'vineyardGroup', label: 'Vineyard mappings run', path: '/api/admin/data-checks/update-vineyard-group' },
  { key: 'driver', label: 'Driver name fixes', path: '/api/admin/data-checks/driver-mapp/run-fixes' },
  { key: 'trailer', label: 'Set trailer type', path: '/api/admin/data-checks/set-trailer-type' },
  {
    key: 'loadsizeTm',
    label: 'Trailermode from load size',
    path: '/api/admin/data-checks/set-trailermode-from-loadsize',
  },
  {
    key: 'step4to5',
    label: 'Step4→5 fix (all eligible)',
    path: '/api/admin/data-checks/step4to5-run-all-eligible',
  },
];

/** Human-readable order for “Run all fixes” (matches run-all-vwork-fixes STEPS). */
const RUN_ALL_FIXES_DESCRIPTIONS: string[] = [
  'Winery names: apply tbl_wine_mapp to delivery_winery on tbl_vworkjobs (save old value in delivery_winery_old).',
  'Vineyard names: apply tbl_vine_mapp to vineyard_name (vineyard_name_old).',
  'Vineyard group: set vineyard_group to NA, then fill from tbl_vineyardgroups where winery + vineyard match.',
  'Driver names: apply tbl_driver_mapp to worker (worker_old).',
  'Trailer type: set trailermode to TT or T from trailer_rego rules.',
  'Trailermode from load size: for rows with loadsize > 0 only, set trailermode to TT where loadsize > Settings “TT Load Size” threshold, else T (uses tbl_settings, default 25.5 if unset). Leaves null/0 loadsize unchanged.',
  'Step4→5: for every eligible job (step4to5=0, step 4 “Job Completed”, step 5 not completed, no step_5_completed_at, step_4_completed_at set), migrate to Arrive Winery / Job Completed layout, synthetic step 4 time, step4to5=1, clear step GPS and fetched flags.',
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
  const [batchRunUi, setBatchRunUi] = useState<{ steps: BatchRunUiStep[] } | null>(null);

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
    setBatchRunUi(null);
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

  const runAllFixes = async () => {
    setAllFixesRunning(true);
    setBatchFixesResult(null);
    setBatchFixesError(null);
    setFixResult(null);
    setFixError(null);
    setBatchRunUi({
      steps: BATCH_STEP_META.map((m, i) => ({
        index: i,
        label: m.label,
        logcat2: m.logcat2,
        phase: 'pending',
      })),
    });

    const finishedSteps: BatchStepRow[] = [];
    try {
      for (let i = 0; i < BATCH_STEP_META.length; i++) {
        setBatchRunUi((prev) =>
          prev
            ? {
                steps: prev.steps.map((s) =>
                  s.index === i ? { ...s, phase: 'running' } : s
                ),
              }
            : null
        );

        const r = await fetch('/api/admin/data-checks/run-all-vwork-fixes/step', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ index: i }),
        });
        const data = (await r.json().catch(() => ({}))) as BatchStepRow & { error?: string };

        let row: BatchStepRow;
        if (!r.ok) {
          const meta = BATCH_STEP_META[i]!;
          row = {
            step: meta.label,
            logcat2: meta.logcat2,
            ok: false,
            error: typeof data.error === 'string' ? data.error : `HTTP ${r.status}`,
          };
        } else {
          row = {
            step: data.step,
            logcat2: data.logcat2,
            ok: data.ok,
            result: data.result,
            error: data.error,
            logid: data.logid,
            logdatetime: data.logdatetime,
          };
        }

        finishedSteps.push(row);
        setBatchRunUi((prev) =>
          prev
            ? {
                steps: prev.steps.map((s) =>
                  s.index === i ? { ...s, phase: 'done', outcome: row } : s
                ),
              }
            : null
        );
      }

      setBatchFixesResult({ ok: finishedSteps.every((s) => s.ok), steps: finishedSteps });
      setLogFilter('AutoRun');
      loadLogs('AutoRun');
    } catch (e) {
      setBatchFixesError(e instanceof Error ? e.message : String(e));
      setBatchRunUi(null);
    } finally {
      setAllFixesRunning(false);
    }
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
          Vwork table updates (same actions as Admin → Data checks). Most steps use mapping tables; Step4→5 updates step fields on{' '}
          <code className="rounded bg-zinc-100 px-1 text-xs dark:bg-zinc-800">tbl_vworkjobs</code> where the row matches the fix rules.
        </p>
        <div className="mt-3 rounded-lg border border-zinc-200 bg-zinc-50/80 px-3 py-2.5 dark:border-zinc-700 dark:bg-zinc-900/40">
          <p className="mb-2 text-xs font-medium text-zinc-700 dark:text-zinc-300">
            Run all fixes runs these seven steps in order:
          </p>
          <ol className="list-decimal space-y-1 pl-5 text-xs text-zinc-600 dark:text-zinc-400">
            {RUN_ALL_FIXES_DESCRIPTIONS.map((line, i) => (
              <li key={i}>{line}</li>
            ))}
          </ol>
        </div>
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
            onClick={() => void runAllFixes()}
            disabled={fixRunning !== null || allFixesRunning}
            className="rounded bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
          >
            {allFixesRunning ? 'Running all…' : 'Run all fixes'}
          </button>
        </div>

        {batchRunUi && (
          <div className="mt-4 rounded-lg border border-indigo-200 bg-white p-4 dark:border-indigo-900 dark:bg-zinc-900">
            <p className="mb-3 text-xs font-medium text-indigo-900 dark:text-indigo-200">
              Run all fixes — progress
            </p>
            <div className="flex flex-wrap items-start justify-center gap-3 sm:justify-start">
              {batchRunUi.steps.map((s) => {
                const doneOk = s.phase === 'done' && s.outcome?.ok;
                const doneFail = s.phase === 'done' && s.outcome && !s.outcome.ok;
                const circleClass =
                  s.phase === 'pending'
                    ? 'border-zinc-300 bg-zinc-50 text-zinc-400 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-500'
                    : s.phase === 'running'
                      ? 'border-indigo-500 bg-indigo-50 text-indigo-700 ring-2 ring-indigo-300 dark:border-indigo-400 dark:bg-indigo-950/50 dark:text-indigo-200 dark:ring-indigo-800'
                      : doneOk
                        ? 'border-emerald-500 bg-emerald-500 text-white dark:border-emerald-600 dark:bg-emerald-600'
                        : doneFail
                          ? 'border-red-500 bg-red-500 text-white dark:border-red-600 dark:bg-red-600'
                          : 'border-zinc-300 bg-zinc-50 text-zinc-600';
                return (
                  <div
                    key={s.index}
                    className="flex w-[4.75rem] shrink-0 flex-col items-center gap-1.5"
                    title={s.outcome ? summarizeBatchStep(s.outcome) : s.label}
                  >
                    <div
                      className={`flex h-10 w-10 items-center justify-center rounded-full border-2 text-sm font-bold transition-colors ${circleClass}`}
                      aria-label={
                        s.phase === 'done'
                          ? `${s.index + 1}: ${s.outcome?.ok ? 'completed' : 'failed'}`
                          : s.phase === 'running'
                            ? `${s.index + 1}: running`
                            : `${s.index + 1}: pending`
                      }
                    >
                      {s.phase === 'done' ? (s.outcome?.ok ? '✓' : '✕') : s.index + 1}
                    </div>
                    <span className="line-clamp-2 text-center text-[10px] leading-tight text-zinc-600 dark:text-zinc-400">
                      {s.label}
                    </span>
                  </div>
                );
              })}
            </div>
            {(() => {
              const cur = batchRunUi.steps.find((s) => s.phase === 'running');
              if (!cur) return null;
              return (
                <p className="mt-3 text-xs text-indigo-700 dark:text-indigo-300">
                  Running step {cur.index + 1} of {batchRunUi.steps.length}:{' '}
                  <span className="font-medium">{cur.label}</span>…
                </p>
              );
            })()}
            {batchRunUi.steps.every((s) => s.phase === 'done') && (
              <ul className="mt-4 space-y-1.5 border-t border-zinc-200 pt-3 text-xs text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                {batchRunUi.steps.map((s) => (
                  <li key={s.index} className="flex gap-2">
                    <span
                      className={`shrink-0 font-mono tabular-nums font-semibold ${
                        s.outcome?.ok
                          ? 'text-emerald-700 dark:text-emerald-400'
                          : 'text-red-600 dark:text-red-400'
                      }`}
                    >
                      {s.index + 1}.
                    </span>
                    <span>
                      {s.outcome ? (
                        <>
                          <span className="font-medium">{s.outcome.step}:</span>{' '}
                          {summarizeBatchStep(s.outcome)}
                          {s.outcome.logid ? (
                            <span className="ml-1 font-mono text-zinc-500 dark:text-zinc-500">
                              log {s.outcome.logid}
                            </span>
                          ) : null}
                        </>
                      ) : (
                        '—'
                      )}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}

        <p className="mt-2 text-xs text-zinc-500 dark:text-zinc-400">
          Each batch step writes one <code className="rounded bg-zinc-100 px-0.5 dark:bg-zinc-800">tbl_logs</code> row (
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

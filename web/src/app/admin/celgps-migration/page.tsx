'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { GEODATA_DB_CONNECTION_CHANGED } from '@/lib/db-connection-events';

type LogLevel = 'info' | 'warn' | 'error' | 'success';
type DbTarget = 'cloudsql' | 'supabase';

interface DebugLogEntry {
  id: number;
  ts: string;
  level: LogLevel;
  message: string;
}

interface ConnectionSummary {
  label?: string;
  host: string;
  port: number;
  user: string;
  database: string;
  source: string;
  maskedUrl: string;
  error?: string;
  hint?: string;
}

interface PreflightResponse {
  ok: boolean;
  error?: string;
  hint?: string | null;
  envFilePath?: string;
  envFileExists?: boolean;
  cli?: {
    psql: { found: boolean; path: string | null; version: string | null };
    pg_dump: { found: boolean; path: string | null; version: string | null };
  };
  connection?: ConnectionSummary | null;
  cloudSql?: ConnectionSummary | null;
  dbTarget?: DbTarget;
  targetFile?: { path: string; exists: boolean; target: DbTarget };
  activeConnection?: ConnectionSummary | null;
  note?: string;
}

interface TestConnectionResponse {
  ok: boolean;
  error?: string;
  hint?: string | null;
  test?: {
    ok: boolean;
    method: string;
    version: string | null;
    currentUser: string | null;
    currentDatabase: string | null;
    durationMs: number;
    error: string | null;
    hint: string | null;
    psqlStdout: string | null;
  };
}

interface DbTargetResponse {
  ok: boolean;
  target: DbTarget;
  targetFile?: { path: string; exists: boolean; target: DbTarget };
  activeConnection?: ConnectionSummary | null;
  activeError?: string | null;
  message?: string;
  error?: string;
  hint?: string | null;
  verify?: {
    cloudsql: { connected: boolean; error: string | null; durationMs: number; database: string | null; label: string | null; host: string | null };
    supabase: { connected: boolean; error: string | null; durationMs: number; database: string | null; label: string | null; host: string | null };
  };
}

interface CompareRow {
  tableName: string;
  cloudSqlCount: number | null;
  supabaseCount: number | null;
  match: boolean;
  cloudSqlError: string | null;
  supabaseError: string | null;
}

interface CompareResponse {
  ok: boolean;
  error?: string | null;
  durationMs?: number;
  rows?: CompareRow[];
  summary?: {
    tableCount: number;
    matching: number;
    mismatched: number;
    cloudSqlErrors: number;
    supabaseErrors: number;
  };
  cloudSql?: ConnectionSummary | null;
  supabase?: ConnectionSummary | null;
}

interface GeomTestCheck {
  name: string;
  ok: boolean;
  detail: string;
  ms: number;
}

interface FenceSampleRow {
  trackingId: number;
  deviceName: string | null;
  positionTimeNz: string | null;
  lat: string | null;
  lon: string | null;
  storedGeofenceId: number | null;
  storedFenceName: string | null;
  computedGeofenceId: number | null;
  computedFenceName: string | null;
  withinStoredFence: boolean;
  matchesComputed: boolean;
}

interface GeomTestResponse {
  ok: boolean;
  error?: string | null;
  durationMs?: number;
  probeDate?: string | null;
  checks?: GeomTestCheck[];
  fenceSamples?: FenceSampleRow[];
  connection?: ConnectionSummary | null;
}

function nowTs(): string {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

function levelClass(level: LogLevel): string {
  switch (level) {
    case 'success':
      return 'text-emerald-700 dark:text-emerald-400';
    case 'warn':
      return 'text-amber-700 dark:text-amber-400';
    case 'error':
      return 'text-red-700 dark:text-red-400';
    default:
      return 'text-zinc-700 dark:text-zinc-300';
  }
}

function formatCount(n: number | null, error: string | null): string {
  if (error && error !== 'Table missing') return '—';
  if (error === 'Table missing') return '—';
  if (n == null) return '—';
  return n.toLocaleString();
}

export default function CelgpsMigrationPage() {
  const [logs, setLogs] = useState<DebugLogEntry[]>([]);
  const logSeqRef = useRef(0);
  const [preflight, setPreflight] = useState<PreflightResponse | null>(null);
  const [preflightLoading, setPreflightLoading] = useState(true);
  const [testing, setTesting] = useState(false);
  const [lastTest, setLastTest] = useState<TestConnectionResponse | null>(null);
  const [dbTarget, setDbTarget] = useState<DbTarget>('cloudsql');
  const [dbVerify, setDbVerify] = useState<DbTargetResponse['verify'] | null>(null);
  const [switchingTarget, setSwitchingTarget] = useState(false);
  const [compareResult, setCompareResult] = useState<CompareResponse | null>(null);
  const [comparing, setComparing] = useState(false);
  const [compareFilter, setCompareFilter] = useState<'all' | 'mismatch'>('all');
  const [geomTesting, setGeomTesting] = useState(false);
  const [geomResult, setGeomResult] = useState<GeomTestResponse | null>(null);
  const [geomProbeDate, setGeomProbeDate] = useState('');

  const appendLog = useCallback((level: LogLevel, message: string) => {
    logSeqRef.current += 1;
    const id = logSeqRef.current;
    setLogs((prev) => [...prev.slice(-199), { id, ts: nowTs(), level, message }]);
  }, []);

  const loadDbTarget = useCallback(async (withVerify = false) => {
    try {
      const url = withVerify
        ? '/api/celgps-migration/db-target?verify=1'
        : '/api/celgps-migration/db-target';
      const res = await fetch(url, { cache: 'no-store' });
      const data = (await res.json()) as DbTargetResponse;
      if (data.target) setDbTarget(data.target);
      if (data.verify) setDbVerify(data.verify);
      else if (!withVerify) setDbVerify(null);
      return data;
    } catch {
      return null;
    }
  }, []);

  const loadPreflight = useCallback(async () => {
    setPreflightLoading(true);
    appendLog('info', 'Running preflight…');
    try {
      const res = await fetch('/api/celgps-migration/preflight');
      const data = (await res.json()) as PreflightResponse;
      setPreflight(data);
      if (data.dbTarget) setDbTarget(data.dbTarget);
      if (data.ok) {
        appendLog('success', `Supabase config OK. Active app DB: ${data.dbTarget ?? 'cloudsql'}.`);
      } else {
        appendLog('error', data.error ?? 'Preflight failed.');
        if (data.hint) appendLog('info', data.hint);
      }
    } catch (e) {
      appendLog('error', e instanceof Error ? e.message : String(e));
      setPreflight(null);
    } finally {
      setPreflightLoading(false);
    }
  }, [appendLog]);

  useEffect(() => {
    loadPreflight();
    loadDbTarget();
  }, [loadPreflight, loadDbTarget]);

  const runTestConnection = async () => {
    setTesting(true);
    setLastTest(null);
    appendLog('info', 'Testing Supabase connection…');
    try {
      const res = await fetch('/api/celgps-migration/test-connection', { method: 'POST' });
      const data = (await res.json()) as TestConnectionResponse;
      setLastTest(data);
      if (data.ok && data.test) {
        appendLog('success', `Supabase OK (${data.test.durationMs}ms) — ${data.test.currentUser}@${data.test.currentDatabase}`);
      } else {
        appendLog('error', data.test?.error ?? data.error ?? 'Connection test failed.');
        if (data.test?.hint ?? data.hint) appendLog('warn', (data.test?.hint ?? data.hint) as string);
      }
    } catch (e) {
      appendLog('error', e instanceof Error ? e.message : String(e));
    } finally {
      setTesting(false);
    }
  };

  const switchDbTarget = async (target: DbTarget) => {
    if (target === dbTarget) return;
    setSwitchingTarget(true);
    appendLog('info', `Switching app DB to ${target === 'supabase' ? 'Supabase' : 'Google Cloud SQL'}…`);
    try {
      const res = await fetch('/api/celgps-migration/db-target', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target }),
      });
      const data = (await res.json()) as DbTargetResponse;
      if (data.ok) {
        setDbTarget(data.target);
        appendLog('success', data.message ?? `Switched to ${data.target}.`);
        window.dispatchEvent(new Event(GEODATA_DB_CONNECTION_CHANGED));
        await Promise.all([loadPreflight(), loadDbTarget()]);
      } else {
        appendLog('error', data.error ?? 'Failed to switch DB target.');
        if (data.hint) appendLog('warn', data.hint);
        await loadDbTarget();
      }
    } catch (e) {
      appendLog('error', e instanceof Error ? e.message : String(e));
    } finally {
      setSwitchingTarget(false);
    }
  };

  const runCompare = async () => {
    setComparing(true);
    setCompareResult(null);
    appendLog('info', 'Starting table-by-table row count cross-check (may take several minutes)…');
    try {
      const res = await fetch('/api/celgps-migration/compare-tables', { method: 'POST' });
      const data = (await res.json()) as CompareResponse;
      setCompareResult(data);
      if (data.ok && data.summary) {
        appendLog(
          'success',
          `Cross-check done in ${((data.durationMs ?? 0) / 1000).toFixed(1)}s — ${data.summary.matching}/${data.summary.tableCount} tables match.`
        );
        if (data.summary.mismatched > 0) {
          appendLog('warn', `${data.summary.mismatched} table(s) have different row counts or are missing.`);
        }
      } else {
        appendLog('error', data.error ?? 'Cross-check failed.');
      }
    } catch (e) {
      appendLog('error', e instanceof Error ? e.message : String(e));
    } finally {
      setComparing(false);
    }
  };

  const runTestGeom = async () => {
    setGeomTesting(true);
    setGeomResult(null);
    appendLog('info', 'Running Supabase geom tests (read-only)…');
    try {
      const body = geomProbeDate.trim() ? { probeDate: geomProbeDate.trim() } : {};
      const res = await fetch('/api/celgps-migration/test-geom', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = (await res.json()) as GeomTestResponse;
      setGeomResult(data);
      if (data.probeDate && !geomProbeDate.trim()) setGeomProbeDate(data.probeDate);
      const passed = data.checks?.filter((c) => c.ok).length ?? 0;
      const total = data.checks?.length ?? 0;
      if (data.ok) {
        appendLog('success', `Geom tests passed (${passed}/${total}) in ${((data.durationMs ?? 0) / 1000).toFixed(1)}s.`);
      } else {
        appendLog('error', data.error ?? `Geom tests failed (${passed}/${total} passed).`);
      }
    } catch (e) {
      appendLog('error', e instanceof Error ? e.message : String(e));
    } finally {
      setGeomTesting(false);
    }
  };

  const filteredCompareRows = useMemo(() => {
    const rows = compareResult?.rows ?? [];
    if (compareFilter === 'mismatch') return rows.filter((r) => !r.match);
    return rows;
  }, [compareResult?.rows, compareFilter]);

  return (
    <div className="w-full min-w-0 p-6">
      <h1 className="mb-2 text-2xl font-semibold text-zinc-900 dark:text-zinc-100">
        CELGPS → Supabase migration
      </h1>
      <p className="mb-6 text-sm text-zinc-500 dark:text-zinc-400">
        Verify Supabase connectivity, cross-check row counts against Google Cloud SQL, and toggle which database
        the app uses. Credentials stay server-side in{' '}
        <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">.env.local</code>.
      </p>

      {/* DB target toggle */}
      <section className="mb-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
          Application DB connection
        </h2>
        <p className="mb-4 text-sm text-zinc-600 dark:text-zinc-400">
          Switches only after a live connection test (5s timeout). Google Cloud SQL is rejected if the instance is stopped.
        </p>
        {dbVerify && (
          <div className="mb-4 grid gap-2 sm:grid-cols-2">
            {(['cloudsql', 'supabase'] as const).map((t) => {
              const row = dbVerify[t];
              return (
                <div
                  key={t}
                  className={`rounded border p-3 text-xs ${
                    row.connected
                      ? 'border-emerald-300 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950/30'
                      : 'border-red-300 bg-red-50 dark:border-red-800 dark:bg-red-950/30'
                  }`}
                >
                  <div className="font-semibold">{row.label ?? t}</div>
                  <div className="text-zinc-600 dark:text-zinc-400">
                    {row.database} @ {row.host}
                  </div>
                  <div className={row.connected ? 'text-emerald-700' : 'text-red-700'}>
                    {row.connected ? `Connected (${row.durationMs}ms)` : `Not connected (${row.durationMs}ms)`}
                  </div>
                  {!row.connected && row.error && <div className="mt-1 text-red-800">{row.error}</div>}
                </div>
              );
            })}
          </div>
        )}
        <div className="mb-4 flex flex-wrap gap-2">
          <button
            type="button"
            disabled={switchingTarget}
            onClick={() => switchDbTarget('cloudsql')}
            className={`rounded px-4 py-2 text-sm font-medium ${
              dbTarget === 'cloudsql'
                ? 'bg-zinc-900 text-white dark:bg-zinc-100 dark:text-zinc-900'
                : 'border border-zinc-300 text-zinc-700 hover:bg-zinc-50 dark:border-zinc-600 dark:text-zinc-300 dark:hover:bg-zinc-800'
            }`}
          >
            Google Cloud SQL
          </button>
          <button
            type="button"
            disabled={switchingTarget}
            onClick={() => switchDbTarget('supabase')}
            className={`rounded px-4 py-2 text-sm font-medium ${
              dbTarget === 'supabase'
                ? 'bg-zinc-900 text-white dark:bg-zinc-100 dark:text-zinc-900'
                : 'border border-zinc-300 text-zinc-700 hover:bg-zinc-50 dark:border-zinc-600 dark:text-zinc-300 dark:hover:bg-zinc-800'
            }`}
          >
            Supabase
          </button>
          <button
            type="button"
            disabled={switchingTarget}
            onClick={() => loadDbTarget(true)}
            className="rounded border border-zinc-300 px-4 py-2 text-sm text-zinc-700 dark:border-zinc-600 dark:text-zinc-300"
          >
            Re-test connections
          </button>
        </div>
        {preflight?.activeConnection && (
          <div className="rounded border border-zinc-200 p-3 text-xs dark:border-zinc-700">
            <div className="font-medium text-zinc-700 dark:text-zinc-300">
              Active: {preflight.activeConnection.label ?? dbTarget} — {preflight.activeConnection.host} /{' '}
              {preflight.activeConnection.database}
            </div>
            <div className="mt-1 break-all font-mono text-zinc-500">{preflight.activeConnection.maskedUrl}</div>
          </div>
        )}
      </section>

      {/* Cross-check */}
      <section className="mb-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
          Cross-check — table row counts
        </h2>
        <p className="mb-4 text-sm text-zinc-600 dark:text-zinc-400">
          Scans every public table on both databases and compares exact <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">COUNT(*)</code> values.
          Cloud SQL uses <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">CELGPS_CLOUD_SQL_DATABASE_URL</code> or{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">DATABASE_URL</code>.
        </p>
        <div className="mb-4 flex flex-wrap gap-2">
          <button
            type="button"
            onClick={runCompare}
            disabled={comparing}
            className="rounded bg-zinc-900 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-800 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
          >
            {comparing ? 'Scanning tables…' : 'Run cross-check'}
          </button>
          {compareResult?.summary && (
            <select
              value={compareFilter}
              onChange={(e) => setCompareFilter(e.target.value as 'all' | 'mismatch')}
              className="rounded border border-zinc-300 px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            >
              <option value="all">All tables ({compareResult.summary.tableCount})</option>
              <option value="mismatch">Mismatches only ({compareResult.summary.mismatched})</option>
            </select>
          )}
        </div>

        {compareResult?.summary && (
          <div className="mb-4 flex flex-wrap gap-4 text-sm">
            <span className="text-emerald-600">{compareResult.summary.matching} matching</span>
            <span className={compareResult.summary.mismatched > 0 ? 'text-amber-600' : 'text-zinc-500'}>
              {compareResult.summary.mismatched} mismatched
            </span>
            <span className="text-zinc-500">{((compareResult.durationMs ?? 0) / 1000).toFixed(1)}s</span>
          </div>
        )}

        {compareResult?.ok && filteredCompareRows.length > 0 && (
          <div className="max-h-[28rem] overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
            <table className="w-full min-w-[32rem] text-left text-xs">
              <thead className="sticky top-0 bg-zinc-100 dark:bg-zinc-800">
                <tr>
                  <th className="px-3 py-2 font-semibold">Table</th>
                  <th className="px-3 py-2 font-semibold text-right">Google Cloud SQL</th>
                  <th className="px-3 py-2 font-semibold text-right">Supabase</th>
                  <th className="px-3 py-2 font-semibold">Status</th>
                </tr>
              </thead>
              <tbody>
                {filteredCompareRows.map((row) => (
                  <tr
                    key={row.tableName}
                    className={`border-t border-zinc-200 dark:border-zinc-700 ${
                      row.match ? '' : 'bg-amber-50/80 dark:bg-amber-950/20'
                    }`}
                  >
                    <td className="px-3 py-1.5 font-mono">{row.tableName}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums">
                      {formatCount(row.cloudSqlCount, row.cloudSqlError)}
                    </td>
                    <td className="px-3 py-1.5 text-right tabular-nums">
                      {formatCount(row.supabaseCount, row.supabaseError)}
                    </td>
                    <td className="px-3 py-1.5">
                      {row.match ? (
                        <span className="text-emerald-600">Match</span>
                      ) : (
                        <span className="text-amber-700 dark:text-amber-400" title={[row.cloudSqlError, row.supabaseError].filter(Boolean).join(' · ')}>
                          {row.cloudSqlError === 'Table missing'
                            ? 'Missing on Cloud SQL'
                            : row.supabaseError === 'Table missing'
                              ? 'Missing on Supabase'
                              : row.cloudSqlCount !== row.supabaseCount
                                ? 'Count differs'
                                : 'Mismatch'}
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {compareResult && !compareResult.ok && (
          <p className="text-sm text-red-700 dark:text-red-400">{compareResult.error}</p>
        )}
      </section>

      {/* Test Geom */}
      <section className="mb-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
          Test Geom — Supabase geo calcs
        </h2>
        <p className="mb-4 text-sm text-zinc-600 dark:text-zinc-400">
          Read-only checks against <strong>Supabase</strong>: PostGIS version,{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">store_*</code> functions, geofence/tracking
          geometries, and an <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">ST_Within</code> containment
          probe (same pattern as <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">store_fences_for_date</code>),
          plus a <strong>real fence sample</strong> from <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">tbl_tracking</code>{' '}
          (stored geofence vs ST_Within recompute). Does not run{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-700">store_fences()</code> or modify data.
        </p>
        <div className="mb-4 flex flex-wrap items-end gap-3">
          <div>
            <label className="mb-1 block text-xs font-medium text-zinc-600 dark:text-zinc-400">
              Probe date (optional)
            </label>
            <input
              type="date"
              value={geomProbeDate}
              onChange={(e) => setGeomProbeDate(e.target.value)}
              className="rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            />
          </div>
          <button
            type="button"
            onClick={runTestGeom}
            disabled={geomTesting}
            className="rounded bg-zinc-900 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-800 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
          >
            {geomTesting ? 'Testing geom…' : 'Test Geom'}
          </button>
        </div>

        {geomResult?.checks && geomResult.checks.length > 0 && (
          <div className="overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
            <table className="w-full min-w-[28rem] text-left text-xs">
              <thead className="bg-zinc-100 dark:bg-zinc-800">
                <tr>
                  <th className="px-3 py-2 font-semibold">Check</th>
                  <th className="px-3 py-2 font-semibold">Result</th>
                  <th className="px-3 py-2 font-semibold text-right">ms</th>
                </tr>
              </thead>
              <tbody>
                {geomResult.checks.map((check, index) => (
                  <tr
                    key={`${check.name}-${index}`}
                    className={`border-t border-zinc-200 dark:border-zinc-700 ${
                      check.ok ? '' : 'bg-amber-50/80 dark:bg-amber-950/20'
                    }`}
                  >
                    <td className="px-3 py-1.5 font-medium">{check.name}</td>
                    <td className="px-3 py-1.5">
                      <span className={check.ok ? 'text-emerald-600' : 'text-amber-700 dark:text-amber-400'}>
                        {check.ok ? 'Pass' : 'Fail'}
                      </span>
                      <span className="ml-2 text-zinc-600 dark:text-zinc-400">{check.detail}</span>
                    </td>
                    <td className="px-3 py-1.5 text-right tabular-nums text-zinc-500">{check.ms}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {geomResult?.fenceSamples && geomResult.fenceSamples.length > 0 && (
          <div className="mt-4">
            <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
              Real fence samples (tbl_tracking)
            </h3>
            <div className="overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
              <table className="w-full min-w-[40rem] text-left text-xs">
                <thead className="bg-zinc-100 dark:bg-zinc-800">
                  <tr>
                    <th className="px-3 py-2 font-semibold">Tracking id</th>
                    <th className="px-3 py-2 font-semibold">Device</th>
                    <th className="px-3 py-2 font-semibold">Time (NZ)</th>
                    <th className="px-3 py-2 font-semibold">Stored fence</th>
                    <th className="px-3 py-2 font-semibold">Computed fence</th>
                    <th className="px-3 py-2 font-semibold">ST_Within stored</th>
                    <th className="px-3 py-2 font-semibold">Match</th>
                  </tr>
                </thead>
                <tbody>
                  {geomResult.fenceSamples.map((row) => (
                    <tr
                      key={row.trackingId}
                      className={`border-t border-zinc-200 dark:border-zinc-700 ${
                        row.withinStoredFence && row.matchesComputed
                          ? ''
                          : 'bg-amber-50/80 dark:bg-amber-950/20'
                      }`}
                    >
                      <td className="px-3 py-1.5 font-mono">{row.trackingId}</td>
                      <td className="px-3 py-1.5">{row.deviceName ?? '—'}</td>
                      <td className="px-3 py-1.5 whitespace-nowrap">{row.positionTimeNz ?? '—'}</td>
                      <td className="px-3 py-1.5">
                        {row.storedFenceName ?? '—'}
                        {row.storedGeofenceId != null && (
                          <span className="ml-1 text-zinc-400">#{row.storedGeofenceId}</span>
                        )}
                      </td>
                      <td className="px-3 py-1.5">
                        {row.computedFenceName ?? '—'}
                        {row.computedGeofenceId != null && (
                          <span className="ml-1 text-zinc-400">#{row.computedGeofenceId}</span>
                        )}
                      </td>
                      <td className="px-3 py-1.5">{row.withinStoredFence ? 'Yes' : 'No'}</td>
                      <td className="px-3 py-1.5">
                        {row.withinStoredFence && row.matchesComputed ? (
                          <span className="text-emerald-600">Yes</span>
                        ) : (
                          <span className="text-amber-700 dark:text-amber-400">No</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {geomResult && !geomResult.ok && !geomResult.checks?.length && (
          <p className="text-sm text-red-700 dark:text-red-400">{geomResult.error}</p>
        )}
      </section>

      {/* Step 1 connection test */}
      <section className="mb-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
          Supabase connection test
        </h2>

        {preflightLoading && <p className="text-sm text-zinc-500">Loading…</p>}

        {!preflightLoading && preflight?.connection && (
          <div className="space-y-4 text-sm">
            <div className="rounded border border-zinc-200 p-3 text-xs dark:border-zinc-700">
              <div className="font-medium">{preflight.connection.host}:{preflight.connection.port}</div>
              <div className="font-mono text-zinc-500">{preflight.connection.maskedUrl}</div>
            </div>
            <button
              type="button"
              onClick={runTestConnection}
              disabled={testing || !preflight.ok}
              className="rounded bg-zinc-900 px-4 py-2 text-sm font-medium text-white disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
            >
              {testing ? 'Testing…' : 'Test Supabase connection'}
            </button>
            {lastTest?.ok && lastTest.test && (
              <div className="rounded border border-emerald-200 bg-emerald-50 p-3 text-xs text-emerald-900 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-200">
                Connected — {lastTest.test.version?.split('\n')[0]}
              </div>
            )}
          </div>
        )}
      </section>

      <section className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
          Debug log
        </h2>
        <div className="max-h-64 overflow-y-auto rounded border border-zinc-200 bg-zinc-50 p-2 font-mono text-xs dark:border-zinc-700 dark:bg-zinc-950">
          {logs.length === 0 ? (
            <div className="text-zinc-400">No log entries yet.</div>
          ) : (
            logs.map((entry, index) => (
              <div key={`${entry.id}-${index}`} className={`mb-1 ${levelClass(entry.level)}`}>
                <span className="text-zinc-400">[{entry.ts}]</span>{' '}
                <span className="uppercase">[{entry.level}]</span> {entry.message}
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}

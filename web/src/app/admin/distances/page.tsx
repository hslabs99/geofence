'use client';

import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import {
  GPS_HARVEST_DEFAULT_MAX_JOBS,
  GPS_HARVEST_DEFAULT_MAX_SUCCESSES,
  GPS_HARVEST_SQL_JOB_LIMIT,
} from '@/lib/gps-harvest-constants';

type DistanceRow = {
  id: number;
  delivery_winery: string;
  vineyard_name: string;
  distance_m: number | null;
  duration_min: string | null;
  /** tbl_vworkjobs rows matching this pair (from list API; not stored on tbl_distances). */
  vwork_job_count?: number | null;
  gps_sample_count?: number | null;
  gps_attempt_count?: number | null;
  gps_avg_distance_m?: number | null;
  gps_avg_duration_min?: string | null;
};

type GpsMappingRow = { vwname: string | null; gpsname: string | null };

type ListResponse = {
  wineryOptions: string[];
  vineyardOptions: string[];
  rows: DistanceRow[];
  gpsWineryMappings?: Record<string, GpsMappingRow[]>;
  gpsVineyardMappings?: Record<string, GpsMappingRow[]>;
};

type DistancesSortKey = 'winery' | 'vineyard' | 'vwork_jobs' | 'meters' | 'minutes' | 'gps_ok';

/** m and min still unset or zero (seed defaults / nothing useful stored yet). */
function rowHasNoDistanceData(row: DistanceRow): boolean {
  const m = row.distance_m;
  const noM = m == null || m === 0;
  const t = row.duration_min;
  if (t == null || String(t).trim() === '') return noM;
  const n = Number(t);
  if (Number.isNaN(n)) return noM;
  return noM && n === 0;
}

function durationMinSortValue(row: DistanceRow): number | null {
  const t = row.duration_min;
  if (t == null || String(t).trim() === '') return null;
  const n = Number(t);
  return Number.isNaN(n) ? null : n;
}

function gpsOkCountForSort(
  row: DistanceRow,
  dryById: Record<number, { successCount: number }>
): number {
  const p = dryById[row.id];
  if (p != null) return p.successCount;
  return row.gps_sample_count ?? 0;
}

function DistancesSortHeader({
  label,
  columnKey,
  sortKey,
  sortDir,
  onSort,
  className = '',
  title,
  center = false,
}: {
  label: string;
  columnKey: DistancesSortKey;
  sortKey: DistancesSortKey;
  sortDir: 'asc' | 'desc';
  onSort: (k: DistancesSortKey) => void;
  className?: string;
  title?: string;
  center?: boolean;
}) {
  const active = sortKey === columnKey;
  return (
    <th className={`px-2 py-2 ${center ? 'text-center' : ''} ${className}`} title={title}>
      <button
        type="button"
        onClick={() => onSort(columnKey)}
        className={`flex w-full items-center gap-0.5 font-semibold hover:text-zinc-950 dark:hover:text-zinc-100 ${
          center ? 'justify-center' : 'justify-start'
        }`}
      >
        <span>{label}</span>
        {active && (
          <span className="font-mono text-zinc-500" aria-hidden>
            {sortDir === 'asc' ? '↑' : '↓'}
          </span>
        )}
      </button>
    </th>
  );
}

function GpsMappingHint({
  kind,
  vworkName,
  maps,
}: {
  kind: 'Winery' | 'Vineyard';
  vworkName: string;
  maps: GpsMappingRow[];
}) {
  return (
    <div className="mt-1 space-y-0.5 border-t border-zinc-100 pt-1 text-[10px] leading-snug text-zinc-500 dark:border-zinc-800 dark:text-zinc-400">
      <div>
        <span className="font-medium text-zinc-600 dark:text-zinc-300">{kind} fence list includes</span>{' '}
        <span className="text-zinc-800 dark:text-zinc-200">{vworkName.trim() || '—'}</span>
        <span className="text-zinc-400"> (vWork name)</span>
      </div>
      {maps.length === 0 ? (
        <div className="italic text-zinc-400">No extra rows in tbl_gpsmappings for this name</div>
      ) : (
        maps.map((m, i) => (
          <div key={i}>
            <span className="text-zinc-500">tbl_gpsmappings:</span>{' '}
            <span className="text-zinc-700 dark:text-zinc-300">{(m.vwname ?? '').trim() || '—'}</span>
            {' → '}
            <span className="text-zinc-700 dark:text-zinc-300">{(m.gpsname ?? '').trim() || '—'}</span>
          </div>
        ))
      )}
    </div>
  );
}

type GpsSampleRow = {
  id: string;
  job_id: string;
  outcome: string;
  failure_reason: string | null;
  winery_tracking_id: string | null;
  vineyard_tracking_id: string | null;
  meters: number | null;
  minutes: number | null;
  segment_point_count: number | null;
  debug_json: unknown;
  run_index: number | null;
  created_at: string;
  updated_at: string;
};

type HarvestAttempt = {
  job_id: string;
  ok: boolean;
  outcome?: string;
  skipReason?: string;
  meters?: number;
  minutes?: number;
  segment_point_count?: number;
  debug?: Record<string, unknown>;
};

type HarvestCandidateJob = {
  job_id: string;
  actual_start_time: string | null;
  worker: string | null;
};

/** Structured dry-run result for the debug popup (no raw JSON). */
type DryRunDebugSnapshot = {
  distanceId: number;
  distancePair: { delivery_winery: string; vineyard_name: string };
  vworkJobsMatchCount: number;
  candidateJobs: HarvestCandidateJob[];
  jobsPolled: number;
  successCount: number;
  failedCount: number;
  maxJobsCap: number;
  maxSuccessesCap: number;
  message?: string;
  attempts: HarvestAttempt[];
};

function StructuredDebugValue({
  value,
  depth = 0,
  maxDepth = 8,
}: {
  value: unknown;
  depth?: number;
  maxDepth?: number;
}): ReactNode {
  if (depth > maxDepth) return <span className="text-zinc-400">…</span>;
  if (value === null || value === undefined) {
    return <span className="text-zinc-400">—</span>;
  }
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return <span className="break-all font-mono text-[11px]">{String(value)}</span>;
  }
  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="text-zinc-400">[]</span>;
    return (
      <ul className="ml-1 list-disc space-y-0.5 border-l border-zinc-200 pl-2 dark:border-zinc-600">
        {value.map((v, i) => (
          <li key={i} className="text-[11px]">
            <StructuredDebugValue value={v} depth={depth + 1} maxDepth={maxDepth} />
          </li>
        ))}
      </ul>
    );
  }
  if (typeof value === 'object') {
    const o = value as Record<string, unknown>;
    const keys = Object.keys(o);
    if (keys.length === 0) return <span className="text-zinc-400">{'{}'}</span>;
    return (
      <div className="space-y-0.5 border-l border-zinc-200 pl-2 text-[11px] dark:border-zinc-600">
        {keys.map((k) => (
          <div key={k}>
            <span className="font-medium text-zinc-600 dark:text-zinc-300">{k}</span>
            <span className="text-zinc-400">: </span>
            <StructuredDebugValue value={o[k]} depth={depth + 1} maxDepth={maxDepth} />
          </div>
        ))}
      </div>
    );
  }
  return <span className="font-mono text-[11px]">{String(value)}</span>;
}

function formatDryRunSummaryText(s: DryRunDebugSnapshot): string {
  const lines: string[] = [];
  lines.push(`Dry run — distance id ${s.distanceId}`);
  lines.push(`${s.distancePair.delivery_winery} → ${s.distancePair.vineyard_name}`);
  lines.push(`vWork jobs matching pair: ${s.vworkJobsMatchCount}`);
  lines.push(
    `Caps: ${s.maxSuccessesCap} success(es) max, ${s.maxJobsCap} job(s) max — polled ${s.jobsPolled}: ${s.successCount} OK, ${s.failedCount} failed`
  );
  if (s.message) lines.push(`Note: ${s.message}`);
  lines.push('');
  for (const a of s.attempts) {
    if (a.ok) {
      lines.push(
        `Job ${a.job_id}: OK — Metres ${a.meters ?? '?'}, Minutes ${typeof a.minutes === 'number' ? a.minutes.toFixed(2) : '?'}, ${a.segment_point_count ?? '?'} pts on path`
      );
    } else {
      lines.push(`Job ${a.job_id}: Fail — ${a.skipReason ?? 'unknown'}`);
    }
  }
  const av = dryRunAveragesFromAttempts(s.attempts);
  const nOk = s.attempts.filter((a) => a.ok === true || a.outcome === 'success').length;
  if (nOk > 0 && (av.avgMeters != null || av.avgMinutes != null)) {
    lines.push('');
    lines.push(
      `Mean (of ${nOk} successful job${nOk === 1 ? '' : 's'} only): ` +
        [
          av.avgMeters != null ? `${Math.round(av.avgMeters)} m` : null,
          av.avgMinutes != null ? `${av.avgMinutes.toFixed(2)} min` : null,
        ]
          .filter(Boolean)
          .join(', ')
    );
  }
  return lines.join('\n');
}

function DryRunDebugModalBody({ snapshot }: { snapshot: DryRunDebugSnapshot }) {
  const workerByJobId = useMemo(() => {
    const m = new Map<string, string | null>();
    for (const c of snapshot.candidateJobs) m.set(String(c.job_id), c.worker);
    return m;
  }, [snapshot.candidateJobs]);

  return (
    <div className="space-y-4 text-sm">
      <div className="rounded-md border border-zinc-200 bg-zinc-50/80 p-3 dark:border-zinc-700 dark:bg-zinc-900/50">
        <p className="font-medium text-zinc-900 dark:text-zinc-100">
          <span className="font-mono">{snapshot.distancePair.delivery_winery}</span>
          <span className="text-zinc-500"> → </span>
          <span className="font-mono">{snapshot.distancePair.vineyard_name}</span>
        </p>
        <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
          Jobs in vWork with this pair: <strong>{snapshot.vworkJobsMatchCount}</strong>
          {snapshot.vworkJobsMatchCount > GPS_HARVEST_SQL_JOB_LIMIT
            ? ` (first ${GPS_HARVEST_SQL_JOB_LIMIT} by job_id ascending loaded for harvest)`
            : ''}
        </p>
        <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
          Caps: up to <strong>{snapshot.maxJobsCap}</strong> job(s) tried, stop after{' '}
          <strong>{snapshot.maxSuccessesCap}</strong> success(es). This run:{' '}
          <span className="text-emerald-700 dark:text-emerald-300">{snapshot.successCount} OK</span>,{' '}
          <span className="text-amber-800 dark:text-amber-200">{snapshot.failedCount} failed</span>,{' '}
          <strong>{snapshot.jobsPolled}</strong> polled
        </p>
        {snapshot.message && (
          <p className="mt-2 text-xs text-amber-800 dark:text-amber-200">{snapshot.message}</p>
        )}
        {(() => {
          const okA = snapshot.attempts.filter((a) => a.ok === true || a.outcome === 'success');
          const nOk = okA.length;
          if (nOk === 0) return null;
          const av = dryRunAveragesFromAttempts(snapshot.attempts);
          if (av.avgMeters == null && av.avgMinutes == null) return null;
          return (
            <p className="mt-2 border-t border-zinc-200/80 pt-2 text-xs text-zinc-700 dark:text-zinc-300">
              <span className="font-medium">Mean (of {nOk} successful job{nOk === 1 ? '' : 's'} only):</span>{' '}
              {av.avgMeters != null && <span>{Math.round(av.avgMeters)} m</span>}
              {av.avgMeters != null && av.avgMinutes != null && ' · '}
              {av.avgMinutes != null && <span>{av.avgMinutes.toFixed(2)} min</span>}
            </p>
          );
        })()}
      </div>

      {snapshot.attempts.length > 0 && (
        <div>
          <h4 className="mb-1 text-xs font-semibold uppercase tracking-wide text-zinc-500">Results (polled jobs)</h4>
          <div className="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-700">
            <table className="w-full min-w-[22rem] text-left text-[11px]">
              <thead className="bg-zinc-100 dark:bg-zinc-800">
                <tr>
                  <th className="px-2 py-1">job_id</th>
                  <th className="px-2 py-1">worker</th>
                  <th className="px-2 py-1">metres</th>
                  <th className="px-2 py-1">minutes / fail</th>
                </tr>
              </thead>
              <tbody>
                {snapshot.attempts.map((a, idx) => (
                  <tr key={`${a.job_id}-${idx}`} className="border-t border-zinc-100 dark:border-zinc-700">
                    <td className="px-2 py-0.5 font-mono">{a.job_id}</td>
                    <td className="px-2 py-0.5 font-mono">
                      {workerByJobId.get(String(a.job_id)) ?? '—'}
                    </td>
                    <td className="px-2 py-0.5 font-mono">
                      {a.ok && typeof a.meters === 'number' ? a.meters : a.ok ? '—' : ''}
                    </td>
                    <td className="px-2 py-0.5">
                      {a.ok && typeof a.minutes === 'number' ? (
                        <span className="font-mono">{a.minutes.toFixed(2)}</span>
                      ) : a.ok ? (
                        '—'
                      ) : (
                        <span className="font-medium text-amber-900 dark:text-amber-100">
                          Fail: {a.skipReason ?? 'Unknown reason'}
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div>
        <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500">
          Harvest attempts (order processed)
        </h4>
        <ul className="space-y-3">
          {snapshot.attempts.map((a, idx) => {
            const dbgAll = { ...((a.debug ?? {}) as Record<string, unknown>) };
            const derived = dbgAll.derived_debug;
            delete dbgAll.derived_debug;
            const dbgRest = dbgAll;
            return (
              <li
                key={`${a.job_id}-${idx}`}
                className={`rounded-lg border p-3 ${
                  a.ok
                    ? 'border-emerald-200 bg-emerald-50/50 dark:border-emerald-900 dark:bg-emerald-950/20'
                    : 'border-amber-200 bg-amber-50/50 dark:border-amber-900 dark:bg-amber-950/20'
                }`}
              >
                <div className="flex flex-wrap items-baseline justify-between gap-2">
                  <span className="font-mono text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                    Job {a.job_id}
                  </span>
                  {a.ok ? (
                    <span className="rounded bg-emerald-200 px-2 py-0.5 text-xs font-medium text-emerald-900 dark:bg-emerald-900 dark:text-emerald-100">
                      OK
                    </span>
                  ) : (
                    <span className="rounded bg-amber-200 px-2 py-0.5 text-xs font-medium text-amber-900 dark:bg-amber-950 dark:text-amber-100">
                      Failed
                    </span>
                  )}
                </div>
                {!a.ok && (
                  <div className="mt-2 text-sm">
                    <span className="font-medium text-zinc-600 dark:text-zinc-400">Fail: </span>
                    <span className="font-medium text-amber-900 dark:text-amber-100">
                      {a.skipReason ?? 'Unknown reason'}
                    </span>
                  </div>
                )}
                {a.ok && (
                  <dl className="mt-2 space-y-0.5 text-sm text-zinc-800 dark:text-zinc-200">
                    <div className="flex flex-wrap gap-x-3 gap-y-0.5">
                      <dt className="text-zinc-500">Metres</dt>
                      <dd className="font-mono font-medium">
                        {a.meters != null ? `${a.meters}` : '—'}
                      </dd>
                    </div>
                    <div className="flex flex-wrap gap-x-3 gap-y-0.5">
                      <dt className="text-zinc-500">Minutes</dt>
                      <dd className="font-mono font-medium">
                        {typeof a.minutes === 'number' ? a.minutes.toFixed(2) : '—'}
                      </dd>
                    </div>
                    {a.segment_point_count != null && (
                      <div className="text-xs text-zinc-500">
                        Path: {a.segment_point_count} GPS points between anchors
                      </div>
                    )}
                  </dl>
                )}

                {Object.keys(dbgRest).length > 0 && (
                  <div className="mt-3 border-t border-zinc-200/80 pt-2 dark:border-zinc-700">
                    <p className="mb-1 text-[10px] font-semibold uppercase text-zinc-500">Context</p>
                    <StructuredDebugValue value={dbgRest} maxDepth={7} />
                  </div>
                )}
                {derived != null && (
                  <details className="mt-2 border-t border-zinc-200/80 pt-2 dark:border-zinc-700">
                    <summary className="cursor-pointer text-[11px] font-medium text-zinc-600 dark:text-zinc-400">
                      Derived-steps trace (fence lookups)
                    </summary>
                    <div className="mt-2 max-h-64 overflow-auto rounded bg-zinc-100/80 p-2 dark:bg-zinc-950/80">
                      <StructuredDebugValue value={derived} maxDepth={6} />
                    </div>
                  </details>
                )}
              </li>
            );
          })}
        </ul>
      </div>
    </div>
  );
}

function parseDistanceInput(s: string): number | null | 'invalid' {
  const t = s.trim();
  if (t === '') return null;
  const n = Number(t);
  if (!Number.isFinite(n) || !Number.isInteger(n)) return 'invalid';
  return n;
}

function parseDurationInput(s: string): number | null | 'invalid' {
  const t = s.trim();
  if (t === '') return null;
  const n = Number(t);
  if (!Number.isFinite(n)) return 'invalid';
  return n;
}

function listParams(winery: string, vineyard: string): string {
  const p = new URLSearchParams();
  if (winery.trim()) p.set('winery', winery.trim());
  if (vineyard.trim()) p.set('vineyard', vineyard.trim());
  const q = p.toString();
  return q ? `?${q}` : '';
}

/** Build the structured debug modal payload from rows already in tbl_distances_gps_samples (e.g. after refresh). */
function dryRunSnapshotFromSavedSamples(row: DistanceRow, samples: GpsSampleRow[]): DryRunDebugSnapshot {
  const attempts: HarvestAttempt[] = samples.map((s) => {
    const ok = s.outcome === 'success';
    const dbg = s.debug_json;
    const debug =
      dbg != null && typeof dbg === 'object' && !Array.isArray(dbg)
        ? (dbg as Record<string, unknown>)
        : undefined;
    return {
      job_id: String(s.job_id),
      ok,
      outcome: s.outcome,
      skipReason: ok ? undefined : (s.failure_reason ?? s.outcome ?? 'failed'),
      meters: typeof s.meters === 'number' ? s.meters : undefined,
      minutes: typeof s.minutes === 'number' ? s.minutes : undefined,
      segment_point_count: s.segment_point_count ?? undefined,
      debug,
    };
  });
  const successCount = attempts.filter((a) => a.ok).length;
  const failedCount = attempts.length - successCount;
  const uniqueJobs = new Set(attempts.map((a) => a.job_id));
  const candidateJobs: HarvestCandidateJob[] = [...uniqueJobs].map((job_id) => ({
    job_id,
    actual_start_time: null,
    worker: null,
  }));
  return {
    distanceId: row.id,
    distancePair: { delivery_winery: row.delivery_winery, vineyard_name: row.vineyard_name },
    vworkJobsMatchCount: row.vwork_job_count ?? 0,
    candidateJobs,
    jobsPolled: attempts.length,
    successCount,
    failedCount,
    maxJobsCap: GPS_HARVEST_DEFAULT_MAX_JOBS,
    maxSuccessesCap: GPS_HARVEST_DEFAULT_MAX_SUCCESSES,
    message:
      attempts.length === 0
        ? 'No rows in tbl_distances_gps_samples for this pair. Run Save on this row to harvest and store attempts.'
        : 'Report from saved rows in tbl_distances_gps_samples (not a new dry run).',
    attempts,
  };
}

/** Mean metres / minutes over successful jobs only (not failed attempts). One average per metric across OK rows. */
function dryRunAveragesFromAttempts(attempts: HarvestAttempt[]): {
  avgMeters: number | null;
  avgMinutes: number | null;
} {
  const ok = attempts.filter((a) => a.ok === true || a.outcome === 'success');
  const n = ok.length;
  if (n === 0) return { avgMeters: null, avgMinutes: null };
  const withM = ok.filter((a): a is HarvestAttempt & { meters: number } => typeof a.meters === 'number');
  const withMin = ok.filter((a): a is HarvestAttempt & { minutes: number } => typeof a.minutes === 'number');
  return {
    avgMeters:
      withM.length === n ? withM.reduce((s, a) => s + a.meters, 0) / n : null,
    avgMinutes:
      withMin.length === n ? withMin.reduce((s, a) => s + a.minutes, 0) / n : null,
  };
}

export default function AdminDistancesPage() {
  const [wineryFilter, setWineryFilter] = useState('');
  const [vineyardFilter, setVineyardFilter] = useState('');
  const [appliedWinery, setAppliedWinery] = useState('');
  const [appliedVineyard, setAppliedVineyard] = useState('');

  const [wineryOptions, setWineryOptions] = useState<string[]>([]);
  const [vineyardOptions, setVineyardOptions] = useState<string[]>([]);
  const [gpsWineryMappings, setGpsWineryMappings] = useState<Record<string, GpsMappingRow[]>>({});
  const [gpsVineyardMappings, setGpsVineyardMappings] = useState<Record<string, GpsMappingRow[]>>({});
  const [rows, setRows] = useState<DistanceRow[]>([]);
  const [draft, setDraft] = useState<Record<number, { d: string; t: string }>>({});

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [seedMsg, setSeedMsg] = useState<string | null>(null);
  const [seeding, setSeeding] = useState(false);
  const [populateVworkMsg, setPopulateVworkMsg] = useState<string | null>(null);
  const [populateVworkBusy, setPopulateVworkBusy] = useState(false);
  const [filterNoDataOnly, setFilterNoDataOnly] = useState(false);
  const [sortKey, setSortKey] = useState<DistancesSortKey>('winery');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [saveError, setSaveError] = useState<string | null>(null);
  const [runAllBusy, setRunAllBusy] = useState(false);
  const [runAllMsg, setRunAllMsg] = useState<string | null>(null);
  const [runAllProgress, setRunAllProgress] = useState<{
    total: number;
    done: number;
    current?: string;
    rows: Array<{
      id: number;
      pair: string;
      status: 'pending' | 'running' | 'ok' | 'error';
      detail?: string;
    }>;
  } | null>(null);

  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [samples, setSamples] = useState<GpsSampleRow[]>([]);
  const [samplesLoading, setSamplesLoading] = useState(false);
  const [samplesError, setSamplesError] = useState<string | null>(null);
  const [expandedSampleId, setExpandedSampleId] = useState<string | null>(null);
  const [sampleFilter, setSampleFilter] = useState<'all' | 'success' | 'failed'>('all');
  const [harvestBusyId, setHarvestBusyId] = useState<number | null>(null);
  const [lastPersistRun, setLastPersistRun] = useState<{
    distanceId: number;
    insertedOrUpdated: number;
    attempts: HarvestAttempt[];
    jobsPolled: number;
    successCount: number;
    failedCount: number;
    distancePair: { delivery_winery: string; vineyard_name: string };
    vworkJobsMatchCount: number;
    candidateJobs: HarvestCandidateJob[];
    message?: string;
  } | null>(null);
  /** Per distance_id: last dry-run snapshot for structured debug popup. */
  const [dryRunDebugSnapshotById, setDryRunDebugSnapshotById] = useState<
    Record<number, DryRunDebugSnapshot>
  >({});
  const [harvestDebugModal, setHarvestDebugModal] = useState<{
    title: string;
    snapshot: DryRunDebugSnapshot;
  } | null>(null);
  /** Opening saved-samples report (no in-memory dry-run snapshot). */
  const [gpsDebugOpeningId, setGpsDebugOpeningId] = useState<number | null>(null);
  /** Per distance_id: last dry-run success count + averages until Save (then DB is source of truth). */
  const [gpsDryRunPreviewById, setGpsDryRunPreviewById] = useState<
    Record<
      number,
      {
        successCount: number;
        jobsPolled: number;
        maxJobsCap: number;
        maxSuccessesCap: number;
        avgMeters: number | null;
        avgMinutes: number | null;
      }
    >
  >({});

  const wineryListId = useMemo(() => 'distances-winery-options', []);
  const vineyardListId = useMemo(() => 'distances-vineyard-options', []);

  const load = useCallback(async (winery: string, vineyard: string) => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch(`/api/admin/distances${listParams(winery, vineyard)}`, {
        cache: 'no-store',
      });
      const json = await r.json();
      if (json.error) throw new Error(json.error);
      const d = json as ListResponse;
      setWineryOptions(d.wineryOptions ?? []);
      setVineyardOptions(d.vineyardOptions ?? []);
      setGpsWineryMappings(d.gpsWineryMappings ?? {});
      setGpsVineyardMappings(d.gpsVineyardMappings ?? {});
      setRows(d.rows ?? []);
      const next: Record<number, { d: string; t: string }> = {};
      for (const c of d.rows ?? []) {
        next[c.id] = {
          d: c.distance_m == null ? '' : String(c.distance_m),
          t: c.duration_min == null ? '' : String(c.duration_min),
        };
      }
      setDraft(next);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  const loadSamples = useCallback(async (distanceId: number) => {
    setSamplesLoading(true);
    setSamplesError(null);
    try {
      const r = await fetch(`/api/admin/distances/gps-samples?distanceId=${distanceId}`, {
        cache: 'no-store',
      });
      const j = await r.json();
      if (j.error) throw new Error(j.error);
      setSamples((j.samples ?? []) as GpsSampleRow[]);
    } catch (e) {
      setSamplesError(e instanceof Error ? e.message : String(e));
      setSamples([]);
    } finally {
      setSamplesLoading(false);
    }
  }, []);

  useEffect(() => {
    void load('', '');
  }, [load]);

  useEffect(() => {
    if (selectedId == null) {
      setSamples([]);
      return;
    }
    void loadSamples(selectedId);
  }, [selectedId, loadSamples]);

  const applyFilters = useCallback(() => {
    setAppliedWinery(wineryFilter);
    setAppliedVineyard(vineyardFilter);
    void load(wineryFilter, vineyardFilter);
  }, [load, wineryFilter, vineyardFilter]);

  const clearFilters = useCallback(() => {
    setWineryFilter('');
    setVineyardFilter('');
    setAppliedWinery('');
    setAppliedVineyard('');
    setFilterNoDataOnly(false);
    void load('', '');
  }, [load]);

  const saveCell = useCallback(
    async (row: DistanceRow, field: 'distance_m' | 'duration_min', raw: string) => {
      setSaveError(null);
      let distance_m: number | null | undefined;
      let duration_min: number | null | undefined;

      if (field === 'distance_m') {
        const p = parseDistanceInput(raw);
        if (p === 'invalid') {
          setSaveError('Distance (m) must be a whole number or empty.');
          setDraft((prev) => ({
            ...prev,
            [row.id]: {
              d: row.distance_m == null ? '' : String(row.distance_m),
              t: prev[row.id]?.t ?? (row.duration_min == null ? '' : String(row.duration_min)),
            },
          }));
          return;
        }
        distance_m = p;
        const unchanged =
          (row.distance_m == null && p == null) || row.distance_m === p;
        if (unchanged) return;
      } else {
        const p = parseDurationInput(raw);
        if (p === 'invalid') {
          setSaveError('Minutes must be a number or empty.');
          setDraft((prev) => ({
            ...prev,
            [row.id]: {
              d: prev[row.id]?.d ?? (row.distance_m == null ? '' : String(row.distance_m)),
              t: row.duration_min == null ? '' : String(row.duration_min),
            },
          }));
          return;
        }
        duration_min = p;
        const prevNum =
          row.duration_min == null || String(row.duration_min).trim() === ''
            ? null
            : Number(row.duration_min);
        const unchanged =
          (prevNum == null && p == null) ||
          (prevNum != null && p != null && prevNum === p);
        if (unchanged) return;
      }

      const body: Record<string, unknown> = { id: row.id };
      if (field === 'distance_m') body.distance_m = distance_m;
      if (field === 'duration_min') body.duration_min = duration_min;

      const r = await fetch('/api/admin/distances', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const j = await r.json();
      if (!r.ok || j.error) {
        setSaveError(j.error ?? r.statusText);
        await load(appliedWinery, appliedVineyard);
        return;
      }
      await load(appliedWinery, appliedVineyard);
    },
    [load, appliedWinery, appliedVineyard]
  );

  const runHarvest = useCallback(
    async (distanceId: number, dryRun: boolean) => {
      setHarvestBusyId(distanceId);
      setError(null);
      try {
        const r = await fetch('/api/admin/distances/gps-harvest', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            distanceId,
            dryRun,
            resetExistingSamples: dryRun ? false : true,
            maxJobs: GPS_HARVEST_DEFAULT_MAX_JOBS,
            maxSuccesses: GPS_HARVEST_DEFAULT_MAX_SUCCESSES,
          }),
        });
        const j = await r.json();
        const attempts = (j.attempts ?? []) as HarvestAttempt[];
        const jobsPolled = Number(j.jobsPolled ?? 0) || 0;
        const successCount = Number(j.successCount ?? 0) || 0;
        const failedCount = Number(j.failedCount ?? 0) || 0;
        const distancePair = (j.distancePair ?? {
          delivery_winery: '',
          vineyard_name: '',
        }) as { delivery_winery: string; vineyard_name: string };
        const vworkJobsMatchCount = Number(j.vworkJobsMatchCount ?? 0) || 0;
        const candidateJobs = (j.candidateJobs ?? []) as HarvestCandidateJob[];
        const maxJobsCap = Number(j.maxJobsCap ?? GPS_HARVEST_DEFAULT_MAX_JOBS) || GPS_HARVEST_DEFAULT_MAX_JOBS;
        const maxSuccessesCap =
          Number(j.maxSuccessesCap ?? GPS_HARVEST_DEFAULT_MAX_SUCCESSES) ||
          GPS_HARVEST_DEFAULT_MAX_SUCCESSES;
        if (dryRun) {
          const snapshot: DryRunDebugSnapshot = {
            distanceId,
            distancePair,
            vworkJobsMatchCount,
            candidateJobs,
            jobsPolled,
            successCount,
            failedCount,
            maxJobsCap,
            maxSuccessesCap,
            message: typeof j.message === 'string' ? j.message : undefined,
            attempts,
          };
          setDryRunDebugSnapshotById((prev) => ({ ...prev, [distanceId]: snapshot }));
          setLastPersistRun(null);
          const av = dryRunAveragesFromAttempts(attempts);
          setGpsDryRunPreviewById((prev) => ({
            ...prev,
            [distanceId]: {
              successCount,
              jobsPolled,
              maxJobsCap,
              maxSuccessesCap,
              avgMeters: av.avgMeters,
              avgMinutes: av.avgMinutes,
            },
          }));
        }
        if (!r.ok || j.error) {
          const extra =
            j && typeof j === 'object' && 'detail' in j && typeof (j as any).detail === 'string'
              ? `\n\nDetail: ${(j as any).detail}`
              : '';
          throw new Error(`${(j as any).error ?? r.statusText}${extra}`);
        }
        if (!dryRun) {
          setLastPersistRun({
            distanceId,
            insertedOrUpdated: j.insertedOrUpdated ?? 0,
            attempts,
            jobsPolled,
            successCount,
            failedCount,
            distancePair,
            vworkJobsMatchCount,
            candidateJobs,
            message: j.message,
          });
          setGpsDryRunPreviewById((prev) => {
            const next = { ...prev };
            delete next[distanceId];
            return next;
          });
          await load(appliedWinery, appliedVineyard);
          if (selectedId === distanceId) void loadSamples(distanceId);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setHarvestBusyId(null);
      }
    },
    [load, appliedWinery, appliedVineyard, selectedId, loadSamples]
  );

  const openGpsScoreDebug = useCallback(
    async (row: DistanceRow) => {
      const cached = dryRunDebugSnapshotById[row.id];
      if (cached) {
        setHarvestDebugModal({
          title: `Dry run — id ${row.id} (${row.delivery_winery} → ${row.vineyard_name})`,
          snapshot: cached,
        });
        return;
      }
      setGpsDebugOpeningId(row.id);
      setError(null);
      try {
        const r = await fetch(`/api/admin/distances/gps-samples?distanceId=${row.id}`, {
          cache: 'no-store',
        });
        const j = await r.json();
        if (j.error) throw new Error(j.error);
        const list = (j.samples ?? []) as GpsSampleRow[];
        const snapshot = dryRunSnapshotFromSavedSamples(row, list);
        setHarvestDebugModal({
          title: `Saved GPS attempts — id ${row.id} (${row.delivery_winery} → ${row.vineyard_name})`,
          snapshot,
        });
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setGpsDebugOpeningId(null);
      }
    },
    [dryRunDebugSnapshotById]
  );

  const runSeed = useCallback(() => {
    setSeeding(true);
    setSeedMsg(null);
    setError(null);
    fetch('/api/admin/distances/seed', { method: 'POST' })
      .then((r) => r.json())
      .then((j) => {
        if (j.error) throw new Error(j.error);
        const ins = Number(j.inserted ?? 0) || 0;
        const del = Number(j.deleted ?? 0) || 0;
        setSeedMsg(
          `Sync: removed ${del} pair(s) with no vWork jobs; inserted ${ins} new pair(s). Existing valid pairs unchanged (distance/duration not updated).`
        );
        return load(appliedWinery, appliedVineyard);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSeeding(false));
  }, [load, appliedWinery, appliedVineyard]);

  const runPopulateVwork = useCallback(() => {
    setPopulateVworkBusy(true);
    setPopulateVworkMsg(null);
    setError(null);
    fetch('/api/admin/vworkjobs/populate-from-distances', { method: 'POST' })
      .then(async (r) => {
        const j = await r.json().catch(() => ({}));
        if (!r.ok || j.error) {
          const extra =
            j && typeof j === 'object' && 'detail' in j && typeof (j as { detail?: string }).detail === 'string'
              ? ` (${(j as { detail: string }).detail})`
              : '';
          throw new Error(`${j.error ?? r.statusText}${extra}`);
        }
        const n = Number(j.updated ?? 0) || 0;
        setPopulateVworkMsg(
          `Updated ${n} job row(s): distance and minutes set from tbl_distances where the winery+vineyard pair matches (trim, case-insensitive). Jobs with no matching pair were left unchanged.`
        );
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setPopulateVworkBusy(false));
  }, []);

  const runAllHarvest = useCallback(
    async () => {
    setRunAllBusy(true);
    setRunAllMsg(null);
    setError(null);
    try {
      const minOkNeeded = GPS_HARVEST_DEFAULT_MAX_SUCCESSES;
      const skippedAlreadyFull = rows.filter((r) => (r.gps_sample_count ?? 0) >= minOkNeeded);
      const targets = rows
        .filter((r) => (r.gps_sample_count ?? 0) < minOkNeeded)
        .map((r) => ({
          id: r.id,
          pair: `${r.delivery_winery} → ${r.vineyard_name}`,
        }));

      if (targets.length === 0) {
        setRunAllProgress(null);
        setRunAllMsg(
          skippedAlreadyFull.length > 0
            ? `No pairs to run: all ${skippedAlreadyFull.length} visible row(s) already have ${minOkNeeded}+ saved OK GPS sample(s).`
            : 'No distance rows in the current list to process.'
        );
        setRunAllBusy(false);
        return;
      }

      setRunAllProgress({
        total: targets.length,
        done: 0,
        current: targets[0]?.pair,
        rows: targets.map((t) => ({ id: t.id, pair: t.pair, status: 'pending' as const })),
      });

      let okCount = 0;
      let errCount = 0;
      let totalUpserts = 0;
      let hardBlocked = false;
      const prereqRegex = /GPS samples table or columns missing/i;

      for (let i = 0; i < targets.length; i++) {
        const t = targets[i]!;
        if (hardBlocked) break;
        setRunAllProgress((prev) => {
          if (!prev) return prev;
          const nextRows = prev.rows.map((x) =>
            x.id === t.id ? { ...x, status: 'running' as const } : x
          );
          return { ...prev, current: t.pair, rows: nextRows };
        });

        try {
          // 1) Dry run first (always)
          const rDry = await fetch('/api/admin/distances/gps-harvest', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              distanceId: t.id,
              dryRun: true,
              resetExistingSamples: false,
              maxJobs: GPS_HARVEST_DEFAULT_MAX_JOBS,
              maxSuccesses: GPS_HARVEST_DEFAULT_MAX_SUCCESSES,
            }),
          });
          const jDry = await rDry.json();
          if (!rDry.ok || jDry.error) {
            const extra =
              jDry && typeof jDry === 'object' && 'detail' in jDry && typeof (jDry as any).detail === 'string'
                ? `\n\nDetail: ${(jDry as any).detail}`
                : '';
            throw new Error(`${(jDry as any).error ?? rDry.statusText}${extra}`);
          }

          const attempts = (jDry.attempts ?? []) as HarvestAttempt[];
          const jobsPolled = Number(jDry.jobsPolled ?? 0) || 0;
          const successCount = Number(jDry.successCount ?? 0) || 0;
          const failedCount = Number(jDry.failedCount ?? 0) || 0;
          const distancePair = (jDry.distancePair ?? {
            delivery_winery: '',
            vineyard_name: '',
          }) as { delivery_winery: string; vineyard_name: string };
          const vworkJobsMatchCount = Number(jDry.vworkJobsMatchCount ?? 0) || 0;
          const candidateJobs = (jDry.candidateJobs ?? []) as HarvestCandidateJob[];
          const maxJobsCapDry =
            Number(jDry.maxJobsCap ?? GPS_HARVEST_DEFAULT_MAX_JOBS) || GPS_HARVEST_DEFAULT_MAX_JOBS;
          const maxSuccessesCapDry =
            Number(jDry.maxSuccessesCap ?? GPS_HARVEST_DEFAULT_MAX_SUCCESSES) ||
            GPS_HARVEST_DEFAULT_MAX_SUCCESSES;

          const snapshot: DryRunDebugSnapshot = {
            distanceId: t.id,
            distancePair,
            vworkJobsMatchCount,
            candidateJobs,
            jobsPolled,
            successCount,
            failedCount,
            maxJobsCap: maxJobsCapDry,
            maxSuccessesCap: maxSuccessesCapDry,
            message: typeof jDry.message === 'string' ? jDry.message : undefined,
            attempts,
          };
          setDryRunDebugSnapshotById((prev) => ({ ...prev, [t.id]: snapshot }));
          const av = dryRunAveragesFromAttempts(attempts);
          setGpsDryRunPreviewById((prev) => ({
            ...prev,
            [t.id]: {
              successCount,
              jobsPolled,
              maxJobsCap: maxJobsCapDry,
              maxSuccessesCap: maxSuccessesCapDry,
              avgMeters: av.avgMeters,
              avgMinutes: av.avgMinutes,
            },
          }));

          // 2) Then save (persist) for this same pair.
          const rSave = await fetch('/api/admin/distances/gps-harvest', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              distanceId: t.id,
              dryRun: false,
              resetExistingSamples: true,
              maxJobs: GPS_HARVEST_DEFAULT_MAX_JOBS,
              maxSuccesses: GPS_HARVEST_DEFAULT_MAX_SUCCESSES,
            }),
          });
          const jSave = await rSave.json();
          if (!rSave.ok || jSave.error) {
            const extra =
              jSave && typeof jSave === 'object' && 'detail' in jSave && typeof (jSave as any).detail === 'string'
                ? `\n\nDetail: ${(jSave as any).detail}`
                : '';
            throw new Error(`${(jSave as any).error ?? rSave.statusText}${extra}`);
          }

          okCount++;
          totalUpserts += Number(jSave.insertedOrUpdated ?? 0) || 0;
          setRunAllProgress((prev) => {
            if (!prev) return prev;
            const nextRows = prev.rows.map((x) =>
              x.id === t.id
                ? {
                    ...x,
                    status: 'ok' as const,
                    detail: `dry ${successCount}/${maxSuccessesCapDry} ok · ${jobsPolled}/${maxJobsCapDry} polled; saved ${Number(jSave.successCount ?? 0) || 0} OK`,
                  }
                : x
            );
            return { ...prev, done: Math.min(prev.total, prev.done + 1), rows: nextRows };
          });
        } catch (e) {
          errCount++;
          const msg = e instanceof Error ? e.message : String(e);
          if (prereqRegex.test(msg)) {
            hardBlocked = true;
            setError(msg);
            setRunAllProgress((prev) => {
              if (!prev) return prev;
              const nextRows = prev.rows.map((x) => {
                if (x.id === t.id) return { ...x, status: 'error' as const, detail: msg };
                if (x.status === 'pending')
                  return { ...x, status: 'error' as const, detail: 'blocked — missing GPS samples table/columns' };
                return x;
              });
              const doneNow = nextRows.filter((r) => r.status === 'ok' || r.status === 'error').length;
              return { ...prev, done: doneNow, current: undefined, rows: nextRows };
            });
            setRunAllMsg('Stopped early: database prerequisites missing (see error above).');
            break;
          }
          setRunAllProgress((prev) => {
            if (!prev) return prev;
            const nextRows = prev.rows.map((x) =>
              x.id === t.id ? { ...x, status: 'error' as const, detail: msg } : x
            );
            return { ...prev, done: Math.min(prev.total, prev.done + 1), rows: nextRows };
          });
        }
      }

      if (!hardBlocked) {
        const skipNote =
          skippedAlreadyFull.length > 0
            ? ` Skipped ${skippedAlreadyFull.length} pair(s) already at ${minOkNeeded}+ OK samples.`
            : '';
        setRunAllMsg(
          `Completed ${targets.length} pair(s): ${okCount} ok, ${errCount} error. Upserted ${totalUpserts} gps sample row(s).${skipNote}`
        );
        setRunAllProgress((prev) => (prev ? { ...prev, current: undefined } : prev));
      }
      await load(appliedWinery, appliedVineyard);
      if (selectedId != null) void loadSamples(selectedId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunAllBusy(false);
    }
    },
    [
      appliedWinery,
      appliedVineyard,
      load,
      selectedId,
      loadSamples,
      rows,
      setDryRunDebugSnapshotById,
      setGpsDryRunPreviewById,
    ]
  );

  const hasRowsInDb = wineryOptions.length > 0 || vineyardOptions.length > 0;
  const filterActive = appliedWinery.trim() !== '' || appliedVineyard.trim() !== '';

  const selectedRow = selectedId != null ? rows.find((r) => r.id === selectedId) : null;

  const filteredSamples = useMemo(() => {
    if (sampleFilter === 'all') return samples;
    if (sampleFilter === 'success') return samples.filter((s) => s.outcome === 'success');
    return samples.filter((s) => s.outcome === 'failed' || s.outcome === 'skipped');
  }, [samples, sampleFilter]);

  const toggleDistancesSort = useCallback((key: DistancesSortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir('asc');
    }
  }, [sortKey]);

  const displayRows = useMemo(() => {
    const list = filterNoDataOnly ? rows.filter(rowHasNoDistanceData) : rows;
    const dir = sortDir === 'asc' ? 1 : -1;
    const cmpLocale = (a: string, b: string) =>
      dir * a.localeCompare(b, undefined, { sensitivity: 'base' });
    const cmpNullLastNum = (a: number | null, b: number | null) => {
      if (a == null && b == null) return 0;
      if (a == null) return 1;
      if (b == null) return -1;
      return dir * (a - b);
    };
    return [...list].sort((a, b) => {
      switch (sortKey) {
        case 'winery':
          return cmpLocale(a.delivery_winery, b.delivery_winery);
        case 'vineyard':
          return cmpLocale(a.vineyard_name, b.vineyard_name);
        case 'vwork_jobs':
          return dir * ((a.vwork_job_count ?? 0) - (b.vwork_job_count ?? 0));
        case 'meters':
          return cmpNullLastNum(
            a.distance_m == null ? null : Number(a.distance_m),
            b.distance_m == null ? null : Number(b.distance_m)
          );
        case 'minutes':
          return cmpNullLastNum(durationMinSortValue(a), durationMinSortValue(b));
        case 'gps_ok':
          return (
            dir * (gpsOkCountForSort(a, gpsDryRunPreviewById) - gpsOkCountForSort(b, gpsDryRunPreviewById))
          );
        default:
          return 0;
      }
    });
  }, [rows, filterNoDataOnly, sortKey, sortDir, gpsDryRunPreviewById]);

  useEffect(() => {
    if (harvestDebugModal == null) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setHarvestDebugModal(null);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [harvestDebugModal]);

  return (
    <div className="min-h-screen bg-zinc-50 p-6 text-zinc-900 dark:bg-zinc-950 dark:text-zinc-100">
      <div className="mx-auto max-w-7xl">
        <h1 className="text-2xl font-semibold tracking-tight">Distances</h1>
        <p className="mt-2 max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
          Winery–vineyard pairs from vWork jobs with distance (metres) and time (minutes).{' '}
          <strong>GPS harvest</strong> uses the same tracking window and fence mappings as derived steps: first winery
          EXIT, first vineyard ENTER, path metres summed along <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">position_time_nz</code>
          order; minutes = enter − exit. Each run tries up to {GPS_HARVEST_DEFAULT_MAX_JOBS} jobs (lowest job_id first in a pool
          of {GPS_HARVEST_SQL_JOB_LIMIT}) and stops after {GPS_HARVEST_DEFAULT_MAX_SUCCESSES} successful samples; Save
          persists those samples and updates <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">distance_m</code> /{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">duration_min</code>. Run{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">create_tbl_distances_gps_samples.sql</code> and{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">add_gps_avg_tbl_distances.sql</code> if needed.{' '}
          <strong>Populate vWork</strong> copies <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">distance_m</code> /{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">duration_min</code> from each matching pair into{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">tbl_vworkjobs.distance</code> and{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">minutes</code> (
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">add_distance_minutes_tbl_vworkjobs.sql</code>).
        </p>

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={runSeed}
            disabled={seeding || loading}
            className="rounded-md bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-500 disabled:opacity-50"
          >
            {seeding ? 'Updating…' : 'Update pairs'}
          </button>
          <button
            type="button"
            onClick={() => void runPopulateVwork()}
            disabled={populateVworkBusy || loading}
            title="Set tbl_vworkjobs.distance and minutes from tbl_distances for each job’s delivery_winery + vineyard_name pair"
            className="rounded-md bg-sky-700 px-4 py-2 text-sm font-medium text-white hover:bg-sky-600 disabled:opacity-50"
          >
            {populateVworkBusy ? 'Populating…' : 'Populate vWork'}
          </button>
          <button
            type="button"
            onClick={() => void runAllHarvest()}
            disabled={runAllBusy || loading}
            className="rounded-md bg-emerald-700 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-600 disabled:opacity-50"
            title={`Dry-run + save only for pairs with fewer than ${GPS_HARVEST_DEFAULT_MAX_SUCCESSES} saved OK GPS samples (skips pairs already at ${GPS_HARVEST_DEFAULT_MAX_SUCCESSES}+). Clears previous samples per pair that runs.`}
          >
            {runAllBusy ? 'Running all…' : 'Run all (dry + save)'}
          </button>
          <button
            type="button"
            onClick={() => void load(appliedWinery, appliedVineyard)}
            disabled={loading}
            className="rounded-md border border-zinc-300 bg-white px-4 py-2 text-sm hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-900 dark:hover:bg-zinc-800"
          >
            Refresh
          </button>
          {seedMsg && (
            <span className="text-sm text-green-700 dark:text-green-400">{seedMsg}</span>
          )}
          {populateVworkMsg && (
            <span className="text-sm text-sky-800 dark:text-sky-200">{populateVworkMsg}</span>
          )}
          {runAllMsg && <span className="text-sm text-emerald-700 dark:text-emerald-400">{runAllMsg}</span>}
        </div>

        {runAllProgress && (
          <div className="mt-3 rounded-lg border border-zinc-200 bg-white p-3 text-sm dark:border-zinc-700 dark:bg-zinc-900">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="font-medium">
                Run all progress: {runAllProgress.done}/{runAllProgress.total}
              </div>
              {runAllProgress.current && (
                <div className="text-xs text-zinc-600 dark:text-zinc-400">
                  Current: <span className="font-mono">{runAllProgress.current}</span>
                </div>
              )}
            </div>
            <div className="mt-2 max-h-56 overflow-auto rounded border border-zinc-100 dark:border-zinc-800">
              <table className="w-full text-left text-[11px]">
                <thead className="bg-zinc-100 dark:bg-zinc-800">
                  <tr>
                    <th className="px-2 py-1">pair</th>
                    <th className="px-2 py-1">outcome</th>
                  </tr>
                </thead>
                <tbody>
                  {runAllProgress.rows.map((r) => (
                    <tr key={r.id} className="border-t border-zinc-100 dark:border-zinc-800">
                      <td className="px-2 py-0.5">{r.pair}</td>
                      <td className="px-2 py-0.5">
                        {r.status === 'pending'
                          ? 'pending'
                          : r.status === 'running'
                            ? 'running…'
                            : r.status === 'ok'
                              ? `ok — ${r.detail ?? ''}`
                              : `error — ${r.detail ?? ''}`}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {error && (
          <div className="mt-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-900 dark:bg-red-950/40 dark:text-red-200">
            {error}
          </div>
        )}
        {saveError && (
          <div className="mt-2 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800 dark:border-red-900 dark:bg-red-950/40 dark:text-red-200">
            {saveError}
          </div>
        )}

        {!loading && !hasRowsInDb && (
          <p className="mt-6 text-sm text-zinc-600 dark:text-zinc-400">
            No rows in <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">tbl_distances</code> yet.
            Run <strong>Update pairs</strong> after creating the table (
            <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">web/sql/create_tbl_distances.sql</code>
            ).
          </p>
        )}

        {hasRowsInDb && (
          <div className="mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <div className="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-end">
              <div className="min-w-[12rem] flex-1">
                <label htmlFor="filter-winery" className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">
                  Filter winery (contains)
                </label>
                <input
                  id="filter-winery"
                  list={wineryListId}
                  value={wineryFilter}
                  onChange={(e) => setWineryFilter(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && applyFilters()}
                  className="mt-1 w-full rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-950"
                  placeholder="e.g. Villa Maria"
                  autoComplete="off"
                />
                <datalist id={wineryListId}>
                  {wineryOptions.map((w) => (
                    <option key={w} value={w} />
                  ))}
                </datalist>
              </div>
              <div className="min-w-[12rem] flex-1">
                <label htmlFor="filter-vineyard" className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">
                  Filter vineyard (contains)
                </label>
                <input
                  id="filter-vineyard"
                  list={vineyardListId}
                  value={vineyardFilter}
                  onChange={(e) => setVineyardFilter(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && applyFilters()}
                  className="mt-1 w-full rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm dark:border-zinc-600 dark:bg-zinc-950"
                  placeholder="e.g. Rapaura"
                  autoComplete="off"
                />
                <datalist id={vineyardListId}>
                  {vineyardOptions.map((v) => (
                    <option key={v} value={v} />
                  ))}
                </datalist>
              </div>
              <div className="flex flex-col justify-end gap-2 sm:flex-row sm:items-center">
                <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
                  <input
                    type="checkbox"
                    checked={filterNoDataOnly}
                    onChange={(e) => setFilterNoDataOnly(e.target.checked)}
                    className="rounded border-zinc-400"
                  />
                  <span title="Metres and minutes still unset or zero (e.g. seed defaults, harvest never filled data)">
                    No data
                  </span>
                </label>
                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={applyFilters}
                    disabled={loading}
                    className="rounded-md bg-zinc-800 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-200 dark:text-zinc-900 dark:hover:bg-zinc-300"
                  >
                    Apply filters
                  </button>
                  <button
                    type="button"
                    onClick={clearFilters}
                    disabled={loading}
                    className="rounded-md border border-zinc-300 px-4 py-2 text-sm hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:hover:bg-zinc-800"
                  >
                    Clear
                  </button>
                </div>
              </div>
            </div>
            <p className="mt-2 text-xs text-zinc-500">
              Case-insensitive partial match on winery/vineyard (Apply reloads from API). <strong>No data</strong> narrows
              the loaded list to pairs with m and min still 0/empty. Column headers sort (toggle ↑/↓).
            </p>
          </div>
        )}

        {loading && <p className="mt-6 text-sm text-zinc-500">Loading…</p>}

        {!loading && hasRowsInDb && (
          <>
            <p className="mt-4 text-sm text-zinc-600 dark:text-zinc-400">
              {filterNoDataOnly ? (
                <>
                  Showing <strong>{displayRows.length}</strong> of {rows.length} loaded row
                  {rows.length === 1 ? '' : 's'} with <strong>No data</strong> (m/min unset or zero).
                </>
              ) : (
                <>
                  {displayRows.length} row{displayRows.length === 1 ? '' : 's'}
                  {filterActive ? ' (winery/vineyard filter from API)' : ''}.
                </>
              )}{' '}
              Click a row to load saved GPS samples and debug JSON.
            </p>
            <div className="mt-2 overflow-x-auto rounded-lg border border-zinc-200 shadow-sm dark:border-zinc-700">
              <table className="w-full min-w-[56rem] border-collapse text-left text-sm">
                <thead>
                  <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                    <DistancesSortHeader
                      label="Winery"
                      columnKey="winery"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                    />
                    <DistancesSortHeader
                      label="Vineyard"
                      columnKey="vineyard"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                    />
                    <DistancesSortHeader
                      label="vWork jobs"
                      columnKey="vwork_jobs"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                      center
                      title="Jobs in tbl_vworkjobs with this winery + vineyard (trim, case-insensitive)"
                    />
                    <DistancesSortHeader
                      label="m"
                      columnKey="meters"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                    />
                    <DistancesSortHeader
                      label="min"
                      columnKey="minutes"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                    />
                    <DistancesSortHeader
                      label="GPS (dry/saved)"
                      columnKey="gps_ok"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                      title="Sort by OK sample count (latest dry-run successes, else saved gps_sample_count)"
                    />
                    <th className="px-2 py-2 font-semibold">Harvest</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.length === 0 ? (
                    <tr>
                      <td colSpan={7} className="px-3 py-8 text-center text-zinc-500">
                        No rows match these filters. Try clearing filters or broadening the text.
                      </td>
                    </tr>
                  ) : displayRows.length === 0 ? (
                    <tr>
                      <td colSpan={7} className="px-3 py-8 text-center text-zinc-500">
                        No rows match <strong>No data</strong> (every loaded row has metres or minutes set). Turn off the
                        checkbox to see all {rows.length} row{rows.length === 1 ? '' : 's'}.
                      </td>
                    </tr>
                  ) : (
                    displayRows.map((row) => {
                      const d0 =
                        draft[row.id]?.d ??
                        (row.distance_m == null ? '' : String(row.distance_m));
                      const t0 =
                        draft[row.id]?.t ??
                        (row.duration_min == null ? '' : String(row.duration_min));
                      const busy = harvestBusyId === row.id;
                      const sel = selectedId === row.id;
                      const dbGpsCount = row.gps_sample_count ?? 0;
                      const dbAttemptCount = row.gps_attempt_count ?? 0;
                      const dryPrev = gpsDryRunPreviewById[row.id];
                      const useSavedGps = dbAttemptCount > 0;
                      /** Latest dry run wins over saved DB counts until Save clears preview (avoids stuck 0/10 from old failed saves). */
                      const showDryPreview = dryPrev != null;
                      const gpsCountShown = showDryPreview
                        ? `${dryPrev.successCount}/${dryPrev.maxSuccessesCap ?? GPS_HARVEST_DEFAULT_MAX_SUCCESSES} ok · ${dryPrev.jobsPolled}/${dryPrev.maxJobsCap ?? GPS_HARVEST_DEFAULT_MAX_JOBS}`
                        : useSavedGps
                          ? `${dbGpsCount}/${dbAttemptCount}`
                          : `—`;
                      let gpsAvgLine: string | null = null;
                      if (showDryPreview) {
                        const parts: string[] = [];
                        if (dryPrev.avgMeters != null)
                          parts.push(`${Math.round(dryPrev.avgMeters)} m`);
                        if (dryPrev.avgMinutes != null)
                          parts.push(`${dryPrev.avgMinutes.toFixed(2)} min`);
                        if (parts.length) {
                          const n = dryPrev.successCount;
                          gpsAvgLine =
                            parts.join(' · ') +
                            ` (mean of ${n} dry OK${n === 1 ? '' : 's'})`;
                        }
                      } else if (useSavedGps) {
                        const parts: string[] = [];
                        if (row.gps_avg_distance_m != null)
                          parts.push(`${Math.round(Number(row.gps_avg_distance_m))} m`);
                        if (
                          row.gps_avg_duration_min != null &&
                          String(row.gps_avg_duration_min).trim() !== ''
                        ) {
                          const dur = Number(row.gps_avg_duration_min);
                          if (!Number.isNaN(dur)) parts.push(`${dur.toFixed(2)} min`);
                        }
                        if (parts.length) {
                          const n = dbGpsCount;
                          gpsAvgLine =
                            parts.join(' · ') +
                            ` (mean of ${n} saved OK${n === 1 ? '' : 's'})`;
                        }
                      }
                      return (
                        <tr
                          key={row.id}
                          role="button"
                          tabIndex={0}
                          onClick={() => setSelectedId(row.id)}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter' || e.key === ' ') {
                              e.preventDefault();
                              setSelectedId(row.id);
                            }
                          }}
                          className={`cursor-pointer border-b border-zinc-100 dark:border-zinc-800 ${
                            sel
                              ? 'bg-amber-50 dark:bg-amber-950/30'
                              : 'odd:bg-white even:bg-zinc-50/80 dark:odd:bg-zinc-950 dark:even:bg-zinc-900/40'
                          }`}
                        >
                          <td className="max-w-[14rem] px-2 py-2 align-top text-zinc-800 dark:text-zinc-200">
                            <span className="line-clamp-2 text-sm" title={row.delivery_winery}>
                              {row.delivery_winery}
                            </span>
                            <GpsMappingHint
                              kind="Winery"
                              vworkName={row.delivery_winery}
                              maps={gpsWineryMappings[row.delivery_winery.trim()] ?? []}
                            />
                          </td>
                          <td className="max-w-[14rem] px-2 py-2 align-top text-zinc-800 dark:text-zinc-200">
                            <span className="line-clamp-2 text-sm" title={row.vineyard_name}>
                              {row.vineyard_name}
                            </span>
                            <GpsMappingHint
                              kind="Vineyard"
                              vworkName={row.vineyard_name}
                              maps={gpsVineyardMappings[row.vineyard_name.trim()] ?? []}
                            />
                          </td>
                          <td className="whitespace-nowrap px-2 py-2 align-middle text-center tabular-nums text-zinc-700 dark:text-zinc-300">
                            {row.vwork_job_count ?? 0}
                          </td>
                          <td className="px-2 py-2 align-middle">
                            <input
                              aria-label={`Distance m for row ${row.id}`}
                              className="w-20 rounded border border-zinc-200 bg-white px-1 py-1 text-zinc-900 dark:border-zinc-600 dark:bg-zinc-950 dark:text-zinc-100"
                              value={d0}
                              onClick={(e) => e.stopPropagation()}
                              onChange={(e) =>
                                setDraft((prev) => ({
                                  ...prev,
                                  [row.id]: { d: e.target.value, t: prev[row.id]?.t ?? t0 },
                                }))
                              }
                              onBlur={(e) => void saveCell(row, 'distance_m', e.target.value)}
                              inputMode="numeric"
                            />
                          </td>
                          <td className="px-2 py-2 align-middle">
                            <input
                              aria-label={`Minutes for row ${row.id}`}
                              className="w-20 rounded border border-zinc-200 bg-white px-1 py-1 text-zinc-900 dark:border-zinc-600 dark:bg-zinc-950 dark:text-zinc-100"
                              value={t0}
                              onClick={(e) => e.stopPropagation()}
                              onChange={(e) =>
                                setDraft((prev) => ({
                                  ...prev,
                                  [row.id]: { d: prev[row.id]?.d ?? d0, t: e.target.value },
                                }))
                              }
                              onBlur={(e) => void saveCell(row, 'duration_min', e.target.value)}
                              inputMode="decimal"
                            />
                          </td>
                          <td className="min-w-[7rem] px-2 py-2 align-middle text-xs text-zinc-600 dark:text-zinc-400">
                            {gpsCountShown !== '—' ? (
                              <button
                                type="button"
                                disabled={gpsDebugOpeningId === row.id}
                                title={
                                  dryRunDebugSnapshotById[row.id]
                                    ? 'Open dry-run report — format: OK vs success cap · jobs tried vs job cap'
                                    : 'Open report from saved samples (or run Dry run first for a fresh in-memory report)'
                                }
                                onClick={(e) => {
                                  e.stopPropagation();
                                  void openGpsScoreDebug(row);
                                }}
                                className="whitespace-nowrap rounded px-0.5 text-left font-medium text-blue-700 underline decoration-blue-400 underline-offset-2 hover:bg-blue-50 enabled:cursor-pointer disabled:opacity-60 dark:text-blue-300 dark:hover:bg-blue-950/50"
                              >
                                {gpsDebugOpeningId === row.id ? 'Loading…' : gpsCountShown}
                                {showDryPreview && gpsDebugOpeningId !== row.id && (
                                  <span className="ml-1 font-normal text-blue-600 dark:text-blue-400">dry</span>
                                )}
                              </button>
                            ) : (
                              <div className="whitespace-nowrap font-medium text-zinc-800 dark:text-zinc-200">
                                {gpsCountShown}
                              </div>
                            )}
                            {gpsAvgLine != null && (
                              <div className="mt-0.5 whitespace-nowrap text-[10px] leading-tight text-zinc-500 dark:text-zinc-400">
                                {gpsAvgLine}
                              </div>
                            )}
                          </td>
                          <td className="whitespace-nowrap px-2 py-2 align-middle">
                            <div className="flex flex-wrap gap-1">
                              <button
                                type="button"
                                disabled={busy}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  void runHarvest(row.id, true);
                                  setSelectedId(row.id);
                                }}
                                className="rounded border border-zinc-300 px-2 py-0.5 text-xs hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-600 dark:hover:bg-zinc-800"
                              >
                                {busy ? '…' : 'Dry run'}
                              </button>
                              <button
                                type="button"
                                disabled={busy}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  void runHarvest(row.id, false);
                                  setSelectedId(row.id);
                                }}
                                className="rounded bg-emerald-700 px-2 py-0.5 text-xs text-white hover:bg-emerald-600 disabled:opacity-50"
                              >
                                Save
                              </button>
                            </div>
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
            <p className="mt-2 text-xs text-zinc-500">
              Manual edits save on blur. <strong>Save</strong> polls up to 5 jobs and writes each to the DB (success or
              failure) with full debug. <strong>Dry run</strong> does not write. Click the <strong>GPS (dry/saved)</strong>{' '}
              score (e.g. 2/7) for a job-by-job report: uses the last dry-run snapshot in this session if present, otherwise
              loads from saved samples in the database.
            </p>

            {(selectedId != null || lastPersistRun != null) && (
              <div className="mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
                <h2 className="text-lg font-semibold">GPS harvest detail</h2>
                {selectedRow && (
                  <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
                    Selected: {selectedRow.delivery_winery} → {selectedRow.vineyard_name} (id {selectedRow.id})
                  </p>
                )}

                {lastPersistRun && (
                  <div className="mt-4 rounded-md border border-emerald-200 bg-emerald-50/80 p-3 text-sm dark:border-emerald-900 dark:bg-emerald-950/40">
                    <div className="font-medium text-emerald-900 dark:text-emerald-100">
                      Last save run: {lastPersistRun.insertedOrUpdated} row(s) upserted in tbl_distances_gps_samples
                    </div>
                    <p className="mt-2 text-xs text-emerald-800 dark:text-emerald-200">
                      Pair from tbl_distances (matched in tbl_vworkjobs):{' '}
                      <span className="font-mono">{lastPersistRun.distancePair.delivery_winery}</span>
                      {' → '}
                      <span className="font-mono">{lastPersistRun.distancePair.vineyard_name}</span>
                    </p>
                    <p className="mt-1 text-xs text-emerald-800 dark:text-emerald-200">
                      Jobs in vWork with this winery + vineyard: <strong>{lastPersistRun.vworkJobsMatchCount}</strong>
                      {lastPersistRun.vworkJobsMatchCount > 24 ? ' (showing first 24 candidates below)' : ''}
                    </p>
                    {lastPersistRun.candidateJobs.length > 0 && (
                      <div className="mt-2 overflow-x-auto rounded border border-emerald-200/80 dark:border-emerald-900/60">
                        <table className="min-w-full text-left text-[11px]">
                          <thead className="bg-emerald-100/60 dark:bg-emerald-950/50">
                            <tr>
                              <th className="px-2 py-1 font-medium">job_id</th>
                              <th className="px-2 py-1 font-medium">actual_start_time</th>
                              <th className="px-2 py-1 font-medium">worker</th>
                            </tr>
                          </thead>
                          <tbody>
                            {lastPersistRun.candidateJobs.map((c) => (
                              <tr key={c.job_id} className="border-t border-emerald-100/80 dark:border-emerald-900/40">
                                <td className="px-2 py-0.5 font-mono">{c.job_id}</td>
                                <td className="px-2 py-0.5 font-mono text-zinc-700 dark:text-zinc-300">
                                  {c.actual_start_time ?? '—'}
                                </td>
                                <td className="px-2 py-0.5">{c.worker ?? '—'}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                    <p className="mt-2 text-xs text-emerald-800 dark:text-emerald-200">
                      Polled {lastPersistRun.jobsPolled} job(s) (up to 5 per run): {lastPersistRun.successCount} success,{' '}
                      {lastPersistRun.failedCount} failed (failures stored with debug).
                    </p>
                    {lastPersistRun.message && (
                      <p className="mt-1 text-emerald-800 dark:text-emerald-200">{lastPersistRun.message}</p>
                    )}
                    <ul className="mt-2 list-inside list-disc space-y-1 text-xs">
                      {lastPersistRun.attempts.map((a, idx) => (
                        <li key={`${a.job_id}-${idx}`}>
                          <span className="font-mono">{a.job_id}</span>:{' '}
                          {a.ok ? `OK ${a.meters} m` : `failed — ${a.skipReason ?? '?'}`}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                {selectedId != null && (
                  <div className="mt-4">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <h3 className="text-sm font-semibold">Stored harvest attempts (database)</h3>
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="text-xs text-zinc-500">Show:</span>
                        {(['all', 'success', 'failed'] as const).map((f) => (
                          <button
                            key={f}
                            type="button"
                            onClick={() => setSampleFilter(f)}
                            className={`rounded px-2 py-0.5 text-xs ${
                              sampleFilter === f
                                ? 'bg-zinc-800 text-white dark:bg-zinc-200 dark:text-zinc-900'
                                : 'border border-zinc-300 dark:border-zinc-600'
                            }`}
                          >
                            {f === 'all' ? 'All' : f === 'success' ? 'OK only' : 'Failed only'}
                          </button>
                        ))}
                        <button
                          type="button"
                          onClick={() => selectedId != null && void loadSamples(selectedId)}
                          className="text-xs text-blue-600 underline dark:text-blue-400"
                        >
                          Refresh list
                        </button>
                      </div>
                    </div>
                    {samplesLoading && <p className="mt-2 text-sm text-zinc-500">Loading samples…</p>}
                    {samplesError && (
                      <p className="mt-2 text-sm text-red-600 dark:text-red-400">{samplesError}</p>
                    )}
                    {!samplesLoading && !samplesError && samples.length === 0 && (
                      <p className="mt-2 text-sm text-zinc-500">No rows yet. Run <strong>Save</strong> on this pair.</p>
                    )}
                    {samples.length > 0 && filteredSamples.length === 0 && (
                      <p className="mt-2 text-sm text-zinc-500">No rows match this filter.</p>
                    )}
                    {filteredSamples.length > 0 && (
                      <ul className="mt-2 space-y-2">
                        {filteredSamples.map((s) => {
                          const ok = s.outcome === 'success';
                          return (
                            <li
                              key={s.id}
                              className={`rounded border p-2 text-xs ${
                                ok
                                  ? 'border-emerald-200 dark:border-emerald-900'
                                  : 'border-amber-200 dark:border-amber-900'
                              }`}
                            >
                              <div className="flex flex-wrap items-center gap-2">
                                <span
                                  className={`rounded px-1.5 py-0.5 font-medium ${
                                    ok
                                      ? 'bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-200'
                                      : 'bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-200'
                                  }`}
                                >
                                  {ok ? 'success' : s.outcome}
                                </span>
                                {s.run_index != null && (
                                  <span className="text-zinc-500">run #{s.run_index}</span>
                                )}
                                <span className="font-mono font-medium">job {s.job_id}</span>
                                {ok && s.meters != null && <span>{s.meters} m</span>}
                                {ok && s.minutes != null && (
                                  <span>{Number(s.minutes).toFixed(2)} min</span>
                                )}
                                {ok && (
                                  <span className="text-zinc-500">{s.segment_point_count ?? '?'} pts</span>
                                )}
                                {!ok && s.failure_reason && (
                                  <span className="text-amber-800 dark:text-amber-200">{s.failure_reason}</span>
                                )}
                                <button
                                  type="button"
                                  className="text-blue-600 underline dark:text-blue-400"
                                  onClick={() =>
                                    setExpandedSampleId((id) => (id === s.id ? null : s.id))
                                  }
                                >
                                  {expandedSampleId === s.id ? 'Hide debug' : 'Show debug'}
                                </button>
                              </div>
                              {expandedSampleId === s.id && (
                                <pre className="mt-2 max-h-80 overflow-auto rounded bg-zinc-100 p-2 text-[11px] dark:bg-zinc-950">
                                  {JSON.stringify(s.debug_json, null, 2)}
                                </pre>
                              )}
                            </li>
                          );
                        })}
                      </ul>
                    )}
                  </div>
                )}
              </div>
            )}
          </>
        )}
      </div>

      {harvestDebugModal != null && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
          role="dialog"
          aria-modal="true"
          aria-labelledby="harvest-debug-modal-title"
          onClick={(e) => e.target === e.currentTarget && setHarvestDebugModal(null)}
        >
          <div
            className="flex max-h-[90vh] w-full max-w-4xl flex-col overflow-hidden rounded-lg border border-zinc-200 bg-white shadow-xl dark:border-zinc-700 dark:bg-zinc-900"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between gap-2 border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
              <h3
                id="harvest-debug-modal-title"
                className="pr-2 text-lg font-medium leading-snug text-zinc-900 dark:text-zinc-100"
              >
                {harvestDebugModal.title}
              </h3>
              <div className="flex shrink-0 items-center gap-2">
                <button
                  type="button"
                  onClick={async () => {
                    try {
                      await navigator.clipboard.writeText(
                        formatDryRunSummaryText(harvestDebugModal.snapshot)
                      );
                    } catch {
                      /* ignore */
                    }
                  }}
                  className="rounded border border-zinc-300 px-2 py-1 text-xs font-medium hover:bg-zinc-100 dark:border-zinc-600 dark:hover:bg-zinc-800"
                >
                  Copy summary
                </button>
                <button
                  type="button"
                  onClick={() => setHarvestDebugModal(null)}
                  className="rounded p-1 text-zinc-500 hover:bg-zinc-100 hover:text-zinc-700 dark:hover:bg-zinc-800 dark:hover:text-zinc-300"
                  aria-label="Close"
                >
                  ✕
                </button>
              </div>
            </div>
            <div className="min-h-0 flex-1 overflow-auto p-4">
              <DryRunDebugModalBody snapshot={harvestDebugModal.snapshot} />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

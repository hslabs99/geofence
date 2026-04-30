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
  gps_plus_sample_count?: number | null;
  gps_plus_avg_distance_m?: number | null;
  gps_plus_avg_duration_min?: string | null;
  distance_via?: string | null;
  pair_clients?: string | null;
  manual_distance_m?: number | null;
  manual_duration_min?: string | null;
  manual_notes?: string | null;
  effective_distance_m?: number | null;
  effective_duration_min?: string | null;
  display_distance_via?: string | null;
  maps_drive_url?: string | null;
};

type GpsMappingRow = { vwname: string | null; gpsname: string | null };

type ListResponse = {
  wineryOptions: string[];
  vineyardOptions: string[];
  customerOptions?: string[];
  rows: DistanceRow[];
  gpsWineryMappings?: Record<string, GpsMappingRow[]>;
  gpsVineyardMappings?: Record<string, GpsMappingRow[]>;
};

type DistancesSortKey =
  | 'winery'
  | 'vineyard'
  | 'vwork_jobs'
  | 'meters'
  | 'minutes'
  | 'tag_avg_m'
  | 'gps_plus_avg_m'
  | 'gps_ok'
  | 'gps_success'
  | 'distance_via';

/** Metres: API effective_distance_m already merges manual over tbl_distances. */
function effectiveDistanceM(row: DistanceRow): number | null {
  const m = row.effective_distance_m ?? row.distance_m;
  if (m == null) return null;
  const n = Number(m);
  return Number.isFinite(n) ? n : null;
}

function effectiveDurationMinStr(row: DistanceRow): string | null {
  const t = row.effective_duration_min ?? row.duration_min;
  if (t == null || String(t).trim() === '') return null;
  return String(t);
}

/** m and min still unset or zero after merging manual distances. */
function rowHasNoDistanceData(row: DistanceRow): boolean {
  const m = effectiveDistanceM(row);
  const noM = m == null || m === 0;
  const t = effectiveDurationMinStr(row);
  if (t == null) return noM;
  const n = Number(t);
  if (Number.isNaN(n)) return noM;
  return noM && n === 0;
}

function durationMinSortValue(row: DistanceRow): number | null {
  const t = effectiveDurationMinStr(row);
  if (t == null) return null;
  const n = Number(t);
  return Number.isNaN(n) ? null : n;
}

function totalDistanceTimesJobs(row: DistanceRow): number | null {
  const m = effectiveDistanceM(row);
  const j = row.vwork_job_count ?? 0;
  if (m == null || j <= 0) return null;
  return m * j;
}

function viaLabelForRow(row: DistanceRow): string {
  return String(row.display_distance_via ?? row.distance_via ?? '').trim();
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
  label: ReactNode;
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
    <th
      scope="col"
      className={`border-b border-zinc-200 bg-zinc-100 px-2 py-2.5 dark:border-zinc-700 dark:bg-zinc-900 ${center ? 'text-center' : 'text-left'} ${className}`}
      title={title}
    >
      <button
        type="button"
        onClick={() => onSort(columnKey)}
        className={`flex w-full gap-1 text-xs font-semibold uppercase tracking-wide text-zinc-700 hover:text-zinc-950 dark:text-zinc-300 dark:hover:text-zinc-100 ${
          center ? 'items-center justify-center' : 'items-start justify-start'
        }`}
      >
        <span>{label}</span>
        {active && (
          <span className="font-mono text-[10px] font-normal normal-case text-zinc-500" aria-hidden>
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
  gps_plus_meters?: number | null;
  gps_plus_minutes?: number | null;
  gps_plus_outcome?: string | null;
};

type GpsPlusSummary = {
  ok: boolean;
  meters?: number;
  minutes?: number;
  skipReason?: string;
  winery_anchor_source?: string;
  vineyard_hit_tracking_id?: number | null;
  segment_point_count?: number;
  detail?: Record<string, unknown>;
};

type HarvestAttempt = {
  job_id: string;
  ok: boolean;
  outcome?: string;
  skipReason?: string;
  meters?: number;
  minutes?: number;
  segment_point_count?: number;
  gps_plus?: GpsPlusSummary;
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
  jobMatchDebug?: {
    countSql: string;
    listSql: string;
    params: unknown[];
    vworkJobsMatchCount: number;
    candidateJobIds: string[];
  };
  candidateJobs: HarvestCandidateJob[];
  jobsPolled: number;
  successCount: number;
  failedCount: number;
  maxJobsCap: number;
  maxSuccessesCap: number;
  message?: string;
  attempts: HarvestAttempt[];
  /** Present when dry run updated tbl_distances from attempts (no samples written). */
  rollup?: {
    distance_via: string | null;
    gps_sample_count: number | null;
    gps_plus_sample_count: number | null;
    distance_m: number | null;
    gps_avg_distance_m: number | null;
    gps_plus_avg_distance_m: number | null;
  };
  dryDistanceRowUpdated?: boolean;
};

type DryHarvestSortCol = 'job_id' | 'tag_m' | 'tag_min' | 'gp_m' | 'gp_min' | 'status' | 'fail_msg';

function DryRunHarvestJobsTable({
  attempts,
  workerByJobId,
}: {
  attempts: HarvestAttempt[];
  workerByJobId: Map<string, string | null>;
}) {
  const fmt2 = (n: unknown) => (typeof n === 'number' && Number.isFinite(n) ? n.toFixed(2) : '');
  const matchedFenceNameFromAttempt = (a: HarvestAttempt, kind: 'winery' | 'vineyard'): string => {
    const dbg = (a.debug ?? {}) as Record<string, unknown>;
    const derived = dbg.derived_debug as any;
    const node = kind === 'winery' ? derived?.winery : derived?.vineyard;
    const step = kind === 'winery' ? node?.step1 : node?.step2;
    const nm = step?.matchedFenceName;
    return nm != null && String(nm).trim() !== '' ? String(nm) : '—';
  };
  const [q, setQ] = useState('');
  const [status, setStatus] = useState<'all' | 'ok' | 'fail'>('all');
  const [sortCol, setSortCol] = useState<DryHarvestSortCol>('job_id');
  const [sortAsc, setSortAsc] = useState(true);

  const toggleSort = (col: DryHarvestSortCol) => {
    if (sortCol === col) setSortAsc((v) => !v);
    else {
      setSortCol(col);
      setSortAsc(true);
    }
  };

  const rows = useMemo(() => {
    const qt = q.trim().toLowerCase();
    let a = attempts.filter((x) => {
      if (status === 'ok') return x.ok === true || x.outcome === 'success';
      if (status === 'fail') return !(x.ok === true || x.outcome === 'success');
      return true;
    });
    if (qt) {
      a = a.filter(
        (x) =>
          String(x.job_id).toLowerCase().includes(qt) ||
          String(x.skipReason ?? '').toLowerCase().includes(qt) ||
          String(x.gps_plus?.skipReason ?? '').toLowerCase().includes(qt)
      );
    }
    const mul = sortAsc ? 1 : -1;
    const numTagM = (x: HarvestAttempt) =>
      x.ok && typeof x.meters === 'number' ? x.meters : Number.NaN;
    const numTagMin = (x: HarvestAttempt) =>
      x.ok && typeof x.minutes === 'number' ? x.minutes : Number.NaN;
    const numGpM = (x: HarvestAttempt) =>
      x.gps_plus?.ok && typeof x.gps_plus.meters === 'number' ? x.gps_plus.meters : Number.NaN;
    const numGpMin = (x: HarvestAttempt) =>
      x.gps_plus?.ok && typeof x.gps_plus.minutes === 'number' ? x.gps_plus.minutes : Number.NaN;
    const cmpNum = (vx: number, vy: number) => {
      const nx = Number.isFinite(vx);
      const ny = Number.isFinite(vy);
      if (!nx && !ny) return 0;
      if (!nx) return 1;
      if (!ny) return -1;
      return vx - vy;
    };
    return [...a].sort((x, y) => {
      let c = 0;
      switch (sortCol) {
        case 'job_id':
          c = String(x.job_id).localeCompare(String(y.job_id));
          break;
        case 'tag_m':
          c = cmpNum(numTagM(x), numTagM(y));
          break;
        case 'tag_min':
          c = cmpNum(numTagMin(x), numTagMin(y));
          break;
        case 'gp_m':
          c = cmpNum(numGpM(x), numGpM(y));
          break;
        case 'gp_min':
          c = cmpNum(numGpMin(x), numGpMin(y));
          break;
        case 'status': {
          const sx = x.ok === true || x.outcome === 'success' ? 1 : 0;
          const sy = y.ok === true || y.outcome === 'success' ? 1 : 0;
          c = sx - sy;
          break;
        }
        case 'fail_msg':
          c = String(x.skipReason ?? '').localeCompare(String(y.skipReason ?? ''));
          break;
        default:
          c = 0;
      }
      if (c !== 0) return mul * c;
      return String(x.job_id).localeCompare(String(y.job_id));
    });
  }, [attempts, q, status, sortCol, sortAsc]);

  const th = (col: DryHarvestSortCol, label: string, className = '') => (
    <th className={`px-2 py-1.5 ${className}`}>
      <button
        type="button"
        onClick={() => toggleSort(col)}
        className="font-semibold text-zinc-800 hover:text-zinc-950 dark:text-zinc-200 dark:hover:text-white"
      >
        {label}
        {sortCol === col && (
          <span className="ml-0.5 font-mono text-zinc-500" aria-hidden>
            {sortAsc ? '↑' : '↓'}
          </span>
        )}
      </button>
    </th>
  );

  return (
    <div>
      <div className="mb-2 flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-0.5 text-[10px] font-medium uppercase tracking-wide text-zinc-500">
          Filter text
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="job_id, fail text…"
            className="w-44 rounded border border-zinc-200 bg-white px-2 py-1 text-xs text-zinc-900 dark:border-zinc-600 dark:bg-zinc-950 dark:text-zinc-100"
          />
        </label>
        <label className="flex flex-col gap-0.5 text-[10px] font-medium uppercase tracking-wide text-zinc-500">
          Primary path
          <select
            value={status}
            onChange={(e) => setStatus(e.target.value as 'all' | 'ok' | 'fail')}
            className="rounded border border-zinc-200 bg-white px-2 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-950"
          >
            <option value="all">All jobs</option>
            <option value="ok">OK only</option>
            <option value="fail">Failed only</option>
          </select>
        </label>
        <span className="pb-1 text-[10px] text-zinc-500">
          Showing <strong>{rows.length}</strong> of {attempts.length}
        </span>
      </div>
      <div className="max-h-[min(52vh,520px)] overflow-auto rounded border border-zinc-200 dark:border-zinc-700">
        <table className="w-full min-w-[40rem] border-collapse text-left text-[11px]">
          <thead className="sticky top-0 z-20 border-b border-zinc-200 bg-zinc-100 shadow-sm dark:border-zinc-600 dark:bg-zinc-800">
            <tr>
              {th('job_id', 'job_id')}
              {th('status', 'Tag', 'text-center')}
              <th className="px-2 py-1.5 font-semibold text-zinc-700 dark:text-zinc-200">worker</th>
              <th className="px-2 py-1.5 font-semibold text-zinc-700 dark:text-zinc-200">Winery fence used</th>
              <th className="px-2 py-1.5 font-semibold text-zinc-700 dark:text-zinc-200">Vineyard fence used</th>
              {th('tag_m', 'Tag m', 'text-right')}
              {th('tag_min', 'Tag min', 'text-right')}
              {th('gp_m', 'GPS+ m', 'text-right')}
              {th('gp_min', 'GPS+ min', 'text-right')}
              {th('fail_msg', 'Primary fail / GPS+ fail')}
            </tr>
          </thead>
          <tbody>
            {rows.map((a, idx) => (
              <tr
                key={`${a.job_id}-${idx}`}
                className={`border-t border-zinc-100 dark:border-zinc-700 ${
                  a.ok ? 'bg-emerald-50/50 dark:bg-emerald-950/20' : 'bg-amber-50/40 dark:bg-amber-950/15'
                }`}
              >
                <td className="px-2 py-1 font-mono">{a.job_id}</td>
                <td className="px-2 py-1 text-center font-medium">
                  {a.ok ? <span className="text-emerald-800 dark:text-emerald-200">OK</span> : '—'}
                </td>
                <td className="px-2 py-1 font-mono text-zinc-700 dark:text-zinc-300">
                  {workerByJobId.get(String(a.job_id)) ?? '—'}
                </td>
                {(() => {
                  const wUsed = matchedFenceNameFromAttempt(a, 'winery');
                  const vUsed = matchedFenceNameFromAttempt(a, 'vineyard');
                  return (
                    <>
                      <td
                        className="max-w-[14rem] truncate px-2 py-1 font-mono text-[10px] text-zinc-700 dark:text-zinc-300"
                        title={wUsed}
                      >
                        {wUsed}
                      </td>
                      <td
                        className="max-w-[14rem] truncate px-2 py-1 font-mono text-[10px] text-zinc-700 dark:text-zinc-300"
                        title={vUsed}
                      >
                        {vUsed}
                      </td>
                    </>
                  );
                })()}
                <td className="px-2 py-1 text-right font-mono">
                  {a.ok && typeof a.meters === 'number' ? fmt2(a.meters) : a.ok ? '—' : ''}
                </td>
                <td className="px-2 py-1 text-right font-mono">
                  {a.ok && typeof a.minutes === 'number' ? a.minutes.toFixed(2) : a.ok ? '—' : '—'}
                </td>
                <td className="px-2 py-1 text-right font-mono">
                  {a.gps_plus?.ok && typeof a.gps_plus.meters === 'number' ? fmt2(a.gps_plus.meters) : ''}
                </td>
                <td className="px-2 py-1 text-right font-mono">
                  {a.gps_plus?.ok && typeof a.gps_plus.minutes === 'number' ? (
                    a.gps_plus.minutes.toFixed(2)
                  ) : a.gps_plus?.ok ? (
                    '—'
                  ) : (
                    <span className="text-[10px] text-zinc-500">{a.gps_plus?.skipReason ?? '—'}</span>
                  )}
                </td>
                <td className="max-w-[14rem] truncate px-2 py-1 text-[10px] text-amber-950 dark:text-amber-100" title={a.skipReason}>
                  {!a.ok ? a.skipReason ?? '—' : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

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
  const nGp = s.attempts.filter((a) => a.gps_plus?.ok === true).length;
  if (nGp > 0 && av.avgGpsPlusMeters != null) {
    lines.push('');
    lines.push(
      `GPS+ mean (${nGp} OK): ${Math.round(av.avgGpsPlusMeters)} m` +
        (av.avgGpsPlusMinutes != null ? `, ${av.avgGpsPlusMinutes.toFixed(2)} min` : '')
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
        {snapshot.jobMatchDebug && (
          <details className="mt-2 rounded border border-zinc-200 bg-white/60 p-2 text-xs dark:border-zinc-700 dark:bg-zinc-950/30">
            <summary className="cursor-pointer select-none font-medium text-zinc-700 dark:text-zinc-300">
              vWork match SQL (first step) — count + first job_ids
            </summary>
            <div className="mt-2 space-y-2">
              <div className="font-mono text-[11px] text-zinc-700 dark:text-zinc-300">
                matched={snapshot.jobMatchDebug.vworkJobsMatchCount} · candidateJobIds={snapshot.jobMatchDebug.candidateJobIds.length}
              </div>
              <div className="space-y-1">
                <div className="text-[10px] font-semibold uppercase tracking-wide text-zinc-500">COUNT SQL</div>
                <pre className="max-h-40 overflow-auto rounded bg-zinc-100 p-2 font-mono text-[10px] text-zinc-800 dark:bg-zinc-900 dark:text-zinc-100">
{snapshot.jobMatchDebug.countSql}
                </pre>
              </div>
              <div className="space-y-1">
                <div className="text-[10px] font-semibold uppercase tracking-wide text-zinc-500">LIST SQL</div>
                <pre className="max-h-40 overflow-auto rounded bg-zinc-100 p-2 font-mono text-[10px] text-zinc-800 dark:bg-zinc-900 dark:text-zinc-100">
{snapshot.jobMatchDebug.listSql}
                </pre>
              </div>
              <div className="space-y-1">
                <div className="text-[10px] font-semibold uppercase tracking-wide text-zinc-500">PARAMS</div>
                <pre className="max-h-24 overflow-auto rounded bg-zinc-100 p-2 font-mono text-[10px] text-zinc-800 dark:bg-zinc-900 dark:text-zinc-100">
{JSON.stringify(snapshot.jobMatchDebug.params)}
                </pre>
              </div>
              {snapshot.jobMatchDebug.candidateJobIds.length > 0 && (
                <div className="space-y-1">
                  <div className="text-[10px] font-semibold uppercase tracking-wide text-zinc-500">FIRST JOB_IDS</div>
                  <div className="flex flex-wrap gap-1">
                    {snapshot.jobMatchDebug.candidateJobIds.slice(0, 24).map((id) => (
                      <span
                        key={id}
                        className="rounded bg-zinc-200 px-1.5 py-0.5 font-mono text-[10px] text-zinc-800 dark:bg-zinc-800 dark:text-zinc-100"
                      >
                        {id}
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </details>
        )}
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
          const av = dryRunAveragesFromAttempts(snapshot.attempts);
          const nGp = snapshot.attempts.filter((a) => a.gps_plus?.ok === true).length;
          if (nOk === 0 && nGp === 0) return null;
          return (
            <div className="mt-2 space-y-1 border-t border-zinc-200/80 pt-2 text-xs text-zinc-700 dark:text-zinc-300">
              {nOk > 0 && (av.avgMeters != null || av.avgMinutes != null) && (
                <p>
                  <span className="font-medium">Mean (of {nOk} successful job{nOk === 1 ? '' : 's'} only):</span>{' '}
                  {av.avgMeters != null && <span>{Math.round(av.avgMeters)} m</span>}
                  {av.avgMeters != null && av.avgMinutes != null && ' · '}
                  {av.avgMinutes != null && <span>{av.avgMinutes.toFixed(2)} min</span>}
                </p>
              )}
              {nGp > 0 && av.avgGpsPlusMeters != null && (
                <p>
                  <span className="font-medium">GPS+ mean ({nGp} OK):</span>{' '}
                  <span>{Math.round(av.avgGpsPlusMeters)} m</span>
                  {av.avgGpsPlusMinutes != null && (
                    <>
                      {' · '}
                      <span>{av.avgGpsPlusMinutes.toFixed(2)} min</span>
                    </>
                  )}
                </p>
              )}
            </div>
          );
        })()}
        {snapshot.dryDistanceRowUpdated && snapshot.rollup && (
          <div className="mt-3 rounded border border-violet-200 bg-violet-50/90 p-2 text-xs text-violet-950 dark:border-violet-900 dark:bg-violet-950/40 dark:text-violet-100">
            <p className="font-semibold">tbl_distances updated from this dry run (samples not saved)</p>
            <p className="mt-1 font-mono">
              distance_via={snapshot.rollup.distance_via ?? 'null'} · distance_m={snapshot.rollup.distance_m ?? 'null'}{' '}
              · tag_OK={snapshot.rollup.gps_sample_count ?? 0} · GPS+_OK={snapshot.rollup.gps_plus_sample_count ?? 0} ·
              avg_tag_m={snapshot.rollup.gps_avg_distance_m ?? 'null'} · avg_GPS+_m=
              {snapshot.rollup.gps_plus_avg_distance_m ?? 'null'}
            </p>
          </div>
        )}
      </div>

      {snapshot.attempts.length > 0 && (
        <div>
          <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-zinc-500">
            Results (polled jobs) — filter and sort
          </h4>
          <DryRunHarvestJobsTable attempts={snapshot.attempts} workerByJobId={workerByJobId} />
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

                {a.gps_plus && (
                  <div className="mt-2 border-t border-zinc-200/80 pt-2 dark:border-zinc-700">
                    <p className="text-[11px] font-semibold uppercase tracking-wide text-violet-700 dark:text-violet-300">
                      GPS+ (vineyard +100m buffer, any hit)
                    </p>
                    {a.gps_plus.ok ? (
                      <dl className="mt-1 space-y-0.5 text-sm text-zinc-800 dark:text-zinc-200">
                        <div className="flex flex-wrap gap-x-3 gap-y-0.5">
                          <dt className="text-zinc-500">Metres</dt>
                          <dd className="font-mono font-medium">
                            {typeof a.gps_plus.meters === 'number' ? a.gps_plus.meters : '—'}
                          </dd>
                        </div>
                        <div className="flex flex-wrap gap-x-3 gap-y-0.5">
                          <dt className="text-zinc-500">Minutes</dt>
                          <dd className="font-mono font-medium">
                            {typeof a.gps_plus.minutes === 'number' ? a.gps_plus.minutes.toFixed(2) : '—'}
                          </dd>
                        </div>
                        {a.gps_plus.winery_anchor_source != null && (
                          <div className="text-xs text-zinc-500">
                            Winery anchor: <span className="font-mono">{a.gps_plus.winery_anchor_source}</span>
                          </div>
                        )}
                        {a.gps_plus.segment_point_count != null && (
                          <div className="text-xs text-zinc-500">Path: {a.gps_plus.segment_point_count} pts</div>
                        )}
                      </dl>
                    ) : (
                      <p className="mt-1 text-xs font-medium text-amber-900 dark:text-amber-100">
                        {a.gps_plus.skipReason ?? 'GPS+ failed'}
                      </p>
                    )}
                    {a.gps_plus.detail != null && Object.keys(a.gps_plus.detail).length > 0 && (
                      <details className="mt-1">
                        <summary className="cursor-pointer text-[10px] text-zinc-500">GPS+ anchors</summary>
                        <div className="mt-1 max-h-40 overflow-auto rounded bg-zinc-100/80 p-2 dark:bg-zinc-950/80">
                          <StructuredDebugValue value={a.gps_plus.detail} maxDepth={5} />
                        </div>
                      </details>
                    )}
                  </div>
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
  const t = s.trim().replace(/,/g, '');
  if (t === '') return null;
  const n = Number(t);
  if (!Number.isFinite(n) || !Number.isInteger(n)) return 'invalid';
  return n;
}

function parseDurationInput(s: string): number | null | 'invalid' {
  const t = s.trim().replace(/,/g, '');
  if (t === '') return null;
  const n = Number(t);
  if (!Number.isFinite(n)) return 'invalid';
  return n;
}

function listParams(winery: string, vineyard: string, client: string): string {
  const p = new URLSearchParams();
  if (winery.trim()) p.set('winery', winery.trim());
  if (vineyard.trim()) p.set('vineyard', vineyard.trim());
  if (client.trim()) p.set('client', client.trim());
  const q = p.toString();
  return q ? `?${q}` : '';
}

/** Build the structured debug modal payload from rows already in tbl_distances_gps_samples (e.g. after refresh). */
function gpsPlusFromSampleRow(s: GpsSampleRow, debug: Record<string, unknown> | undefined): GpsPlusSummary | undefined {
  const fromDebug = debug?.gps_plus;
  if (fromDebug != null && typeof fromDebug === 'object' && !Array.isArray(fromDebug)) {
    const o = fromDebug as Record<string, unknown>;
    if (o.ok === true) {
      return {
        ok: true,
        meters: typeof o.meters === 'number' ? o.meters : undefined,
        minutes: typeof o.minutes === 'number' ? o.minutes : undefined,
        winery_anchor_source: typeof o.winery_anchor_source === 'string' ? o.winery_anchor_source : undefined,
        vineyard_hit_tracking_id:
          typeof o.vineyard_hit_tracking_id === 'number' ? o.vineyard_hit_tracking_id : null,
        segment_point_count: typeof o.segment_point_count === 'number' ? o.segment_point_count : undefined,
        detail: o.detail != null && typeof o.detail === 'object' && !Array.isArray(o.detail)
          ? (o.detail as Record<string, unknown>)
          : undefined,
      };
    }
    if (o.ok === false || o.ok === undefined) {
      return {
        ok: false,
        skipReason: typeof o.skipReason === 'string' ? o.skipReason : 'GPS+ not OK',
      };
    }
  }
  if (s.gps_plus_outcome === 'success' && typeof s.gps_plus_meters === 'number') {
    return {
      ok: true,
      meters: s.gps_plus_meters,
      minutes: typeof s.gps_plus_minutes === 'number' ? s.gps_plus_minutes : undefined,
    };
  }
  if (s.gps_plus_outcome === 'failed') {
    return { ok: false, skipReason: 'GPS+ failed (saved row)' };
  }
  return undefined;
}

function dryRunSnapshotFromSavedSamples(row: DistanceRow, samples: GpsSampleRow[]): DryRunDebugSnapshot {
  const attempts: HarvestAttempt[] = samples.map((s) => {
    const ok = s.outcome === 'success';
    const dbg = s.debug_json;
    const debug =
      dbg != null && typeof dbg === 'object' && !Array.isArray(dbg)
        ? (dbg as Record<string, unknown>)
        : undefined;
    const gps_plus = gpsPlusFromSampleRow(s, debug);
    return {
      job_id: String(s.job_id),
      ok,
      outcome: s.outcome,
      skipReason: ok ? undefined : (s.failure_reason ?? s.outcome ?? 'failed'),
      meters: typeof s.meters === 'number' ? s.meters : undefined,
      minutes: typeof s.minutes === 'number' ? s.minutes : undefined,
      segment_point_count: s.segment_point_count ?? undefined,
      gps_plus,
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
  avgGpsPlusMeters: number | null;
  avgGpsPlusMinutes: number | null;
} {
  const ok = attempts.filter((a) => a.ok === true || a.outcome === 'success');
  const n = ok.length;
  const withM = ok.filter((a): a is HarvestAttempt & { meters: number } => typeof a.meters === 'number');
  const withMin = ok.filter((a): a is HarvestAttempt & { minutes: number } => typeof a.minutes === 'number');
  const gpOk = attempts.filter((a) => a.gps_plus?.ok === true);
  const ng = gpOk.length;
  const withGpM = gpOk.filter((a): a is HarvestAttempt & { gps_plus: { meters: number } } =>
    typeof a.gps_plus?.meters === 'number'
  );
  const withGpMin = gpOk.filter((a): a is HarvestAttempt & { gps_plus: { minutes: number } } =>
    typeof a.gps_plus?.minutes === 'number'
  );
  return {
    avgMeters: n === 0 ? null : withM.length === n ? withM.reduce((s, a) => s + a.meters, 0) / n : null,
    avgMinutes: n === 0 ? null : withMin.length === n ? withMin.reduce((s, a) => s + a.minutes, 0) / n : null,
    avgGpsPlusMeters:
      ng === 0 ? null : withGpM.length === ng ? withGpM.reduce((s, a) => s + a.gps_plus!.meters!, 0) / ng : null,
    avgGpsPlusMinutes:
      ng === 0 ? null : withGpMin.length === ng ? withGpMin.reduce((s, a) => s + a.gps_plus!.minutes!, 0) / ng : null,
  };
}

export default function AdminDistancesPage() {
  const [wineryFilter, setWineryFilter] = useState('');
  const [vineyardFilter, setVineyardFilter] = useState('');
  const [appliedWinery, setAppliedWinery] = useState('');
  const [appliedVineyard, setAppliedVineyard] = useState('');
  const [clientFilter, setClientFilter] = useState('');
  const [appliedClient, setAppliedClient] = useState('');

  const [wineryOptions, setWineryOptions] = useState<string[]>([]);
  const [vineyardOptions, setVineyardOptions] = useState<string[]>([]);
  const [customerOptions, setCustomerOptions] = useState<string[]>([]);
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
  /** GPS rollup FAIL and no manual override (pairs that still need a drive distance). */
  const [filterFailNoManualOnly, setFilterFailNoManualOnly] = useState(false);
  const [sortKey, setSortKey] = useState<DistancesSortKey>('winery');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  /** Client-side Via filter on loaded rows (after API apply). */
  const [tableGridFilterVia, setTableGridFilterVia] = useState<'' | 'GPSTAGS' | 'GPS+' | 'FAIL' | 'MANUAL'>('');
  /** When true, dry run also UPDATEs tbl_distances for that pair (no sample rows written). */
  const [dryRunUpdatePairRow, setDryRunUpdatePairRow] = useState(true);
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
    rollup?: {
      distance_via: string | null;
      gps_sample_count: number | null;
      gps_plus_sample_count: number | null;
      distance_m: number | null;
      gps_avg_distance_m: number | null;
      gps_plus_avg_distance_m: number | null;
    };
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
  const [manualDm, setManualDm] = useState('');
  const [manualDur, setManualDur] = useState('');
  const [manualNotes, setManualNotes] = useState('');
  const [manualBusy, setManualBusy] = useState(false);
  const [manualMsg, setManualMsg] = useState<string | null>(null);

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
        avgGpsPlusMeters: number | null;
        avgGpsPlusMinutes: number | null;
        gpsPlusSuccessCount: number;
      }
    >
  >({});

  const wineryListId = useMemo(() => 'distances-winery-options', []);
  const vineyardListId = useMemo(() => 'distances-vineyard-options', []);

  const load = useCallback(async (winery: string, vineyard: string, client: string) => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch(`/api/admin/distances${listParams(winery, vineyard, client)}`, {
        cache: 'no-store',
      });
      const json = (await r.json()) as {
        error?: string;
        debug?: Record<string, unknown>;
        rows?: unknown;
      };
      if (!r.ok || json.error) {
        const parts = [json.error ?? `HTTP ${r.status}`];
        if (json.debug && typeof json.debug === 'object') {
          parts.push(JSON.stringify(json.debug, null, 2));
        }
        if (!json.debug && r.status >= 500) {
          parts.push(
            'Tip: open /api/admin/distances?debug=1 for PostgreSQL code/detail, or check the server terminal for [GET /api/admin/distances].'
          );
        }
        throw new Error(parts.join('\n\n'));
      }
      const d = json as ListResponse;
      setWineryOptions(d.wineryOptions ?? []);
      setVineyardOptions(d.vineyardOptions ?? []);
      const custOpts = d.customerOptions ?? [];
      setCustomerOptions(custOpts);
      setClientFilter((prev) => {
        if (!prev) return prev;
        if (custOpts.length === 0) return '';
        return custOpts.includes(prev) ? prev : '';
      });
      setAppliedClient((applied) => {
        if (!applied) return applied;
        if (custOpts.length === 0) return '';
        return custOpts.includes(applied) ? applied : '';
      });
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
    void load('', '', '');
  }, [load]);

  useEffect(() => {
    if (selectedId == null) {
      setSamples([]);
      return;
    }
    void loadSamples(selectedId);
  }, [selectedId, loadSamples]);

  useEffect(() => {
    const row = rows.find((r) => r.id === selectedId);
    setManualMsg(null);
    if (!row) {
      setManualDm('');
      setManualDur('');
      setManualNotes('');
      return;
    }
    setManualDm(row.manual_distance_m != null && row.manual_distance_m > 0 ? String(row.manual_distance_m) : '');
    const md = row.manual_duration_min;
    setManualDur(md != null && String(md).trim() !== '' && !Number.isNaN(Number(md)) ? String(Number(md)) : '');
    setManualNotes(row.manual_notes ?? '');
  }, [selectedId, rows]);

  const applyFilters = useCallback(() => {
    const c = clientFilter.trim();
    setAppliedWinery(wineryFilter);
    setAppliedVineyard(vineyardFilter);
    setAppliedClient(c);
    void load(wineryFilter, vineyardFilter, c);
  }, [load, wineryFilter, vineyardFilter, clientFilter]);

  const clearFilters = useCallback(() => {
    setWineryFilter('');
    setVineyardFilter('');
    setClientFilter('');
    setAppliedWinery('');
    setAppliedVineyard('');
    setAppliedClient('');
    setFilterNoDataOnly(false);
    setFilterFailNoManualOnly(false);
    setTableGridFilterVia('');
    void load('', '', '');
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
        await load(appliedWinery, appliedVineyard, appliedClient);
        return;
      }
      await load(appliedWinery, appliedVineyard, appliedClient);
    },
    [load, appliedWinery, appliedVineyard, appliedClient]
  );

  const saveManualDistance = useCallback(async () => {
    const row = rows.find((r) => r.id === selectedId);
    if (!row) return;
    setManualBusy(true);
    setManualMsg(null);
    try {
      const m = Math.round(Number(manualDm));
      if (!Number.isFinite(m) || m <= 0) {
        setManualMsg('Manual distance (m) must be a positive whole number.');
        return;
      }
      let duration_min: number | null = null;
      if (manualDur.trim() !== '') {
        const t = Number(manualDur);
        if (Number.isNaN(t) || t < 0) {
          setManualMsg('Manual minutes must be empty or a non-negative number.');
          return;
        }
        duration_min = t;
      }
      const r = await fetch('/api/admin/distances/manual', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          delivery_winery: row.delivery_winery,
          vineyard_name: row.vineyard_name,
          distance_m: m,
          duration_min,
          notes: manualNotes.trim() === '' ? null : manualNotes.trim(),
        }),
      });
      const j = await r.json();
      if (!r.ok || j.error) throw new Error(j.error ?? r.statusText);
      await load(appliedWinery, appliedVineyard, appliedClient);
      setManualMsg('Saved manual distance for this pair.');
    } catch (e) {
      setManualMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setManualBusy(false);
    }
  }, [rows, selectedId, manualDm, manualDur, manualNotes, load, appliedWinery, appliedVineyard, appliedClient]);

  const clearManualDistance = useCallback(async () => {
    const row = rows.find((r) => r.id === selectedId);
    if (!row) return;
    setManualBusy(true);
    setManualMsg(null);
    try {
      const r = await fetch('/api/admin/distances/manual', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          delivery_winery: row.delivery_winery,
          vineyard_name: row.vineyard_name,
        }),
      });
      const j = await r.json();
      if (!r.ok || j.error) throw new Error(j.error ?? r.statusText);
      await load(appliedWinery, appliedVineyard, appliedClient);
      setManualMsg('Cleared manual override.');
    } catch (e) {
      setManualMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setManualBusy(false);
    }
  }, [rows, selectedId, load, appliedWinery, appliedVineyard, appliedClient]);

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
            dryRunUpdateDistanceRow: dryRun && dryRunUpdatePairRow,
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
          const jobMatchDebug =
            j && typeof j === 'object' && 'jobMatchDebug' in j && j.jobMatchDebug != null && typeof j.jobMatchDebug === 'object'
              ? (j.jobMatchDebug as DryRunDebugSnapshot['jobMatchDebug'])
              : undefined;
          const rollupFromApi =
            j && typeof j === 'object' && 'rollup' in j && j.rollup != null && typeof j.rollup === 'object'
              ? (j.rollup as DryRunDebugSnapshot['rollup'])
              : undefined;
          const dryRowUp =
            j && typeof j === 'object' && 'dryDistanceRowUpdated' in j && j.dryDistanceRowUpdated === true;
          const snapshot: DryRunDebugSnapshot = {
            distanceId,
            distancePair,
            vworkJobsMatchCount,
            jobMatchDebug,
            candidateJobs,
            jobsPolled,
            successCount,
            failedCount,
            maxJobsCap,
            maxSuccessesCap,
            message: typeof j.message === 'string' ? j.message : undefined,
            attempts,
            rollup: rollupFromApi,
            dryDistanceRowUpdated: dryRowUp,
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
              avgGpsPlusMeters: av.avgGpsPlusMeters,
              avgGpsPlusMinutes: av.avgGpsPlusMinutes,
              gpsPlusSuccessCount: attempts.filter((a) => a.gps_plus?.ok === true).length,
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
          const rollup =
            j && typeof j === 'object' && 'rollup' in j && j.rollup != null && typeof j.rollup === 'object'
              ? (j.rollup as {
                  distance_via?: string | null;
                  gps_sample_count?: number | null;
                  gps_plus_sample_count?: number | null;
                  distance_m?: number | null;
                  gps_avg_distance_m?: number | null;
                  gps_plus_avg_distance_m?: number | null;
                })
              : undefined;
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
            rollup: rollup
              ? {
                  distance_via: rollup.distance_via ?? null,
                  gps_sample_count: rollup.gps_sample_count ?? null,
                  gps_plus_sample_count: rollup.gps_plus_sample_count ?? null,
                  distance_m: rollup.distance_m ?? null,
                  gps_avg_distance_m: rollup.gps_avg_distance_m ?? null,
                  gps_plus_avg_distance_m: rollup.gps_plus_avg_distance_m ?? null,
                }
              : undefined,
          });
          setGpsDryRunPreviewById((prev) => {
            const next = { ...prev };
            delete next[distanceId];
            return next;
          });
          await load(appliedWinery, appliedVineyard, appliedClient);
          if (selectedId === distanceId) void loadSamples(distanceId);
        }
        if (
          dryRun &&
          j &&
          typeof j === 'object' &&
          'dryDistanceRowUpdated' in j &&
          (j as { dryDistanceRowUpdated?: boolean }).dryDistanceRowUpdated === true
        ) {
          await load(appliedWinery, appliedVineyard, appliedClient);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setHarvestBusyId(null);
      }
    },
    [load, appliedWinery, appliedVineyard, appliedClient, selectedId, loadSamples, dryRunUpdatePairRow]
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
        return load(appliedWinery, appliedVineyard, appliedClient);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSeeding(false));
  }, [load, appliedWinery, appliedVineyard, appliedClient]);

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
          `Updated ${n} job row(s): distance set to round-trip km (pair m÷1000×2), minutes to one-way duration, where winery+vineyard matches (trim, case-insensitive). Jobs with no matching pair were left unchanged.`
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
              dryRunUpdateDistanceRow: false,
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
              avgGpsPlusMeters: av.avgGpsPlusMeters,
              avgGpsPlusMinutes: av.avgGpsPlusMinutes,
              gpsPlusSuccessCount: attempts.filter((a) => a.gps_plus?.ok === true).length,
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
                    detail: `dry ${successCount}/${maxSuccessesCapDry} ok · ${jobsPolled}/${maxJobsCapDry} polled; saved ${Number(jSave.successCount ?? 0) || 0} OK${
                      jSave &&
                      typeof jSave === 'object' &&
                      'rollup' in jSave &&
                      jSave.rollup != null &&
                      typeof (jSave.rollup as { distance_via?: string }).distance_via === 'string'
                        ? ` · via ${(jSave.rollup as { distance_via: string }).distance_via}`
                        : ''
                    }`,
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
      await load(appliedWinery, appliedVineyard, appliedClient);
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
      appliedClient,
      load,
      selectedId,
      loadSamples,
      rows,
      setDryRunDebugSnapshotById,
      setGpsDryRunPreviewById,
    ]
  );

  const hasRowsInDb = wineryOptions.length > 0 || vineyardOptions.length > 0;
  const filterActive =
    appliedWinery.trim() !== '' || appliedVineyard.trim() !== '' || appliedClient.trim() !== '';

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

  const tableGridFilteredRows = useMemo(() => {
    let list = rows;
    if (filterNoDataOnly) list = list.filter(rowHasNoDistanceData);
    if (filterFailNoManualOnly) {
      list = list.filter((r) => {
        const rv = (r.distance_via ?? '').trim().toUpperCase();
        const noManual = r.manual_distance_m == null || Number(r.manual_distance_m) <= 0;
        return rv === 'FAIL' && noManual;
      });
    }
    return list.filter((r) => {
      if (tableGridFilterVia) {
        const rollup = (r.distance_via ?? '').trim().toUpperCase();
        if (tableGridFilterVia === 'MANUAL') {
          if (viaLabelForRow(r).toUpperCase() !== 'MANUAL') return false;
        } else if (rollup !== tableGridFilterVia) return false;
      }
      return true;
    });
  }, [rows, filterNoDataOnly, filterFailNoManualOnly, tableGridFilterVia]);

  const displayRows = useMemo(() => {
    const list = tableGridFilteredRows;
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
          return cmpNullLastNum(effectiveDistanceM(a), effectiveDistanceM(b));
        case 'minutes':
          return cmpNullLastNum(durationMinSortValue(a), durationMinSortValue(b));
        case 'tag_avg_m':
          return cmpNullLastNum(
            a.gps_avg_distance_m == null ? null : Number(a.gps_avg_distance_m),
            b.gps_avg_distance_m == null ? null : Number(b.gps_avg_distance_m)
          );
        case 'gps_plus_avg_m':
          return cmpNullLastNum(
            a.gps_plus_avg_distance_m == null ? null : Number(a.gps_plus_avg_distance_m),
            b.gps_plus_avg_distance_m == null ? null : Number(b.gps_plus_avg_distance_m)
          );
        case 'gps_ok':
          return (
            dir * (gpsOkCountForSort(a, gpsDryRunPreviewById) - gpsOkCountForSort(b, gpsDryRunPreviewById))
          );
        case 'gps_success':
          return dir * ((a.gps_sample_count ?? 0) - (b.gps_sample_count ?? 0));
        case 'distance_via': {
          const rank = (row: DistanceRow) => {
            const t = viaLabelForRow(row).toUpperCase();
            if (t === 'FAIL' || t === '') return 0;
            if (t === 'GPS+') return 1;
            if (t === 'GPSTAGS') return 2;
            if (t === 'MANUAL') return 3;
            return 4;
          };
          return dir * (rank(a) - rank(b));
        }
        default:
          return 0;
      }
    });
  }, [tableGridFilteredRows, sortKey, sortDir, gpsDryRunPreviewById]);

  const tableColumnFiltersActive = useMemo(
    () => tableGridFilterVia !== '' || filterFailNoManualOnly,
    [tableGridFilterVia, filterFailNoManualOnly]
  );

  const noDataRowCount = useMemo(() => rows.filter(rowHasNoDistanceData).length, [rows]);

  const exportXlsx = useCallback(async () => {
    setError(null);
    try {
      const XLSX = await import('xlsx');
      const data = displayRows.map((r) => {
        const via = viaLabelForRow(r).toUpperCase();
        const used =
          via === 'MANUAL'
            ? 'MANUAL'
            : via === 'GPSTAGS'
              ? 'GPS'
              : via === 'GPS+'
                ? 'GPS+'
                : 'NA';
        const effM = effectiveDistanceM(r);
        const jobs = r.vwork_job_count ?? 0;
        const totalM = totalDistanceTimesJobs(r);
        return {
          Used: used,
          Winery: r.delivery_winery,
          Vineyard: r.vineyard_name,
          Client: (r.pair_clients ?? '').trim() || null,
          Jobs: jobs,
          Total_m_times_jobs: totalM == null ? null : totalM,
          'GPS avg m': r.gps_avg_distance_m == null ? null : Number(r.gps_avg_distance_m),
          'GPS avg min':
            r.gps_avg_duration_min == null || String(r.gps_avg_duration_min).trim() === ''
              ? null
              : Number(r.gps_avg_duration_min),
          'GPS+ avg m': r.gps_plus_avg_distance_m == null ? null : Number(r.gps_plus_avg_distance_m),
          'GPS+ avg min':
            r.gps_plus_avg_duration_min == null || String(r.gps_plus_avg_duration_min).trim() === ''
              ? null
              : Number(r.gps_plus_avg_duration_min),
          distance_via_rollup: r.distance_via ?? '',
          display_via: via || null,
          distance_m_tbl: r.distance_m == null ? null : Number(r.distance_m),
          duration_min_tbl:
            r.duration_min == null || String(r.duration_min).trim() === '' ? null : Number(r.duration_min),
          effective_distance_m: effM == null ? null : effM,
          effective_duration_min: (() => {
            const t = effectiveDurationMinStr(r);
            if (t == null) return null;
            const n = Number(t);
            return Number.isNaN(n) ? null : n;
          })(),
          manual_notes: (r.manual_notes ?? '').trim() || null,
          maps_drive_url: r.maps_drive_url ?? null,
        };
      });
      const ws = XLSX.utils.json_to_sheet(data, {
        header: [
          'Used',
          'Winery',
          'Vineyard',
          'Client',
          'Jobs',
          'Total_m_times_jobs',
          'GPS avg m',
          'GPS avg min',
          'GPS+ avg m',
          'GPS+ avg min',
          'distance_via_rollup',
          'display_via',
          'distance_m_tbl',
          'duration_min_tbl',
          'effective_distance_m',
          'effective_duration_min',
          'manual_notes',
          'maps_drive_url',
        ],
      });
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, 'Distances');
      const out = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
      const blob = new Blob([out], {
        type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'distances_export.xlsx';
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [displayRows]);

  useEffect(() => {
    if (harvestDebugModal == null) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setHarvestDebugModal(null);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [harvestDebugModal]);

  return (
    <div className="min-h-screen bg-zinc-50 px-4 py-6 text-zinc-900 dark:bg-zinc-950 dark:text-zinc-100">
      <div className="w-full max-w-none">
        <h1 className="text-2xl font-semibold tracking-tight">Distances</h1>
        <p className="mt-2 max-w-3xl text-sm text-zinc-600 dark:text-zinc-400">
          Winery–vineyard pairs from vWork jobs with distance (metres) and time (minutes).{' '}
          <strong>GPS harvest</strong> uses the same tracking window and fence mappings as derived steps: first winery
          EXIT, first vineyard ENTER, path metres summed along <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">position_time_nz</code>
          order; minutes = enter − exit. Each run tries up to {GPS_HARVEST_DEFAULT_MAX_JOBS} jobs (lowest job_id first in a pool
          of {GPS_HARVEST_SQL_JOB_LIMIT}) and stops after {GPS_HARVEST_DEFAULT_MAX_SUCCESSES} successful samples; Save
          persists those samples and updates <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">distance_m</code> /{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">duration_min</code>. Run{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">create_tbl_distances_gps_samples.sql</code>,{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">add_gps_avg_tbl_distances.sql</code>,{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">add_gps_plus_distances.sql</code>, and{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">add_distance_via_tbl_distances.sql</code> if needed.{' '}
          Rollup sets <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">distance_via</code> to GPSTAGS, GPS+, or FAIL
          and writes <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">distance_m</code> from tag averages when possible,
          else from GPS+ averages.{' '}
          <strong>Populate vWork</strong> sets <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">tbl_vworkjobs.distance</code> to{' '}
          round-trip <strong>km</strong>{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">(effective distance_m / 1000) × 2</code> (to the vineyard and back;
          manual overrides <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">tbl_distances</code>) and{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">minutes</code> to one-way{' '}
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">duration_min</code> (
          <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">add_distance_minutes_tbl_vworkjobs.sql</code>). Optional
          manual overrides: <code className="rounded bg-zinc-200 px-1 dark:bg-zinc-800">create_tbl_distances_manual.sql</code>.
        </p>

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={runSeed}
            disabled={seeding || loading}
            title={
              'Step 1 — Update pairs (tbl_distances seed):\n' +
              '- Deletes tbl_distances rows whose winery→vineyard pair no longer exists in tbl_vworkjobs\n' +
              '- Inserts any new distinct (trimmed) delivery_winery + vineyard_name pairs from tbl_vworkjobs\n' +
              "- Does NOT overwrite existing distance_m / duration_min values\n" +
              '- Also deletes any saved GPS samples for deleted pairs (via cascade)'
            }
            className="rounded-md bg-amber-600 px-4 py-2 text-sm font-medium text-white hover:bg-amber-500 disabled:opacity-50"
          >
            {seeding ? '1) Updating…' : '1) Update pairs'}
          </button>
          <button
            type="button"
            onClick={() => void runAllHarvest()}
            disabled={runAllBusy || loading}
            className="rounded-md bg-emerald-700 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-600 disabled:opacity-50"
            title={
              'Step 2 — Run all (dry + save):\n' +
              `- For each visible pair with < ${GPS_HARVEST_DEFAULT_MAX_SUCCESSES} saved OK primary GPS sample(s), runs Dry run then Save\n` +
              `- Skips pairs already at ${GPS_HARVEST_DEFAULT_MAX_SUCCESSES}+ saved OK primary GPS samples\n` +
              '- Save clears previous samples for that pair, writes new per-job samples, then updates rollups (distance_via / averages / distance_m / duration_min)\n' +
              '- Uses caps: maxJobs / maxSuccesses shown at top; jobs are taken by job_id asc (up to SQL limit)\n' +
              '- Dry step in Run all never updates tbl_distances directly (only Save does)'
            }
          >
            {runAllBusy ? '2) Running all…' : '2) Run all (dry + save)'}
          </button>
          <button
            type="button"
            onClick={() => void runPopulateVwork()}
            disabled={populateVworkBusy || loading}
            title={
              'Step 3 — Populate vWork:\n' +
              '- distance = (effective pair metres / 1000) × 2 round-trip km; minutes = one-way duration_min\n' +
              '- COALESCE(tbl_distances_manual, tbl_distances) for effective metres/minutes when manual table exists\n' +
              '- Match by delivery_winery + vineyard_name (case-insensitive trim)\n' +
              '- Requires: add_distance_minutes_tbl_vworkjobs.sql; optional: create_tbl_distances_manual.sql'
            }
            className="rounded-md bg-sky-700 px-4 py-2 text-sm font-medium text-white hover:bg-sky-600 disabled:opacity-50"
          >
            {populateVworkBusy ? '3) Populating…' : '3) Populate vWork'}
          </button>
          <button
            type="button"
            onClick={() => void load(appliedWinery, appliedVineyard, appliedClient)}
            disabled={loading}
            className="rounded-md border border-zinc-300 bg-white px-4 py-2 text-sm hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-900 dark:hover:bg-zinc-800"
          >
            Refresh
          </button>
          <button
            type="button"
            onClick={() => void exportXlsx()}
            disabled={loading || displayRows.length === 0}
            title="Export visible pairs: Client, Total (m×jobs), effective distance, rollup vs display via, Maps URL, manual notes, GPS averages."
            className="rounded-md border border-zinc-300 bg-white px-4 py-2 text-sm hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-900 dark:hover:bg-zinc-800"
          >
            Export XLSX
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
                Run all progress:{' '}
                <span className="tabular-nums">
                  {runAllProgress.done}/{runAllProgress.total}
                </span>{' '}
                completed
                {runAllProgress.current ? (
                  <span className="text-zinc-600 dark:text-zinc-400">
                    {' '}
                    (processing{' '}
                    <span className="tabular-nums">
                      {Math.min(runAllProgress.done + 1, runAllProgress.total)}/{runAllProgress.total}
                    </span>
                    )
                  </span>
                ) : null}
              </div>
              {runAllProgress.current && (
                <div className="text-xs text-zinc-600 dark:text-zinc-400">
                  Current: <span className="font-mono">{runAllProgress.current}</span>
                </div>
              )}
            </div>
            <div className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
              {(() => {
                const ok = runAllProgress.rows.filter((r) => r.status === 'ok').length;
                const err = runAllProgress.rows.filter((r) => r.status === 'error').length;
                const running = runAllProgress.rows.filter((r) => r.status === 'running').length;
                const pending = runAllProgress.rows.filter((r) => r.status === 'pending').length;
                return (
                  <>
                    <span className="tabular-nums">{ok}</span> ok ·{' '}
                    <span className="tabular-nums">{err}</span> error ·{' '}
                    <span className="tabular-nums">{running}</span> running ·{' '}
                    <span className="tabular-nums">{pending}</span> pending
                  </>
                );
              })()}
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
          <div className="mt-6 space-y-3 border-b border-zinc-200 pb-5 dark:border-zinc-700">
            <div className="flex flex-col gap-4">
              <div className="flex flex-col gap-3 lg:flex-row lg:flex-wrap lg:items-end">
                <div className="min-w-[10rem] flex-1">
                  <label htmlFor="filter-winery" className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">
                    Winery (contains)
                  </label>
                  <input
                    id="filter-winery"
                    list={wineryListId}
                    value={wineryFilter}
                    onChange={(e) => setWineryFilter(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && applyFilters()}
                    className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-950"
                    placeholder="e.g. Villa Maria"
                    autoComplete="off"
                  />
                  <datalist id={wineryListId}>
                    {wineryOptions.map((w) => (
                      <option key={w} value={w} />
                    ))}
                  </datalist>
                </div>
                <div className="min-w-[10rem] flex-1">
                  <label htmlFor="filter-vineyard" className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">
                    Vineyard (contains)
                  </label>
                  <input
                    id="filter-vineyard"
                    list={vineyardListId}
                    value={vineyardFilter}
                    onChange={(e) => setVineyardFilter(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && applyFilters()}
                    className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-950"
                    placeholder="e.g. Rapaura"
                    autoComplete="off"
                  />
                  <datalist id={vineyardListId}>
                    {vineyardOptions.map((v) => (
                      <option key={v} value={v} />
                    ))}
                  </datalist>
                </div>
                <div className="min-w-[12rem] max-w-[20rem] flex-1">
                  <label htmlFor="filter-client" className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">
                    Client
                  </label>
                  <select
                    id="filter-client"
                    value={clientFilter}
                    onChange={(e) => setClientFilter(e.target.value)}
                    className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-950"
                    title={
                      customerOptions.length === 0
                        ? 'No customers in tbl_vworkjobs (or customer column missing).'
                        : 'tbl_vworkjobs.customer — pairs whose aggregated Client list includes this value'
                    }
                  >
                    <option value="">All clients</option>
                    {customerOptions.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="min-w-[8rem]">
                  <label htmlFor="filter-via" className="block text-xs font-medium text-zinc-600 dark:text-zinc-400">
                    Via (loaded rows)
                  </label>
                  <select
                    id="filter-via"
                    value={tableGridFilterVia}
                    onChange={(e) =>
                      setTableGridFilterVia(e.target.value as '' | 'GPSTAGS' | 'GPS+' | 'FAIL' | 'MANUAL')
                    }
                    className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-950"
                  >
                    <option value="">Any</option>
                    <option value="GPSTAGS">GPSTAGS</option>
                    <option value="GPS+">GPS+</option>
                    <option value="FAIL">FAIL</option>
                    <option value="MANUAL">MANUAL</option>
                  </select>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <button
                    type="button"
                    onClick={applyFilters}
                    disabled={loading}
                    className="rounded border border-zinc-800 bg-zinc-800 px-3 py-1.5 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:border-zinc-200 dark:bg-zinc-200 dark:text-zinc-900 dark:hover:bg-zinc-300"
                  >
                    Apply
                  </button>
                  <button
                    type="button"
                    onClick={clearFilters}
                    disabled={loading}
                    className="rounded border border-zinc-300 bg-white px-3 py-1.5 text-sm hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-900 dark:hover:bg-zinc-800"
                  >
                    Clear all
                  </button>
                </div>
              </div>
              <div className="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center">
                <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
                  <input
                    type="checkbox"
                    checked={filterNoDataOnly}
                    onChange={(e) => setFilterNoDataOnly(e.target.checked)}
                    className="rounded border-zinc-400"
                  />
                  <span title="After merging manual distances: effective metres and minutes still unset or zero">
                    No data
                  </span>
                </label>
                <label className="flex cursor-pointer items-center gap-2 text-sm text-zinc-700 dark:text-zinc-300">
                  <input
                    type="checkbox"
                    checked={filterFailNoManualOnly}
                    onChange={(e) => setFilterFailNoManualOnly(e.target.checked)}
                    className="rounded border-zinc-400"
                  />
                  <span title="GPS rollup is FAIL and there is no manual distance row for this pair">
                    FAIL, no manual
                  </span>
                </label>
                <label className="flex max-w-xl cursor-pointer items-start gap-2 text-sm text-zinc-700 dark:text-zinc-300">
                  <input
                    type="checkbox"
                    className="mt-0.5 rounded border-zinc-400"
                    checked={dryRunUpdatePairRow}
                    onChange={(e) => setDryRunUpdatePairRow(e.target.checked)}
                  />
                  <span>
                    Dry run updates <code className="text-xs">tbl_distances</code> (m, min, via, averages) — no sample
                    rows
                  </span>
                </label>
              </div>
            </div>
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              <strong>Apply</strong> reloads from the API (case-insensitive partial winery/vineyard;{' '}
              <strong>Client</strong> is a vWork customer chosen from the dropdown). <strong>No data</strong> and{' '}
              <strong>Via</strong> narrow the rows already loaded. Sort by clicking column headers (↑/↓).
            </p>
          </div>
        )}

        {loading && <p className="mt-6 text-sm text-zinc-500">Loading…</p>}

        {!loading && hasRowsInDb && (
          <>
            <p className="mt-4 text-sm text-zinc-600 dark:text-zinc-400">
              Showing <strong>{displayRows.length}</strong> pair{displayRows.length === 1 ? '' : 's'}
              {filterNoDataOnly ? (
                <>
                  {' '}
                  (of <strong>{noDataRowCount}</strong> with <strong>No data</strong> in this load)
                </>
              ) : (
                <>
                  {' '}
                  of <strong>{rows.length}</strong> loaded
                </>
              )}
              {tableColumnFiltersActive ? (
                <span className="text-zinc-500">
                  {' '}
                  — Via / FAIL-no-manual refinement active (client-side)
                </span>
              ) : null}
              {filterActive ? (
                <span className="text-zinc-500"> — API winery / vineyard / client filter</span>
              ) : null}
              . Click a row to load saved GPS samples and debug JSON.
            </p>
            <div className="mt-4 w-full overflow-x-auto rounded-md border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-950">
                <table className="w-full table-fixed border-collapse text-left text-xs text-zinc-900 dark:text-zinc-100">
                  <colgroup>
                    <col className="w-[12%]" />
                    <col className="w-[12%]" />
                    <col className="w-[10%]" />
                    <col className="w-[5%]" />
                    <col className="w-[6%]" />
                    <col className="w-[5%]" />
                    <col className="w-[5%]" />
                    <col className="w-[6%]" />
                    <col className="w-[6%]" />
                    <col className="w-[5%]" />
                    <col className="w-[6%]" />
                    <col className="w-[9%]" />
                    <col className="w-[5%]" />
                    <col className="w-[8%]" />
                  </colgroup>
                  <thead className="sticky top-0 z-20 bg-zinc-100 dark:bg-zinc-900">
                    <tr>
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
                    <th
                      scope="col"
                      className="border-b border-zinc-200 bg-zinc-100 px-2 py-2.5 text-left text-xs font-semibold uppercase tracking-wide text-zinc-700 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300"
                      title="Distinct tbl_vworkjobs.customer for this pair"
                    >
                      Client
                    </th>
                    <DistancesSortHeader
                      label="vWork jobs"
                      columnKey="vwork_jobs"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                      center
                      title="Jobs in tbl_vworkjobs with this winery + vineyard (trim, case-insensitive)"
                    />
                    <th
                      scope="col"
                      className="border-b border-zinc-200 bg-zinc-100 px-2 py-2.5 text-center text-xs font-semibold uppercase tracking-wide text-zinc-700 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300"
                      title="Effective metres × job count (manual overrides tbl_distances)"
                    >
                      Total
                    </th>
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
                      label="Tag m"
                      columnKey="tag_avg_m"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                      center
                      title="Mean primary GPS path (fence tag / Steps+ enter), saved rollup"
                    />
                    <DistancesSortHeader
                      label="GPS+ m"
                      columnKey="gps_plus_avg_m"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                      center
                      title="Mean GPS+ path (100m vineyard buffer), saved rollup"
                    />
                    <DistancesSortHeader
                      label="# tag OK"
                      columnKey="gps_success"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                      center
                      title="Successful primary GPS samples (asc = low counts first)"
                    />
                    <DistancesSortHeader
                      label="Via"
                      columnKey="distance_via"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                      center
                      title="MANUAL when a manual pair row exists; else GPSTAGS / GPS+ / FAIL from GPS rollup"
                    />
                    <DistancesSortHeader
                      label={
                        <>
                          GPS
                          <span className="block text-[10px] font-normal normal-case leading-tight text-zinc-500">
                            dry / saved
                          </span>
                        </>
                      }
                      columnKey="gps_ok"
                      sortKey={sortKey}
                      sortDir={sortDir}
                      onSort={toggleDistancesSort}
                      title="Sort by OK sample count (latest dry-run successes, else saved gps_sample_count). Column is narrow — text wraps."
                    />
                    <th
                      scope="col"
                      className="border-b border-zinc-200 bg-zinc-100 px-2 py-2.5 text-center text-xs font-semibold uppercase tracking-wide text-zinc-700 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300"
                      title="Google Maps driving directions between winery and vineyard fence centroids"
                    >
                      Maps
                    </th>
                    <th
                      scope="col"
                      className="border-b border-zinc-200 bg-zinc-100 px-2 py-2.5 text-center text-xs font-semibold uppercase tracking-wide text-zinc-700 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300"
                      title="Harvest — Dry run / Save"
                    >
                      H
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {rows.length === 0 ? (
                    <tr>
                      <td colSpan={14} className="px-3 py-8 text-center text-zinc-500">
                        No rows match these filters. Try clearing filters or broadening the text.
                      </td>
                    </tr>
                  ) : displayRows.length === 0 ? (
                    <tr>
                      <td colSpan={14} className="px-3 py-8 text-center text-zinc-500">
                        {filterNoDataOnly && noDataRowCount === 0 ? (
                          <>
                            Every loaded pair already has metres or minutes set. Turn off <strong>No data</strong> to see
                            all {rows.length} row{rows.length === 1 ? '' : 's'}.
                          </>
                        ) : tableColumnFiltersActive && tableGridFilteredRows.length === 0 ? (
                          <>
                            No pairs match <strong>Via</strong> or <strong>FAIL, no manual</strong>. Use{' '}
                            <strong>Clear all</strong> or change those filters.
                          </>
                        ) : (
                          <>No rows in view. Adjust filters.</>
                        )}
                      </td>
                    </tr>
                  ) : (
                    displayRows.map((row) => {
                      const fmtComma2 = (n: number) =>
                        n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                      const d0 =
                        draft[row.id]?.d ??
                        (row.distance_m == null ? '' : fmtComma2(Number(row.distance_m)));
                      const t0 =
                        draft[row.id]?.t ??
                        (row.duration_min == null || String(row.duration_min).trim() === ''
                          ? ''
                          : fmtComma2(Number(row.duration_min)));
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
                        if (dryPrev.avgGpsPlusMeters != null) {
                          const gpParts: string[] = [`GPS+ ${Math.round(dryPrev.avgGpsPlusMeters)} m`];
                          if (dryPrev.avgGpsPlusMinutes != null)
                            gpParts.push(`${dryPrev.avgGpsPlusMinutes.toFixed(2)} min`);
                          const nGp = dryPrev.gpsPlusSuccessCount ?? 0;
                          gpsAvgLine =
                            (gpsAvgLine ? `${gpsAvgLine}; ` : '') +
                            gpParts.join(' · ') +
                            ` (mean of ${nGp} GPS+ OK${nGp === 1 ? '' : 's'})`;
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
                        if (row.distance_via != null && String(row.distance_via).trim() !== '') {
                          gpsAvgLine =
                            (gpsAvgLine ? `${gpsAvgLine} · ` : '') + `via ${String(row.distance_via).trim()}`;
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
                          className={`cursor-pointer border-b border-zinc-200/80 dark:border-zinc-800 ${
                            sel
                              ? 'bg-amber-50 dark:bg-amber-950/30'
                              : 'even:bg-zinc-50/70 dark:even:bg-zinc-900/35'
                          }`}
                        >
                          <td className="min-w-0 break-words px-2 py-2 align-top text-zinc-800 dark:text-zinc-200">
                            <span className="text-sm leading-snug" title={row.delivery_winery}>
                              {row.delivery_winery}
                            </span>
                            <GpsMappingHint
                              kind="Winery"
                              vworkName={row.delivery_winery}
                              maps={gpsWineryMappings[row.delivery_winery.trim()] ?? []}
                            />
                          </td>
                          <td className="min-w-0 break-words px-2 py-2 align-top text-zinc-800 dark:text-zinc-200">
                            <span className="text-sm leading-snug" title={row.vineyard_name}>
                              {row.vineyard_name}
                            </span>
                            <GpsMappingHint
                              kind="Vineyard"
                              vworkName={row.vineyard_name}
                              maps={gpsVineyardMappings[row.vineyard_name.trim()] ?? []}
                            />
                          </td>
                          <td className="min-w-0 break-words px-2 py-2 align-top text-xs text-zinc-600 dark:text-zinc-400">
                            <span
                              className="leading-snug"
                              title={(row.pair_clients ?? '').trim() || undefined}
                            >
                              {(row.pair_clients ?? '').trim() || '—'}
                            </span>
                          </td>
                          <td className="whitespace-nowrap px-2 py-2 align-middle text-center tabular-nums text-zinc-700 dark:text-zinc-300">
                            {row.vwork_job_count ?? 0}
                          </td>
                          <td className="whitespace-nowrap px-2 py-2 align-middle text-center tabular-nums text-xs text-zinc-600 dark:text-zinc-400">
                            {totalDistanceTimesJobs(row) == null
                              ? '—'
                              : Math.round(totalDistanceTimesJobs(row)!).toLocaleString()}
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
                          <td className="whitespace-nowrap px-2 py-2 text-center align-middle font-mono text-xs text-zinc-700 dark:text-zinc-300">
                            {row.gps_avg_distance_m != null ? fmtComma2(Number(row.gps_avg_distance_m)) : '—'}
                          </td>
                          <td className="whitespace-nowrap px-2 py-2 text-center align-middle font-mono text-xs text-zinc-700 dark:text-zinc-300">
                            {row.gps_plus_avg_distance_m != null
                              ? fmtComma2(Number(row.gps_plus_avg_distance_m))
                              : '—'}
                          </td>
                          <td className="whitespace-nowrap px-2 py-2 text-center align-middle font-mono text-xs text-zinc-700 dark:text-zinc-300">
                            {row.gps_sample_count ?? 0}
                          </td>
                          <td className="px-2 py-2 text-center align-middle">
                            {viaLabelForRow(row) !== '' ? (
                              <span
                                className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase ${
                                  viaLabelForRow(row) === 'GPSTAGS'
                                    ? 'bg-emerald-100 text-emerald-900 dark:bg-emerald-900/40 dark:text-emerald-100'
                                    : viaLabelForRow(row) === 'GPS+'
                                      ? 'bg-violet-100 text-violet-900 dark:bg-violet-900/40 dark:text-violet-100'
                                      : viaLabelForRow(row) === 'MANUAL'
                                        ? 'bg-slate-200 text-slate-900 dark:bg-slate-700 dark:text-slate-100'
                                        : 'bg-amber-100 text-amber-950 dark:bg-amber-900/40 dark:text-amber-100'
                                }`}
                              >
                                {viaLabelForRow(row)}
                              </span>
                            ) : (
                              <span className="text-zinc-400">—</span>
                            )}
                          </td>
                          <td className="min-w-0 break-words px-1.5 py-2 align-top text-left text-[10px] leading-snug text-zinc-600 dark:text-zinc-400">
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
                                className="w-full break-words rounded px-0.5 text-left font-medium text-blue-700 underline decoration-blue-400 underline-offset-2 hover:bg-blue-50 enabled:cursor-pointer disabled:opacity-60 dark:text-blue-300 dark:hover:bg-blue-950/50"
                              >
                                {gpsDebugOpeningId === row.id ? 'Loading…' : gpsCountShown}
                                {showDryPreview && gpsDebugOpeningId !== row.id && (
                                  <span className="ml-1 font-normal text-blue-600 dark:text-blue-400">dry</span>
                                )}
                              </button>
                            ) : (
                              <div className="font-medium text-zinc-800 dark:text-zinc-200">{gpsCountShown}</div>
                            )}
                            {gpsAvgLine != null && (
                              <div className="mt-0.5 break-words text-[10px] leading-snug text-zinc-500 dark:text-zinc-400">
                                {gpsAvgLine}
                              </div>
                            )}
                          </td>
                          <td className="px-1 py-2 align-middle text-center">
                            {row.maps_drive_url ? (
                              <a
                                href={row.maps_drive_url}
                                target="_blank"
                                rel="noopener noreferrer"
                                onClick={(e) => e.stopPropagation()}
                                className="text-xs font-medium text-blue-700 underline underline-offset-2 dark:text-blue-300"
                              >
                                Drive
                              </a>
                            ) : (
                              <span className="text-xs text-zinc-400">—</span>
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
            <p className="mt-2 text-xs text-zinc-500 dark:text-zinc-400">
              Manual edits save on blur. <strong>Save</strong> polls jobs and writes per-job rows to{' '}
              <code className="text-[11px]">tbl_distances_gps_samples</code>, then refreshes rollups on{' '}
              <code className="text-[11px]">tbl_distances</code>. <strong>Dry run</strong> does not write samples; if
              &quot;Dry run updates tbl_distances&quot; is checked, it still updates that pair&apos;s distance, minutes,
              via, and averages from the dry attempt summary only. Click the <strong>GPS dry / saved</strong> cell for a
              job-by-job report (session dry snapshot if present, else saved samples).
            </p>

            {(selectedId != null || lastPersistRun != null) && (
              <div className="mt-6 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
                <h2 className="text-lg font-semibold">GPS harvest detail</h2>
                {selectedRow && (
                  <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
                    Selected: {selectedRow.delivery_winery} → {selectedRow.vineyard_name} (id {selectedRow.id})
                  </p>
                )}

                {selectedRow && (
                  <div className="mt-4 rounded-md border border-zinc-200 bg-zinc-50/80 p-3 text-sm dark:border-zinc-700 dark:bg-zinc-950/40">
                    <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                      Manual drive distance{' '}
                      <span className="font-normal text-zinc-500 dark:text-zinc-400">
                        (<code className="text-[11px]">tbl_distances_manual</code>)
                      </span>
                    </h3>
                    <p className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">
                      Use <strong>Drive</strong> in the grid for Google Maps directions between fence centroids, then
                      enter the road distance here. <strong>Populate vWork</strong> uses manual metres when present and
                      writes <code className="text-[11px]">tbl_vworkjobs.distance</code> as round-trip km (m÷1000×2).
                    </p>
                    <div className="mt-3 flex flex-wrap items-end gap-3">
                      <label className="flex flex-col gap-0.5 text-xs font-medium text-zinc-600 dark:text-zinc-400">
                        Distance (m)
                        <input
                          value={manualDm}
                          onChange={(e) => setManualDm(e.target.value)}
                          className="w-28 rounded border border-zinc-300 bg-white px-2 py-1 font-mono text-sm dark:border-zinc-600 dark:bg-zinc-900"
                          inputMode="numeric"
                        />
                      </label>
                      <label className="flex flex-col gap-0.5 text-xs font-medium text-zinc-600 dark:text-zinc-400">
                        Minutes (optional)
                        <input
                          value={manualDur}
                          onChange={(e) => setManualDur(e.target.value)}
                          className="w-24 rounded border border-zinc-300 bg-white px-2 py-1 font-mono text-sm dark:border-zinc-600 dark:bg-zinc-900"
                          inputMode="decimal"
                        />
                      </label>
                      <label className="flex min-w-[12rem] flex-1 flex-col gap-0.5 text-xs font-medium text-zinc-600 dark:text-zinc-400">
                        Notes
                        <input
                          value={manualNotes}
                          onChange={(e) => setManualNotes(e.target.value)}
                          className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-900"
                          placeholder="e.g. measured 2026-04-01"
                        />
                      </label>
                    </div>
                    <div className="mt-3 flex flex-wrap gap-2">
                      <button
                        type="button"
                        disabled={manualBusy}
                        onClick={() => void saveManualDistance()}
                        className="rounded-md bg-slate-800 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-700 disabled:opacity-50 dark:bg-slate-200 dark:text-zinc-900 dark:hover:bg-slate-300"
                      >
                        {manualBusy ? 'Saving…' : 'Save manual'}
                      </button>
                      <button
                        type="button"
                        disabled={manualBusy}
                        onClick={() => void clearManualDistance()}
                        className="rounded-md border border-zinc-300 px-3 py-1.5 text-xs hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-600 dark:hover:bg-zinc-800"
                      >
                        Clear manual
                      </button>
                    </div>
                    {manualMsg && (
                      <p className="mt-2 text-xs text-zinc-700 dark:text-zinc-300">{manualMsg}</p>
                    )}
                  </div>
                )}

                {lastPersistRun && (
                  <div className="mt-4 rounded-md border border-emerald-200 bg-emerald-50/80 p-3 text-sm dark:border-emerald-900 dark:bg-emerald-950/40">
                    <div className="font-medium text-emerald-900 dark:text-emerald-100">
                      Last save run: {lastPersistRun.insertedOrUpdated} row(s) upserted in tbl_distances_gps_samples
                    </div>
                    {lastPersistRun.rollup && (
                      <p className="mt-2 font-mono text-xs text-emerald-950 dark:text-emerald-100">
                        Rollup: distance_via={lastPersistRun.rollup.distance_via ?? 'null'} · tag_OK=
                        {lastPersistRun.rollup.gps_sample_count ?? '—'} · gps+_OK=
                        {lastPersistRun.rollup.gps_plus_sample_count ?? '—'} · distance_m=
                        {lastPersistRun.rollup.distance_m ?? 'null'} · avg_tag_m=
                        {lastPersistRun.rollup.gps_avg_distance_m ?? 'null'} · avg_gps+_m=
                        {lastPersistRun.rollup.gps_plus_avg_distance_m ?? 'null'}
                      </p>
                    )}
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

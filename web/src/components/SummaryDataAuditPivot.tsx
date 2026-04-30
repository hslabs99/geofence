'use client';

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { formatIntNz } from '@/lib/format-nz';

type Row = Record<string, unknown>;

/** Match Summary query tables: center header labels horizontally and vertically */
const SUMMARY_THEAD_TH_ALIGNMENT = '[&_th]:align-middle [&_th]:text-center';

/** Same notion as Summary: excluded=1 omits job from rollup counts. */
function includedInRollup(row: Row): boolean {
  const ex = row.excluded;
  if (ex == null || ex === '') return true;
  return Number(ex) !== 1;
}

/** Parse timestamp to ms (same logic as Summary page). */
function parseToMs(v: unknown): number | null {
  if (v == null || v === '') return null;
  const s = typeof v === 'string' ? v.trim() : v instanceof Date ? v.toISOString() : String(v);
  if (!s) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d.getTime();
}

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

function travelMins(row: Row): number | null {
  const m2 = minsBetween(row, 1, 2);
  const m4 = minsBetween(row, 3, 4);
  if (m2 == null && m4 == null) return null;
  return (m2 ?? 0) + (m4 ?? 0);
}

function totalMins(row: Row): number | null {
  const m2 = minsBetween(row, 1, 2);
  const m3 = minsBetween(row, 2, 3);
  const m4 = minsBetween(row, 3, 4);
  const m5 = minsBetween(row, 4, 5);
  if (m2 == null && m3 == null && m4 == null && m5 == null) return null;
  return (m2 ?? 0) + (m3 ?? 0) + (m4 ?? 0) + (m5 ?? 0);
}

function jobAgg(row: Row): MinsAgg {
  return {
    count: 1,
    travel: travelMins(row) ?? 0,
    inVineyard: minsBetween(row, 2, 3) ?? 0,
    inWinery: minsBetween(row, 4, 5) ?? 0,
    total: totalMins(row) ?? 0,
  };
}

export const DATA_AUDIT_PIVOT_FIELD_IDS = [
  'client',
  'template',
  'winery',
  'vineyard',
  'trailermode',
  'loadsize',
  'vineyard_gps',
] as const;

export type DataAuditPivotFieldId = (typeof DATA_AUDIT_PIVOT_FIELD_IDS)[number];

const FIELD_META: Record<DataAuditPivotFieldId, { label: string }> = {
  client: { label: 'Client' },
  template: { label: 'Template' },
  winery: { label: 'Winery' },
  vineyard: { label: 'Vineyard' },
  trailermode: { label: 'Trailer mode' },
  loadsize: { label: 'Load size' },
  vineyard_gps: { label: 'Has Vineyard GPS' },
};

const DEFAULT_ROW_FIELDS: DataAuditPivotFieldId[] = ['client', 'template', 'trailermode'];

/** tbl_settings: Summary → Data Audit tab row labels (order preserved). Same pattern as Inspect sort. */
const DATA_AUDIT_ROW_FIELDS_SETTING_TYPE = 'System';
const DATA_AUDIT_ROW_FIELDS_SETTING_NAME = 'SummaryDataAuditRowFields';

const DND_TYPE = 'application/x-geodata-pivot-field';

const VALID_FIELD_ID = new Set<string>(DATA_AUDIT_PIVOT_FIELD_IDS);

function isDataAuditPivotFieldId(s: string): s is DataAuditPivotFieldId {
  return VALID_FIELD_ID.has(s);
}

/** Parse saved JSON array; invalid entries dropped; order kept; duplicates skipped. */
function parseRowFieldsSetting(raw: string | null | undefined): DataAuditPivotFieldId[] | null {
  if (raw == null || String(raw).trim() === '') return null;
  try {
    const arr = JSON.parse(String(raw)) as unknown;
    if (!Array.isArray(arr)) return null;
    const out: DataAuditPivotFieldId[] = [];
    const seen = new Set<string>();
    for (const x of arr) {
      if (typeof x !== 'string' || !isDataAuditPivotFieldId(x) || seen.has(x)) continue;
      seen.add(x);
      out.push(x);
    }
    return out.length > 0 ? out : null;
  } catch {
    return null;
  }
}

type MinsAgg = {
  count: number;
  travel: number;
  inVineyard: number;
  inWinery: number;
  total: number;
};

const zeroAgg = (): MinsAgg => ({
  count: 0,
  travel: 0,
  inVineyard: 0,
  inWinery: 0,
  total: 0,
});

function addAgg(a: MinsAgg, b: MinsAgg): MinsAgg {
  return {
    count: a.count + b.count,
    travel: a.travel + b.travel,
    inVineyard: a.inVineyard + b.inVineyard,
    inWinery: a.inWinery + b.inWinery,
    total: a.total + b.total,
  };
}

function cellLabel(v: unknown): string {
  if (v == null || v === '') return '(blank)';
  const s = typeof v === 'string' ? v.trim() : String(v).trim();
  return s || '(blank)';
}

/**
 * GPS-derived step_via values that match Summary By Job “green” via (not ORIDE).
 * Covers GPS, GPS*, VineFence+, VineFenceV+, VineSr1.
 */
function stepViaIsGpsDerived(viaVal: unknown): boolean {
  if (viaVal == null || viaVal === '') return false;
  const viaStr = typeof viaVal === 'string' ? viaVal.trim() : String(viaVal).trim();
  if (!viaStr) return false;
  const viaLower = viaStr.toLowerCase();
  if (viaStr.toUpperCase() === 'ORIDE') return false;
  const isGps = viaLower === 'gps';
  const isGpsStar = viaStr === 'GPS*' || viaLower === 'gps*';
  const isVineFencePlus = viaLower === 'vinefence+';
  const isVineFenceVPlus = viaLower === 'vinefencev+';
  const isVineSr1 = viaLower === 'vinesr1';
  return isGps || isGpsStar || isVineFencePlus || isVineFenceVPlus || isVineSr1;
}

/** Same keys as Inspect GPS Step Data column (`step_n_gps_completed_at`). */
function hasGpsCompletedAtForStep(row: Row, n: 2 | 3): boolean {
  const k = `step_${n}_gps_completed_at`;
  const kAlt = `Step_${n}_GPS_completed_at`;
  const v = row[k] ?? row[kAlt];
  return v != null && String(v).trim() !== '';
}

/**
 * Vineyard segment = steps 2 and 3 (arrive / in vineyard).
 * True when both steps are GPS-style via (By Job green Via), **or** when both have GPS completion
 * timestamps — `step_*_via` is sometimes still blank after import even though GPS rows exist (Inspect / CalcNotes).
 */
function hasVineyardGps(row: Row): boolean {
  const viaBoth = stepViaIsGpsDerived(row.step_2_via) && stepViaIsGpsDerived(row.step_3_via);
  if (viaBoth) return true;
  return hasGpsCompletedAtForStep(row, 2) && hasGpsCompletedAtForStep(row, 3);
}

function fieldValue(row: Row, id: DataAuditPivotFieldId): string {
  switch (id) {
    case 'client':
      return cellLabel(row.Customer ?? row.customer);
    case 'template':
      return cellLabel(row.Template ?? row.template);
    case 'winery':
      return cellLabel(row.delivery_winery);
    case 'vineyard':
      return cellLabel(row.vineyard_name);
    case 'trailermode':
      return cellLabel(row.trailermode);
    case 'loadsize':
      return cellLabel(row.loadsize);
    case 'vineyard_gps':
      return hasVineyardGps(row) ? 'True' : 'False';
    default:
      return '(blank)';
  }
}

type AggNode = { leafAgg: MinsAgg | null; children: Map<string, AggNode> };

function emptyAggNode(): AggNode {
  return { leafAgg: null, children: new Map() };
}

function sortedKeys(m: Map<string, AggNode>): string[] {
  return [...m.keys()].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
}

function insertJob(root: AggNode, fields: DataAuditPivotFieldId[], row: Row, depth: number): void {
  const fl = fields.length;
  if (depth === fl) {
    const j = jobAgg(row);
    root.leafAgg = root.leafAgg ? addAgg(root.leafAgg, j) : { ...j };
    return;
  }
  const k = fieldValue(row, fields[depth]!);
  if (!root.children.has(k)) root.children.set(k, emptyAggNode());
  insertJob(root.children.get(k)!, fields, row, depth + 1);
}

type PivotRow =
  | { kind: 'detail'; path: string[]; agg: MinsAgg }
  | { kind: 'subtotal'; path: string[]; agg: MinsAgg };

function collectRows(
  node: AggNode,
  path: string[],
  depth: number,
  fieldsLen: number,
  out: PivotRow[],
): MinsAgg {
  if (depth === fieldsLen) {
    const agg = node.leafAgg ?? zeroAgg();
    if (agg.count > 0) {
      out.push({ kind: 'detail', path: [...path], agg });
    }
    return agg;
  }
  let sum = zeroAgg();
  for (const k of sortedKeys(node.children)) {
    const child = node.children.get(k)!;
    sum = addAgg(sum, collectRows(child, [...path, k], depth + 1, fieldsLen, out));
  }
  if (path.length > 0 && sum.count > 0) {
    out.push({ kind: 'subtotal', path: [...path], agg: sum });
  }
  return sum;
}

function grandAggFromJobs(jobs: Row[]): MinsAgg {
  let g = zeroAgg();
  for (const row of jobs) {
    g = addAgg(g, jobAgg(row));
  }
  return g;
}

function formatMins(n: number): string {
  return formatIntNz(n);
}

const AUDIT_INSPECT_JOBS_PREFIX = 'geodata_inspect_audit_jobs_';
const MAX_AUDIT_INSPECT_IDS = 5000;

function jobMatchesPivotPath(
  row: Row,
  rowFields: DataAuditPivotFieldId[],
  pathPrefix: string[],
): boolean {
  for (let i = 0; i < pathPrefix.length; i++) {
    const id = rowFields[i];
    if (!id) return false;
    if (fieldValue(row, id) !== pathPrefix[i]) return false;
  }
  return true;
}

function collectJobIdsForPivotPath(
  jobs: Row[],
  rowFields: DataAuditPivotFieldId[],
  path: string[],
): string[] {
  const out: string[] = [];
  for (const r of jobs) {
    if (!jobMatchesPivotPath(r, rowFields, path)) continue;
    const raw = r.job_id ?? (r as { Job_id?: unknown }).Job_id;
    const s = raw != null && raw !== '' ? String(raw).trim() : '';
    if (s) out.push(s);
  }
  return [...new Set(out)];
}

function openInspectWithJobIds(jobIds: string[]) {
  const capped = jobIds.slice(0, MAX_AUDIT_INSPECT_IDS);
  if (capped.length === 0) return;
  const ref =
    typeof crypto !== 'undefined' && 'randomUUID' in crypto
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
  try {
    localStorage.setItem(`${AUDIT_INSPECT_JOBS_PREFIX}${ref}`, JSON.stringify({ ids: capped }));
  } catch {
    return;
  }
  window.open(`/query/inspect?auditJobIdsRef=${encodeURIComponent(ref)}`, '_blank', 'noopener,noreferrer');
}

type Props = {
  jobs: Row[];
  loading: boolean;
};

export default function SummaryDataAuditPivot({ jobs, loading }: Props) {
  const [rowFields, setRowFields] = useState<DataAuditPivotFieldId[]>(() => [...DEFAULT_ROW_FIELDS]);
  /** After initial GET /api/settings for row fields (avoid clobbering before load). */
  const [rowFieldsSettingsReady, setRowFieldsSettingsReady] = useState(false);
  const [dragOverRowZone, setDragOverRowZone] = useState(false);
  const [dragOverAvail, setDragOverAvail] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const q = new URLSearchParams({
      type: DATA_AUDIT_ROW_FIELDS_SETTING_TYPE,
      name: DATA_AUDIT_ROW_FIELDS_SETTING_NAME,
    }).toString();
    fetch(`/api/settings?${q}`, { cache: 'no-store' })
      .then((r) => r.json())
      .then((data: { settingvalue?: string | null }) => {
        if (cancelled) return;
        const parsed = parseRowFieldsSetting(data?.settingvalue ?? null);
        if (parsed) setRowFields(parsed);
      })
      .catch(() => {})
      .finally(() => {
        if (!cancelled) setRowFieldsSettingsReady(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!rowFieldsSettingsReady) return;
    const t = window.setTimeout(() => {
      fetch('/api/settings', {
        method: 'PUT',
        cache: 'no-store',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type: DATA_AUDIT_ROW_FIELDS_SETTING_TYPE,
          settingname: DATA_AUDIT_ROW_FIELDS_SETTING_NAME,
          settingvalue: JSON.stringify(rowFields),
        }),
      }).catch(() => {});
    }, 450);
    return () => window.clearTimeout(t);
  }, [rowFields, rowFieldsSettingsReady]);

  const rollupJobs = useMemo(() => jobs.filter(includedInRollup), [jobs]);

  const pivotModel = useMemo(() => {
    const grand = grandAggFromJobs(rollupJobs);
    if (rowFields.length === 0) {
      return { rows: [] as PivotRow[], grand };
    }
    const root = emptyAggNode();
    for (const row of rollupJobs) {
      insertJob(root, rowFields, row, 0);
    }
    const rows: PivotRow[] = [];
    collectRows(root, [], 0, rowFields.length, rows);
    return { rows, grand };
  }, [rollupJobs, rowFields]);

  const addField = useCallback((id: DataAuditPivotFieldId) => {
    setRowFields((prev) => (prev.includes(id) ? prev : [...prev, id]));
  }, []);

  const insertFieldAt = useCallback((id: DataAuditPivotFieldId, index: number) => {
    setRowFields((prev) => {
      if (prev.includes(id)) return prev;
      const next = [...prev];
      const at = Math.max(0, Math.min(index, next.length));
      next.splice(at, 0, id);
      return next;
    });
  }, []);

  const removeField = useCallback((id: DataAuditPivotFieldId) => {
    setRowFields((prev) => prev.filter((x) => x !== id));
  }, []);

  const moveField = useCallback((from: number, to: number) => {
    setRowFields((prev) => {
      if (from === to || from < 0 || from >= prev.length || to < 0 || to >= prev.length) return prev;
      const next = [...prev];
      const [removed] = next.splice(from, 1);
      if (removed === undefined) return prev;
      next.splice(to, 0, removed);
      return next;
    });
  }, []);

  const handlePivotRowOpenInspect = useCallback(
    (path: string[]) => {
      const ids = collectJobIdsForPivotPath(rollupJobs, rowFields, path);
      if (ids.length === 0) return;
      openInspectWithJobIds(ids);
    },
    [rollupJobs, rowFields],
  );

  const onDragStartField = useCallback((e: React.DragEvent, id: DataAuditPivotFieldId) => {
    e.dataTransfer.setData(DND_TYPE, id);
    e.dataTransfer.effectAllowed = 'copyMove';
  }, []);

  const onDropToRowZone = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragOverRowZone(false);
      const raw = e.dataTransfer.getData(DND_TYPE);
      if (!raw || !DATA_AUDIT_PIVOT_FIELD_IDS.includes(raw as DataAuditPivotFieldId)) return;
      addField(raw as DataAuditPivotFieldId);
    },
    [addField],
  );

  const onDropOnRowSlot = useCallback(
    (e: React.DragEvent, dropIndex: number) => {
      e.preventDefault();
      e.stopPropagation();
      const idxRaw = e.dataTransfer.getData('text/plain');
      const idxM = idxRaw.match(/^idx:(\d+)$/);
      if (idxM) {
        const from = parseInt(idxM[1]!, 10);
        if (!Number.isNaN(from)) moveField(from, dropIndex);
        return;
      }
      const raw = e.dataTransfer.getData(DND_TYPE);
      if (raw && DATA_AUDIT_PIVOT_FIELD_IDS.includes(raw as DataAuditPivotFieldId)) {
        insertFieldAt(raw as DataAuditPivotFieldId, dropIndex);
      }
    },
    [insertFieldAt, moveField],
  );

  const onDropToAvail = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragOverAvail(false);
      const raw = e.dataTransfer.getData(DND_TYPE);
      if (!raw || !DATA_AUDIT_PIVOT_FIELD_IDS.includes(raw as DataAuditPivotFieldId)) return;
      removeField(raw as DataAuditPivotFieldId);
    },
    [removeField],
  );

  const fl = rowFields.length;

  function pathLabelCell(cell: string, fieldIndex: number, variant: 'detail' | 'subtotal') {
    const fieldId = rowFields[fieldIndex];
    const base =
      variant === 'detail'
        ? 'border-r border-zinc-100 px-3 py-1.5 text-zinc-800 dark:border-zinc-800 dark:text-zinc-200'
        : 'border-r border-zinc-100 bg-zinc-50/90 px-3 py-1.5 font-medium text-zinc-800 dark:border-zinc-800 dark:bg-zinc-800/50 dark:text-zinc-100';
    const inner =
      fieldId === 'vineyard_gps'
        ? cell === 'True'
          ? (
              <span className="font-medium text-emerald-600 dark:text-emerald-400">True</span>
            )
          : cell === 'False'
            ? (
                <span className="font-medium text-red-600 dark:text-red-400">False</span>
              )
            : (
                cell
              )
        : (
            cell
          );
    return (
      <td key={fieldIndex} className={base}>
        {inner}
      </td>
    );
  }

  function valueCells(agg: MinsAgg, bold: boolean) {
    const c = bold ? 'font-semibold' : '';
    return (
      <>
        <td className={`px-3 py-1.5 text-right tabular-nums text-zinc-900 dark:text-zinc-100 ${c}`}>{formatIntNz(agg.count)}</td>
        <td className={`px-3 py-1.5 text-right tabular-nums text-zinc-800 dark:text-zinc-200 ${c}`}>{formatMins(agg.travel)}</td>
        <td className={`px-3 py-1.5 text-right tabular-nums text-zinc-800 dark:text-zinc-200 ${c}`}>{formatMins(agg.inVineyard)}</td>
        <td className={`px-3 py-1.5 text-right tabular-nums text-zinc-800 dark:text-zinc-200 ${c}`}>{formatMins(agg.inWinery)}</td>
        <td className={`px-3 py-1.5 text-right tabular-nums text-zinc-900 dark:text-zinc-100 ${c}`}>{formatMins(agg.total)}</td>
      </>
    );
  }

  return (
    <div className="space-y-4">
      <p className="text-sm text-zinc-600 dark:text-zinc-400">
        Field list and row labels are on the left (like Excel). The table shows job count and summed minutes (Travel = steps 2+4; Vineyard = step 3; Winery = step 5; Total = steps 2–5), same basis as other Summary tabs. Excluded jobs are omitted from counts and sums.{' '}
        <strong className="font-medium text-zinc-800 dark:text-zinc-200">Has Vineyard GPS</strong> is True when both step 2 and step 3 have GPS-style via (same as green Via on By Job), or when both have GPS step completion times (same basis as Inspect’s GPS Step Data)—so jobs with imported GPS times but empty via fields still count.
      </p>

      <div className="flex flex-col gap-4 lg:flex-row lg:items-start">
        {/* Left: Field list + Row labels (stacked, Excel-style) */}
        <div className="flex w-full shrink-0 flex-col gap-3 lg:w-64 xl:w-72">
          <div
            className={`rounded border border-zinc-300 bg-zinc-50 p-3 shadow-sm dark:border-zinc-600 dark:bg-zinc-900/80 ${
              dragOverAvail ? 'ring-2 ring-sky-500' : ''
            }`}
            onDragOver={(e) => {
              e.preventDefault();
              e.dataTransfer.dropEffect = 'move';
            }}
            onDragEnter={() => setDragOverAvail(true)}
            onDragLeave={() => setDragOverAvail(false)}
            onDrop={onDropToAvail}
          >
            <div className="mb-2 border-b border-zinc-200 pb-1 text-xs font-semibold uppercase tracking-wide text-zinc-600 dark:border-zinc-600 dark:text-zinc-400">
              Field list
            </div>
            <p className="mb-2 text-[11px] leading-snug text-zinc-500 dark:text-zinc-500">
              Drag down to Row labels, or use + .
            </p>
            <ul className="space-y-1.5">
              {DATA_AUDIT_PIVOT_FIELD_IDS.map((id) => {
                const inRows = rowFields.includes(id);
                return (
                  <li key={id}>
                    <div
                      draggable={!inRows}
                      onDragStart={(e) => {
                        if (inRows) {
                          e.preventDefault();
                          return;
                        }
                        onDragStartField(e, id);
                      }}
                      className={`flex items-center justify-between gap-2 rounded border px-2 py-1.5 text-sm ${
                        inRows
                          ? 'border-dashed border-zinc-200 bg-zinc-100/80 text-zinc-400 dark:border-zinc-700 dark:bg-zinc-800/50 dark:text-zinc-500'
                          : 'cursor-grab border-zinc-200 bg-white active:cursor-grabbing dark:border-zinc-600 dark:bg-zinc-800'
                      }`}
                    >
                      <span className="select-none">{FIELD_META[id].label}</span>
                      {!inRows && (
                        <button
                          type="button"
                          onClick={() => addField(id)}
                          className="rounded bg-zinc-200 px-1.5 text-xs font-medium text-zinc-800 hover:bg-zinc-300 dark:bg-zinc-600 dark:text-zinc-100 dark:hover:bg-zinc-500"
                          title="Add to row labels"
                        >
                          +
                        </button>
                      )}
                    </div>
                  </li>
                );
              })}
            </ul>
          </div>

          <div
            className={`rounded border border-zinc-300 bg-white p-3 shadow-sm dark:border-zinc-600 dark:bg-zinc-900/80 ${
              dragOverRowZone ? 'ring-2 ring-sky-500' : ''
            }`}
            onDragOver={(e) => {
              e.preventDefault();
              e.dataTransfer.dropEffect = 'copy';
            }}
            onDragEnter={() => setDragOverRowZone(true)}
            onDragLeave={() => setDragOverRowZone(false)}
            onDrop={onDropToRowZone}
          >
            <div className="mb-2 border-b border-zinc-200 pb-1 text-xs font-semibold uppercase tracking-wide text-zinc-600 dark:border-zinc-600 dark:text-zinc-400">
              Row labels
            </div>
            {rowFields.length === 0 ? (
              <p className="py-4 text-center text-sm text-zinc-500 dark:text-zinc-400">
                Drag fields here. Empty = grand total only.
              </p>
            ) : (
              <ol className="space-y-1.5">
                {rowFields.map((id, idx) => (
                  <li key={`${id}-${idx}`}>
                    <div
                      draggable
                      onDragStart={(e) => {
                        e.dataTransfer.setData(DND_TYPE, id);
                        e.dataTransfer.setData('text/plain', `idx:${idx}`);
                        e.dataTransfer.effectAllowed = 'move';
                      }}
                      onDragOver={(e) => {
                        e.preventDefault();
                        const types = Array.from(e.dataTransfer.types);
                        e.dataTransfer.dropEffect = types.includes(DND_TYPE) ? 'copy' : 'move';
                      }}
                      onDrop={(e) => onDropOnRowSlot(e, idx)}
                      className="flex cursor-grab items-center justify-between gap-2 rounded border border-sky-200 bg-sky-50/80 px-2 py-1.5 text-sm active:cursor-grabbing dark:border-sky-900/50 dark:bg-sky-950/40"
                    >
                      <span className="font-medium text-zinc-800 dark:text-zinc-200">
                        {idx + 1}. {FIELD_META[id].label}
                      </span>
                      <button
                        type="button"
                        onClick={() => removeField(id)}
                        className="rounded px-1.5 text-xs text-zinc-500 hover:bg-zinc-200 hover:text-zinc-800 dark:hover:bg-zinc-700 dark:hover:text-zinc-100"
                        title="Remove from row labels"
                      >
                        ×
                      </button>
                    </div>
                  </li>
                ))}
              </ol>
            )}
          </div>
        </div>

        {/* Right: full-width pivot table */}
        <div className="min-w-0 flex-1 rounded border border-zinc-300 bg-white dark:border-zinc-600 dark:bg-zinc-900">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-zinc-200 bg-zinc-100 px-3 py-2 dark:border-zinc-600 dark:bg-zinc-800">
            <span className="text-xs font-semibold uppercase tracking-wide text-zinc-600 dark:text-zinc-400">
              Values
            </span>
            <span className="text-[11px] text-zinc-500 dark:text-zinc-400">
              Jobs · Travel · Vineyard · Winery · Total (sum of mins)
            </span>
          </div>

          <div className="max-h-[58vh] overflow-auto">
            {loading ? (
              <p className="p-6 text-center text-sm text-zinc-500">Loading…</p>
            ) : fl === 0 ? (
              <table className="w-full text-left text-sm">
                <thead className={`${SUMMARY_THEAD_TH_ALIGNMENT} bg-zinc-100 dark:bg-zinc-800`}>
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    <th className="px-3 py-2 text-xs font-semibold text-zinc-700 dark:text-zinc-200">Grand total</th>
                    <th className="px-3 py-2 text-xs font-semibold text-zinc-700 dark:text-zinc-200">Jobs</th>
                    <th className="px-3 py-2 text-xs font-semibold text-zinc-700 dark:text-zinc-200">Travel</th>
                    <th className="px-3 py-2 text-xs font-semibold text-zinc-700 dark:text-zinc-200">Vineyard</th>
                    <th className="px-3 py-2 text-xs font-semibold text-zinc-700 dark:text-zinc-200">Winery</th>
                    <th className="px-3 py-2 text-xs font-semibold text-zinc-700 dark:text-zinc-200">Total</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    className="cursor-pointer border-b border-zinc-200 font-semibold hover:bg-zinc-50 dark:border-zinc-700 dark:hover:bg-zinc-800/40"
                    onClick={() => handlePivotRowOpenInspect([])}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        handlePivotRowOpenInspect([]);
                      }
                    }}
                    role="button"
                    tabIndex={0}
                    title="Open these jobs in Inspect (new tab)"
                  >
                    <td className="px-3 py-3 text-zinc-800 dark:text-zinc-100">All</td>
                    {valueCells(pivotModel.grand, true)}
                  </tr>
                </tbody>
              </table>
            ) : (
              <table className="w-full min-w-[48rem] border-collapse text-left text-sm">
                <thead
                  className={`${SUMMARY_THEAD_TH_ALIGNMENT} sticky top-0 z-[1] bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.08)] dark:bg-zinc-800`}
                >
                  <tr className="border-b border-zinc-300 dark:border-zinc-600">
                    {rowFields.map((id) => (
                      <th
                        key={id}
                        className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 text-xs font-semibold text-zinc-700 dark:border-zinc-600 dark:text-zinc-200"
                      >
                        {FIELD_META[id].label}
                      </th>
                    ))}
                    <th className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 text-xs font-semibold text-zinc-700 dark:border-zinc-600 dark:text-zinc-200">
                      Jobs
                    </th>
                    <th className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 text-xs font-semibold text-zinc-700 dark:border-zinc-600 dark:text-zinc-200">
                      Travel
                    </th>
                    <th className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 text-xs font-semibold text-zinc-700 dark:border-zinc-600 dark:text-zinc-200">
                      Vineyard
                    </th>
                    <th className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 text-xs font-semibold text-zinc-700 dark:border-zinc-600 dark:text-zinc-200">
                      Winery
                    </th>
                    <th className="whitespace-nowrap px-3 py-2 text-xs font-semibold text-zinc-700 dark:text-zinc-200">
                      Total
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {rollupJobs.length === 0 ? (
                    <tr>
                      <td colSpan={fl + 5} className="px-3 py-8 text-center text-zinc-500 dark:text-zinc-400">
                        No jobs for the current filters.
                      </td>
                    </tr>
                  ) : pivotModel.rows.length === 0 ? (
                    <tr>
                      <td colSpan={fl + 5} className="px-3 py-8 text-center text-zinc-500 dark:text-zinc-400">
                        No rows.
                      </td>
                    </tr>
                  ) : (
                    pivotModel.rows.map((pr, i) => (
                      <tr
                        key={i}
                        className={
                          pr.kind === 'subtotal'
                            ? 'cursor-pointer border-b border-zinc-200 bg-zinc-50/90 dark:border-zinc-800 dark:bg-zinc-800/40'
                            : 'cursor-pointer border-b border-zinc-100 hover:bg-zinc-50 dark:border-zinc-800 dark:hover:bg-zinc-800/30'
                        }
                        onClick={() => handlePivotRowOpenInspect(pr.path)}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            handlePivotRowOpenInspect(pr.path);
                          }
                        }}
                        role="button"
                        tabIndex={0}
                        title="Open these jobs in Inspect (new tab)"
                      >
                        {pr.kind === 'detail' ? (
                          <>
                            {pr.path.map((cell, j) => pathLabelCell(cell, j, 'detail'))}
                            {valueCells(pr.agg, false)}
                          </>
                        ) : (
                          <>
                            {pr.path.map((cell, j) => pathLabelCell(cell, j, 'subtotal'))}
                            <td className="border-r border-zinc-100 bg-zinc-50/90 px-3 py-1.5 text-xs font-semibold uppercase tracking-wide text-zinc-600 dark:border-zinc-800 dark:text-zinc-400">
                              Subtotal
                            </td>
                            {Array.from({ length: Math.max(0, fl - pr.path.length - 1) }, (_, j) => (
                              <td
                                key={`pad-${j}`}
                                className="border-r border-zinc-100 bg-zinc-50/90 px-3 py-1.5 text-zinc-400 dark:border-zinc-800"
                              >
                                —
                              </td>
                            ))}
                            {valueCells(pr.agg, true)}
                          </>
                        )}
                      </tr>
                    ))
                  )}
                </tbody>
                {fl > 0 && rollupJobs.length > 0 && (
                  <tfoot>
                    <tr
                      className="cursor-pointer border-t-2 border-zinc-300 bg-zinc-100 font-semibold hover:bg-zinc-200/90 dark:border-zinc-600 dark:bg-zinc-800/90 dark:hover:bg-zinc-700/90"
                      onClick={() => handlePivotRowOpenInspect([])}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          handlePivotRowOpenInspect([]);
                        }
                      }}
                      role="button"
                      tabIndex={0}
                      title="Open these jobs in Inspect (new tab)"
                    >
                      <td colSpan={fl} className="px-3 py-2 text-zinc-900 dark:text-zinc-100">
                        Grand total
                      </td>
                      {valueCells(pivotModel.grand, true)}
                    </tr>
                  </tfoot>
                )}
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

'use client';

import React, { Suspense, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { useViewMode, type ViewMode } from '@/contexts/ViewModeContext';
import { useSummaryHistory } from '@/contexts/SummaryHistoryContext';
import {
  getSummaryHistoryEntry,
  normalizeSummarySplitMode,
  type SummaryHistoryPayload,
  type SummarySplitMode,
  type SummaryTabMode,
} from '@/lib/summary-history-storage';
import SummaryDataAuditPivot from '@/components/SummaryDataAuditPivot';
import { formatIntNz, formatKmNz2 } from '@/lib/format-nz';
import { findMatchingWineryMinuteRow } from '@/lib/wineryminutes-limit-match';
import {
  SUMMARY_COLUMN_COLOR_DEFAULTS,
  SUMMARY_COLUMN_COLOR_SETTING_NAMES,
  SUMMARY_COLUMN_COLOR_SETTINGS_TYPE,
  type SummaryColumnColorKey,
} from '@/lib/summary-column-color-setting-names';

/** Center all header cell labels horizontally and vertically within summary thead blocks */
const SUMMARY_THEAD_TH_ALIGNMENT = '[&_th]:align-middle [&_th]:text-center';

type Row = Record<string, unknown>;

/** Integer table/chart display; null/NaN → em dash. */
function fmtNz(n: number | null | undefined): string {
  if (n == null || (typeof n === 'number' && Number.isNaN(n))) return '—';
  return formatIntNz(n);
}

function sortVineyardNames(arr: readonly string[]): string[] {
  return [...new Set(arr.map((x) => String(x).trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b));
}

function firstDayOfMonthYmd(year: number, month1to12: number): string {
  return `${year}-${String(month1to12).padStart(2, '0')}-01`;
}

/** Last calendar day of month (month1–12). */
function lastDayOfMonthYmd(year: number, month1to12: number): string {
  const d = new Date(year, month1to12, 0);
  return `${year}-${String(month1to12).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

/** Summary rollups and job counts: excluded=1 omits job (still visible on By Job). */
function includedInRollup(row: Row): boolean {
  const ex = row.excluded;
  if (ex == null || ex === '') return true;
  return Number(ex) !== 1;
}

/** Map GET /api/admin/wineryminutes single row (or list row) into header threshold keys for red Av vs limits. */
function thresholdsFromWineryMinutesApiRow(row: Record<string, unknown> | null | undefined): Record<string, string> | null {
  if (!row || row.error) return null;
  const toV = (row.ToVineMins ?? row.tovinemins) != null ? String(row.ToVineMins ?? row.tovinemins) : '';
  const inV = (row.InVineMins ?? row.invinemins) != null ? String(row.InVineMins ?? row.invinemins) : '';
  const toW = (row.ToWineMins ?? row.towinemins) != null ? String(row.ToWineMins ?? row.towinemins) : '';
  const inW = (row.InWineMins ?? row.inwinemins) != null ? String(row.InWineMins ?? row.inwinemins) : '';
  const tv = row.ToVineMins ?? row.tovinemins;
  const tw = row.ToWineMins ?? row.towinemins;
  const travelVal = tv != null && tw != null ? String(Number(tv) + Number(tw)) : '';
  const totalVal = (row.TotalMins ?? row.totalmins) != null ? String(row.TotalMins ?? row.totalmins) : '';
  return {
    '2': toV,
    '3': inV,
    '4': toW,
    '5': inW,
    travel: travelVal,
    in_vineyard: inV,
    in_winery: inW,
    total: totalVal,
  };
}

const SORT_SETTING_TYPE = 'System';
const SORT_SETTING_NAME = 'SummarySort';

const LEAD_COLUMNS = [
  { key: 'Customer', label: 'Customer' },
  { key: 'job_id', label: 'Job ID' },
  { key: 'worker', label: 'Worker' },
  { key: 'trailermode', label: 'TT' },
  { key: 'delivery_winery', label: 'Winery' },
  { key: 'vineyard_name', label: 'Vineyard' },
] as const;

/** By Job table: lead columns with Limits (X/-) to the right of TT. */
const BY_JOB_LEAD_COLUMNS = [
  { key: 'Customer', label: 'Customer' },
  { key: 'job_id', label: 'Job ID' },
  { key: 'worker', label: 'Worker' },
  { key: 'trailermode', label: 'TT' },
  { key: 'loadsize', label: 'Load' },
  { key: 'limits_breached', label: 'Limits' },
  { key: 'delivery_winery', label: 'Winery' },
  { key: 'vineyard_name', label: 'Vineyard' },
  { key: 'distance', label: 'Distance' },
  { key: 'vineyard_group', label: 'Vineyard group' },
] as const;

const STEP_NUMS = [1, 2, 3, 4, 5] as const;

const STEP_LABELS: Record<number, string> = {
  1: '1. Start',
  2: '2. Travel To Vineyard',
  3: '3. Time In Vineyard',
  4: '4. Travel To Winery',
  5: '5. Unloading Winery',
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

/** 18 columns for aligned Time Limits — col1 Winery, cols 2–6 Vineyard group, 12 metric subcols (Jobs/Actual/Limit). */
function SeasonClientSplitColgroupLimits() {
  const widths = [
    '18%',
    '6%',
    '6%',
    '6%',
    '6%',
    '6%',
    '4.333%',
    '4.333%',
    '4.333%',
    '4.333%',
    '4.333%',
    '4.333%',
    '4.333%',
    '4.333%',
    '4.333%',
    '4.333%',
    '4.333%',
    '4.333%',
  ] as const;
  return (
    <colgroup>
      {widths.map((w, i) => (
        <col key={i} style={{ width: w }} />
      ))}
    </colgroup>
  );
}

/** 16 columns for Season Summary (client + split): Winery, Vineyard Group, TT, Over/Under, Jobs, Vyards, then Travel–Total metrics (two subcols each). */
function SeasonClientSplitColgroupSeasonData() {
  const widths = ['10%', '8%', '8%', '8%', '6%', '6%', '6%', '6%', '6%', '6%', '6%', '6%', '6%', '6%', '6%', '6%'] as const;
  return (
    <colgroup>
      {widths.map((w, i) => (
        <col key={i} style={{ width: w }} />
      ))}
    </colgroup>
  );
}

/** 15 columns: same as above without Over/Under (Winery / Group / TT only). */
function SeasonClientSplitColgroupSeasonDataNoOU() {
  const widths = ['10%', '9%', '9%', '6%', '6%', '6%', '6%', '6%', '6%', '6%', '6%', '6%', '6%', '6%', '6%'] as const;
  return (
    <colgroup>
      {widths.map((w, i) => (
        <col key={i} style={{ width: w }} />
      ))}
    </colgroup>
  );
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

/** Same quad math as season rollup (empty → av 0). */
function rollupQuadFromValues(values: number[]): { total: number; max: number; min: number; av: number } {
  const valid = values.filter((v) => v != null && !Number.isNaN(v));
  const n = valid.length;
  const total = valid.reduce((a, b) => a + b, 0);
  return {
    total,
    max: n ? Math.max(...valid) : 0,
    min: n ? Math.min(...valid) : 0,
    av: n ? Math.round(total / n) : 0,
  };
}

/** T / TT for season average charts; TTT and unknown → other (excluded from T / TT / T+TT average charts). */
/** tbl_vworkjobs.vineyard_group — trim; empty key groups blanks together. */
function vineyardGroupKey(row: Row): string {
  const v = (row as Record<string, unknown>).vineyard_group;
  return v == null || v === '' ? '' : String(v).trim();
}

function jobTrailerChartBucket(row: Row): 'T' | 'TT' | 'other' {
  const raw = (row.trailermode ?? '').toString().trim().toUpperCase().replace(/\s+/g, '');
  if (raw === 'TT' || raw === 'T_T' || raw === 'T+T' || raw === 'T&T') return 'TT';
  if (raw === 'T') return 'T';
  return 'other';
}

function jobIsTOrTT(row: Row): boolean {
  const b = jobTrailerChartBucket(row);
  return b === 'T' || b === 'TT';
}

/** Season Summary split-by-limits: TT column labels (match bar chart wording). */
const SEASON_TT_LABEL_T = 'Truck Only';
const SEASON_TT_LABEL_TT = 'Truck & Trailer';
const SEASON_TT_LABEL_OTHER = 'Other';

/** Per-job average AVs for Travel / In Vineyard / In Winery / Total (for bar charts). */
function seasonChartAverageAvs(jobs: Row[]) {
  const travelVals = jobs.map((row) => travelMins(row)).filter((v): v is number => v != null);
  const m3 = jobs.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null);
  const m5 = jobs.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null);
  const tot = jobs.map((row) => totalMins(row)).filter((v): v is number => v != null);
  const tq = rollupQuadFromValues(travelVals);
  const q3 = rollupQuadFromValues(m3);
  const q5 = rollupQuadFromValues(m5);
  const qt = rollupQuadFromValues(tot);
  return { travel: tq.av, inVineyard: q3.av, inWinery: q5.av, total: qt.av };
}

type SeasonChartAvs = ReturnType<typeof seasonChartAverageAvs>;

/** True when T vs TT average bars match on all four phases (cloned limits / identical rollups). */
function seasonChartAvsEqual(a: SeasonChartAvs, b: SeasonChartAvs): boolean {
  return (
    a.travel === b.travel &&
    a.inVineyard === b.inVineyard &&
    a.inWinery === b.inWinery &&
    a.total === b.total
  );
}

/** Row-level outlier flag on Job ID / Daily date (not per-cell; keeps limit reds on cells). */
const SUMMARY_OUTLIER_RED = 'bg-red-200 text-red-900 dark:bg-red-900/40 dark:text-red-200';
const SUMMARY_OUTLIER_YELLOW = 'bg-yellow-200 text-yellow-900 dark:bg-yellow-900/35 dark:text-yellow-100';

/** Minute columns aligned with By Job: steps 2–5, travel, total. */
function jobRowMinuteValues(row: Row): number[] {
  const out: number[] = [];
  for (let n = 2; n <= 5; n++) {
    const m = minsBetween(row, n - 1, n);
    if (m != null) out.push(m);
  }
  const t = travelMins(row);
  if (t != null) out.push(t);
  const tot = totalMins(row);
  if (tot != null) out.push(tot);
  return out;
}

/** Any &lt; 0 → red; else any &lt; lowMinutesThreshold → yellow. */
function timeOutlierSeverity(values: number[], lowMinutesThreshold: number): 'red' | 'yellow' | null {
  if (values.length === 0) return null;
  if (values.some((v) => v < 0)) return 'red';
  if (values.some((v) => v < lowMinutesThreshold)) return 'yellow';
  return null;
}

type RollupQuads = {
  jobs: Row[];
  /** Jobs included in minute rollups (for By Day outlier / empty checks). */
  rollupJobCount?: number;
  mins_2: { total: number; max: number; min: number; av: number };
  mins_3: { total: number; max: number; min: number; av: number };
  mins_4: { total: number; max: number; min: number; av: number };
  mins_5: { total: number; max: number; min: number; av: number };
  travel: { total: number; max: number; min: number; av: number };
  total: { total: number; max: number; min: number; av: number };
};

function dayRollupOutlierSeverity(
  r: RollupQuads,
  lowMinutesThreshold: number,
  exclusionSet?: Set<string>,
): 'red' | 'yellow' | null {
  const jobs = r.jobs
    .filter(includedInRollup)
    .filter((j) => {
      if (!exclusionSet || exclusionSet.size === 0) return true;
      const v = String(j.vineyard_name ?? '').trim().toLowerCase();
      if (!v) return true;
      return !exclusionSet.has(v);
    });
  if (jobs.length === 0) return null;
  const vals = jobs.flatMap(jobRowMinuteValues);
  return timeOutlierSeverity(vals, lowMinutesThreshold);
}

function footerStatsOutlierSeverity(
  stats: Record<string, { total: number; max: number; min: number; av: number }>,
  jobCount: number,
  lowMinutesThreshold: number,
): 'red' | 'yellow' | null {
  if (jobCount === 0) return null;
  const vals: number[] = [];
  for (const q of Object.values(stats)) {
    vals.push(q.total, q.max, q.min, q.av);
  }
  return timeOutlierSeverity(vals, lowMinutesThreshold);
}

function uniqueVineyardCount(jobs: Row[]): number {
  const s = new Set<string>();
  for (const j of jobs) {
    const v = String(j.vineyard_name ?? '').trim();
    if (v) s.add(v);
  }
  return s.size;
}

/** `tbl_vworkjobs.distance`: round-trip km (Populate vWork: pair m÷1000×2); null if missing/invalid. */
function distanceRoundTripKmFromRow(row: Row): number | null {
  const raw = (row as Record<string, unknown>).distance;
  const n = typeof raw === 'number' ? raw : typeof raw === 'string' ? parseFloat(raw) : NaN;
  return Number.isFinite(n) ? n : null;
}

/** Sum of per-job round-trip km (integer display). */
function roundTripKms(jobs: Row[]): number {
  let km = 0;
  for (const j of jobs) {
    const k = distanceRoundTripKmFromRow(j);
    if (k != null) km += k;
  }
  return Math.round(km);
}

/** Season charts: bar order Travel → Vineyard → Winery → Total. */
const SEASON_PHASE_BAR_FILLS = ['rgb(34 197 94)', 'rgb(234 179 8)', 'rgb(239 68 68)', 'rgb(59 130 246)'] as const;

function seasonBarChartSpecs(travel: number, inVineyard: number, inWinery: number, total: number) {
  return [
    { label: 'Travel', value: travel, fill: SEASON_PHASE_BAR_FILLS[0] },
    { label: 'In Vineyard', value: inVineyard, fill: SEASON_PHASE_BAR_FILLS[1] },
    { label: 'In Winery', value: inWinery, fill: SEASON_PHASE_BAR_FILLS[2] },
    { label: 'Total', value: total, fill: SEASON_PHASE_BAR_FILLS[3] },
  ];
}

/** Integer % of total mins for Travel / Vineyard / Winery; always sums to 100 (largest remainder). */
function phaseMinutesPercents(
  travel: number,
  inVineyard: number,
  inWinery: number,
  totalMins: number,
): [number, number, number] {
  if (totalMins <= 0) return [0, 0, 0];
  const raw = [travel, inVineyard, inWinery].map((p) => (p / totalMins) * 100);
  const floors = raw.map((r) => Math.floor(r));
  let remainder = 100 - floors.reduce((s, t) => s + t, 0);
  const order = [0, 1, 2].sort((i, j) => raw[j] - floors[j] - (raw[i] - floors[i]));
  const out: [number, number, number] = [floors[0], floors[1], floors[2]];
  for (let r = 0; r < remainder; r++) {
    out[order[r]]++;
  }
  return out;
}

function SeasonSummaryBarSvg({
  bars,
  title,
  /** When set (e.g. split season), all charts in the row use this max so bar heights are comparable. */
  valueMax,
  /** Smaller SVG for stacked T / TT / Both average charts (~75% scale via viewBox). */
  compact,
  /** Same length as bars: integer % of total mins (Total mins bars only); null = minutes only. */
  barPercents,
}: {
  bars: ReturnType<typeof seasonBarChartSpecs>;
  title: string;
  valueMax?: number;
  compact?: boolean;
  barPercents?: (number | null)[];
}) {
  const barMax = valueMax != null && valueMax > 0 ? valueMax : Math.max(...bars.map((b) => b.value), 1);
  return (
    <div className={`flex w-full flex-col items-center gap-1 ${compact ? 'min-w-0' : ''}`}>
      <span
        className={`text-center font-medium leading-tight text-zinc-600 dark:text-zinc-400 ${compact ? 'text-[10px] md:text-[11px]' : 'text-xs'}`}
      >
        {title}
      </span>
      <svg
        width={compact ? undefined : 320}
        height={compact ? undefined : 180}
        viewBox="0 0 320 180"
        preserveAspectRatio="xMidYMid meet"
        className={
          compact
            ? 'h-[min(32vw,118px)] w-full sm:h-[min(30vw,128px)] md:h-[min(28vw,142px)]'
            : 'max-w-full overflow-visible'
        }
      >
        <line x1={40} y1={140} x2={300} y2={140} stroke="rgb(161 161 170)" strokeWidth={1} />
        <line x1={40} y1={20} x2={40} y2={140} stroke="rgb(161 161 170)" strokeWidth={1} />
        {bars.map((b, i) => {
          const x = 50 + i * 68;
          const h = barMax > 0 ? (b.value / barMax) * 100 : 0;
          const pct = barPercents?.[i];
          /** With phase %: bold % above bar, minutes in brackets below. Minutes-only charts: single value line. */
          const pctY = pct != null ? 124 - h : 135 - h;
          const minsY = pct != null ? 136 - h : 135 - h;
          return (
            <g key={b.label}>
              <rect x={x} y={140 - h} width={48} height={h} fill={b.fill} stroke="rgb(255 255 255)" strokeWidth={1} />
              <text x={x + 24} y={155} textAnchor="middle" className="fill-zinc-600 text-[10px] font-medium dark:fill-zinc-400">
                {b.label}
              </text>
              {pct != null ? (
                <>
                  <text
                    x={x + 24}
                    y={pctY}
                    textAnchor="middle"
                    className="fill-zinc-900 text-[11px] font-bold dark:fill-zinc-100"
                  >
                    {pct}%
                  </text>
                  <text
                    x={x + 24}
                    y={minsY}
                    textAnchor="middle"
                    className="fill-zinc-500 text-[9px] font-medium dark:fill-zinc-400"
                  >
                    ({formatIntNz(b.value)})
                  </text>
                </>
              ) : (
                <text x={x + 24} y={135 - h} textAnchor="middle" className="fill-zinc-800 text-[10px] font-medium dark:fill-zinc-200">
                  {formatIntNz(b.value)}
                </text>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function SeasonSummaryPieBlock({
  travel,
  inVineyard,
  inWinery,
  totalMins,
  title,
}: {
  travel: number;
  inVineyard: number;
  inWinery: number;
  totalMins: number;
  title: string;
}) {
  const size = 200;
  const cx = size / 2;
  const cy = size / 2;
  const r = size / 2 - 4;
  const pieSum = travel + inVineyard + inWinery;
  const denomPct = totalMins > 0 ? totalMins : pieSum;
  const [pctTravel, pctVineyard, pctWinery] = phaseMinutesPercents(travel, inVineyard, inWinery, denomPct);
  const pieSegments =
    pieSum > 0
      ? [
          {
            start: 0,
            sweep: (travel / pieSum) * 360,
            fill: 'rgb(34 197 94)',
            pct: pctTravel,
            mins: travel,
          },
          {
            start: (travel / pieSum) * 360,
            sweep: (inVineyard / pieSum) * 360,
            fill: 'rgb(234 179 8)',
            pct: pctVineyard,
            mins: inVineyard,
          },
          {
            start: ((travel + inVineyard) / pieSum) * 360,
            sweep: (inWinery / pieSum) * 360,
            fill: 'rgb(239 68 68)',
            pct: pctWinery,
            mins: inWinery,
          },
        ]
      : [];
  const labelR = r * 0.58;
  return (
    <div className="flex w-full flex-col items-center gap-1">
      <span className="text-center text-xs font-medium text-zinc-600 dark:text-zinc-400">{title}</span>
      <span className="text-center text-[11px] text-zinc-500 dark:text-zinc-400">Travel + Vineyard + Winery = total mins</span>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} className="overflow-visible">
        {pieSum <= 0 ? (
          <circle cx={cx} cy={cy} r={r} fill="rgb(228 228 231)" stroke="rgb(161 161 170)" strokeWidth={1} />
        ) : (
          pieSegments.map((seg, i) => {
            const startDeg = seg.start - 90;
            const endDeg = startDeg + seg.sweep;
            const x1 = cx + r * Math.cos((startDeg * Math.PI) / 180);
            const y1 = cy + r * Math.sin((startDeg * Math.PI) / 180);
            const x2 = cx + r * Math.cos((endDeg * Math.PI) / 180);
            const y2 = cy + r * Math.sin((endDeg * Math.PI) / 180);
            const large = seg.sweep > 180 ? 1 : 0;
            const d = `M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2} Z`;
            const midDeg = startDeg + seg.sweep / 2;
            const midRad = (midDeg * Math.PI) / 180;
            const lx = cx + labelR * Math.cos(midRad);
            const ly = cy + labelR * Math.sin(midRad);
            const showSlicePct = seg.sweep >= 14;
            const yellowSlice = i === 1;
            const pctFill = yellowSlice ? '#18181b' : '#fafafa';
            const pctStroke = yellowSlice ? 'rgba(255,255,255,0.55)' : 'rgba(0,0,0,0.35)';
            const minsFill = yellowSlice ? '#3f3f46' : 'rgba(250,250,250,0.9)';
            const minsStroke = yellowSlice ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.3)';
            return (
              <g key={i}>
                <path d={d} fill={seg.fill} stroke="rgb(255 255 255)" strokeWidth={1} />
                {showSlicePct && (
                  <>
                    <text
                      x={lx}
                      y={ly - 5}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill={pctFill}
                      stroke={pctStroke}
                      strokeWidth={0.45}
                      paintOrder="stroke fill"
                      fontSize={12}
                      fontWeight={700}
                      className="pointer-events-none"
                    >
                      {seg.pct}%
                    </text>
                    <text
                      x={lx}
                      y={ly + 8}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill={minsFill}
                      stroke={minsStroke}
                      strokeWidth={0.3}
                      paintOrder="stroke fill"
                      fontSize={9}
                      fontWeight={500}
                      className="pointer-events-none"
                    >
                      ({formatIntNz(seg.mins)})
                    </text>
                  </>
                )}
              </g>
            );
          })
        )}
        <circle cx={cx} cy={cy} r={r} fill="none" stroke="rgb(161 161 170)" strokeWidth={1} />
      </svg>
      <div className="flex flex-wrap justify-center gap-3 text-xs">
        <span className="flex flex-wrap items-baseline gap-x-1 gap-y-0">
          <span className="inline-block h-3 w-3 shrink-0 rounded-sm bg-green-500" />
          <span className="text-zinc-600 dark:text-zinc-400">Travel:</span>{' '}
          <span className="font-bold text-zinc-900 dark:text-zinc-100">{pctTravel}%</span>{' '}
          <span className="text-zinc-500 dark:text-zinc-400">({formatIntNz(travel)})</span>
        </span>
        <span className="flex flex-wrap items-baseline gap-x-1 gap-y-0">
          <span className="inline-block h-3 w-3 shrink-0 rounded-sm bg-yellow-500" />
          <span className="text-zinc-600 dark:text-zinc-400">In Vineyard:</span>{' '}
          <span className="font-bold text-zinc-900 dark:text-zinc-100">{pctVineyard}%</span>{' '}
          <span className="text-zinc-500 dark:text-zinc-400">({formatIntNz(inVineyard)})</span>
        </span>
        <span className="flex flex-wrap items-baseline gap-x-1 gap-y-0">
          <span className="inline-block h-3 w-3 shrink-0 rounded-sm bg-red-500" />
          <span className="text-zinc-600 dark:text-zinc-400">In Winery:</span>{' '}
          <span className="font-bold text-zinc-900 dark:text-zinc-100">{pctWinery}%</span>{' '}
          <span className="text-zinc-500 dark:text-zinc-400">({formatIntNz(inWinery)})</span>
        </span>
        <span className="flex flex-wrap items-baseline gap-x-1">
          <span className="text-zinc-600 dark:text-zinc-400">Total:</span>{' '}
          {denomPct > 0 ? (
            <>
              <span className="font-bold text-zinc-900 dark:text-zinc-100">100%</span>{' '}
              <span className="text-zinc-500 dark:text-zinc-400">({formatIntNz(totalMins)})</span>
            </>
          ) : (
            <span className="text-zinc-500 dark:text-zinc-400">—</span>
          )}
        </span>
      </div>
    </div>
  );
}

/** Backgrounds for Over Allowance | Within Allowance | Season chart columns (split mode). */
const SEASON_CHART_COLUMN_BG: [string, string, string] = [
  'rounded-lg bg-zinc-50 px-2 py-4 ring-1 ring-inset ring-zinc-200/90 dark:bg-zinc-800/45 dark:ring-zinc-600/50 md:px-3',
  'rounded-lg bg-white px-2 py-4 ring-1 ring-inset ring-zinc-200/70 dark:bg-zinc-900 dark:ring-zinc-700/70 md:px-3',
  'rounded-lg bg-zinc-100/90 px-2 py-4 ring-1 ring-inset ring-zinc-200/90 dark:bg-zinc-800/60 dark:ring-zinc-600/50 md:px-3',
];

/** One column: T | TT | Average Total bars in one row → total pie → total bars (Season tab). Hides Average T when T and TT averages match on all four phases (clone). */
function SeasonSummaryChartColumn({
  rowType,
  jobs,
  travel,
  mins_3,
  mins_5,
  total,
  sharedAvgBarMax,
  /** 0 = Over Allowance, 1 = Within Allowance, 2 = Season total — alternating panel background when split. */
  columnStripeIndex,
}: {
  rowType: string;
  jobs: Row[];
  travel: { total: number; av: number };
  mins_3: { total: number; av: number };
  mins_5: { total: number; av: number };
  total: { total: number; av: number };
  /** Same vertical scale for every average bar (all columns × T / TT / Both). */
  sharedAvgBarMax?: number;
  columnStripeIndex?: number;
}) {
  const jobsT = jobs.filter((j) => jobTrailerChartBucket(j) === 'T');
  const jobsTT = jobs.filter((j) => jobTrailerChartBucket(j) === 'TT');
  const jobsTPlusTT = jobs.filter(jobIsTOrTT);
  const avT = seasonChartAverageAvs(jobsT);
  const avTT = seasonChartAverageAvs(jobsTT);
  const avBoth = seasonChartAverageAvs(jobsTPlusTT);
  const hideDuplicateTAvg = seasonChartAvsEqual(avT, avTT);
  const barsT = seasonBarChartSpecs(avT.travel, avT.inVineyard, avT.inWinery, avT.total);
  const barsTT = seasonBarChartSpecs(avTT.travel, avTT.inVineyard, avTT.inWinery, avTT.total);
  const barsBoth = seasonBarChartSpecs(avBoth.travel, avBoth.inVineyard, avBoth.inWinery, avBoth.total);
  const totBars = seasonBarChartSpecs(travel.total, mins_3.total, mins_5.total, total.total);
  const denomTot =
    total.total > 0 ? total.total : travel.total + mins_3.total + mins_5.total;
  let totBarPercents: (number | null)[] = [null, null, null, null];
  if (denomTot > 0) {
    const [a, b, c] = phaseMinutesPercents(travel.total, mins_3.total, mins_5.total, denomTot);
    totBarPercents = [a, b, c, 100];
  }
  const stripeClass =
    columnStripeIndex != null ? SEASON_CHART_COLUMN_BG[columnStripeIndex % SEASON_CHART_COLUMN_BG.length] : '';
  return (
    <div
      className={`flex min-w-0 flex-col items-center gap-5 border-b border-zinc-200 pb-8 last:border-b-0 dark:border-zinc-700 md:border-b-0 md:pb-0 ${stripeClass}`}
    >
      <span className="text-sm font-semibold text-zinc-800 dark:text-zinc-200">{rowType}</span>
      <div className="flex w-full max-w-full flex-row flex-nowrap items-start justify-center gap-0.5 sm:gap-1">
        {!hideDuplicateTAvg && (
          <div className="min-w-0 flex-[1_1_0]" title="Average minutes per job — T (truck only)">
            <SeasonSummaryBarSvg bars={barsT} title="Average T" valueMax={sharedAvgBarMax} compact />
          </div>
        )}
        <div className="min-w-0 flex-[1_1_0]" title="Average minutes per job — TT (truck & trailer)">
          <SeasonSummaryBarSvg bars={barsTT} title="Average TT" valueMax={sharedAvgBarMax} compact />
        </div>
        <div className="min-w-0 flex-[1_1_0]" title="Average minutes per job — T + TT combined">
          <SeasonSummaryBarSvg bars={barsBoth} title="Average Total" valueMax={sharedAvgBarMax} compact />
        </div>
      </div>
      <SeasonSummaryPieBlock
        travel={travel.total}
        inVineyard={mins_3.total}
        inWinery={mins_5.total}
        totalMins={total.total}
        title="Total mins (pie)"
      />
      <SeasonSummaryBarSvg bars={totBars} title="Total mins (bars)" barPercents={totBarPercents} />
    </div>
  );
}

function compare(a: unknown, b: unknown): number {
  const va = a == null ? '' : a;
  const vb = b == null ? '' : b;
  if (va === vb) return 0;
  if (typeof va === 'number' && typeof vb === 'number') return va - vb;
  return String(va).localeCompare(String(vb));
}

/** Resolve sort value for a row by key (handles mins_*, travel, total, Customer, limits_breached). */
function sortValueFor(row: Row, key: string): unknown {
  const minsMatch = key.match(/^mins_(\d+)$/);
  if (minsMatch) return minsBetween(row, parseInt(minsMatch[1], 10) - 1, parseInt(minsMatch[1], 10));
  if (key === 'travel') return travelMins(row);
  if (key === 'in_vineyard') return minsBetween(row, 2, 3);
  if (key === 'in_winery') return minsBetween(row, 4, 5);
  if (key === 'total') return totalMins(row);
  if (key === 'distance') return distanceRoundTripKmFromRow(row);
  if (key === 'Customer') return row['Customer'] ?? row['customer'];
  if (key === 'limits_breached') return row.limits_breached ?? '-';
  return row[key];
}

function inspectJumpMetaForRow(row: Row): { jobId: string; truckId: string } | null {
  const jobId = row.job_id != null ? String(row.job_id).trim() : '';
  if (!jobId) return null;
  const truckId = row.truck_id != null ? String(row.truck_id).trim() : '';
  return { jobId, truckId };
}

/** Inspect: locate job + optional truck only (no date filters; Inspect clears UI filters when opening this link). */
function inspectUrlForRow(row: Row): string {
  const meta = inspectJumpMetaForRow(row);
  if (!meta) return '/query/inspect';
  const params = new URLSearchParams();
  params.set('locateJobId', meta.jobId);
  if (meta.truckId) params.set('truckId', meta.truckId);
  return `/query/inspect?${params.toString()}`;
}

function timeLimitRowTravelVal(r: {
  ToVineMins?: number | null;
  ToWineMins?: number | null;
}): number | null {
  const toV = r.ToVineMins != null ? Number(r.ToVineMins) : null;
  const toW = r.ToWineMins != null ? Number(r.ToWineMins) : null;
  if (toV == null && toW == null) return null;
  return (toV ?? 0) + (toW ?? 0);
}

/** Same limits + grouping as another row (TT not in key) — client table: hide TT when a T row has identical limit columns. */
function timeLimitRowComparableKey(r: {
  Customer?: string | null;
  Template?: string | null;
  Winery?: string | null;
  vineyardgroup?: string | null;
  ToVineMins?: number | null;
  InVineMins?: number | null;
  ToWineMins?: number | null;
  InWineMins?: number | null;
  TotalMins?: number | null;
}): string {
  const tv = timeLimitRowTravelVal(r);
  return [
    String(r.Customer ?? '').trim(),
    String(r.Template ?? '').trim(),
    String(r.Winery ?? '').trim(),
    String(r.vineyardgroup ?? '').trim(),
    r.ToVineMins ?? '',
    r.InVineMins ?? '',
    r.ToWineMins ?? '',
    r.InWineMins ?? '',
    r.TotalMins ?? '',
    tv ?? '',
  ].join('\u0001');
}

/** Safe fragment for download filenames (customer / template / date). */
function sanitizeSummaryExportFilenamePart(s: string): string {
  const t = String(s ?? '')
    .trim()
    .replace(/[^\w\-_.]+/g, '_')
    .replace(/^_+|_+$/g, '');
  return t.slice(0, 60) || 'x';
}

function summaryExportQuadCells(q: { total: number; max: number; min: number; av: number }): number[] {
  return [q.total, q.max, q.min, q.av];
}

/** Rollup layout control — used in the Season Summary tab header (wide select). */
function SeasonRollupLayoutSelect({
  splitMode,
  setSplitMode,
  viewMode,
  className = '',
}: {
  splitMode: SummarySplitMode;
  setSplitMode: (v: SummarySplitMode) => void;
  viewMode: ViewMode;
  className?: string;
}) {
  const uiValue =
    viewMode === 'client' && splitMode === 'winery_group_tt_over' ? 'winery_group_tt' : splitMode;
  return (
    <select
      value={uiValue}
      onChange={(e) => setSplitMode(e.target.value as SummarySplitMode)}
      aria-label="Rollup layout"
      title="Rollup layout"
      className={`w-full min-w-[12rem] max-w-[20rem] rounded border border-zinc-300 bg-white py-1 pl-2 pr-7 text-xs font-normal text-zinc-800 shadow-sm dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-100 ${className}`}
    >
      <option value="summary">Summary (single total row)</option>
      {viewMode !== 'client' && (
        <option value="winery_group_tt_over">Winery / Group / TT / Over–Under</option>
      )}
      <option value="winery_group_tt">Winery / Group / TT</option>
    </select>
  );
}

async function downloadSummaryXlsx(
  rows: (string | number | null | undefined)[][],
  sheetName: string,
  filename: string,
): Promise<void> {
  const XLSX = await import('xlsx');
  const ws = XLSX.utils.aoa_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  const safeSheet = sheetName.replace(/[\[\]*\/\\?:]/g, '').slice(0, 31) || 'Sheet1';
  XLSX.utils.book_append_sheet(wb, ws, safeSheet);
  const out = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
  const blob = new Blob([out], {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  });
  const a = document.createElement('a');
  const url = URL.createObjectURL(blob);
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

/** Stable pastel band per winery name for Time Limits table rows (light + dark). */
function wineryLimitRowBgClass(winery: string | null | undefined): string {
  const s = String(winery ?? '').trim();
  if (!s) return 'bg-zinc-50/90 dark:bg-zinc-800/40';
  const palette = [
    'bg-amber-50/90 dark:bg-amber-950/35',
    'bg-emerald-50/90 dark:bg-emerald-950/35',
    'bg-violet-50/90 dark:bg-violet-950/35',
    'bg-cyan-50/90 dark:bg-cyan-950/35',
    'bg-orange-50/90 dark:bg-orange-950/35',
    'bg-teal-50/90 dark:bg-teal-950/35',
    'bg-rose-50/90 dark:bg-rose-950/35',
    'bg-lime-50/90 dark:bg-lime-950/35',
    'bg-indigo-50/90 dark:bg-indigo-950/35',
    'bg-fuchsia-50/90 dark:bg-fuchsia-950/35',
  ];
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619);
  return palette[Math.abs(h) % palette.length];
}

function SummaryPageInner() {
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [jobsReloadNonce, setJobsReloadNonce] = useState(0);
  const [filterActualFrom, setFilterActualFrom] = useState('');
  const [filterActualTo, setFilterActualTo] = useState('');
  // Customer is always taken from sidebar (clientCustomer); no local dropdown to avoid disconnect in admin/super view
  const [filterTemplate, setFilterTemplate] = useState('');
  const [filterTruckId, setFilterTruckId] = useState('');
  const [filterWorker, setFilterWorker] = useState('');
  const [filterTrailermode, setFilterTrailermode] = useState('');
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  // Default dropdown sorting: Vineyard, then None/None (matches Summary tab expectation).
  const [sortColumns, setSortColumns] = useState<[string, string, string]>(['vineyard_name', '', '']);
  const sortColumnsInitialized = useRef(false);
  const [sortSaveStatus, setSortSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const [headerOpen, setHeaderOpen] = useState(true);
  const [lowMinutesHighlight, setLowMinutesHighlight] = useState<string>('5');
  const [lowMinutesExclusions, setLowMinutesExclusions] = useState<string[]>([]);
  const [lowMinutesExclusionsOpen, setLowMinutesExclusionsOpen] = useState(false);
  const [lowMinutesExclusionsDraft, setLowMinutesExclusionsDraft] = useState<string[]>([]);
  const lowMinutesExclusionsDraftRef = useRef<string[]>([]);
  lowMinutesExclusionsDraftRef.current = lowMinutesExclusionsDraft;
  const [summaryColumnColors, setSummaryColumnColors] = useState<Record<SummaryColumnColorKey, string>>(
    () => ({ ...SUMMARY_COLUMN_COLOR_DEFAULTS }),
  );
  const { viewMode, clientCustomer, setClientCustomer, clientCustomerLocked } = useViewMode();
  const router = useRouter();
  const searchParams = useSearchParams();
  const { registerSummarySnapshot, registerPendingInspectForHistory } = useSummaryHistory();
  /** Used when applying session restore (layout runs before locked state is wrong on first paint). */
  const clientCustomerLockedRef = useRef(clientCustomerLocked);
  clientCustomerLockedRef.current = clientCustomerLocked;
  /** Customer always mirrors sidebar selection (single source of truth; avoids disconnect in admin/super). */
  const effectiveCustomer = clientCustomer;
  const [summaryTab, setSummaryTab] = useState<SummaryTabMode>('season');
  const [filterWinery, setFilterWinery] = useState('');
  const filterWineryRef = useRef(filterWinery);
  filterWineryRef.current = filterWinery;
  const [filterVineyardGroup, setFilterVineyardGroup] = useState('');
  const [filterVineyards, setFilterVineyards] = useState<string[]>([]);
  const [vineyardPickerOpen, setVineyardPickerOpen] = useState(false);
  /** Edits apply to the jobs filter only when the picker closes (outside click or toggle button). */
  const [vineyardPickerDraft, setVineyardPickerDraft] = useState<string[]>([]);
  const vineyardPickerDraftRef = useRef<string[]>([]);
  vineyardPickerDraftRef.current = vineyardPickerDraft;
  const vineyardPickerAnchorRef = useRef<HTMLDivElement | null>(null);
  const vineyardPickerPanelRef = useRef<HTMLDivElement | null>(null);
  const [summaryExportMenuOpen, setSummaryExportMenuOpen] = useState(false);
  const summaryExportMenuRef = useRef<HTMLDivElement | null>(null);
  /** All vineyard_name values for the customer (from filter-options), independent of vineyard filter — avoids “stuck” list after reload. */
  const [vineyardOptionsAll, setVineyardOptionsAll] = useState<string[]>([]);
  /** Distinct templates for the customer (filter-options) so the Template dropdown works before jobs are loaded. */
  const [templateOptionsForCustomer, setTemplateOptionsForCustomer] = useState<string[]>([]);
  const vineyardFilterSet = useMemo(() => new Set(filterVineyards), [filterVineyards]);
  const vineyardDraftSet = useMemo(() => new Set(vineyardPickerDraft), [vineyardPickerDraft]);

  const commitVineyardPicker = useCallback(() => {
    setFilterVineyards(sortVineyardNames(vineyardPickerDraftRef.current));
    setVineyardPickerOpen(false);
  }, []);
  const [splitMode, setSplitMode] = useState<SummarySplitMode>('summary');
  const splitSeasonGrouped = splitMode !== 'summary';
  /** Over–Under layout is admin-only; client still uses grouped rows if `splitMode` is `winery_group_tt_over` but without Within/Over splits. */
  const splitByOverUnder = splitMode === 'winery_group_tt_over' && viewMode !== 'client';
  const [minsThresholds, setMinsThresholds] = useState<Record<string, string>>({
    '2': '', '3': '', '4': '', '5': '', travel: '', in_vineyard: '', in_winery: '', total: '',
  });
  const setMinsThreshold = (key: string, value: string) => {
    setMinsThresholds((prev) => ({ ...prev, [key]: value }));
  };
  /** Time limit rows for the selected customer (from tbl_wineryminutes), shown in Time Limits section. */
  type TimeLimitRow = {
    id: number;
    Customer?: string | null;
    Template?: string | null;
    vineyardgroup?: string | null;
    Winery?: string | null;
    TT?: string | null;
    ToVineMins?: number | null;
    InVineMins?: number | null;
    ToWineMins?: number | null;
    InWineMins?: number | null;
    TotalMins?: number | null;
  };
  const [timeLimitRows, setTimeLimitRows] = useState<TimeLimitRow[]>([]);
  /** Client view: hide TT when a T row exists with the same template/winery/VG and identical limit minutes (show T only). */
  const timeLimitRowsForTable = useMemo(() => {
    const raw = timeLimitRows;
    const afterDedup =
      viewMode === 'client'
        ? (() => {
            const tKeys = new Set<string>();
            for (const r of raw) {
              if (String(r.TT ?? '').trim() !== 'T') continue;
              tKeys.add(timeLimitRowComparableKey(r));
            }
            return raw.filter((r) => {
              if (String(r.TT ?? '').trim() !== 'TT') return true;
              return !tKeys.has(timeLimitRowComparableKey(r));
            });
          })()
        : raw;
    return [...afterDedup].sort((a, b) => {
      const t = String(a.Template ?? '').localeCompare(String(b.Template ?? ''), undefined, { sensitivity: 'base' });
      if (t !== 0) return t;
      const w = String(a.Winery ?? '').localeCompare(String(b.Winery ?? ''), undefined, { sensitivity: 'base' });
      if (w !== 0) return w;
      const vg = String(a.vineyardgroup ?? '').localeCompare(String(b.vineyardgroup ?? ''), undefined, {
        sensitivity: 'base',
      });
      if (vg !== 0) return vg;
      return String(a.TT ?? '').localeCompare(String(b.TT ?? ''), undefined, { sensitivity: 'base' });
    });
  }, [timeLimitRows, viewMode]);
  /** When user clicks a Time Limits row, we show that row's limits in the header white boxes (for info only). */
  const [selectedTimeLimitRowId, setSelectedTimeLimitRowId] = useState<number | null>(null);
  /** Show/hide the Time Limits table (can get in the way). */
  const [showLimitsTable, setShowLimitsTable] = useState(true);
  const [jobsPage, setJobsPage] = useState(0);
  const [jobsPageSize, setJobsPageSize] = useState(500);
  const [totalJobsFromApi, setTotalJobsFromApi] = useState(0);
  /** SQL debug from /api/vworkjobs (customer + template only; subsidiary filters are client-side). */
  const [jobsQueryDebug, setJobsQueryDebug] = useState<{
    debugSql: string;
    debugSqlParams: unknown[];
    debugSqlLiteral: string;
  } | null>(null);

  /** After sidebar history restore (?sh=): suppress jobs reset + cascade wipes (same as Client hydrate). */
  const jobsPageResetSuppressCountRef = useRef(0);
  const skipCustomerCascadeRef = useRef(false);
  const appliedHistoryIdRef = useRef<string | null>(null);
  /** After ?sh= restore: scroll/highlight this job row once it appears in By Job. */
  const pendingScrollToJobIdRef = useRef<string | null>(null);
  const [focusHighlightJobId, setFocusHighlightJobId] = useState<string | null>(null);
  /** Scroll parent for By Job table (`max-h-[70vh] overflow-auto`); `scrollIntoView` on the row is unreliable here. */
  const byJobTableScrollRef = useRef<HTMLDivElement | null>(null);
  /** Avoid treating context filling `'' → customer` (Client refreshUser) as a user customer change. */
  const prevEffectiveCustomerForCascadeRef = useRef<string | null>(null);

  const shParam = searchParams.get('sh');

  useLayoutEffect(() => {
    const id = shParam?.trim() ?? '';
    if (!id) {
      appliedHistoryIdRef.current = null;
      pendingScrollToJobIdRef.current = null;
      return;
    }
    if (appliedHistoryIdRef.current === id) return;
    const entry = getSummaryHistoryEntry(id);
    if (!entry) {
      router.replace('/query/summary');
      return;
    }
    appliedHistoryIdRef.current = id;
    jobsPageResetSuppressCountRef.current = clientCustomerLockedRef.current ? 2 : 1;
    skipCustomerCascadeRef.current = true;
    const s = entry.payload;
    if (!clientCustomerLockedRef.current && s.clientCustomer !== undefined) {
      setClientCustomer(s.clientCustomer);
    }
    setFilterActualFrom(s.filterActualFrom ?? '');
    setFilterActualTo(s.filterActualTo ?? '');
    setFilterTemplate(s.filterTemplate ?? '');
    setFilterTruckId(s.filterTruckId ?? '');
    setFilterWorker(s.filterWorker ?? '');
    setFilterTrailermode(s.filterTrailermode ?? '');
    {
      const t = s.summaryTab;
      setSummaryTab(
        t === 'season' || t === 'by_day' || t === 'by_job' || t === 'data_audit' || t === 'vineyards' ? t : 'season',
      );
    }
    setFilterWinery(s.filterWinery ?? '');
    setFilterVineyardGroup(s.filterVineyardGroup ?? '');
    {
      const legacy = s as SummaryHistoryPayload & { filterVineyard?: string };
      let fv: string[] = [];
      if (Array.isArray(legacy.filterVineyards) && legacy.filterVineyards.length > 0) {
        fv = legacy.filterVineyards.map((x) => String(x).trim()).filter(Boolean);
      } else if (typeof legacy.filterVineyard === 'string' && legacy.filterVineyard.trim()) {
        fv = [legacy.filterVineyard.trim()];
      }
      setFilterVineyards(fv);
    }
    setSplitMode(normalizeSummarySplitMode(s.splitMode, s.splitByLimits));
    if (s.minsThresholds && typeof s.minsThresholds === 'object') {
      setMinsThresholds((prev) => ({ ...prev, ...s.minsThresholds }));
    }
    setSelectedTimeLimitRowId(
      typeof s.selectedTimeLimitRowId === 'number' ? s.selectedTimeLimitRowId : null,
    );
    setShowLimitsTable(s.showLimitsTable !== false);
    setSortKey(s.sortKey ?? null);
    setSortDir(s.sortDir === 'desc' ? 'desc' : 'asc');
    if (Array.isArray(s.sortColumns) && s.sortColumns.length === 3) {
      const trip = s.sortColumns as [string, string, string];
      setSortColumns(trip);
      if (trip.some(Boolean)) sortColumnsInitialized.current = true;
    }
    if (typeof s.jobsPage === 'number' && s.jobsPage >= 0) setJobsPage(s.jobsPage);
    const fj = s.focusJobId != null ? String(s.focusJobId).trim() : '';
    pendingScrollToJobIdRef.current = fj || null;
    router.replace('/query/summary');
  }, [shParam, router, setClientCustomer]);

  /** Reset to page 0 when filters change (suppress after history restore / Client hydrate). */
  useEffect(() => {
    if (jobsPageResetSuppressCountRef.current > 0) {
      jobsPageResetSuppressCountRef.current -= 1;
      return;
    }
    setJobsPage(0);
  }, [effectiveCustomer, filterTemplate, filterActualFrom, filterActualTo, filterWinery, filterVineyardGroup, filterVineyards, filterTruckId, filterWorker, filterTrailermode]);

  useEffect(() => {
    const customerOk = effectiveCustomer.trim();
    const templateOk = filterTemplate.trim();
    if (!customerOk || !templateOk) {
      setRows([]);
      setTotalJobsFromApi(0);
      setJobsQueryDebug(null);
      setError(null);
      setLoading(false);
      return;
    }
    /** Load full job set for customer + template only; dates / winery / truck / worker / TT filter on the client so dropdowns and month presets stay stable. */
    const params = new URLSearchParams();
    params.set('customer', customerOk);
    params.set('template', templateOk);
    /** Full result set for rollups; By Job tab paginates client-side (was capped at 500 rows server-side). */
    setLoading(true);
    fetch(`/api/vworkjobs?${params}`, { cache: 'no-store' })
      .then(async (res) => {
        const data = await res.json();
        if (!res.ok) throw new Error(data?.error ?? res.statusText);
        return data;
      })
      .then((data) => {
        setRows(data.rows ?? []);
        setTotalJobsFromApi(typeof data.total === 'number' ? data.total : (data.rows ?? []).length);
        if (typeof data.debugSql === 'string' && Array.isArray(data.debugSqlParams)) {
          setJobsQueryDebug({
            debugSql: data.debugSql,
            debugSqlParams: data.debugSqlParams,
            debugSqlLiteral: typeof data.debugSqlLiteral === 'string' ? data.debugSqlLiteral : data.debugSql,
          });
        } else {
          setJobsQueryDebug(null);
        }
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [effectiveCustomer, filterTemplate, jobsReloadNonce]);

  useEffect(() => {
    if (!vineyardPickerOpen) return;
    const onDocClick = (e: MouseEvent) => {
      const t = e.target as Node | null;
      if (!t) return;
      if (vineyardPickerAnchorRef.current?.contains(t)) return;
      if (vineyardPickerPanelRef.current?.contains(t)) return;
      commitVineyardPicker();
    };
    document.addEventListener('click', onDocClick);
    return () => document.removeEventListener('click', onDocClick);
  }, [vineyardPickerOpen, commitVineyardPicker]);

  useEffect(() => {
    if (!summaryExportMenuOpen) return;
    const onDoc = (e: MouseEvent) => {
      const t = e.target as Node | null;
      if (!t) return;
      if (summaryExportMenuRef.current?.contains(t)) return;
      setSummaryExportMenuOpen(false);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, [summaryExportMenuOpen]);

  useEffect(() => {
    const c = effectiveCustomer.trim();
    if (!c) {
      setVineyardOptionsAll([]);
      setTemplateOptionsForCustomer([]);
      return;
    }
    let cancelled = false;
    fetch(`/api/vworkjobs/filter-options?${new URLSearchParams({ customer: c })}`, { cache: 'no-store' })
      .then((r) => r.json())
      .then((data) => {
        if (cancelled || data?.error) return;
        const names = data?.vineyardNames;
        if (Array.isArray(names)) {
          setVineyardOptionsAll(
            names.map((x: unknown) => String(x).trim()).filter((s: string) => s !== ''),
          );
        } else {
          setVineyardOptionsAll([]);
        }
        const tmpl = data?.templates;
        if (Array.isArray(tmpl)) {
          setTemplateOptionsForCustomer(
            tmpl.map((x: unknown) => String(x).trim()).filter((s: string) => s !== ''),
          );
        } else {
          setTemplateOptionsForCustomer([]);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setVineyardOptionsAll([]);
          setTemplateOptionsForCustomer([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [effectiveCustomer]);

  /** Picker list: every vineyard for the customer, plus any saved/draft picks not in that list. */
  const vineyardPickerOptions = useMemo(() => {
    const set = new Set<string>();
    for (const v of vineyardOptionsAll) {
      const t = String(v).trim();
      if (t) set.add(t);
    }
    for (const v of filterVineyards) {
      const t = String(v).trim();
      if (t) set.add(t);
    }
    for (const v of vineyardPickerDraft) {
      const t = String(v).trim();
      if (t) set.add(t);
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [vineyardOptionsAll, filterVineyards, vineyardPickerDraft]);

  /** Keep latest Summary state for sidebar history when navigating away from /query/summary. */
  useEffect(() => {
    if (clientCustomerLocked && !effectiveCustomer.trim()) {
      registerSummarySnapshot(null);
      return;
    }
    const payload: SummaryHistoryPayload = {
      filterActualFrom,
      filterActualTo,
      filterTemplate,
      filterTruckId,
      filterWorker,
      filterTrailermode,
      summaryTab,
      filterWinery,
      filterVineyardGroup,
      filterVineyards,
      splitMode,
      minsThresholds: { ...minsThresholds },
      selectedTimeLimitRowId,
      showLimitsTable,
      sortKey,
      sortDir,
      sortColumns: [...sortColumns] as [string, string, string],
      jobsPage,
      clientCustomer: effectiveCustomer,
    };
    registerSummarySnapshot(payload);
  }, [
    registerSummarySnapshot,
    effectiveCustomer,
    clientCustomerLocked,
    filterActualFrom,
    filterActualTo,
    filterTemplate,
    filterTruckId,
    filterWorker,
    filterTrailermode,
    summaryTab,
    filterWinery,
    filterVineyardGroup,
    filterVineyards,
    splitMode,
    minsThresholds,
    selectedTimeLimitRowId,
    showLimitsTable,
    sortKey,
    sortDir,
    sortColumns,
    jobsPage,
  ]);

  const distinctTruckIds = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      const v = row.truck_id;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows]);

  /** Distinct workers in the loaded job set (customer + template); subsidiary filters do not shrink this list. */
  const distinctWorkers = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      const w = row.worker;
      if (w != null && String(w).trim() !== '') set.add(String(w).trim());
    }
    return Array.from(set).sort();
  }, [rows]);

  /** Distinct trailermode in the loaded job set (same scope as distinctWorkers). */
  const distinctTrailermodes = useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      const t = row.trailermode;
      if (t != null && String(t).trim() !== '') set.add(String(t).trim());
    }
    return Array.from(set).sort();
  }, [rows]);

  /** Templates for the customer: `/api/vworkjobs/filter-options` list only when present so the dropdown never shrinks to the single loaded template. */
  const distinctTemplates = useMemo(() => {
    if (!effectiveCustomer.trim()) return [];
    if (templateOptionsForCustomer.length > 0) {
      return [...templateOptionsForCustomer].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
    }
    const set = new Set<string>();
    for (const row of rows) {
      const cust = row.Customer ?? row.customer;
      if (cust == null || String(cust).trim() !== effectiveCustomer) continue;
      const v = row.template;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  }, [rows, effectiveCustomer, templateOptionsForCustomer]);

  /** Wineries (delivery_winery) only for the effective customer. */
  const distinctWineries = useMemo(() => {
    if (!effectiveCustomer.trim()) return [];
    const set = new Set<string>();
    for (const row of rows) {
      const cust = row.Customer ?? row.customer;
      if (cust == null || String(cust).trim() !== effectiveCustomer) continue;
      const v = row.delivery_winery;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows, effectiveCustomer]);

  /** Vineyard groups (vineyard_group from tbl_vworkjobs) only for the effective customer. */
  const distinctVineyardGroups = useMemo(() => {
    if (!effectiveCustomer.trim()) return [];
    const set = new Set<string>();
    for (const row of rows) {
      const cust = row.Customer ?? row.customer;
      if (cust == null || String(cust).trim() !== effectiveCustomer) continue;
      const v = (row as Record<string, unknown>).vineyard_group;
      if (v != null && String(v).trim() !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort();
  }, [rows, effectiveCustomer]);

  /**
   * Clear template, winery, … when the *user* changes customer in the sidebar.
   * Skip: (1) once after session restore; (2) transition empty → non-empty (Client: refreshUser runs after
   * our layout restore and would otherwise wipe restored template / tab).
   */
  useEffect(() => {
    if (skipCustomerCascadeRef.current) {
      skipCustomerCascadeRef.current = false;
      prevEffectiveCustomerForCascadeRef.current = effectiveCustomer;
      return;
    }
    const prev = prevEffectiveCustomerForCascadeRef.current;
    if (prev === null) {
      prevEffectiveCustomerForCascadeRef.current = effectiveCustomer;
      return;
    }
    if (prev === effectiveCustomer) return;
    if (prev.trim() === '' && effectiveCustomer.trim() !== '') {
      prevEffectiveCustomerForCascadeRef.current = effectiveCustomer;
      return;
    }
    prevEffectiveCustomerForCascadeRef.current = effectiveCustomer;
    setFilterTemplate('');
    setFilterWinery('');
    setFilterVineyardGroup('');
    setFilterVineyards([]);
    setVineyardPickerDraft([]);
    setVineyardPickerOpen(false);
    setFilterWorker('');
    setFilterTrailermode('');
  }, [effectiveCustomer]);

  /** When Customer + Template + Winery (and optionally Vineyard group) are selected, load minute limits from tbl_wineryminutes (2=ToVine, 3=InVine, 4=ToWine, 5=InWine, travel=ToVine+ToWine, in_vineyard=InVine, in_winery=InWine, total=TotalMins). */
  useEffect(() => {
    if (!effectiveCustomer.trim() || !filterTemplate.trim() || !filterWinery.trim()) return;
    const params = new URLSearchParams({ customer: effectiveCustomer, template: filterTemplate, winery: filterWinery });
    if (filterVineyardGroup.trim()) params.set('vineyard_group', filterVineyardGroup.trim());
    fetch(`/api/admin/wineryminutes?${params}`)
      .then((r) => r.json())
      .then((row) => {
        const t = thresholdsFromWineryMinutesApiRow(row as Record<string, unknown>);
        if (t) setMinsThresholds(t);
      })
      .catch(() => {});
  }, [effectiveCustomer, filterTemplate, filterWinery, filterVineyardGroup]);

  /** When Winery is not filtered, still load limits from the first template row so Av columns can go red vs limits (same API as Time Limits list). */
  useEffect(() => {
    if (!effectiveCustomer.trim() || !filterTemplate.trim() || filterWinery.trim()) return;
    let cancelled = false;
    fetch(`/api/admin/wineryminutes?${new URLSearchParams({ customer: effectiveCustomer, template: filterTemplate })}`)
      .then((r) => r.json())
      .then((data) => {
        if (cancelled || filterWineryRef.current.trim()) return;
        const rows = data?.rows;
        if (!Array.isArray(rows) || rows.length === 0) return;
        const t = thresholdsFromWineryMinutesApiRow(rows[0] as Record<string, unknown>);
        if (t) setMinsThresholds(t);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [effectiveCustomer, filterTemplate, filterWinery]);

  /** When a customer is selected, load time limit rows for the Time Limits section (tbl_wineryminutes by customer; if template selected, filter on template too). */
  useEffect(() => {
    if (!effectiveCustomer.trim()) {
      setTimeLimitRows([]);
      return;
    }
    const params = new URLSearchParams({ customer: effectiveCustomer });
    if (filterTemplate.trim()) params.set('template', filterTemplate.trim());
    fetch(`/api/admin/wineryminutes?${params}`)
      .then((r) => r.json())
      .then((data) => {
        if (data?.rows && Array.isArray(data.rows)) setTimeLimitRows(data.rows);
        else setTimeLimitRows([]);
      })
      .catch(() => setTimeLimitRows([]));
  }, [effectiveCustomer, filterTemplate]);

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
      if (filterWinery) {
        const v = row.delivery_winery;
        if (v == null || String(v).trim() !== filterWinery) return false;
      }
      if (filterVineyardGroup) {
        const v = (row as Record<string, unknown>).vineyard_group;
        if (v == null || String(v).trim() !== filterVineyardGroup) return false;
      }
      if (filterVineyards.length > 0) {
        const v = row.vineyard_name;
        const s = v == null ? '' : String(v).trim();
        if (!vineyardFilterSet.has(s)) return false;
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
      if (filterTrailermode) {
        const v = row.trailermode;
        if (v == null || String(v).trim() !== filterTrailermode) return false;
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
  }, [rows, effectiveCustomer, filterTemplate, filterWinery, filterVineyardGroup, filterVineyards, vineyardFilterSet, filterTruckId, filterWorker, filterTrailermode, filterActualFrom, filterActualTo]);

  /** Distinct year-months from the loaded job rows (customer + template); not narrowed by date or subsidiary filters so users can switch months without clearing. */
  const actualStartMonthOptions = useMemo(() => {
    const seen = new Set<string>();
    const out: { key: string; label: string; y: number; m: number }[] = [];
    for (const row of rows) {
      const v = row.actual_start_time;
      if (v == null || v === '') continue;
      const str = String(v).trim();
      const m = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
      if (!m) continue;
      const y = Number(m[1]);
      const mo = Number(m[2]);
      const key = `${y}-${String(mo).padStart(2, '0')}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const label = new Date(y, mo - 1, 1).toLocaleDateString('en-NZ', { month: 'short', year: 'numeric' });
      out.push({ key, label, y, m: mo });
    }
    out.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    return out;
  }, [rows]);

  const actualStartMonthQuickPickValue = useMemo(() => {
    if (!filterActualFrom?.trim() || !filterActualTo?.trim()) return '';
    const f = filterActualFrom.trim().slice(0, 10);
    const t = filterActualTo.trim().slice(0, 10);
    const fm = f.match(/^(\d{4})-(\d{2})/);
    const tm = t.match(/^(\d{4})-(\d{2})/);
    if (!fm || !tm || fm[0] !== tm[0]) return '';
    const y = Number(fm[1]);
    const mo = Number(fm[2]);
    if (f !== firstDayOfMonthYmd(y, mo) || t !== lastDayOfMonthYmd(y, mo)) return '';
    return `${y}-${String(mo).padStart(2, '0')}`;
  }, [filterActualFrom, filterActualTo]);

  /** Find the time limit row that matches this job (tbl_vworkjobs ↔ tbl_wineryminutes). Vineyard group exact after trim; blank limit row only for jobs with no vineyard_group. TT match or TTT fallback for T/TT. */
  const getMatchingLimitsRow = (job: Row): TimeLimitRow | null => {
    const customer = effectiveCustomer.trim();
    if (!customer) return null;
    const tmpl =
      filterTemplate.trim() ||
      (job.template != null ? String(job.template).trim() : '');
    const winery = (job.delivery_winery ?? '').toString().trim();
    if (!winery) return null;
    const jobTT = (job.trailermode ?? '').toString().trim();
    const jobVg = ((job as Record<string, unknown>).vineyard_group ?? '').toString().trim();
    return findMatchingWineryMinuteRow(timeLimitRows, {
      customer,
      template: tmpl,
      winery,
      vineyardGroup: jobVg,
      jobTT,
    }) as TimeLimitRow | null;
  };

  /** Get limit value from a limits row for a given key. Travel = ToVine + ToWine. */
  const getLimitFromRow = (lim: TimeLimitRow | null, key: string): number | null => {
    if (!lim) return null;
    if (key === '2') return lim.ToVineMins ?? null;
    if (key === '3' || key === 'in_vineyard') return lim.InVineMins ?? null;
    if (key === '4') return lim.ToWineMins ?? null;
    if (key === '5' || key === 'in_winery') return lim.InWineMins ?? null;
    if (key === 'travel') {
      const toV = lim.ToVineMins != null ? Number(lim.ToVineMins) : null;
      const toW = lim.ToWineMins != null ? Number(lim.ToWineMins) : null;
      if (toV == null && toW == null) return null;
      const sum = (toV ?? 0) + (toW ?? 0);
      return Number.isNaN(sum) ? null : sum;
    }
    if (key === 'total') return lim.TotalMins ?? null;
    return null;
  };

  /** True if value exceeds the limit from the given limits row (used for per-job coloring by delivery_winery). */
  const isOverLimitWithRow = (val: number | null, lim: TimeLimitRow | null, key: string): boolean => {
    const limit = getLimitFromRow(lim, key);
    if (val == null || limit == null) return false;
    return val > limit;
  };

  /** For step columns 3 and 5 we check both step key and in_vineyard/in_winery. */
  const isOverLimitStepWithRow = (val: number | null, lim: TimeLimitRow | null, stepNum: number): boolean => {
    if (stepNum === 3) return isOverLimitWithRow(val, lim, '3') || isOverLimitWithRow(val, lim, 'in_vineyard');
    if (stepNum === 5) return isOverLimitWithRow(val, lim, '5') || isOverLimitWithRow(val, lim, 'in_winery');
    return isOverLimitWithRow(val, lim, String(stepNum));
  };

  /** For Limits column: X if job total mins exceeds winery total limit, else -. */
  const limitsBreachedForRow = (job: Row): 'X' | '-' => {
    const lim = getMatchingLimitsRow(job);
    const totalLimit = getLimitFromRow(lim, 'total');
    const jobTotal = totalMins(job);
    if (totalLimit == null || jobTotal == null) return '-';
    return jobTotal > totalLimit ? 'X' : '-';
  };

  /** Rows with computed limits_breached for sorting and display. */
  const rowsWithLimits = useMemo((): (Row & { limits_breached: '-' | 'X' })[] =>
    filteredRows.map((r) => ({ ...r, limits_breached: limitsBreachedForRow(r) })), [filteredRows, timeLimitRows, effectiveCustomer, filterTemplate]);

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

  /** By Day: one row per day, or when Over/Under split three rows per day (Over Allowance, Within Allowance, Daily Total). */
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
    type DayRow = {
      date: string; dateLabel: string; rowType?: 'Over Allowance' | 'Within Allowance' | 'Daily Total'; jobs: Row[];
      rollupJobCount: number;
      mins_2: MinsQuad; mins_3: MinsQuad; mins_4: MinsQuad; mins_5: MinsQuad; travel: MinsQuad; total: MinsQuad;
    };
    const out: DayRow[] = [];
    const sourceRows = splitByOverUnder ? rowsWithLimits : filteredRows;
    /** One pass: bucket by calendar day; dedupe by job_id per day so duplicate API rows don't inflate Jobs / mins. No min→max calendar walk (avoids hang on bad span). */
    const jobIdForDedupe = (row: Row): string | null => {
      const id = row.job_id;
      if (id == null || id === '') return null;
      const s = String(id).trim();
      return s === '' ? null : s;
    };
    const jobsByDate = new Map<string, Row[]>();
    const seenJobIdByDate = new Map<string, Set<string>>();
    for (const row of sourceRows) {
      const rd = getRowDate(row);
      if (rd == null || rd < min || rd > max) continue;
      const jid = jobIdForDedupe(row);
      let seen = seenJobIdByDate.get(rd);
      if (!seen) {
        seen = new Set<string>();
        seenJobIdByDate.set(rd, seen);
      }
      if (jid != null) {
        if (seen.has(jid)) continue;
        seen.add(jid);
      }
      let list = jobsByDate.get(rd);
      if (!list) {
        list = [];
        jobsByDate.set(rd, list);
      }
      list.push(row);
    }
    const sortedDates = [...jobsByDate.keys()].sort();
    for (const dateStr of sortedDates) {
      const dayJobs = jobsByDate.get(dateStr)!;
      if (dayJobs.length === 0) continue;
      const [y, mo, day] = dateStr.split('-');
      const dateLabel = `${day}/${mo}/${y.slice(2)}`;
      if (splitByOverUnder) {
        const overJobs = dayJobs.filter((row) => (row as Row & { limits_breached?: string }).limits_breached === 'X');
        const withinJobs = dayJobs.filter((row) => (row as Row & { limits_breached?: string }).limits_breached === '-');
        for (const { label, jobs } of [
          { label: 'Over Allowance' as const, jobs: overJobs },
          { label: 'Within Allowance' as const, jobs: withinJobs },
          { label: 'Daily Total' as const, jobs: dayJobs },
        ]) {
          const roll = jobs.filter(includedInRollup);
          const m2vals = roll.map((row) => minsBetween(row, 1, 2)).filter((v): v is number => v != null);
          const m3vals = roll.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null);
          const m4vals = roll.map((row) => minsBetween(row, 3, 4)).filter((v): v is number => v != null);
          const m5vals = roll.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null);
          const travelVals = roll.map((row) => travelMins(row)).filter((v): v is number => v != null);
          const totalVals = roll.map((row) => totalMins(row)).filter((v): v is number => v != null);
          out.push({
            date: dateStr,
            dateLabel,
            rowType: label,
            jobs: roll,
            rollupJobCount: roll.length,
            mins_2: fromValues(m2vals),
            mins_3: fromValues(m3vals),
            mins_4: fromValues(m4vals),
            mins_5: fromValues(m5vals),
            travel: fromValues(travelVals),
            total: fromValues(totalVals),
          });
        }
      } else {
        const roll = dayJobs.filter(includedInRollup);
        const m2vals = roll.map((row) => minsBetween(row, 1, 2)).filter((v): v is number => v != null);
        const m3vals = roll.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null);
        const m4vals = roll.map((row) => minsBetween(row, 3, 4)).filter((v): v is number => v != null);
        const m5vals = roll.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null);
        const travelVals = roll.map((row) => travelMins(row)).filter((v): v is number => v != null);
        const totalVals = roll.map((row) => totalMins(row)).filter((v): v is number => v != null);
        out.push({
          date: dateStr,
          dateLabel,
          jobs: roll,
          rollupJobCount: roll.length,
          mins_2: fromValues(m2vals),
          mins_3: fromValues(m3vals),
          mins_4: fromValues(m4vals),
          mins_5: fromValues(m5vals),
          travel: fromValues(travelVals),
          total: fromValues(totalVals),
        });
      }
    }
    return out;
  }, [filteredRows, rowsWithLimits, splitByOverUnder, dayRange]);

  const fromValuesToQuad = (values: number[]): MinsQuad => rollupQuadFromValues(values);

  /** Season rollups: only jobs with excluded ≠ 1 (same filters as Daily / By Job). */
  const seasonRollup = useMemo((): {
    jobs: Row[];
    mins_2: MinsQuad; mins_3: MinsQuad; mins_4: MinsQuad; mins_5: MinsQuad; travel: MinsQuad; total: MinsQuad;
  } | null => {
    if (filteredRows.length === 0) return null;
    const roll = filteredRows.filter(includedInRollup);
    const m2vals = roll.map((row) => minsBetween(row, 1, 2)).filter((v): v is number => v != null);
    const m3vals = roll.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null);
    const m4vals = roll.map((row) => minsBetween(row, 3, 4)).filter((v): v is number => v != null);
    const m5vals = roll.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null);
    const travelVals = roll.map((row) => travelMins(row)).filter((v): v is number => v != null);
    const totalVals = roll.map((row) => totalMins(row)).filter((v): v is number => v != null);
    return {
      jobs: roll,
      mins_2: fromValuesToQuad(m2vals),
      mins_3: fromValuesToQuad(m3vals),
      mins_4: fromValuesToQuad(m4vals),
      mins_5: fromValuesToQuad(m5vals),
      travel: fromValuesToQuad(travelVals),
      total: fromValuesToQuad(totalVals),
    };
  }, [filteredRows]);

  /** Season table: non-split = one row; split = Season → Winery → Vineyard Group → TT → Within/Over/Total, plus vineyard-group, winery, and season totals. */
  type SeasonRollupRow = {
    rowKey: string;
    tier: 'over_within' | 'tt_total' | 'vg_total' | 'winery_total' | 'season_total';
    limitLabel: string;
    wineryCol: string;
    vineyardGroupCol: string;
    ttCol: string;
    jobs: Row[];
    mins_2: MinsQuad;
    mins_3: MinsQuad;
    mins_4: MinsQuad;
    mins_5: MinsQuad;
    travel: MinsQuad;
    total: MinsQuad;
  };

  type RowWithLimitsBreached = Row & { limits_breached: '-' | 'X' };

  /** When Over/Under split: full-season Over | Within | Season charts (not per winery). */
  const seasonSplitChartTriplet = useMemo((): Pick<SeasonRollupRow, 'rowKey' | 'limitLabel' | 'jobs' | 'mins_2' | 'mins_3' | 'mins_4' | 'mins_5' | 'travel' | 'total'>[] | null => {
    if (!splitByOverUnder || filteredRows.length === 0) return null;
    const withLimits: RowWithLimitsBreached[] = filteredRows.map((r) => ({
      ...r,
      limits_breached: limitsBreachedForRow(r),
    }));
    const overJobs = withLimits.filter(
      (r) => includedInRollup(r) && (r as Row & { limits_breached?: string }).limits_breached === 'X',
    );
    const withinJobs = withLimits.filter(
      (r) => includedInRollup(r) && (r as Row & { limits_breached?: string }).limits_breached === '-',
    );
    const build = (jobs: Row[]) => ({
      jobs,
      mins_2: fromValuesToQuad(jobs.map((row) => minsBetween(row, 1, 2)).filter((v): v is number => v != null)),
      mins_3: fromValuesToQuad(jobs.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null)),
      mins_4: fromValuesToQuad(jobs.map((row) => minsBetween(row, 3, 4)).filter((v): v is number => v != null)),
      mins_5: fromValuesToQuad(jobs.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null)),
      travel: fromValuesToQuad(jobs.map((row) => travelMins(row)).filter((v): v is number => v != null)),
      total: fromValuesToQuad(jobs.map((row) => totalMins(row)).filter((v): v is number => v != null)),
    });
    return [
      { rowKey: 'chart-over', limitLabel: 'Over Allowance', ...build(overJobs) },
      { rowKey: 'chart-within', limitLabel: 'Within Allowance', ...build(withinJobs) },
      { rowKey: 'chart-season', limitLabel: 'Season Total', ...build(filteredRows.filter(includedInRollup)) },
    ];
  }, [filteredRows, splitByOverUnder, timeLimitRows, effectiveCustomer, filterTemplate]);

  const seasonRollupRows = useMemo((): SeasonRollupRow[] | null => {
    if (filteredRows.length === 0) return null;
    const withLimits: RowWithLimitsBreached[] = filteredRows.map((r) => ({
      ...r,
      limits_breached: limitsBreachedForRow(r),
    }));
    const build = (jobs: Row[]) => ({
      jobs,
      mins_2: fromValuesToQuad(jobs.map((row) => minsBetween(row, 1, 2)).filter((v): v is number => v != null)),
      mins_3: fromValuesToQuad(jobs.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null)),
      mins_4: fromValuesToQuad(jobs.map((row) => minsBetween(row, 3, 4)).filter((v): v is number => v != null)),
      mins_5: fromValuesToQuad(jobs.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null)),
      travel: fromValuesToQuad(jobs.map((row) => travelMins(row)).filter((v): v is number => v != null)),
      total: fromValuesToQuad(jobs.map((row) => totalMins(row)).filter((v): v is number => v != null)),
    });
    if (!splitSeasonGrouped) {
      return seasonRollup
        ? [
            {
              rowKey: 'season-only',
              tier: 'season_total',
              limitLabel: 'Season Total',
              wineryCol: '',
              vineyardGroupCol: '',
              ttCol: '',
              ...seasonRollup,
            },
          ]
        : null;
    }
    const byWinery = new Map<string, RowWithLimitsBreached[]>();
    for (const r of withLimits) {
      if (!includedInRollup(r)) continue;
      const w = (r.delivery_winery ?? '').toString().trim();
      if (!byWinery.has(w)) byWinery.set(w, []);
      byWinery.get(w)!.push(r);
    }
    const wineries = [...byWinery.keys()].sort((a, b) => {
      if (a === '' && b !== '') return 1;
      if (b === '' && a !== '') return -1;
      return a.localeCompare(b, undefined, { sensitivity: 'base' });
    });
    const ttOrder: { bucket: ReturnType<typeof jobTrailerChartBucket>; label: string }[] = [
      { bucket: 'T', label: SEASON_TT_LABEL_T },
      { bucket: 'TT', label: SEASON_TT_LABEL_TT },
      { bucket: 'other', label: SEASON_TT_LABEL_OTHER },
    ];
    const out: SeasonRollupRow[] = [];
    for (const w of wineries) {
      const wineryJobs = byWinery.get(w)!;
      const displayWinery = w === '' ? '—' : w;
      const byVg = new Map<string, RowWithLimitsBreached[]>();
      for (const r of wineryJobs) {
        const k = vineyardGroupKey(r);
        if (!byVg.has(k)) byVg.set(k, []);
        byVg.get(k)!.push(r);
      }
      const vgKeys = [...byVg.keys()].sort((a, b) => {
        if (a === '' && b !== '') return 1;
        if (b === '' && a !== '') return -1;
        return a.localeCompare(b, undefined, { sensitivity: 'base' });
      });
      let isFirstRowOfWinery = true;
      for (const vgk of vgKeys) {
        const vgJobs = byVg.get(vgk)!;
        const displayVg = vgk === '' ? '—' : vgk;
        let isFirstRowOfVg = true;
        for (const { bucket, label } of ttOrder) {
          const bucketJobs = vgJobs.filter((r) => jobTrailerChartBucket(r) === bucket);
          if (bucketJobs.length === 0) continue;
          if (splitByOverUnder) {
            const withinJobs = bucketJobs.filter((r) => (r as Row & { limits_breached?: string }).limits_breached === '-');
            const overJobs = bucketJobs.filter((r) => (r as Row & { limits_breached?: string }).limits_breached === 'X');
            const segments: { tier: SeasonRollupRow['tier']; limitLabel: string; jobs: Row[] }[] = [
              { tier: 'over_within', limitLabel: 'Within Allowance', jobs: withinJobs },
              { tier: 'over_within', limitLabel: 'Over Allowance', jobs: overJobs },
              { tier: 'tt_total', limitLabel: 'Total', jobs: bucketJobs },
            ];
            segments.forEach((seg, segIdx) => {
              const firstOfTt = segIdx === 0;
              out.push({
                rowKey: `${w}|${vgk}|${bucket}|${seg.limitLabel}`,
                tier: seg.tier,
                limitLabel: seg.limitLabel,
                wineryCol: isFirstRowOfWinery && firstOfTt ? displayWinery : '',
                vineyardGroupCol: isFirstRowOfVg && firstOfTt ? displayVg : '',
                ttCol: firstOfTt ? label : '',
                ...build(seg.jobs),
              });
              if (firstOfTt) {
                isFirstRowOfWinery = false;
                isFirstRowOfVg = false;
              }
            });
          } else {
            out.push({
              rowKey: `${w}|${vgk}|${bucket}|roll`,
              tier: 'tt_total',
              limitLabel: '',
              wineryCol: isFirstRowOfWinery ? displayWinery : '',
              vineyardGroupCol: isFirstRowOfVg ? displayVg : '',
              ttCol: label,
              ...build(bucketJobs),
            });
            isFirstRowOfWinery = false;
            isFirstRowOfVg = false;
          }
        }
        out.push({
          rowKey: `${w}|${vgk}|vg-total`,
          tier: 'vg_total',
          limitLabel: 'Total',
          wineryCol: '',
          vineyardGroupCol: 'Vineyard Group Total',
          ttCol: '',
          ...build(vgJobs),
        });
      }
      out.push({
        rowKey: `${w}|winery-total`,
        tier: 'winery_total',
        limitLabel: 'Total',
        wineryCol: '',
        vineyardGroupCol: '',
        ttCol: 'Winery Total',
        ...build(wineryJobs),
      });
    }
    const allRoll = filteredRows.filter(includedInRollup);
    out.push({
      rowKey: 'season-grand-total',
      tier: 'season_total',
      limitLabel: splitByOverUnder ? 'Season Total' : '',
      wineryCol: '',
      vineyardGroupCol: '',
      ttCol: splitByOverUnder ? '' : 'Season Total',
      ...build(allRoll),
    });
    return out;
  }, [filteredRows, seasonRollup, splitSeasonGrouped, splitByOverUnder, timeLimitRows, effectiveCustomer, filterTemplate]);

  /** By Day footer: Total, Max, Min, Av. Max/Min across harvest days (rows with ≥1 job). Av = grand total ÷ total jobs (same as each day row: column Total ÷ Jobs), not sum of daily Av cells and not ÷ day count. */
  type ByDayFooterSet = {
    rowType?: 'Over Allowance' | 'Within Allowance' | 'Daily Total';
    jobCount: number;
    /** Sum of per-row unique vineyard counts (matches summing the Vyards column). */
    vyardsTotal: number;
    /** Sum of per-row round-trip KMs (admin daily body column). */
    kmsTotal: number;
    stats: Record<string, MinsQuad>;
  };
  const byDayFooterSets = useMemo((): ByDayFooterSet[] => {
    const keys = ['mins_2', 'mins_3', 'mins_4', 'mins_5', 'travel', 'total'] as const;
    const build = (rowsForFooter: typeof rowsByDay): ByDayFooterSet => {
      const jobCount = rowsForFooter.reduce((a, r) => a + r.rollupJobCount, 0);
      const vyardsTotal = rowsForFooter.reduce(
        (a, r) => a + uniqueVineyardCount((r as RollupQuads).jobs ?? []),
        0,
      );
      const kmsTotal = rowsForFooter.reduce(
        (a, r) => a + roundTripKms((r as RollupQuads).jobs ?? []),
        0,
      );
      const harvestRows = rowsForFooter.filter((r) => r.rollupJobCount > 0);
      const stats: Record<string, MinsQuad> = {};
      for (const k of keys) {
        const quadsAll = rowsForFooter.map((r) => r[k]);
        const sumTotal = quadsAll.reduce((a, q) => a + q.total, 0);
        const quadsHarvest = harvestRows.map((r) => r[k]);
        stats[k] = {
          total: sumTotal,
          max: quadsHarvest.length ? Math.max(...quadsHarvest.map((q) => q.max)) : 0,
          min: quadsHarvest.length ? Math.min(...quadsHarvest.map((q) => q.min)) : 0,
          av: jobCount > 0 ? Math.round(sumTotal / jobCount) : 0,
        };
      }
      return { jobCount, vyardsTotal, kmsTotal, stats };
    };
    if (splitByOverUnder) {
      const over = rowsByDay.filter((r) => (r as { rowType?: string }).rowType === 'Over Allowance');
      const within = rowsByDay.filter((r) => (r as { rowType?: string }).rowType === 'Within Allowance');
      const dailyTotal = rowsByDay.filter((r) => (r as { rowType?: string }).rowType === 'Daily Total');
      return [
        { rowType: 'Over Allowance', ...build(over) },
        { rowType: 'Within Allowance', ...build(within) },
        { rowType: 'Daily Total', ...build(dailyTotal) },
      ];
    }
    return [{ ...build(rowsByDay) }];
  }, [rowsByDay, splitByOverUnder]);

  /** Distinct dates with ≥1 non-excluded job in Daily Summary (split-by-limits: 3 rows per date). */
  const summaryHarvestDayCount = useMemo(() => {
    const dates = new Set<string>();
    for (const r of rowsByDay) {
      if (r.rollupJobCount > 0) dates.add(r.date);
    }
    return dates.size;
  }, [rowsByDay]);

  /** Single set for non-split footer (first set). */
  const byDayStats = useMemo(() => byDayFooterSets[0]?.stats ?? ({} as Record<string, MinsQuad>), [byDayFooterSets]);

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

  const allColumns = useMemo(() => [...BY_JOB_LEAD_COLUMNS, ...stepColumns, { key: 'travel', label: 'Travel' }, { key: 'in_vineyard', label: 'In Vineyard' }, { key: 'in_winery', label: 'In Winery' }, { key: 'total', label: 'Total' }], [stepColumns]);
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
        setSortColumns(['vineyard_name', '', '']);
      })
      .catch(() => setSortColumns(['vineyard_name', '', '']));
  }, [sortableKeys]);

  const getThresholdNum = (key: string): number | null => {
    const s = minsThresholds[key]?.trim() ?? '';
    if (s === '') return null;
    const n = parseFloat(s);
    return Number.isNaN(n) ? null : n;
  };
  const isMinsOverThreshold = (val: number | null, key: string): boolean => {
    const limit = getThresholdNum(key);
    const numVal = val != null ? Number(val) : NaN;
    return !Number.isNaN(numVal) && limit != null && numVal > limit;
  };
  /** Threshold keys for step columns: 3 and 5 also check in_vineyard/in_winery so limits apply consistently. */
  const getThresholdKeysForStep = (stepNum: number): string[] => {
    if (stepNum === 3) return ['3', 'in_vineyard'];
    if (stepNum === 5) return ['5', 'in_winery'];
    return [String(stepNum)];
  };
  const isMinsOverThresholdForStepCol = (val: number | null, stepNum: number): boolean =>
    getThresholdKeysForStep(stepNum).some((k) => isMinsOverThreshold(val, k));
  /** Threshold key for season/daily column: mins_2 -> '2', travel -> 'travel', etc. */
  const thresholdKey = (col: string) => (col.startsWith('mins_') ? col.replace('mins_', '') : col);
  const limitDisplay = (key: string) => {
    const n = getThresholdNum(key);
    return n != null ? String(n) : '—';
  };
  /** `!` so limit red wins over inherited table/td text color (Tailwind v4). */
  const redIfOver = (val: number | null, key: string) =>
    isMinsOverThreshold(val, key) ? 'text-red-700! dark:text-red-300!' : '';
  /** Red if over any threshold for this step column (used for step 3 and 5 to match 2 and 4). */
  const redIfOverStepCol = (val: number | null, stepNum: number) =>
    isMinsOverThresholdForStepCol(val, stepNum) ? 'text-red-700! dark:text-red-300!' : '';

  const sortedRows = useMemo(() => {
    const out = [...rowsWithLimits];
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
  }, [rowsWithLimits, sortKey, sortDir, sortColumns]);

  /** By Job table shows one page; season/daily use full sortedRows via filteredRows. */
  const sortedRowsPage = useMemo(() => {
    if (summaryTab !== 'by_job') return sortedRows;
    const start = jobsPage * jobsPageSize;
    return sortedRows.slice(start, start + jobsPageSize);
  }, [sortedRows, summaryTab, jobsPage, jobsPageSize]);

  const jobsTotalPages = useMemo(() => {
    if (jobsPageSize <= 0) return 1;
    return Math.max(1, Math.ceil(sortedRows.length / jobsPageSize));
  }, [sortedRows.length, jobsPageSize]);

  useEffect(() => {
    setJobsPage((p) => Math.max(0, Math.min(p, jobsTotalPages - 1)));
  }, [jobsTotalPages]);

  useEffect(() => {
    const jid = pendingScrollToJobIdRef.current;
    if (!jid || summaryTab !== 'by_job') return;
    if (loading) return;
    const onPage = sortedRowsPage.some((r) => String(r.job_id ?? '').trim() === jid);
    if (!onPage) {
      pendingScrollToJobIdRef.current = null;
      return;
    }
    let cancelled = false;
    const run = () => {
      if (cancelled) return;
      const rowEl = document.querySelector(
        `[data-summary-focus-job="${CSS.escape(jid)}"]`,
      ) as HTMLElement | null;
      const scrollParent = byJobTableScrollRef.current;
      if (!rowEl) return;
      pendingScrollToJobIdRef.current = null;
      setFocusHighlightJobId(jid);
      /** Space below sticky multi-row thead inside the scroll box. */
      const stickyHeaderPad = 100;
      if (scrollParent) {
        const prect = scrollParent.getBoundingClientRect();
        const rrect = rowEl.getBoundingClientRect();
        const nextTop = scrollParent.scrollTop + (rrect.top - prect.top) - stickyHeaderPad;
        scrollParent.scrollTo({ top: Math.max(0, nextTop), behavior: 'smooth' });
      } else {
        rowEl.scrollIntoView({ block: 'start', behavior: 'smooth' });
      }
    };
    const id1 = window.requestAnimationFrame(() => {
      window.requestAnimationFrame(run);
    });
    return () => {
      cancelled = true;
      cancelAnimationFrame(id1);
    };
  }, [loading, summaryTab, sortedRowsPage]);

  useEffect(() => {
    if (!focusHighlightJobId) return;
    const t = window.setTimeout(() => setFocusHighlightJobId(null), 20000);
    return () => clearTimeout(t);
  }, [focusHighlightJobId]);

  /** Sum and average of mins columns over sorted rows (excluded=1 omitted from stats). */
  const columnStats = useMemo(() => {
    const rollRows = sortedRows.filter(includedInRollup);
    const stats: Record<string, { sum: number; avg: number | null; count: number }> = {};
    for (const n of [2, 3, 4, 5]) {
      const values = rollRows.map((r) => minsBetween(r, n - 1, n)).filter((v): v is number => v != null);
      const sum = values.reduce((a, b) => a + b, 0);
      const count = values.length;
      stats[`mins_${n}`] = { sum, avg: count ? Math.round(sum / count) : null, count };
    }
    const travelValues = rollRows.map(travelMins).filter((v): v is number => v != null);
    const travelSum = travelValues.reduce((a, b) => a + b, 0);
    const travelCount = travelValues.length;
    stats.travel = { sum: travelSum, avg: travelCount ? Math.round(travelSum / travelCount) : null, count: travelCount };
    const inVineyardValues = rollRows.map((r) => minsBetween(r, 2, 3)).filter((v): v is number => v != null);
    const inVineyardSum = inVineyardValues.reduce((a, b) => a + b, 0);
    const inVineyardCount = inVineyardValues.length;
    stats.in_vineyard = { sum: inVineyardSum, avg: inVineyardCount ? Math.round(inVineyardSum / inVineyardCount) : null, count: inVineyardCount };
    const inWineryValues = rollRows.map((r) => minsBetween(r, 4, 5)).filter((v): v is number => v != null);
    const inWinerySum = inWineryValues.reduce((a, b) => a + b, 0);
    const inWineryCount = inWineryValues.length;
    stats.in_winery = { sum: inWinerySum, avg: inWineryCount ? Math.round(inWinerySum / inWineryCount) : null, count: inWineryCount };
    const totalValues = rollRows.map(totalMins).filter((v): v is number => v != null);
    const totalSum = totalValues.reduce((a, b) => a + b, 0);
    const totalCount = totalValues.length;
    stats.total = { sum: totalSum, avg: totalCount ? Math.round(totalSum / totalCount) : null, count: totalCount };
    return stats;
  }, [sortedRows]);

  const rollupJobsCount = useMemo(() => filteredRows.filter(includedInRollup).length, [filteredRows]);

  /**
   * Distinct vineyards in the current filter set (rollup jobs only), sorted; total minutes = sum of step 2–5 minutes per job where computable.
   * KMs: sum of each job's `tbl_vworkjobs.distance` (via API row.distance), not a roll-up keyed off `tbl_distances` alone.
   */
  const vineyardSummaryList = useMemo(() => {
    type Agg = { jobCount: number; totalMinsSum: number; distanceRoundTripKmSum: number };
    const byName = new Map<string, Agg>();
    for (const row of filteredRows) {
      if (!includedInRollup(row)) continue;
      const raw = String(row.vineyard_name ?? '').trim();
      const key = raw === '' ? '\0' : raw;
      let a = byName.get(key);
      if (!a) {
        a = { jobCount: 0, totalMinsSum: 0, distanceRoundTripKmSum: 0 };
        byName.set(key, a);
      }
      a.jobCount += 1;
      const tm = totalMins(row);
      if (tm != null && !Number.isNaN(tm)) a.totalMinsSum += tm;
      const k = distanceRoundTripKmFromRow(row);
      if (k != null) a.distanceRoundTripKmSum += k;
    }
    const out = [...byName.entries()].map(([key, a]) => ({
      vineyardLabel: key === '\0' ? '—' : key,
      jobCount: a.jobCount,
      totalMinsSum: a.totalMinsSum,
      kmsRoundTrip: a.distanceRoundTripKmSum,
    }));
    out.sort((x, y) => {
      const bx = x.vineyardLabel === '—';
      const by = y.vineyardLabel === '—';
      if (bx !== by) return bx ? 1 : -1;
      return x.vineyardLabel.localeCompare(y.vineyardLabel, undefined, { sensitivity: 'base' });
    });
    return out;
  }, [filteredRows]);

  const dayClientView = summaryTab === 'by_day' && viewMode === 'client';
  const jobClientView = summaryTab === 'by_job' && viewMode === 'client';
  const seasonClientView = summaryTab === 'season' && viewMode === 'client';
  /** Client + split season: Time Limits uses `SeasonClientSplitColgroupLimits`; Season table uses `SeasonClientSplitColgroupSeasonData` (same cols 1–6 cumulative %, wider TT/Over). */
  const alignClientTimeLimitsToSeasonAv =
    viewMode === 'client' && splitSeasonGrouped && summaryTab === 'season';
  /**
   * Per limit row: Av columns match Season table math (rollupQuadFromValues on jobs in the silo that
   * map to this limit row). Only jobs with includedInRollup (excluded ≠ 1), same as season rollups.
   */
  const timeLimitRowActualById = useMemo(() => {
    const map = new Map<
      number,
      {
        metrics: {
          toVine: { jobs: number; actual: number | null };
          inVine: { jobs: number; actual: number | null };
          toWine: { jobs: number; actual: number | null };
          inWine: { jobs: number; actual: number | null };
          travel: { jobs: number; actual: number | null };
          inVineyard: { jobs: number; actual: number | null };
          inWinery: { jobs: number; actual: number | null };
          total: { jobs: number; actual: number | null };
        };
      }
    >();
    if (timeLimitRowsForTable.length === 0) return map;
    const customer = effectiveCustomer.trim();
    if (!customer) return map;
    for (const r of timeLimitRowsForTable) {
      /** Client: one visible row can represent T + hidden TT with identical limits — merge job stats for all those limit row ids. */
      const mergedLimitIds =
        viewMode === 'client'
          ? new Set(
              timeLimitRows
                .filter((row) => timeLimitRowComparableKey(row) === timeLimitRowComparableKey(r))
                .map((row) => row.id),
            )
          : new Set<number>([r.id]);
      const matching = filteredRows.filter((job) => {
        if (!includedInRollup(job)) return false;
        const winery = String(job.delivery_winery ?? '').trim();
        if (!winery) return false;
        const tmpl =
          filterTemplate.trim() ||
          (job.template != null ? String(job.template).trim() : '');
        const lim = findMatchingWineryMinuteRow(timeLimitRows, {
          customer,
          template: tmpl,
          winery,
          vineyardGroup: String((job as Record<string, unknown>).vineyard_group ?? '').trim(),
          jobTT: String(job.trailermode ?? '').trim(),
        }) as TimeLimitRow | null;
        return lim != null && mergedLimitIds.has(lim.id);
      });
      const toVineVals = matching.map((row) => minsBetween(row, 1, 2)).filter((v): v is number => v != null);
      const inVineVals = matching.map((row) => minsBetween(row, 2, 3)).filter((v): v is number => v != null);
      const toWineVals = matching.map((row) => minsBetween(row, 3, 4)).filter((v): v is number => v != null);
      const inWineVals = matching.map((row) => minsBetween(row, 4, 5)).filter((v): v is number => v != null);
      const travelVals = matching.map((row) => travelMins(row)).filter((v): v is number => v != null);
      const totalVals = matching.map((row) => totalMins(row)).filter((v): v is number => v != null);
      const toStat = (vals: number[]) => ({ jobs: vals.length, actual: vals.length > 0 ? rollupQuadFromValues(vals).av : null });
      map.set(r.id, {
        metrics: {
          toVine: toStat(toVineVals),
          inVine: toStat(inVineVals),
          toWine: toStat(toWineVals),
          inWine: toStat(inWineVals),
          travel: toStat(travelVals),
          inVineyard: toStat(inVineVals),
          inWinery: toStat(inWineVals),
          total: toStat(totalVals),
        },
      });
    }
    return map;
  }, [timeLimitRowsForTable, filteredRows, effectiveCustomer, filterTemplate, timeLimitRows, viewMode]);

  const runExportLimitsTableXlsx = useCallback(async () => {
    const day = new Date().toISOString().slice(0, 10);
    const cust = sanitizeSummaryExportFilenamePart(effectiveCustomer);
    const tmpl = sanitizeSummaryExportFilenamePart(filterTemplate);
    const header = [
      'id',
      'Customer',
      'Template',
      'Winery',
      'Vineyard group',
      'TT',
      'To Vine Jobs',
      'To Vine Actual (Av)',
      'To Vine Limit',
      'In Vine Jobs',
      'In Vine Actual (Av)',
      'In Vine Limit',
      'To Wine Jobs',
      'To Wine Actual (Av)',
      'To Wine Limit',
      'In Wine Jobs',
      'In Wine Actual (Av)',
      'In Wine Limit',
      'Travel Jobs',
      'Travel Actual (Av)',
      'Travel Limit',
      'In Vineyard Jobs',
      'In Vineyard Actual (Av)',
      'In Vineyard Limit',
      'In Winery Jobs',
      'In Winery Actual (Av)',
      'In Winery Limit',
      'Total Jobs',
      'Total Actual (Av)',
      'Total Limit',
    ];
    const aoa: (string | number | null | undefined)[][] = [header];
    for (const r of timeLimitRowsForTable) {
      const stats = timeLimitRowActualById.get(r.id)?.metrics;
      const travelLim = timeLimitRowTravelVal(r);
      aoa.push([
        r.id,
        r.Customer ?? '',
        r.Template ?? '',
        r.Winery ?? '',
        r.vineyardgroup ?? '',
        r.TT ?? '',
        stats?.toVine.jobs ?? '',
        stats?.toVine.actual ?? '',
        r.ToVineMins ?? '',
        stats?.inVine.jobs ?? '',
        stats?.inVine.actual ?? '',
        r.InVineMins ?? '',
        stats?.toWine.jobs ?? '',
        stats?.toWine.actual ?? '',
        r.ToWineMins ?? '',
        stats?.inWine.jobs ?? '',
        stats?.inWine.actual ?? '',
        r.InWineMins ?? '',
        stats?.travel.jobs ?? '',
        stats?.travel.actual ?? '',
        travelLim ?? '',
        stats?.inVineyard.jobs ?? '',
        stats?.inVineyard.actual ?? '',
        r.InVineMins ?? '',
        stats?.inWinery.jobs ?? '',
        stats?.inWinery.actual ?? '',
        r.InWineMins ?? '',
        stats?.total.jobs ?? '',
        stats?.total.actual ?? '',
        r.TotalMins ?? '',
      ]);
    }
    await downloadSummaryXlsx(aoa, 'Time Limits', `summary-limits_${cust}_${tmpl}_${day}.xlsx`);
  }, [timeLimitRowsForTable, timeLimitRowActualById, effectiveCustomer, filterTemplate]);

  const runExportSeasonDataXlsx = useCallback(async () => {
    const day = new Date().toISOString().slice(0, 10);
    const cust = sanitizeSummaryExportFilenamePart(effectiveCustomer);
    const tmpl = sanitizeSummaryExportFilenamePart(filterTemplate);
    const quadHeaders = (prefix: string) => [`${prefix}_total`, `${prefix}_max`, `${prefix}_min`, `${prefix}_av`];
    const header = [
      'tier',
      'rowKey',
      'limitLabel',
      'customer',
      'template',
      'winery',
      'vineyardGroup',
      'tt',
      'jobs',
      'vyards',
      'kmsRoundTrip',
      ...quadHeaders('step2_toVine'),
      ...quadHeaders('step3_inVine'),
      ...quadHeaders('step4_toWine'),
      ...quadHeaders('step5_inWinery'),
      ...quadHeaders('travel'),
      ...quadHeaders('inVineyard'),
      ...quadHeaders('inWinery'),
      ...quadHeaders('grandTotal'),
    ];
    const aoa: (string | number | null | undefined)[][] = [header];
    const list = seasonRollupRows ?? [];
    for (const rollup of list) {
      aoa.push([
        rollup.tier,
        rollup.rowKey,
        rollup.limitLabel,
        effectiveCustomer,
        filterTemplate,
        rollup.wineryCol,
        rollup.vineyardGroupCol,
        rollup.ttCol,
        rollup.jobs.length,
        uniqueVineyardCount(rollup.jobs),
        roundTripKms(rollup.jobs),
        ...summaryExportQuadCells(rollup.mins_2),
        ...summaryExportQuadCells(rollup.mins_3),
        ...summaryExportQuadCells(rollup.mins_4),
        ...summaryExportQuadCells(rollup.mins_5),
        ...summaryExportQuadCells(rollup.travel),
        ...summaryExportQuadCells(rollup.mins_3),
        ...summaryExportQuadCells(rollup.mins_5),
        ...summaryExportQuadCells(rollup.total),
      ]);
    }
    await downloadSummaryXlsx(aoa, 'Season Data', `summary-season_${cust}_${tmpl}_${day}.xlsx`);
  }, [seasonRollupRows, effectiveCustomer, filterTemplate]);

  /** Jobs API runs only with both set so payloads stay scoped to one customer + template. */
  const canLoadJobs = effectiveCustomer.trim() !== '' && filterTemplate.trim() !== '';

  useEffect(() => {
    if (viewMode === 'client' && summaryTab === 'data_audit') {
      setSummaryTab('season');
    }
  }, [summaryTab, viewMode]);

  useEffect(() => {
    try {
      const raw = localStorage.getItem('SummaryHeaderOpen');
      if (raw === '0') setHeaderOpen(false);
      if (raw === '1') setHeaderOpen(true);
    } catch {
      /* ignore */
    }
  }, []);
  const setHeaderOpenAndPersist = useCallback((open: boolean) => {
    setHeaderOpen(open);
    try {
      localStorage.setItem('SummaryHeaderOpen', open ? '1' : '0');
    } catch {
      /* ignore */
    }
  }, []);

  const LOW_MINUTES_HIGHLIGHT_SETTING_TYPE = 'System';
  const LOW_MINUTES_HIGHLIGHT_SETTING_NAME = 'SummaryLowMinutesHighlight';
  const JOBS_PAGE_SIZE_SETTING_TYPE = 'System';
  const JOBS_PAGE_SIZE_SETTING_NAME = 'SummaryJobsRowsPerPage';

  useEffect(() => {
    fetch(
      `/api/settings?${new URLSearchParams({
        type: LOW_MINUTES_HIGHLIGHT_SETTING_TYPE,
        name: LOW_MINUTES_HIGHLIGHT_SETTING_NAME,
      })}`,
      { cache: 'no-store' },
    )
      .then((r) => r.json())
      .then((data) => {
        const raw = String(data?.settingvalue ?? '').trim();
        const n = raw === '' ? NaN : parseFloat(raw);
        if (Number.isFinite(n) && n >= 0) setLowMinutesHighlight(String(n));
        else setLowMinutesHighlight('5');
      })
      .catch(() => setLowMinutesHighlight('5'));
  }, []);

  useEffect(() => {
    fetch(
      `/api/settings?${new URLSearchParams({
        type: JOBS_PAGE_SIZE_SETTING_TYPE,
        name: JOBS_PAGE_SIZE_SETTING_NAME,
      })}`,
      { cache: 'no-store' },
    )
      .then((r) => r.json())
      .then((data) => {
        const raw = String(data?.settingvalue ?? '').trim();
        const n = raw === '' ? NaN : parseInt(raw, 10);
        if (Number.isFinite(n) && n > 0) setJobsPageSize(Math.max(50, Math.min(2000, n)));
      })
      .catch(() => {});
  }, []);

  const saveJobsPageSize = useCallback((n: number) => {
    fetch('/api/settings', {
      method: 'PUT',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: JOBS_PAGE_SIZE_SETTING_TYPE,
        settingname: JOBS_PAGE_SIZE_SETTING_NAME,
        settingvalue: String(n),
      }),
    }).catch(() => {});
  }, []);

  const lowMinutesHighlightNum = useMemo(() => {
    const n = parseFloat(String(lowMinutesHighlight).trim());
    if (!Number.isFinite(n) || n < 0) return 5;
    return n;
  }, [lowMinutesHighlight]);
  /** Flag when any mins value is strictly less than this threshold. */
  const lowMinutesHighlightThreshold = lowMinutesHighlightNum;

  const saveLowMinutesHighlight = useCallback(() => {
    const raw = String(lowMinutesHighlight).trim();
    if (raw === '') return;
    const n = parseFloat(raw);
    if (!Number.isFinite(n) || n < 0) return;
    fetch('/api/settings', {
      method: 'PUT',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: LOW_MINUTES_HIGHLIGHT_SETTING_TYPE,
        settingname: LOW_MINUTES_HIGHLIGHT_SETTING_NAME,
        settingvalue: String(n),
      }),
    }).catch(() => {});
  }, [lowMinutesHighlight]);

  useEffect(() => {
    fetch('/api/summary/low-minutes-exclusions', { cache: 'no-store' })
      .then((r) => r.json())
      .then((data) => {
        const arr = Array.isArray(data?.vineyards) ? (data.vineyards as unknown[]) : [];
        const cleaned = arr.filter((x): x is string => typeof x === 'string').map((s) => s.trim()).filter(Boolean);
        setLowMinutesExclusions(cleaned);
      })
      .catch(() => setLowMinutesExclusions([]));
  }, []);

  const lowMinutesExclusionSet = useMemo(
    () => new Set(lowMinutesExclusions.map((s) => s.trim().toLowerCase()).filter(Boolean)),
    [lowMinutesExclusions],
  );
  const lowMinutesDraftSet = useMemo(() => new Set(lowMinutesExclusionsDraft), [lowMinutesExclusionsDraft]);

  const commitLowMinutesExclusions = useCallback(() => {
    const next = sortVineyardNames(lowMinutesExclusionsDraftRef.current);
    setLowMinutesExclusions(next);
    setLowMinutesExclusionsOpen(false);
    fetch('/api/summary/low-minutes-exclusions', {
      method: 'PUT',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ vineyards: next }),
    }).catch(() => {});
  }, []);

  const lowMinutesValuesForRow = useCallback(
    (row: Row): number[] => {
      if (lowMinutesExclusionSet.size === 0) return jobRowMinuteValues(row);
      const v = String(row.vineyard_name ?? '').trim().toLowerCase();
      if (v && lowMinutesExclusionSet.has(v)) return [];
      return jobRowMinuteValues(row);
    },
    [lowMinutesExclusionSet],
  );

  useEffect(() => {
    const type = SUMMARY_COLUMN_COLOR_SETTINGS_TYPE;
    const keys = Object.keys(SUMMARY_COLUMN_COLOR_SETTING_NAMES) as SummaryColumnColorKey[];
    Promise.all(
      keys.map((k) =>
        fetch(
          `/api/settings?${new URLSearchParams({ type, name: SUMMARY_COLUMN_COLOR_SETTING_NAMES[k] })}`,
          { cache: 'no-store' },
        )
          .then((r) => r.json())
          .then((data) => ({ k, v: String(data?.settingvalue ?? '').trim() }))
          .catch(() => ({ k, v: '' })),
      ),
    ).then((pairs) => {
      setSummaryColumnColors((prev) => {
        const next = { ...prev };
        for (const p of pairs) next[p.k] = p.v;
        return next;
      });
    });
  }, []);

  const bgFor = useCallback(
    (k: SummaryColumnColorKey): React.CSSProperties | undefined => {
      const v = (summaryColumnColors[k] ?? '').trim();
      if (!v) return undefined;
      return { backgroundColor: v };
    },
    [summaryColumnColors],
  );

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-4">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Summary</h1>
      </div>
      {error && (
        <div className="rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          <p className="font-medium">{error}</p>
        </div>
      )}
      {!error && (
        <>
          <div className="mb-3 rounded-lg border border-zinc-200 bg-white p-2 dark:border-zinc-700 dark:bg-zinc-900">
            <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
              <h2 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">Filters & sorting</h2>

              <div className="flex flex-1 flex-wrap items-center justify-start gap-x-3 gap-y-2">
                <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                  <label className="text-[11px] font-medium text-zinc-500">Customer</label>
                  <div
                    title="Set in sidebar (Client view)"
                    className="rounded border border-zinc-200 bg-zinc-50 px-2 py-0.5 text-[11px] text-zinc-700 dark:border-zinc-700 dark:bg-zinc-800 dark:text-zinc-300"
                  >
                    {clientCustomer || '— All —'}
                  </div>
                </div>

                <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                  <label className="text-[11px] font-medium text-zinc-500">Template</label>
                  <select
                    value={filterTemplate}
                    onChange={(e) => setFilterTemplate(e.target.value)}
                    disabled={!effectiveCustomer.trim()}
                    className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                  >
                    <option value="">— Select template —</option>
                    {distinctTemplates.map((t) => (
                      <option key={t} value={t}>{t}</option>
                    ))}
                  </select>
                </div>

                <label className="flex cursor-pointer items-center gap-2 text-xs text-zinc-600 dark:text-zinc-400">
                  <input
                    type="checkbox"
                    checked={headerOpen}
                    onChange={(e) => setHeaderOpenAndPersist(e.target.checked)}
                    className="rounded border-zinc-300"
                  />
                  Show all filters
                </label>
              </div>
            </div>

            {headerOpen && (
            <div className="grid gap-3 xl:grid-cols-4">
              {/* Filters: 3 columns, 4 rows each (label + value). */}
              <div className="grid gap-3 md:grid-cols-3 xl:col-span-3">
                <div className="flex min-w-0 flex-col gap-2">
                  <div>
                    <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">Winery</label>
                      <select
                        value={filterWinery}
                        onChange={(e) => setFilterWinery(e.target.value)}
                        disabled={!canLoadJobs}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                      >
                        <option value="">— All —</option>
                        {distinctWineries.map((w) => (
                          <option key={w} value={w}>{w}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                  <div>
                    <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">V group</label>
                      <select
                        value={filterVineyardGroup}
                        onChange={(e) => setFilterVineyardGroup(e.target.value)}
                        disabled={!canLoadJobs}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                      >
                        <option value="">— All —</option>
                        {distinctVineyardGroups.map((g) => (
                          <option key={g} value={g}>{g}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                </div>

                <div className="flex min-w-0 flex-col gap-2">
                  <div className="relative min-w-0" ref={vineyardPickerAnchorRef}>
                    <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">Vineyard</label>
                      <button
                        type="button"
                        disabled={!canLoadJobs}
                        onClick={() => {
                          if (vineyardPickerOpen) {
                            commitVineyardPicker();
                          } else {
                            setVineyardPickerDraft(sortVineyardNames(filterVineyards));
                            setVineyardPickerOpen(true);
                          }
                        }}
                        className="flex w-full min-w-0 items-center justify-between gap-2 rounded border border-zinc-300 bg-white px-2 py-0.5 text-left text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                        title={
                          filterVineyards.length === 0
                            ? 'All vineyards'
                            : filterVineyards.length <= 3
                              ? filterVineyards.join(', ')
                              : `${filterVineyards.length} selected`
                        }
                      >
                        <span className="min-w-0 truncate">
                          {filterVineyards.length === 0
                            ? '— All —'
                            : filterVineyards.length <= 2
                              ? filterVineyards.join(', ')
                              : `${filterVineyards.length} selected`}
                        </span>
                        <span className="shrink-0 text-zinc-400" aria-hidden>
                          ▾
                        </span>
                      </button>
                    </div>
                  </div>

                  <div>
                    <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">Start From</label>
                      <input
                        type="date"
                        value={filterActualFrom}
                        onChange={(e) => setFilterActualFrom(e.target.value)}
                        disabled={!canLoadJobs}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                      />
                    </div>
                  </div>
                  <div>
                    <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">Start To</label>
                      <input
                        type="date"
                        value={filterActualTo}
                        onChange={(e) => setFilterActualTo(e.target.value)}
                        disabled={!canLoadJobs}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                      />
                    </div>
                  </div>
                  <div>
                    <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">Month</label>
                      <select
                        value={actualStartMonthQuickPickValue}
                        onChange={(e) => {
                          const v = e.target.value;
                          if (!v) {
                            setFilterActualFrom('');
                            setFilterActualTo('');
                            return;
                          }
                          const [ys, ms] = v.split('-');
                          const y = Number(ys);
                          const mo = Number(ms);
                          setFilterActualFrom(firstDayOfMonthYmd(y, mo));
                          setFilterActualTo(lastDayOfMonthYmd(y, mo));
                        }}
                        disabled={!canLoadJobs || actualStartMonthOptions.length === 0}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                        title="Months from all loaded jobs for this customer and template (not narrowed by other filters). Sets from/to to first and last day of the month."
                      >
                        <option value="">— Dates: manual —</option>
                        {actualStartMonthOptions.map((opt) => (
                          <option key={opt.key} value={opt.key}>
                            {opt.label}
                          </option>
                        ))}
                      </select>
                    </div>
                  </div>
                </div>

                <div className="flex min-w-0 flex-col gap-2">
                  <div>
                    <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">Truck</label>
                      <select
                        value={filterTruckId}
                        onChange={(e) => setFilterTruckId(e.target.value)}
                        disabled={!canLoadJobs}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                      >
                        <option value="">— All —</option>
                        {distinctTruckIds.map((id) => (
                          <option key={id} value={id}>{id}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                  <div>
                    <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">Worker</label>
                      <select
                        value={filterWorker}
                        onChange={(e) => setFilterWorker(e.target.value)}
                        disabled={!canLoadJobs}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                        title="Workers appearing in jobs for this customer and template (list does not shrink when you filter by date, winery, etc.)"
                      >
                        <option value="">— All —</option>
                        {distinctWorkers.map((w) => (
                          <option key={w} value={w}>{w}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                  <div>
                    <div className="grid grid-cols-[5rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">TT</label>
                      <select
                        value={filterTrailermode}
                        onChange={(e) => setFilterTrailermode(e.target.value)}
                        disabled={!canLoadJobs}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                        title="Filter by trailermode (T = truck only, T_T = truck + trailer)"
                      >
                        <option value="">— All —</option>
                        {distinctTrailermodes.map((t) => (
                          <option key={t} value={t}>{t}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                </div>
              </div>

              {/* Sorting: 4th column, 3 rows + separate save frame. */}
              <div className="rounded-md border border-zinc-200 bg-zinc-50 p-2 shadow-sm dark:border-zinc-700 dark:bg-zinc-900/70">
                <div className="mb-1 flex items-center justify-between gap-2 border-b border-zinc-200 pb-0.5 dark:border-zinc-700">
                  <div className="text-xs font-semibold uppercase tracking-wide text-zinc-600 dark:text-zinc-400">
                    Sorting
                  </div>
                  <button
                    type="button"
                    onClick={() => saveSortColumns(sortColumns)}
                    disabled={sortSaveStatus === 'saving'}
                    className="rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] font-medium text-zinc-700 hover:bg-zinc-50 disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-200 dark:hover:bg-zinc-800"
                    title="Save sort config"
                  >
                    {sortSaveStatus === 'saving'
                      ? 'Saving…'
                      : sortSaveStatus === 'saved'
                        ? 'Saved ✓'
                        : sortSaveStatus === 'error'
                          ? 'Save failed'
                          : 'Save'}
                  </button>
                </div>
                <div className="grid gap-2">
                  <div>
                    <div className="grid grid-cols-[3.75rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">Sort 1</label>
                      <select
                        value={sortColumns[0]}
                        onChange={(e) => setSortColumn(0, e.target.value)}
                        disabled={!canLoadJobs}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                      >
                        <option value="">— None —</option>
                        {sortableKeys.map((k) => (
                          <option key={k} value={k}>{allColumns.find((c) => c.key === k)?.label ?? k}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                  <div>
                    <div className="grid grid-cols-[3.75rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">Sort 2</label>
                      <select
                        value={sortColumns[1]}
                        onChange={(e) => setSortColumn(1, e.target.value)}
                        disabled={!canLoadJobs}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                      >
                        <option value="">— None —</option>
                        {sortableKeys.map((k) => (
                          <option key={k} value={k}>{allColumns.find((c) => c.key === k)?.label ?? k}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                  <div>
                    <div className="grid grid-cols-[3.75rem_1fr] items-center gap-1.5">
                      <label className="text-[11px] font-medium text-zinc-500">Sort 3</label>
                      <select
                        value={sortColumns[2]}
                        onChange={(e) => setSortColumn(2, e.target.value)}
                        disabled={!canLoadJobs}
                        className="w-full rounded border border-zinc-300 bg-white px-2 py-0.5 text-[11px] disabled:cursor-not-allowed disabled:opacity-70 dark:border-zinc-600 dark:bg-zinc-800"
                      >
                        <option value="">— None —</option>
                        {sortableKeys.map((k) => (
                          <option key={k} value={k}>{allColumns.find((c) => c.key === k)?.label ?? k}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            )}
          </div>
            {vineyardPickerOpen &&
              effectiveCustomer.trim() &&
              typeof document !== 'undefined' &&
              createPortal(
                <div
                  ref={vineyardPickerPanelRef}
                  className="fixed left-1/2 top-1/2 z-[100] flex h-[50vh] w-[50vw] max-h-[50vh] max-w-[50vw] min-w-0 -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-lg border border-zinc-200 bg-white shadow-2xl dark:border-zinc-600 dark:bg-zinc-900"
                  role="dialog"
                  aria-label="Choose vineyards"
                >
                  <div className="flex shrink-0 justify-end border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
                    <button
                      type="button"
                      className="rounded border border-zinc-300 px-3 py-1 text-sm hover:bg-zinc-100 dark:border-zinc-600 dark:hover:bg-zinc-800"
                      onClick={() => setVineyardPickerDraft([])}
                    >
                      Clear all
                    </button>
                  </div>
                  <div className="min-h-0 flex-1 overflow-y-auto p-4">
                    <div className="grid grid-cols-3 gap-x-8 gap-y-2 text-sm">
                      {vineyardPickerOptions.map((v) => (
                        <label
                          key={v}
                          className="flex cursor-pointer items-center gap-2 rounded px-1 py-1 hover:bg-zinc-50 dark:hover:bg-zinc-800"
                        >
                          <input
                            type="checkbox"
                            className="shrink-0"
                            checked={vineyardDraftSet.has(v)}
                            onChange={() => {
                              setVineyardPickerDraft((prev) => {
                                const next = new Set(prev);
                                if (next.has(v)) next.delete(v);
                                else next.add(v);
                                return sortVineyardNames(Array.from(next));
                              });
                            }}
                          />
                          <span className="min-w-0 whitespace-nowrap" title={v}>
                            {v}
                          </span>
                        </label>
                      ))}
                    </div>
                  </div>
                </div>,
                document.body,
              )}
          {/* Time Limits: tbl_wineryminutes for selected customer (shown before jobs load so limits are visible while choosing template). */}
          {effectiveCustomer.trim() && (
            <div className="mb-4 rounded-lg border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900">
              <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                <h2 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">Time Limits</h2>
                <div className="flex flex-wrap items-center justify-start gap-x-4 gap-y-1">
                  <label className="flex items-center gap-2 text-xs text-zinc-600 dark:text-zinc-400">
                    Low minutes highlight
                    <input
                      type="number"
                      min={0}
                      step="1"
                      value={lowMinutesHighlight}
                      onChange={(e) => setLowMinutesHighlight(e.target.value)}
                      onBlur={saveLowMinutesHighlight}
                      className="w-16 rounded border border-zinc-300 bg-white px-2 py-1 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                      title="Highlight day/job row when any mins value is below this threshold."
                    />
                  </label>
                  <button
                    type="button"
                    onClick={() => setJobsReloadNonce((n) => n + 1)}
                    disabled={loading || !canLoadJobs}
                    className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs text-zinc-700 hover:bg-zinc-50 disabled:opacity-60 dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-200 dark:hover:bg-zinc-800"
                    title="Reload jobs from API (customer + template)"
                  >
                    Refresh
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setLowMinutesExclusionsDraft(sortVineyardNames(lowMinutesExclusions));
                      setLowMinutesExclusionsOpen(true);
                    }}
                    className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs text-zinc-700 hover:bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-900 dark:text-zinc-200 dark:hover:bg-zinc-800"
                    title="Exclude vineyards from low-minutes highlighting"
                  >
                    Exclusions… ({lowMinutesExclusions.length})
                  </button>
                  <label className="flex cursor-pointer items-center gap-2 text-xs text-zinc-600 dark:text-zinc-400">
                    <input
                      type="checkbox"
                      checked={showLimitsTable}
                      onChange={(e) => setShowLimitsTable(e.target.checked)}
                      className="rounded border-zinc-300"
                    />
                    Show limits table
                  </label>
                </div>
              </div>
              {lowMinutesExclusionsOpen &&
                typeof document !== 'undefined' &&
                createPortal(
                  <div
                    className="fixed left-1/2 top-1/2 z-[110] flex h-[60vh] w-[60vw] max-h-[60vh] max-w-[60vw] min-w-0 -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-lg border border-zinc-200 bg-white shadow-2xl dark:border-zinc-600 dark:bg-zinc-900"
                    role="dialog"
                    aria-label="Low minutes highlight exclusions"
                  >
                    <div className="flex shrink-0 items-center justify-between gap-3 border-b border-zinc-200 px-4 py-3 dark:border-zinc-700">
                      <div>
                        <div className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Low minutes highlight exclusions</div>
                        <div className="text-xs text-zinc-500 dark:text-zinc-400">
                          Selected vineyards will be excluded from low-minutes highlighting (use for very close vineyards).
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <button
                          type="button"
                          className="rounded border border-zinc-300 px-3 py-1 text-sm hover:bg-zinc-100 dark:border-zinc-600 dark:hover:bg-zinc-800"
                          onClick={() => setLowMinutesExclusionsDraft([])}
                        >
                          Clear all
                        </button>
                        <button
                          type="button"
                          className="rounded bg-blue-600 px-3 py-1 text-sm font-medium text-white hover:bg-blue-700 dark:bg-blue-700 dark:hover:bg-blue-600"
                          onClick={commitLowMinutesExclusions}
                        >
                          Save
                        </button>
                        <button
                          type="button"
                          className="rounded border border-zinc-300 px-3 py-1 text-sm hover:bg-zinc-100 dark:border-zinc-600 dark:hover:bg-zinc-800"
                          onClick={() => setLowMinutesExclusionsOpen(false)}
                        >
                          Close
                        </button>
                      </div>
                    </div>
                    <div className="min-h-0 flex-1 overflow-y-auto p-4">
                      <div className="mb-2 text-xs text-zinc-500 dark:text-zinc-400">
                        {lowMinutesExclusionsDraft.length} selected
                      </div>
                      <div className="grid grid-cols-3 gap-x-8 gap-y-2 text-sm">
                        {vineyardOptionsAll.map((v) => (
                          <label
                            key={v}
                            className="flex cursor-pointer items-center gap-2 rounded px-1 py-1 hover:bg-zinc-50 dark:hover:bg-zinc-800"
                          >
                            <input
                              type="checkbox"
                              className="shrink-0"
                              checked={lowMinutesDraftSet.has(v)}
                              onChange={() => {
                                setLowMinutesExclusionsDraft((prev) => {
                                  const next = new Set(prev);
                                  if (next.has(v)) next.delete(v);
                                  else next.add(v);
                                  return sortVineyardNames(Array.from(next));
                                });
                              }}
                            />
                            <span className="min-w-0 whitespace-nowrap" title={v}>
                              {v}
                            </span>
                          </label>
                        ))}
                      </div>
                    </div>
                  </div>,
                  document.body,
                )}
              {showLimitsTable && timeLimitRows.length === 0 ? (
                <p className="text-sm text-zinc-500 dark:text-zinc-400">No time limit rows for this customer.</p>
              ) : showLimitsTable && timeLimitRows.length > 0 ? (
                <>
                  <p className="mb-2 text-xs text-zinc-500 dark:text-zinc-400">
                    {viewMode === 'client'
                      ? 'Click a row to show its limits in the header boxes. Travel limits = to vineyard + to winery. Jobs | Actual | Limit: stats combine all jobs mapped to this row or any hidden duplicate limit row (same customer/template/winery/group and same limit minutes; e.g. T + TT). Actual uses Season Summary Av math; excluded=1 omitted. Duplicate rows are shown once in the table.'
                      : 'Click a row to show its limits in the header boxes. Jobs | Actual | Limit for every metric: Actual matches Season Summary Av math for jobs mapped to this limit row (Customer, Template, Winery, vineyard group, TT; excluded=1 omitted from rollups).'}
                  </p>
                  <div
                    className={
                      alignClientTimeLimitsToSeasonAv
                        ? 'overflow-x-auto -mx-3'
                        : 'overflow-x-auto'
                    }
                  >
                    {viewMode === 'client' ? (
                      alignClientTimeLimitsToSeasonAv ? (
                        <table className="w-full min-w-0 table-fixed text-left text-xs">
                          <SeasonClientSplitColgroupLimits />
                          <thead className={SUMMARY_THEAD_TH_ALIGNMENT}>
                            <tr className="border-b border-zinc-200 dark:border-zinc-700">
                              <th
                                rowSpan={2}
                                className="min-w-[9rem] border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                              >
                                Winery
                              </th>
                              <th
                                rowSpan={2}
                                colSpan={5}
                                className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                              >
                                Vineyard group
                              </th>
                              <th
                                style={bgFor('travel')}
                                colSpan={3}
                                className="border-r border-zinc-200 px-2 py-1.5 text-center text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100"
                              >
                                Travel
                              </th>
                              <th
                                style={bgFor('inVineyard')}
                                colSpan={3}
                                className={`border-r border-zinc-200 px-2 py-1.5 text-center text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[3]}`}
                              >
                                In Vineyard
                              </th>
                              <th
                                style={bgFor('inWinery')}
                                colSpan={3}
                                className={`border-r border-zinc-200 px-2 py-1.5 text-center text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[5]}`}
                              >
                                In Winery
                              </th>
                              <th
                                style={bgFor('total')}
                                colSpan={3}
                                className="border-zinc-200 px-2 py-1.5 text-center text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100"
                              >
                                Total
                              </th>
                            </tr>
                            <tr className="border-b border-zinc-200 dark:border-zinc-700">
                              {(['travel', 'in_vineyard', 'in_winery', 'total'] as const).map((step) => {
                                const groupBg =
                                  step === 'in_vineyard' ? STEP_GROUP_BG[3] : step === 'in_winery' ? STEP_GROUP_BG[5] : '';
                                const colorKey =
                                  step === 'travel'
                                    ? 'travel'
                                    : step === 'in_vineyard'
                                      ? 'inVineyard'
                                      : step === 'in_winery'
                                        ? 'inWinery'
                                        : 'total';
                                return (['Jobs', 'Actual', 'Limit'] as const).map((sub) => (
                                  <th
                                    key={`${step}-${sub}`}
                                    style={bgFor(colorKey)}
                                    className={`whitespace-nowrap border-r border-zinc-200 bg-white px-2 py-1 text-center text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900 ${groupBg} ${step === 'total' && sub === 'Limit' ? 'border-r-0' : ''}`}
                                  >
                                    {sub}
                                  </th>
                                ));
                              })}
                            </tr>
                          </thead>
                          <tbody>
                            {timeLimitRowsForTable.map((r) => {
                              const travelVal = timeLimitRowTravelVal(r);
                              const isSelected = selectedTimeLimitRowId === r.id;
                              const wineryBg = wineryLimitRowBgClass(r.Winery);
                              const stats = timeLimitRowActualById.get(r.id)?.metrics;
                              const onPick = () => {
                                setSelectedTimeLimitRowId(r.id);
                                const toV = r.ToVineMins != null ? String(r.ToVineMins) : '';
                                const inV = r.InVineMins != null ? String(r.InVineMins) : '';
                                const toW = r.ToWineMins != null ? String(r.ToWineMins) : '';
                                const inW = r.InWineMins != null ? String(r.InWineMins) : '';
                                const travelStr = travelVal != null ? String(travelVal) : '';
                                const totalStr = r.TotalMins != null ? String(r.TotalMins) : '';
                                setMinsThresholds({
                                  '2': toV,
                                  '3': inV,
                                  '4': toW,
                                  '5': inW,
                                  travel: travelStr,
                                  in_vineyard: inV,
                                  in_winery: inW,
                                  total: totalStr,
                                });
                              };
                              return (
                                <tr
                                  key={r.id}
                                  role="button"
                                  tabIndex={0}
                                  onClick={onPick}
                                  onKeyDown={(e) => {
                                    if (e.key === 'Enter' || e.key === ' ') {
                                      e.preventDefault();
                                      onPick();
                                    }
                                  }}
                                  className={`border-b border-zinc-100 dark:border-zinc-800 cursor-pointer ${wineryBg} ${isSelected ? 'ring-2 ring-inset ring-sky-500 dark:ring-sky-400' : 'hover:brightness-[0.97] dark:hover:brightness-110'}`}
                                >
                                  <td className="min-w-[9rem] border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                                    {formatCell(r.Winery)}
                                  </td>
                                  <td
                                    colSpan={5}
                                    className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300"
                                  >
                                    {formatCell(r.vineyardgroup)}
                                  </td>
                                  <td style={bgFor('travel')} className="border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                                    {stats != null ? formatIntNz(stats.travel.jobs) : '0'}
                                  </td>
                                  <td
                                    style={bgFor('travel')}
                                    className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${
                                      stats?.travel.actual != null && isOverLimitWithRow(stats.travel.actual, r, 'travel')
                                        ? 'text-red-700 dark:text-red-300'
                                        : 'text-zinc-600 dark:text-zinc-400'
                                    }`}
                                  >
                                    {stats?.travel.actual != null ? formatIntNz(stats.travel.actual) : '—'}
                                  </td>
                                  <td style={bgFor('travel')} className="border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
                                    {travelVal != null ? formatIntNz(Number(travelVal)) : '—'}
                                  </td>
                                  <td style={bgFor('inVineyard')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>
                                    {stats != null ? formatIntNz(stats.inVineyard.jobs) : '0'}
                                  </td>
                                  <td
                                    style={bgFor('inVineyard')}
                                    className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${STEP_GROUP_BG[3]} ${
                                      stats?.inVineyard.actual != null && isOverLimitWithRow(stats.inVineyard.actual, r, 'in_vineyard')
                                        ? 'text-red-700 dark:text-red-300'
                                        : 'text-zinc-600 dark:text-zinc-400'
                                    }`}
                                  >
                                    {stats?.inVineyard.actual != null ? formatIntNz(stats.inVineyard.actual) : '—'}
                                  </td>
                                  <td style={bgFor('inVineyard')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>
                                    {r.InVineMins != null ? formatIntNz(Number(r.InVineMins)) : '—'}
                                  </td>
                                  <td style={bgFor('inWinery')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>
                                    {stats != null ? formatIntNz(stats.inWinery.jobs) : '0'}
                                  </td>
                                  <td
                                    style={bgFor('inWinery')}
                                    className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${STEP_GROUP_BG[5]} ${
                                      stats?.inWinery.actual != null && isOverLimitWithRow(stats.inWinery.actual, r, 'in_winery')
                                        ? 'text-red-700 dark:text-red-300'
                                        : 'text-zinc-600 dark:text-zinc-400'
                                    }`}
                                  >
                                    {stats?.inWinery.actual != null ? formatIntNz(stats.inWinery.actual) : '—'}
                                  </td>
                                  <td style={bgFor('inWinery')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>
                                    {r.InWineMins != null ? formatIntNz(Number(r.InWineMins)) : '—'}
                                  </td>
                                  <td style={bgFor('total')} className="border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                                    {stats != null ? formatIntNz(stats.total.jobs) : '0'}
                                  </td>
                                  <td
                                    style={bgFor('total')}
                                    className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums font-medium ${
                                      stats?.total.actual != null && isOverLimitWithRow(stats.total.actual, r, 'total')
                                        ? 'text-red-700 dark:text-red-300'
                                        : 'text-zinc-700 dark:text-zinc-300'
                                    }`}
                                  >
                                    {stats?.total.actual != null ? formatIntNz(stats.total.actual) : '—'}
                                  </td>
                                  <td style={bgFor('total')} className="px-2 py-1.5 text-right tabular-nums font-medium text-zinc-700 dark:text-zinc-300">
                                    {r.TotalMins != null ? formatIntNz(Number(r.TotalMins)) : '—'}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      ) : (
                        <table className="w-full min-w-[24rem] text-left text-xs">
                          <thead className={SUMMARY_THEAD_TH_ALIGNMENT}>
                            <tr className="border-b border-zinc-200 dark:border-zinc-700">
                              <th
                                rowSpan={2}
                                className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                              >
                                Winery
                              </th>
                              <th
                                rowSpan={2}
                                className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                              >
                                Vineyard group
                              </th>
                              <th
                                style={bgFor('travel')}
                                colSpan={3}
                                className="border-r border-zinc-200 px-2 py-1.5 text-center font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                              >
                                Travel
                              </th>
                              <th
                                style={bgFor('inVineyard')}
                                colSpan={3}
                                className={`border-r border-zinc-200 px-2 py-1.5 text-center font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}
                              >
                                In Vineyard
                              </th>
                              <th
                                style={bgFor('inWinery')}
                                colSpan={3}
                                className={`border-r border-zinc-200 px-2 py-1.5 text-center font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}
                              >
                                In Winery
                              </th>
                              <th style={bgFor('total')} colSpan={3} className="px-2 py-1.5 text-center font-medium text-zinc-600 dark:text-zinc-400">
                                Total
                              </th>
                            </tr>
                            <tr className="border-b border-zinc-200 dark:border-zinc-700">
                              {(['travel', 'in_vineyard', 'in_winery', 'total'] as const).map((step) => {
                                const groupBg =
                                  step === 'in_vineyard' ? STEP_GROUP_BG[3] : step === 'in_winery' ? STEP_GROUP_BG[5] : '';
                                const colorKey =
                                  step === 'travel'
                                    ? 'travel'
                                    : step === 'in_vineyard'
                                      ? 'inVineyard'
                                      : step === 'in_winery'
                                        ? 'inWinery'
                                        : 'total';
                                return (['Jobs', 'Actual', 'Limit'] as const).map((sub) => (
                                  <th
                                    key={`${step}-${sub}`}
                                    style={bgFor(colorKey)}
                                    className={`whitespace-nowrap border-r border-zinc-200 bg-white px-2 py-1 text-center text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900 ${groupBg} ${step === 'total' && sub === 'Limit' ? 'border-r-0' : ''}`}
                                  >
                                    {sub}
                                  </th>
                                ));
                              })}
                            </tr>
                          </thead>
                          <tbody>
                            {timeLimitRowsForTable.map((r) => {
                              const travelVal = timeLimitRowTravelVal(r);
                              const isSelected = selectedTimeLimitRowId === r.id;
                              const wineryBg = wineryLimitRowBgClass(r.Winery);
                              const stats = timeLimitRowActualById.get(r.id)?.metrics;
                              return (
                                <tr
                                  key={r.id}
                                  role="button"
                                  tabIndex={0}
                                  onClick={() => {
                                    setSelectedTimeLimitRowId(r.id);
                                    const toV = r.ToVineMins != null ? String(r.ToVineMins) : '';
                                    const inV = r.InVineMins != null ? String(r.InVineMins) : '';
                                    const toW = r.ToWineMins != null ? String(r.ToWineMins) : '';
                                    const inW = r.InWineMins != null ? String(r.InWineMins) : '';
                                    const travelStr = travelVal != null ? String(travelVal) : '';
                                    const totalStr = r.TotalMins != null ? String(r.TotalMins) : '';
                                    setMinsThresholds({
                                      '2': toV, '3': inV, '4': toW, '5': inW,
                                      travel: travelStr, in_vineyard: inV, in_winery: inW, total: totalStr,
                                    });
                                  }}
                                  onKeyDown={(e) => {
                                    if (e.key === 'Enter' || e.key === ' ') {
                                      e.preventDefault();
                                      (e.target as HTMLTableRowElement).click();
                                    }
                                  }}
                                  className={`border-b border-zinc-100 dark:border-zinc-800 cursor-pointer ${wineryBg} ${isSelected ? 'ring-2 ring-inset ring-sky-500 dark:ring-sky-400' : 'hover:brightness-[0.97] dark:hover:brightness-110'}`}
                                >
                                  <td className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">{formatCell(r.Winery)}</td>
                                  <td className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">{formatCell(r.vineyardgroup)}</td>
                                  <td style={bgFor('travel')} className="border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                                    {stats != null ? formatIntNz(stats.travel.jobs) : '0'}
                                  </td>
                                  <td
                                    style={bgFor('travel')}
                                    className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${
                                      stats?.travel.actual != null && isOverLimitWithRow(stats.travel.actual, r, 'travel')
                                        ? 'text-red-700 dark:text-red-300'
                                        : 'text-zinc-600 dark:text-zinc-400'
                                    }`}
                                  >
                                    {stats?.travel.actual != null ? formatIntNz(stats.travel.actual) : '—'}
                                  </td>
                                  <td style={bgFor('travel')} className="border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
                                    {travelVal != null ? formatIntNz(Number(travelVal)) : '—'}
                                  </td>
                                  <td style={bgFor('inVineyard')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>
                                    {stats != null ? formatIntNz(stats.inVineyard.jobs) : '0'}
                                  </td>
                                  <td
                                    style={bgFor('inVineyard')}
                                    className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${STEP_GROUP_BG[3]} ${
                                      stats?.inVineyard.actual != null && isOverLimitWithRow(stats.inVineyard.actual, r, 'in_vineyard')
                                        ? 'text-red-700 dark:text-red-300'
                                        : 'text-zinc-600 dark:text-zinc-400'
                                    }`}
                                  >
                                    {stats?.inVineyard.actual != null ? formatIntNz(stats.inVineyard.actual) : '—'}
                                  </td>
                                  <td style={bgFor('inVineyard')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>
                                    {r.InVineMins != null ? formatIntNz(Number(r.InVineMins)) : '—'}
                                  </td>
                                  <td style={bgFor('inWinery')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>
                                    {stats != null ? formatIntNz(stats.inWinery.jobs) : '0'}
                                  </td>
                                  <td
                                    style={bgFor('inWinery')}
                                    className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${STEP_GROUP_BG[5]} ${
                                      stats?.inWinery.actual != null && isOverLimitWithRow(stats.inWinery.actual, r, 'in_winery')
                                        ? 'text-red-700 dark:text-red-300'
                                        : 'text-zinc-600 dark:text-zinc-400'
                                    }`}
                                  >
                                    {stats?.inWinery.actual != null ? formatIntNz(stats.inWinery.actual) : '—'}
                                  </td>
                                  <td style={bgFor('inWinery')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>
                                    {r.InWineMins != null ? formatIntNz(Number(r.InWineMins)) : '—'}
                                  </td>
                                  <td style={bgFor('total')} className="border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                                    {stats != null ? formatIntNz(stats.total.jobs) : '0'}
                                  </td>
                                  <td
                                    style={bgFor('total')}
                                    className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums font-medium ${
                                      stats?.total.actual != null && isOverLimitWithRow(stats.total.actual, r, 'total')
                                        ? 'text-red-700 dark:text-red-300'
                                        : 'text-zinc-700 dark:text-zinc-300'
                                    }`}
                                  >
                                    {stats?.total.actual != null ? formatIntNz(stats.total.actual) : '—'}
                                  </td>
                                  <td style={bgFor('total')} className="px-2 py-1.5 text-right tabular-nums font-medium text-zinc-700 dark:text-zinc-300">
                                    {r.TotalMins != null ? formatIntNz(Number(r.TotalMins)) : '—'}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      )
                    ) : (
                      <table className="w-full min-w-[64rem] text-left text-xs">
                        <thead className={SUMMARY_THEAD_TH_ALIGNMENT}>
                          <tr className="border-b border-zinc-200 dark:border-zinc-700">
                            <th
                              rowSpan={2}
                              className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                            >
                              Template
                            </th>
                            <th
                              rowSpan={2}
                              className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                            >
                              Winery
                            </th>
                            <th
                              rowSpan={2}
                              className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                            >
                              Vineyard group
                            </th>
                            <th
                              rowSpan={2}
                              className="border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                            >
                              TT
                            </th>
                            <th
                              colSpan={3}
                              style={bgFor('step2')}
                              className={`border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[2]}`}
                            >
                              To Vine
                            </th>
                            <th
                              colSpan={3}
                              style={bgFor('step3')}
                              className={`border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}
                            >
                              In Vine
                            </th>
                            <th
                              colSpan={3}
                              style={bgFor('step4')}
                              className={`border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[4]}`}
                            >
                              To Wine
                            </th>
                            <th
                              colSpan={3}
                              style={bgFor('step5')}
                              className={`border-r border-zinc-200 px-2 py-1.5 font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}
                            >
                              In Wine
                            </th>
                            <th
                              style={bgFor('travel')}
                              colSpan={3}
                              className="border-r border-zinc-200 px-2 py-1.5 text-center font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                            >
                              Travel
                            </th>
                            <th
                              style={bgFor('inVineyard')}
                              colSpan={3}
                              className={`border-r border-zinc-200 px-2 py-1.5 text-center font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}
                            >
                              In Vineyard
                            </th>
                            <th
                              style={bgFor('inWinery')}
                              colSpan={3}
                              className={`border-r border-zinc-200 px-2 py-1.5 text-center font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}
                            >
                              In Winery
                            </th>
                            <th style={bgFor('total')} colSpan={3} className="px-2 py-1.5 text-center font-medium text-zinc-600 dark:text-zinc-400">
                              Total
                            </th>
                          </tr>
                          <tr className="border-b border-zinc-200 dark:border-zinc-700">
                            {(['step2', 'step3', 'step4', 'step5', 'travel', 'in_vineyard', 'in_winery', 'total'] as const).map((step) => {
                              const groupBg =
                                step === 'step2'
                                  ? STEP_GROUP_BG[2]
                                  : step === 'step3' || step === 'in_vineyard'
                                    ? STEP_GROUP_BG[3]
                                    : step === 'step4'
                                      ? STEP_GROUP_BG[4]
                                      : step === 'step5' || step === 'in_winery'
                                        ? STEP_GROUP_BG[5]
                                        : '';
                              const colorKey =
                                step === 'step2'
                                  ? 'step2'
                                  : step === 'step3'
                                    ? 'step3'
                                    : step === 'step4'
                                      ? 'step4'
                                      : step === 'step5'
                                        ? 'step5'
                                        : step === 'travel'
                                  ? 'travel'
                                  : step === 'in_vineyard'
                                    ? 'inVineyard'
                                    : step === 'in_winery'
                                      ? 'inWinery'
                                      : 'total';
                                return (['Jobs', 'Actual', 'Limit'] as const).map((sub) => (
                                <th
                                  key={`admin-${step}-${sub}`}
                                  style={bgFor(colorKey)}
                                    className={`whitespace-nowrap border-r border-zinc-200 bg-white px-2 py-1 text-center text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900 ${groupBg} ${step === 'total' && sub === 'Limit' ? 'border-r-0' : ''}`}
                                >
                                  {sub}
                                </th>
                              ));
                            })}
                          </tr>
                        </thead>
                        <tbody>
                          {timeLimitRowsForTable.map((r) => {
                            const travelVal = timeLimitRowTravelVal(r);
                            const isSelected = selectedTimeLimitRowId === r.id;
                            const wineryBg = wineryLimitRowBgClass(r.Winery);
                              const stats = timeLimitRowActualById.get(r.id)?.metrics;
                            return (
                              <tr
                                key={r.id}
                                role="button"
                                tabIndex={0}
                                onClick={() => {
                                  setSelectedTimeLimitRowId(r.id);
                                  const toV = r.ToVineMins != null ? String(r.ToVineMins) : '';
                                  const inV = r.InVineMins != null ? String(r.InVineMins) : '';
                                  const toW = r.ToWineMins != null ? String(r.ToWineMins) : '';
                                  const inW = r.InWineMins != null ? String(r.InWineMins) : '';
                                  const travelStr = travelVal != null ? String(travelVal) : '';
                                  const totalStr = r.TotalMins != null ? String(r.TotalMins) : '';
                                  setMinsThresholds({
                                    '2': toV, '3': inV, '4': toW, '5': inW,
                                    travel: travelStr, in_vineyard: inV, in_winery: inW, total: totalStr,
                                  });
                                }}
                                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); (e.target as HTMLTableRowElement).click(); } }}
                                className={`border-b border-zinc-100 dark:border-zinc-800 cursor-pointer ${wineryBg} ${isSelected ? 'ring-2 ring-inset ring-sky-500 dark:ring-sky-400' : 'hover:brightness-[0.97] dark:hover:brightness-110'}`}
                              >
                                <td className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">{formatCell(r.Template)}</td>
                                <td className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">{formatCell(r.Winery)}</td>
                                <td className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">{formatCell(r.vineyardgroup)}</td>
                                <td className="border-r border-zinc-200 px-2 py-1.5 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">{formatCell(r.TT)}</td>
                                <td style={bgFor('step2')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[2]}`}>{stats != null ? formatIntNz(stats.toVine.jobs) : '0'}</td>
                                <td style={bgFor('step2')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${STEP_GROUP_BG[2]} ${stats?.toVine.actual != null && isOverLimitWithRow(stats.toVine.actual, r, '2') ? 'text-red-700 dark:text-red-300' : 'text-zinc-600 dark:text-zinc-400'}`}>{stats?.toVine.actual != null ? formatIntNz(stats.toVine.actual) : '—'}</td>
                                <td style={bgFor('step2')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[2]}`}>{r.ToVineMins != null ? formatIntNz(Number(r.ToVineMins)) : '—'}</td>
                                <td style={bgFor('step3')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>{stats != null ? formatIntNz(stats.inVine.jobs) : '0'}</td>
                                <td style={bgFor('step3')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${STEP_GROUP_BG[3]} ${stats?.inVine.actual != null && isOverLimitWithRow(stats.inVine.actual, r, '3') ? 'text-red-700 dark:text-red-300' : 'text-zinc-600 dark:text-zinc-400'}`}>{stats?.inVine.actual != null ? formatIntNz(stats.inVine.actual) : '—'}</td>
                                <td style={bgFor('step3')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>{r.InVineMins != null ? formatIntNz(Number(r.InVineMins)) : '—'}</td>
                                <td style={bgFor('step4')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[4]}`}>{stats != null ? formatIntNz(stats.toWine.jobs) : '0'}</td>
                                <td style={bgFor('step4')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${STEP_GROUP_BG[4]} ${stats?.toWine.actual != null && isOverLimitWithRow(stats.toWine.actual, r, '4') ? 'text-red-700 dark:text-red-300' : 'text-zinc-600 dark:text-zinc-400'}`}>{stats?.toWine.actual != null ? formatIntNz(stats.toWine.actual) : '—'}</td>
                                <td style={bgFor('step4')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[4]}`}>{r.ToWineMins != null ? formatIntNz(Number(r.ToWineMins)) : '—'}</td>
                                <td style={bgFor('step5')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>{stats != null ? formatIntNz(stats.inWine.jobs) : '0'}</td>
                                <td style={bgFor('step5')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${STEP_GROUP_BG[5]} ${stats?.inWine.actual != null && isOverLimitWithRow(stats.inWine.actual, r, '5') ? 'text-red-700 dark:text-red-300' : 'text-zinc-600 dark:text-zinc-400'}`}>{stats?.inWine.actual != null ? formatIntNz(stats.inWine.actual) : '—'}</td>
                                <td style={bgFor('step5')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>{r.InWineMins != null ? formatIntNz(Number(r.InWineMins)) : '—'}</td>
                                <td style={bgFor('travel')} className="border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">{stats != null ? formatIntNz(stats.travel.jobs) : '0'}</td>
                                <td style={bgFor('travel')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${stats?.travel.actual != null && isOverLimitWithRow(stats.travel.actual, r, 'travel') ? 'text-red-700 dark:text-red-300' : 'text-zinc-600 dark:text-zinc-400'}`}>{stats?.travel.actual != null ? formatIntNz(stats.travel.actual) : '—'}</td>
                                <td style={bgFor('travel')} className="border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">{travelVal != null ? formatIntNz(Number(travelVal)) : '—'}</td>
                                <td style={bgFor('inVineyard')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>{stats != null ? formatIntNz(stats.inVineyard.jobs) : '0'}</td>
                                <td style={bgFor('inVineyard')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${STEP_GROUP_BG[3]} ${stats?.inVineyard.actual != null && isOverLimitWithRow(stats.inVineyard.actual, r, 'in_vineyard') ? 'text-red-700 dark:text-red-300' : 'text-zinc-600 dark:text-zinc-400'}`}>{stats?.inVineyard.actual != null ? formatIntNz(stats.inVineyard.actual) : '—'}</td>
                                <td style={bgFor('inVineyard')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[3]}`}>{r.InVineMins != null ? formatIntNz(Number(r.InVineMins)) : '—'}</td>
                                <td style={bgFor('inWinery')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>{stats != null ? formatIntNz(stats.inWinery.jobs) : '0'}</td>
                                <td style={bgFor('inWinery')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums dark:border-zinc-700 ${STEP_GROUP_BG[5]} ${stats?.inWinery.actual != null && isOverLimitWithRow(stats.inWinery.actual, r, 'in_winery') ? 'text-red-700 dark:text-red-300' : 'text-zinc-600 dark:text-zinc-400'}`}>{stats?.inWinery.actual != null ? formatIntNz(stats.inWinery.actual) : '—'}</td>
                                <td style={bgFor('inWinery')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${STEP_GROUP_BG[5]}`}>{r.InWineMins != null ? formatIntNz(Number(r.InWineMins)) : '—'}</td>
                                <td style={bgFor('total')} className="border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">{stats != null ? formatIntNz(stats.total.jobs) : '0'}</td>
                                <td style={bgFor('total')} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums font-medium ${stats?.total.actual != null && isOverLimitWithRow(stats.total.actual, r, 'total') ? 'text-red-700 dark:text-red-300' : 'text-zinc-700 dark:text-zinc-300'}`}>{stats?.total.actual != null ? formatIntNz(stats.total.actual) : '—'}</td>
                                <td style={bgFor('total')} className="px-2 py-1.5 text-right tabular-nums font-medium text-zinc-700 dark:text-zinc-300">{r.TotalMins != null ? formatIntNz(Number(r.TotalMins)) : '—'}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    )}
                  </div>
                </>
              ) : null}
            </div>
          )}
          {canLoadJobs && loading && (
            <p className="mb-3 text-zinc-600 dark:text-zinc-400">Loading jobs…</p>
          )}
          {!canLoadJobs && (
            <p className="mb-4 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-zinc-800 dark:border-amber-900/40 dark:bg-amber-950/25 dark:text-zinc-200">
              Select a <strong className="font-semibold">customer</strong> in the sidebar and a <strong className="font-semibold">template</strong> to load jobs and rollups. Winery, vineyard, date and other filters apply after data has loaded.
            </p>
          )}
          {canLoadJobs && !loading && (
          <>
          <div className="mb-4 flex flex-wrap items-end gap-4 border-b border-zinc-200 dark:border-zinc-700">
            <div className="flex items-end gap-2">
              {summaryTab === 'season' ? (
                <div
                  className={`flex min-w-[20rem] max-w-[28rem] items-stretch rounded-t border border-zinc-300 border-b-0 bg-zinc-200 dark:border-zinc-600 dark:bg-zinc-700`}
                >
                  <button
                    type="button"
                    onClick={() => {
                      setFilterActualFrom('');
                      setFilterActualTo('');
                      setSummaryTab('season');
                    }}
                    className="shrink-0 px-3 py-2 text-sm font-medium text-zinc-900 dark:text-zinc-100"
                  >
                    Season Summary
                  </button>
                  <div
                    className="flex min-w-0 flex-1 items-center border-l border-zinc-300 px-2 py-1 dark:border-zinc-600"
                    onMouseDown={(e) => e.stopPropagation()}
                    onClick={(e) => e.stopPropagation()}
                  >
                    <SeasonRollupLayoutSelect
                      splitMode={splitMode}
                      setSplitMode={setSplitMode}
                      viewMode={viewMode}
                    />
                  </div>
                </div>
              ) : (
                <button
                  type="button"
                  onClick={() => {
                    setFilterActualFrom('');
                    setFilterActualTo('');
                    setSummaryTab('season');
                  }}
                  className="rounded-t px-4 py-2 text-sm font-medium bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750"
                >
                  Season Summary
                </button>
              )}
              <button
                type="button"
                onClick={() => {
                  const a = filterActualFrom.trim().slice(0, 10);
                  const b = filterActualTo.trim().slice(0, 10);
                  const singleDay = a.length > 0 && b.length > 0 && a === b;
                  if (singleDay) {
                    setFilterActualFrom('');
                    setFilterActualTo('');
                  }
                  setSummaryTab('by_day');
                }}
                className={`rounded-t px-4 py-2 text-sm font-medium ${summaryTab === 'by_day' ? 'bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100' : 'bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750'}`}
              >
                Daily Summary
              </button>
              <button
                type="button"
                onClick={() => setSummaryTab('by_job')}
                className={`rounded-t px-4 py-2 text-sm font-medium ${summaryTab === 'by_job' ? 'bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100' : 'bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750'}`}
              >
                By Job
              </button>
              {viewMode !== 'client' && (
                <button
                  type="button"
                  onClick={() => setSummaryTab('data_audit')}
                  className={`rounded-t px-4 py-2 text-sm font-medium ${summaryTab === 'data_audit' ? 'bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100' : 'bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750'}`}
                >
                  Data Audits
                </button>
              )}
              <button
                type="button"
                onClick={() => setSummaryTab('vineyards')}
                className={`rounded-t px-4 py-2 text-sm font-medium ${summaryTab === 'vineyards' ? 'bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100' : 'bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750'}`}
              >
                Vineyards
              </button>
              {viewMode !== 'client' && (
                <div className="relative" ref={summaryExportMenuRef}>
                  <button
                    type="button"
                    aria-label="Export summary data"
                    aria-expanded={summaryExportMenuOpen}
                    aria-haspopup="menu"
                    onClick={() => setSummaryExportMenuOpen((o) => !o)}
                    className={`rounded-t px-3 py-2 text-sm font-medium tabular-nums ${summaryExportMenuOpen ? 'bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100' : 'bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-150 dark:hover:bg-zinc-750'}`}
                  >
                    ⋯
                  </button>
                  {summaryExportMenuOpen && (
                    <div
                      role="menu"
                      className="absolute left-0 top-full z-50 mt-0.5 min-w-[13rem] rounded-md border border-zinc-200 bg-white py-1 shadow-lg dark:border-zinc-600 dark:bg-zinc-900"
                    >
                      <button
                        type="button"
                        role="menuitem"
                        className="block w-full px-3 py-2 text-left text-sm text-zinc-800 hover:bg-zinc-100 dark:text-zinc-200 dark:hover:bg-zinc-800"
                        onClick={() => {
                          setSummaryExportMenuOpen(false);
                          void runExportLimitsTableXlsx();
                        }}
                      >
                        Export Limits Table
                      </button>
                      <button
                        type="button"
                        role="menuitem"
                        className="block w-full px-3 py-2 text-left text-sm text-zinc-800 hover:bg-zinc-100 dark:text-zinc-200 dark:hover:bg-zinc-800"
                        onClick={() => {
                          setSummaryExportMenuOpen(false);
                          void runExportSeasonDataXlsx();
                        }}
                      >
                        Export Season Data
                      </button>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
          {summaryTab === 'season' && (
            <div className="max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
              <p className="mb-2 text-sm text-zinc-500">
                {seasonClientView
                  ? 'Season rollup for your account. Travel + In Vineyard + In Winery = Total mins.'
                  : 'Rollup of all jobs for the selected Customer and Template (same column sections as Daily Summary for audit). Rollup layout is in the Season Summary tab header.'}
              </p>
              <table
                className={`w-full text-left text-xs ${seasonClientView ? 'min-w-[24rem]' : 'min-w-[64rem]'} ${seasonClientView && splitSeasonGrouped ? 'table-fixed' : ''}`}
              >
                {seasonClientView && splitSeasonGrouped
                  ? splitByOverUnder
                    ? <SeasonClientSplitColgroupSeasonData />
                    : <SeasonClientSplitColgroupSeasonDataNoOU />
                  : null}
                <thead
                  className={`${SUMMARY_THEAD_TH_ALIGNMENT} sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]`}
                >
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    {!seasonClientView && (
                      <th
                        rowSpan={2}
                        className="border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400"
                      >
                        Season
                      </th>
                    )}
                    {splitSeasonGrouped && (
                      <>
                        <th
                          rowSpan={2}
                          className="min-w-[9rem] border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700"
                        >
                          Winery
                        </th>
                        <th
                          rowSpan={2}
                          className="min-w-[7rem] border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700"
                        >
                          Vineyard Group
                        </th>
                        <th
                          rowSpan={2}
                          className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700"
                        >
                          TT
                        </th>
                        {splitByOverUnder && (
                          <th
                            rowSpan={2}
                            className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700"
                          >
                            Over/Under
                          </th>
                        )}
                      </>
                    )}
                    <th
                      rowSpan={2}
                      className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700"
                    >
                      Jobs
                    </th>
                    <th
                      rowSpan={2}
                      className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700"
                    >
                      Vyards
                    </th>
                    <th
                      rowSpan={2}
                      className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700"
                    >
                      KMs
                    </th>
                    {!seasonClientView && (
                      <>
                        <th style={bgFor('step2')} colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[2]}`}>2. Travel To Vineyard</th>
                        <th style={bgFor('step3')} colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[3]}`}>3. Time In Vineyard</th>
                        <th style={bgFor('step4')} colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[4]}`}>4. Travel To Winery</th>
                        <th style={bgFor('step5')} colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[5]}`}>5. Unloading Winery</th>
                      </>
                    )}
                    <th style={bgFor('travel')} colSpan={seasonClientView ? 2 : 4} className="border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Travel</th>
                    <th style={bgFor('inVineyard')} colSpan={seasonClientView ? 2 : 4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[3]}`}>In Vineyard</th>
                    <th style={bgFor('inWinery')} colSpan={seasonClientView ? 2 : 4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[5]}`}>In Winery</th>
                    <th style={bgFor('total')} colSpan={seasonClientView ? 2 : 4} className="border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Total</th>
                  </tr>
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    {(!seasonClientView ? ([2, 3, 4, 5, 'travel', 'in_vineyard', 'in_winery', 'total'] as const) : (['travel', 'in_vineyard', 'in_winery', 'total'] as const)).map((step, stepIdx) => {
                      const groupBg = !seasonClientView && stepIdx < 4 ? STEP_GROUP_BG[(stepIdx + 2) as 2 | 3 | 4 | 5] : (step === 'in_vineyard' ? STEP_GROUP_BG[3] : step === 'in_winery' ? STEP_GROUP_BG[5] : '');
                      return (seasonClientView ? (['Total', 'Av'] as const) : (['Total', 'Max', 'Min', 'Av'] as const)).map((sub) => (
                        <th
                          key={`${step}-${sub}`}
                          className={`whitespace-nowrap border-r border-zinc-200 bg-white px-2 py-1 text-center text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900 ${groupBg} ${step === 'total' && sub === 'Av' ? 'border-r-0' : ''}`}
                        >
                          {sub}
                        </th>
                      ));
                    })}
                  </tr>
                </thead>
                <tbody>
                  {!seasonRollupRows || seasonRollupRows.length === 0 ? (
                    <tr>
                      <td
                        colSpan={
                          (seasonClientView
                            ? splitSeasonGrouped
                              ? splitByOverUnder
                                ? 13
                                : 12
                              : 9
                            : splitSeasonGrouped
                              ? splitByOverUnder
                                ? 35
                                : 34
                              : 34) +
                          (!seasonClientView && splitSeasonGrouped ? (splitByOverUnder ? 4 : 3) : 0) +
                          2
                        }
                        className="px-3 py-6 text-center text-zinc-500"
                      >
                        {effectiveCustomer || filterTemplate
                          ? 'No jobs for the selected Customer and Template.'
                          : 'Select Customer (and optionally Template) above to see season rollup.'}
                      </td>
                    </tr>
                  ) : (
                    seasonRollupRows.map((rollup) => {
                      const isSubTotal = splitSeasonGrouped && splitByOverUnder && rollup.tier === 'over_within';
                      const isTtBlockTotal = splitSeasonGrouped && rollup.tier === 'tt_total';
                      const isVgTotal = splitSeasonGrouped && rollup.tier === 'vg_total';
                      const isWineryTotal = splitSeasonGrouped && rollup.tier === 'winery_total';
                      const isSeasonTotal = rollup.tier === 'season_total';
                      const seasonRowClass = [
                        'border-b border-zinc-100 dark:border-zinc-800',
                        isSubTotal
                          ? 'bg-zinc-100 dark:bg-zinc-800/50'
                          : isWineryTotal
                            ? 'border-t border-zinc-300 bg-zinc-100 font-semibold dark:border-zinc-600 dark:bg-zinc-800/50'
                            : isSeasonTotal
                              ? 'border-t-2 border-zinc-300 dark:border-zinc-600 bg-white dark:bg-zinc-900 font-semibold'
                              : isVgTotal
                                ? 'border-t border-zinc-200 bg-zinc-50 font-medium dark:border-zinc-700 dark:bg-zinc-800/40'
                                : isTtBlockTotal
                                  ? 'bg-zinc-50/50 dark:bg-zinc-800/30'
                                  : 'bg-zinc-50/50 dark:bg-zinc-800/30',
                      ].filter(Boolean).join(' ');
                      const totalRowCell = isSeasonTotal ? 'bg-white dark:bg-zinc-900' : '';
                      return (
                    <tr key={rollup.rowKey} className={seasonRowClass}>
                      {!seasonClientView && (
                        <td className={`whitespace-nowrap border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${totalRowCell}`}>
                          {effectiveCustomer && filterTemplate ? `${effectiveCustomer} / ${filterTemplate}` : effectiveCustomer || 'All'}
                        </td>
                      )}
                      {splitSeasonGrouped && (
                        <>
                          <td className={`min-w-[9rem] whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-800 dark:border-zinc-700 dark:text-zinc-200 ${totalRowCell}`}>{rollup.wineryCol || '\u00A0'}</td>
                          <td className={`min-w-[7rem] whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-700 dark:border-zinc-700 dark:text-zinc-300 ${totalRowCell}`}>{rollup.vineyardGroupCol || '\u00A0'}</td>
                          <td className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${totalRowCell}`}>{rollup.ttCol || '\u00A0'}</td>
                          {splitByOverUnder && (
                            <td className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400 ${totalRowCell}`}>{rollup.limitLabel}</td>
                          )}
                        </>
                      )}
                      <td className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${totalRowCell}`}>
                        {formatIntNz(rollup.jobs.length)}
                      </td>
                      <td className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${totalRowCell}`}>
                        {formatIntNz(uniqueVineyardCount(rollup.jobs))}
                      </td>
                      <td className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400 ${totalRowCell}`}>
                        {formatIntNz(roundTripKms(rollup.jobs))}
                      </td>
                      {!seasonClientView && [rollup.mins_2, rollup.mins_3, rollup.mins_4, rollup.mins_5].map((q, i) =>
                        (['total', 'max', 'min', 'av'] as const).map((key) => {
                          const stepNum = i + 2;
                          const styleKey = stepNum === 2 ? 'step2' : stepNum === 3 ? 'step3' : stepNum === 4 ? 'step4' : 'step5';
                          const redClass = key === 'av' ? redIfOverStepCol(q[key], stepNum) : '';
                          return (
                            <td
                              key={`${i}-${key}`}
                              style={isSeasonTotal ? undefined : bgFor(styleKey)}
                              className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums ${isSeasonTotal ? totalRowCell : redClass ? redClass : `${STEP_GROUP_BG[(i + 2) as 2 | 3 | 4 | 5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}
                            >
                              {fmtNz(q[key])}
                            </td>
                          );
                        })
                      )}
                      {(seasonClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                        const redClass = key === 'av' ? redIfOver(rollup.travel[key], 'travel') : '';
                        return (
                          <td
                            key={`travel-${key}`}
                            style={isSeasonTotal ? undefined : bgFor('travel')}
                            className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums ${isSeasonTotal ? totalRowCell : redClass || 'text-zinc-600 dark:border-zinc-700 dark:text-zinc-400'}`}
                          >
                            {fmtNz(rollup.travel[key])}
                          </td>
                        );
                      })}
                      {(seasonClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                        const redClass = key === 'av' ? redIfOver(rollup.mins_3[key], 'in_vineyard') : '';
                        return (
                          <td
                            key={`in_vineyard-${key}`}
                            style={isSeasonTotal ? undefined : bgFor('inVineyard')}
                            className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums ${isSeasonTotal ? totalRowCell : redClass ? redClass : `${STEP_GROUP_BG[3]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}
                          >
                            {fmtNz(rollup.mins_3[key])}
                          </td>
                        );
                      })}
                      {(seasonClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                        const redClass = key === 'av' ? redIfOver(rollup.mins_5[key], 'in_winery') : '';
                        return (
                          <td
                            key={`in_winery-${key}`}
                            style={isSeasonTotal ? undefined : bgFor('inWinery')}
                            className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums ${isSeasonTotal ? totalRowCell : redClass ? redClass : `${STEP_GROUP_BG[5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}
                          >
                            {fmtNz(rollup.mins_5[key])}
                          </td>
                        );
                      })}
                      {(seasonClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                        const redClass = key === 'av' ? redIfOver(rollup.total[key], 'total') : '';
                        return (
                          <td
                            key={`total-${key}`}
                            style={isSeasonTotal ? undefined : bgFor('total')}
                            className={`whitespace-nowrap border-zinc-200 px-2 py-2 text-right tabular-nums font-medium ${isSeasonTotal ? totalRowCell : redClass || 'text-zinc-700 dark:border-zinc-700 dark:text-zinc-300'}`}
                          >
                            {fmtNz(rollup.total[key])}
                          </td>
                        );
                      })}
                    </tr>
                    ); })
                  )}
                </tbody>
              </table>
              {(() => {
                /** Split: 3 columns (Over Allowance | Within Allowance | Season), full season — not per winery. Non-split: one column. */
                const chartSources =
                  splitByOverUnder && seasonSplitChartTriplet && seasonSplitChartTriplet.length === 3
                    ? seasonSplitChartTriplet.map((s) => ({
                        rowKey: s.rowKey,
                        rowType: s.limitLabel as 'Over Allowance' | 'Within Allowance' | 'Season Total',
                        jobs: s.jobs,
                        travel: s.travel,
                        mins_3: s.mins_3,
                        mins_5: s.mins_5,
                        total: s.total,
                      }))
                    : seasonRollup
                      ? [{ rowKey: 'season-chart-unsplit', rowType: 'Season Total' as const, ...seasonRollup }]
                      : [];
                if (chartSources.length === 0) return null;
                const multiCol = chartSources.length === 3;
                let sharedAvgBarMax = 1;
                for (const s of chartSources) {
                  const jobsT = s.jobs.filter((j) => jobTrailerChartBucket(j) === 'T');
                  const jobsTT = s.jobs.filter((j) => jobTrailerChartBucket(j) === 'TT');
                  const jobsTPlusTT = s.jobs.filter(jobIsTOrTT);
                  const avT = seasonChartAverageAvs(jobsT);
                  const avTT = seasonChartAverageAvs(jobsTT);
                  const avBoth = seasonChartAverageAvs(jobsTPlusTT);
                  sharedAvgBarMax = Math.max(
                    sharedAvgBarMax,
                    avT.travel,
                    avT.inVineyard,
                    avT.inWinery,
                    avT.total,
                    avTT.travel,
                    avTT.inVineyard,
                    avTT.inWinery,
                    avTT.total,
                    avBoth.travel,
                    avBoth.inVineyard,
                    avBoth.inWinery,
                    avBoth.total,
                  );
                }
                return (
                  <div className="mt-6 border-t border-zinc-200 px-4 py-6 dark:border-zinc-700">
                    <div
                      className={
                        multiCol
                          ? 'grid grid-cols-1 gap-10 md:grid-cols-3 md:gap-6'
                          : 'mx-auto flex max-w-sm flex-col'
                      }
                    >
                      {chartSources.map((source, colIdx) => (
                        <SeasonSummaryChartColumn
                          key={source.rowKey}
                          rowType={source.rowType}
                          jobs={source.jobs}
                          travel={source.travel}
                          mins_3={source.mins_3}
                          mins_5={source.mins_5}
                          total={source.total}
                          sharedAvgBarMax={sharedAvgBarMax}
                          columnStripeIndex={multiCol ? colIdx : undefined}
                        />
                      ))}
                    </div>
                  </div>
                );
              })()}
            </div>
          )}
          {summaryTab === 'by_day' && (
            <div className="max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
              <table className="w-full min-w-[64rem] text-left text-xs">
                <thead
                  className={`${SUMMARY_THEAD_TH_ALIGNMENT} sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]`}
                >
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    <th className="border-r border-zinc-200 px-2 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-700">Limit (red if &gt;)</th>
                    {splitByOverUnder && <th className="border-r border-zinc-200 px-2 py-1 dark:border-zinc-700" />}
                    {(dayClientView ? [0, 1, 2] : [0, 1, 2, 3]).map((i) => (
                      <th key={`day-limit-pad-${i}`} className="border-r border-zinc-200 px-2 py-1 dark:border-zinc-700" />
                    ))}
                    {(dayClientView ? (['travel', 'in_vineyard', 'in_winery', 'total'] as const) : (['2', '3', '4', '5', 'travel', 'in_vineyard', 'in_winery', 'total'] as const)).map((key) =>
                      (dayClientView ? (['Total', 'Av'] as const) : (['Total', 'Max', 'Min', 'Av'] as const)).map((sub) => (
                        <th key={`limit-${key}-${sub}`} className="whitespace-nowrap border-r border-zinc-200 px-2 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-700">
                          {sub === 'Total' ? limitDisplay(key) : '\u00A0'}
                        </th>
                      ))
                    )}
                  </tr>
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Date</th>
                    {splitByOverUnder && (
                      <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700">Type</th>
                    )}
                    <th
                      rowSpan={2}
                      className="w-0 max-w-[4rem] min-w-0 border-r border-zinc-200 px-1.5 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700"
                    >
                      Jobs
                    </th>
                    <th
                      rowSpan={2}
                      className="w-0 max-w-[4rem] min-w-0 border-r border-zinc-200 px-1.5 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700"
                    >
                      Vyards
                    </th>
                    {!dayClientView && (
                      <th rowSpan={2} className="border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700">KMs</th>
                    )}
                    {!dayClientView && (
                      <>
                        <th style={bgFor('step2')} colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[2]}`}>2. Travel To Vineyard</th>
                        <th style={bgFor('step3')} colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[3]}`}>3. Time In Vineyard</th>
                        <th style={bgFor('step4')} colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[4]}`}>4. Travel To Winery</th>
                        <th style={bgFor('step5')} colSpan={4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[5]}`}>5. Unloading Winery</th>
                      </>
                    )}
                    <th style={bgFor('travel')} colSpan={dayClientView ? 2 : 4} className="border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Travel</th>
                    <th style={bgFor('inVineyard')} colSpan={dayClientView ? 2 : 4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[3]}`}>In Vineyard</th>
                    <th style={bgFor('inWinery')} colSpan={dayClientView ? 2 : 4} className={`border-r border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100 ${STEP_GROUP_BG[5]}`}>In Winery</th>
                    <th style={bgFor('total')} colSpan={dayClientView ? 2 : 4} className="border-zinc-200 px-2 py-1.5 text-xs font-medium text-zinc-900 dark:border-zinc-700 dark:text-zinc-100">Total</th>
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
                          className={`whitespace-nowrap border-r border-zinc-200 bg-white px-2 py-1 text-center text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900 ${groupBg} ${step === 'total' && sub === 'Av' ? 'border-r-0' : ''}`}
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
                      <td colSpan={(dayClientView ? 9 : 34) + (splitByOverUnder ? 1 : 0) + 2} className="px-3 py-6 text-center text-zinc-500">
                        No days {filteredRows.length === 0 ? '(no jobs match filters)' : '(no job dates in range)'}.
                      </td>
                    </tr>
                  ) : (
                    rowsByDay.map((r, idx) => {
                      const rowType = (r as { rowType?: string }).rowType;
                      const isDailyTotal = splitByOverUnder && rowType === 'Daily Total';
                      const isFirstRowOfDay =
                        !splitByOverUnder || idx === 0 || (rowsByDay[idx - 1] as { date?: string }).date !== r.date;
                      const dailyRowClass = [
                        'border-b border-zinc-100 dark:border-zinc-800',
                        splitByOverUnder && isFirstRowOfDay ? 'border-t-2 border-zinc-300 dark:border-zinc-600' : '',
                        splitByOverUnder && (rowType === 'Over Allowance' || rowType === 'Within Allowance')
                          ? 'bg-zinc-100 dark:bg-zinc-800/50'
                          : '',
                        isDailyTotal ? 'bg-white dark:bg-zinc-900 font-semibold' : '',
                        !splitByOverUnder
                          ? 'hover:bg-zinc-50 dark:hover:bg-zinc-800/50'
                          : rowType === 'Daily Total'
                            ? ''
                            : 'hover:bg-zinc-200/50 dark:hover:bg-zinc-700/50',
                      ].filter(Boolean).join(' ');
                      const dayTimeOutlier = isFirstRowOfDay
                        ? dayRollupOutlierSeverity(r as RollupQuads, lowMinutesHighlightThreshold, lowMinutesExclusionSet)
                        : null;
                      const dateCellFlag =
                        dayTimeOutlier === 'red'
                          ? SUMMARY_OUTLIER_RED
                          : dayTimeOutlier === 'yellow'
                            ? SUMMARY_OUTLIER_YELLOW
                            : '';
                      return (
                      <tr key={`${r.date}-${rowType ?? 'day'}`} className={dailyRowClass}>
                        <td
                          className={`whitespace-nowrap border-r border-zinc-200 px-3 py-2 dark:border-zinc-700${dateCellFlag ? ` ${dateCellFlag}` : ''}`}
                        >
                          {isFirstRowOfDay ? (
                            <button
                              type="button"
                              onClick={() => {
                                setFilterActualFrom(r.date);
                                setFilterActualTo(r.date);
                                setSummaryTab('by_job');
                              }}
                              className={
                                dateCellFlag
                                  ? 'font-medium text-inherit underline decoration-current hover:opacity-90'
                                  : 'font-medium text-blue-600 underline hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300'
                              }
                            >
                              {r.dateLabel}
                            </button>
                          ) : (
                            '\u00A0'
                          )}
                        </td>
                        {splitByOverUnder && (
                          <td className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">{rowType}</td>
                        )}
                        <td className="w-0 max-w-[4rem] min-w-0 whitespace-nowrap border-r border-zinc-200 px-1.5 py-2 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                          {formatIntNz(r.rollupJobCount)}
                        </td>
                        <td className="w-0 max-w-[4rem] min-w-0 whitespace-nowrap border-r border-zinc-200 px-1.5 py-2 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                          {formatIntNz(uniqueVineyardCount((r as RollupQuads).jobs ?? []))}
                        </td>
                        {!dayClientView && (
                          <td className="whitespace-nowrap border-r border-zinc-200 px-3 py-2 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">{formatIntNz(roundTripKms((r as RollupQuads).jobs ?? []))}</td>
                        )}
                        {!dayClientView && [r.mins_2, r.mins_3, r.mins_4, r.mins_5].map((q, i) =>
                          (['total', 'max', 'min', 'av'] as const).map((key) => {
                            const stepNum = i + 2;
                            const styleKey = stepNum === 2 ? 'step2' : stepNum === 3 ? 'step3' : stepNum === 4 ? 'step4' : 'step5';
                            const redClass = key === 'av' ? redIfOverStepCol(q[key], stepNum) : '';
                            return (
                              <td
                                key={`${i}-${key}`}
                                style={isDailyTotal ? undefined : bgFor(styleKey)}
                                className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums ${redClass ? redClass : `${STEP_GROUP_BG[(i + 2) as 2 | 3 | 4 | 5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}
                              >
                                {fmtNz(q[key])}
                              </td>
                            );
                          })
                        )}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                          const redClass = key === 'av' ? redIfOver(r.travel[key], 'travel') : '';
                          return (
                            <td
                              key={`travel-${key}`}
                              style={isDailyTotal ? undefined : bgFor('travel')}
                              className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums ${redClass || 'text-zinc-600 dark:border-zinc-700 dark:text-zinc-400'}`}
                            >
                              {fmtNz(r.travel[key])}
                            </td>
                          );
                        })}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                          const redClass = key === 'av' ? redIfOver(r.mins_3[key], 'in_vineyard') : '';
                          return (
                            <td
                              key={`in_vineyard-${key}`}
                              style={isDailyTotal ? undefined : bgFor('inVineyard')}
                              className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums ${redClass ? redClass : `${STEP_GROUP_BG[3]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}
                            >
                              {fmtNz(r.mins_3[key])}
                            </td>
                          );
                        })}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                          const redClass = key === 'av' ? redIfOver(r.mins_5[key], 'in_winery') : '';
                          return (
                            <td
                              key={`in_winery-${key}`}
                              style={isDailyTotal ? undefined : bgFor('inWinery')}
                              className={`whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums ${redClass ? redClass : `${STEP_GROUP_BG[5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}
                            >
                              {fmtNz(r.mins_5[key])}
                            </td>
                          );
                        })}
                        {(dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const)).map((key) => {
                          const redClass = key === 'av' ? redIfOver(r.total[key], 'total') : '';
                          return (
                            <td
                              key={`total-${key}`}
                              style={isDailyTotal ? undefined : bgFor('total')}
                              className={`whitespace-nowrap border-zinc-200 px-2 py-2 text-right tabular-nums font-medium ${redClass || 'text-zinc-700 dark:border-zinc-700 dark:text-zinc-300'}`}
                            >
                              {fmtNz(r.total[key])}
                            </td>
                          );
                        })}
                      </tr>
                    ); })
                  )}
                </tbody>
                {rowsByDay.length > 0 && (
                  <tfoot className="border-t-2 border-zinc-300 bg-zinc-100 dark:border-zinc-600 dark:bg-zinc-800">
                    {byDayFooterSets.map((set) => {
                      const s = set.stats;
                      const subKeys = dayClientView ? (['total', 'av'] as const) : (['total', 'max', 'min', 'av'] as const);
                      const footOutlier = footerStatsOutlierSeverity(s, set.jobCount, lowMinutesHighlightThreshold);
                      const footFlag =
                        footOutlier === 'red'
                          ? SUMMARY_OUTLIER_RED
                          : footOutlier === 'yellow'
                            ? SUMMARY_OUTLIER_YELLOW
                            : '';
                      return (
                        <tr key={set.rowType ?? 'all'} className="border-b border-zinc-200 dark:border-zinc-700">
                          <td
                            className={`border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400${footFlag ? ` ${footFlag}` : ''}`}
                          >
                            Total / Av
                          </td>
                          {splitByOverUnder && (
                            <td className="border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
                              {set.rowType}
                            </td>
                          )}
                          <td className="w-0 max-w-[4rem] min-w-0 border-r border-zinc-200 px-1.5 py-1.5 text-right tabular-nums text-xs text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                            {formatIntNz(set.jobCount)}
                          </td>
                          <td className="w-0 max-w-[4rem] min-w-0 border-r border-zinc-200 px-1.5 py-1.5 text-right tabular-nums text-xs text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                            {formatIntNz(set.vyardsTotal)}
                          </td>
                          {!dayClientView && (
                            <td className="border-r border-zinc-200 px-3 py-1.5 text-right tabular-nums text-xs text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                              {formatIntNz(set.kmsTotal)}
                            </td>
                          )}
                          {!dayClientView && [s.mins_2, s.mins_3, s.mins_4, s.mins_5].map((quad, i) =>
                            subKeys.map((key) => {
                              const val = key === 'total' ? quad.total : key === 'max' ? quad.max : key === 'min' ? quad.min : quad.av;
                              const redClass = key === 'av' && val != null ? redIfOverStepCol(val, i + 2) : '';
                              return (
                                <td key={`${i}-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-xs ${redClass ? redClass : `${STEP_GROUP_BG[(i + 2) as 2 | 3 | 4 | 5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                                  {val != null ? formatIntNz(val) : '—'}
                                </td>
                              );
                            })
                          )}
                          {subKeys.map((key) => {
                            const val = key === 'total' ? s.travel.total : key === 'max' ? s.travel.max : key === 'min' ? s.travel.min : s.travel.av;
                            const redClass = key === 'av' && val != null ? redIfOver(val, 'travel') : '';
                            return (
                              <td key={`travel-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-xs ${redClass || (val != null ? 'text-zinc-600 dark:border-zinc-700 dark:text-zinc-400' : 'text-zinc-600 dark:border-zinc-700 dark:text-zinc-400')}`}>
                                {val != null ? formatIntNz(val) : '—'}
                              </td>
                            );
                          })}
                          {subKeys.map((key) => {
                            const val = key === 'total' ? s.mins_3.total : key === 'max' ? s.mins_3.max : key === 'min' ? s.mins_3.min : s.mins_3.av;
                            const redClass = key === 'av' && val != null ? redIfOver(val, 'in_vineyard') : '';
                            return (
                              <td key={`in_vineyard-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-xs ${redClass ? redClass : `${STEP_GROUP_BG[3]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                                {val != null ? formatIntNz(val) : '—'}
                              </td>
                            );
                          })}
                          {subKeys.map((key) => {
                            const val = key === 'total' ? s.mins_5.total : key === 'max' ? s.mins_5.max : key === 'min' ? s.mins_5.min : s.mins_5.av;
                            const redClass = key === 'av' && val != null ? redIfOver(val, 'in_winery') : '';
                            return (
                              <td key={`in_winery-${key}`} className={`border-r border-zinc-200 px-2 py-1.5 text-right tabular-nums text-xs ${redClass ? redClass : `${STEP_GROUP_BG[5]} text-zinc-600 dark:text-zinc-400 dark:border-zinc-700`}`}>
                                {val != null ? formatIntNz(val) : '—'}
                              </td>
                            );
                          })}
                          {subKeys.map((key) => {
                            const val = key === 'total' ? s.total.total : key === 'max' ? s.total.max : key === 'min' ? s.total.min : s.total.av;
                            const redClass = key === 'av' && val != null ? redIfOver(val, 'total') : '';
                            return (
                              <td key={`total-${key}`} className={`border-zinc-200 px-2 py-1.5 text-right tabular-nums text-xs font-medium ${redClass || (val != null ? 'text-zinc-700 dark:border-zinc-700 dark:text-zinc-300' : 'text-zinc-700 dark:border-zinc-700 dark:text-zinc-300')}`}>
                                {val != null ? formatIntNz(val) : '—'}
                              </td>
                            );
                          })}
                        </tr>
                      );
                    })}
                  </tfoot>
                )}
              </table>
            </div>
          )}
          {summaryTab === 'by_job' && (
          <div ref={byJobTableScrollRef} className="w-full min-w-0">
            {(() => {
              const n = sortedRows.length;
              if (n <= jobsPageSize) return null;
              const start = n === 0 ? 0 : jobsPage * jobsPageSize + 1;
              const end = Math.min((jobsPage + 1) * jobsPageSize, n);
              return (
                <div className="mb-2 flex flex-wrap items-center justify-start gap-2 text-xs text-zinc-500">
                  <span className="whitespace-nowrap">
                    {formatIntNz(start)}–{formatIntNz(end)} of {formatIntNz(n)} jobs
                  </span>
                  <span className="flex items-center gap-2">
                    <label className="flex items-center gap-1">
                      <span className="text-[11px] text-zinc-500">Rows</span>
                      <select
                        value={jobsPageSize}
                        onChange={(e) => {
                          const v = parseInt(e.target.value, 10);
                          if (!Number.isFinite(v) || v <= 0) return;
                          setJobsPage(0);
                          setJobsPageSize(v);
                          saveJobsPageSize(v);
                        }}
                        className="h-7 rounded border border-zinc-300 bg-white px-2 text-xs dark:border-zinc-600 dark:bg-zinc-800"
                      >
                        {[100, 250, 500, 1000, 2000].map((v) => (
                          <option key={v} value={v}>
                            {v}
                          </option>
                        ))}
                      </select>
                    </label>
                    <button
                      type="button"
                      disabled={jobsPage === 0 || loading}
                      onClick={() => setJobsPage((p) => Math.max(0, p - 1))}
                      className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800"
                    >
                      Prev
                    </button>
                    <span className="text-[11px] tabular-nums">
                      Page {formatIntNz(jobsPage + 1)} of {formatIntNz(jobsTotalPages)}
                    </span>
                    <button
                      type="button"
                      disabled={jobsPage >= jobsTotalPages - 1 || loading}
                      onClick={() => setJobsPage((p) => p + 1)}
                      className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800"
                    >
                      Next
                    </button>
                  </span>
                </div>
              );
            })()}
            <table className="w-max min-w-[64rem] text-left text-xs">
              <thead
                className={`${SUMMARY_THEAD_TH_ALIGNMENT} sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]`}
              >
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  <th
                    // Top limit header row must account for Step 1 (Time/Via) columns in admin mode,
                    // otherwise the per-column limit inputs/stats shift left.
                    colSpan={BY_JOB_LEAD_COLUMNS.length + (jobClientView ? 1 : 2)}
                    className="border-r border-zinc-200 px-2 py-0.5 text-[11px] font-medium text-zinc-500 dark:border-zinc-700"
                    title={
                      timeLimitRows.length > 0
                        ? "Per job: red if value > limit from row matching Customer, Template, Winery, vineyard group (exact, blank only matches blank), and TT (or TTT fallback for T/TT). Boxes below are reference only."
                        : undefined
                    }
                  >
                    Limit (red if &gt;){timeLimitRows.length > 0 ? ' (per winery)' : ''}
                  </th>
                  {!jobClientView && (
                    <>
                      {[2, 3, 4, 5].map((n) => {
                        const key = `mins_${n}`;
                        const st = columnStats[key];
                        const groupBg = STEP_GROUP_BG[n];
                        const styleKey = n === 2 ? 'step2' : n === 3 ? 'step3' : n === 4 ? 'step4' : 'step5';
                        return (
                          <React.Fragment key={n}>
                            <th
                              style={bgFor(styleKey)}
                              colSpan={3}
                              className={`border-r border-zinc-200 px-2 py-0.5 dark:border-zinc-700 ${groupBg}`}
                            >
                              {st && st.count > 0 ? (
                                <span className="text-[10px] text-zinc-500 dark:text-zinc-400 tabular-nums">
                                  Σ {formatIntNz(st.sum)} · avg {st.avg != null ? formatIntNz(st.avg) : '—'}
                                </span>
                              ) : (
                                '\u00A0'
                              )}
                            </th>
                          </React.Fragment>
                        );
                      })}
                    </>
                  )}
                  <th style={bgFor('travel')} className="border-r border-zinc-200 px-1 py-0.5 dark:border-zinc-700">
                    {columnStats.travel?.count > 0 ? (
                      <span className="text-[10px] text-zinc-500 dark:text-zinc-400 tabular-nums">
                        Σ {formatIntNz(columnStats.travel.sum)} · avg {columnStats.travel.avg != null ? formatIntNz(columnStats.travel.avg) : '—'}
                      </span>
                    ) : (
                      '\u00A0'
                    )}
                  </th>
                  <th style={bgFor('inVineyard')} className="border-r border-zinc-200 px-1 py-0.5 dark:border-zinc-700">
                    {columnStats.in_vineyard?.count > 0 ? (
                      <span className="text-[10px] text-zinc-500 dark:text-zinc-400 tabular-nums">
                        Σ {formatIntNz(columnStats.in_vineyard.sum)} · avg {columnStats.in_vineyard.avg != null ? formatIntNz(columnStats.in_vineyard.avg) : '—'}
                      </span>
                    ) : (
                      '\u00A0'
                    )}
                  </th>
                  <th style={bgFor('inWinery')} className="border-r border-zinc-200 px-1 py-0.5 dark:border-zinc-700">
                    {columnStats.in_winery?.count > 0 ? (
                      <span className="text-[10px] text-zinc-500 dark:text-zinc-400 tabular-nums">
                        Σ {formatIntNz(columnStats.in_winery.sum)} · avg {columnStats.in_winery.avg != null ? formatIntNz(columnStats.in_winery.avg) : '—'}
                      </span>
                    ) : (
                      '\u00A0'
                    )}
                  </th>
                  <th style={bgFor('total')} className="border-zinc-200 px-1 py-0.5 dark:border-zinc-700">
                    {columnStats.total?.count > 0 ? (
                      <span className="text-[10px] text-zinc-500 dark:text-zinc-400 tabular-nums">
                        Σ {formatIntNz(columnStats.total.sum)} · avg {columnStats.total.avg != null ? formatIntNz(columnStats.total.avg) : '—'}
                      </span>
                    ) : (
                      '\u00A0'
                    )}
                  </th>
                </tr>
                <tr className="border-b border-zinc-200 dark:border-zinc-700">
                  {BY_JOB_LEAD_COLUMNS.map(({ key, label }) => {
                    const narrowVineyard = key === 'vineyard_name';
                    const narrowGroup = key === 'vineyard_group';
                    return (
                    <th
                      key={key}
                      rowSpan={jobClientView ? 1 : 2}
                      onClick={() => handleSort(key)}
                      className={`cursor-pointer select-none border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === key ? 'bg-zinc-200 dark:bg-zinc-700' : ''} ${
                        narrowVineyard
                          ? 'max-w-[7.5rem] w-[7.5rem] whitespace-normal break-words'
                          : narrowGroup
                            ? 'max-w-[4rem] w-[4rem] whitespace-normal break-words text-center leading-tight'
                            : 'whitespace-nowrap'
                      }`}
                      title={key === 'limits_breached' ? 'X = total time exceeds winery total limit; click to sort' : 'Click to sort'}
                    >
                      {label} {sortKey === key ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                    </th>
                    );
                  })}
                  {jobClientView && (
                    <th
                      onClick={() => handleSort('step_1_actual_time')}
                      className={`cursor-pointer select-none whitespace-nowrap border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === 'step_1_actual_time' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                    >
                      Start Time {sortKey === 'step_1_actual_time' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                    </th>
                  )}
                  {!jobClientView && (
                    <>
                      <th colSpan={2} className={`border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 dark:border-zinc-700 ${STEP_GROUP_BG[1]}`}>
                        {STEP_LABELS[1]}
                      </th>
                      {[2, 3, 4, 5].map((n) => (
                        <th
                          key={n}
                          colSpan={3}
                          style={bgFor(n === 2 ? 'step2' : n === 3 ? 'step3' : n === 4 ? 'step4' : 'step5')}
                          className={`border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 dark:border-zinc-700 ${STEP_GROUP_BG[n]}`}
                        >
                          {STEP_LABELS[n]}
                        </th>
                      ))}
                    </>
                  )}
                  <th
                    rowSpan={2}
                    onClick={() => handleSort('travel')}
                    style={bgFor('travel')}
                    className={`cursor-pointer select-none whitespace-nowrap border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === 'travel' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                  >
                    Travel {sortKey === 'travel' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th
                    rowSpan={2}
                    onClick={() => handleSort('in_vineyard')}
                    style={bgFor('inVineyard')}
                    className={`cursor-pointer select-none whitespace-nowrap border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === 'in_vineyard' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                  >
                    In Vineyard {sortKey === 'in_vineyard' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th
                    rowSpan={2}
                    onClick={() => handleSort('in_winery')}
                    style={bgFor('inWinery')}
                    className={`cursor-pointer select-none whitespace-nowrap border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-900 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-100 dark:hover:bg-zinc-700 ${sortKey === 'in_winery' ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                  >
                    In Winery {sortKey === 'in_winery' ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th
                    rowSpan={2}
                    onClick={() => handleSort('total')}
                    style={bgFor('total')}
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
                    const styleKey = n === 2 ? 'step2' : n === 3 ? 'step3' : n === 4 ? 'step4' : 'step5';
                    return (
                      <React.Fragment key={n}>
                        <th
                          onClick={() => handleSort(`mins_${n}`)}
                          style={bgFor(styleKey)}
                          className={`cursor-pointer select-none whitespace-nowrap border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-700 ${groupBg} ${sortKey === `mins_${n}` ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                        >
                        Mins {sortKey === `mins_${n}` ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                        </th>
                        <th
                          onClick={() => handleSort(`step_${n}_actual_time`)}
                          style={bgFor(styleKey)}
                          className={`cursor-pointer select-none whitespace-nowrap border-r border-zinc-200 px-3 py-1.5 text-xs font-medium text-zinc-600 hover:bg-zinc-200 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-700 ${groupBg} ${sortKey === `step_${n}_actual_time` ? 'bg-zinc-200 dark:bg-zinc-700' : ''}`}
                        >
                          Time {sortKey === `step_${n}_actual_time` ? (sortDir === 'asc' ? '↑' : '↓') : ''}
                        </th>
                        <th
                          onClick={() => handleSort(`step_${n}_via`)}
                          style={bgFor(styleKey)}
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
                    <td colSpan={jobClientView ? BY_JOB_LEAD_COLUMNS.length + 1 : allColumns.length} className="px-3 py-6 text-center text-zinc-500">
                      No rows {filterActualFrom || filterActualTo || effectiveCustomer || filterTemplate || filterTruckId || filterWorker.trim() || filterTrailermode ? '(try relaxing filters)' : ''}.
                    </td>
                  </tr>
                ) : (
                  sortedRowsPage.map((row, i) => {
                    const isHistoryFocusRow =
                      focusHighlightJobId != null &&
                      String(focusHighlightJobId).trim() !== '' &&
                      String(row.job_id ?? '').trim() === String(focusHighlightJobId).trim();
                    const historyPlainLeadHighlight = (k: string) =>
                      isHistoryFocusRow &&
                      (k === 'Customer' || k === 'job_id' || k === 'worker')
                        ? ' bg-sky-100 dark:bg-sky-900/45'
                        : '';
                    const jobTimeOutlier = timeOutlierSeverity(lowMinutesValuesForRow(row), lowMinutesHighlightThreshold);
                    const ghostExcluded = !includedInRollup(row);
                    return (
                    <tr
                      key={i}
                      data-summary-focus-job={
                        row.job_id != null && String(row.job_id).trim() !== ''
                          ? String(row.job_id).trim()
                          : undefined
                      }
                      className={`border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50${ghostExcluded ? ' opacity-55' : ''}`}
                      title={ghostExcluded ? 'Excluded from summaries (rollups); still visible here' : undefined}
                    >
                      {BY_JOB_LEAD_COLUMNS.map(({ key }) => {
                        const jobIdOutlierBg =
                          key === 'job_id'
                            ? jobTimeOutlier === 'red'
                              ? ` ${SUMMARY_OUTLIER_RED}`
                              : jobTimeOutlier === 'yellow'
                                ? ` ${SUMMARY_OUTLIER_YELLOW}`
                                : historyPlainLeadHighlight(key)
                            : historyPlainLeadHighlight(key);
                        const vineyardW =
                          key === 'vineyard_name'
                            ? ' max-w-[7.5rem] w-[7.5rem] whitespace-normal break-words'
                            : key === 'vineyard_group'
                              ? ' max-w-[4rem] w-[4rem] whitespace-normal break-words'
                              : key === 'distance'
                                ? ' max-w-[4.75rem] w-[4.75rem]'
                              : '';
                        const isDistance = key === 'distance';
                        return (
                        <td
                          key={key}
                          className={`${vineyardW || 'whitespace-nowrap'} px-2 py-1.5 dark:text-zinc-300${jobIdOutlierBg}${
                            key === 'limits_breached'
                              ? ' text-center font-medium tabular-nums ' + (row.limits_breached === 'X' ? 'text-red-700 dark:text-red-300' : 'text-emerald-700 dark:text-emerald-300')
                              : isDistance
                                ? ' text-right tabular-nums text-zinc-600'
                                : ' text-zinc-700'
                          }`}
                        >
                          {key === 'limits_breached' ? (
                            row.limits_breached ?? '-'
                          ) : isDistance ? (
                            (() => {
                              const km = distanceRoundTripKmFromRow(row);
                              return km == null ? '—' : formatKmNz2(km);
                            })()
                          ) : key === 'job_id' && viewMode !== 'client' ? (
                            <Link
                              href={inspectUrlForRow(row)}
                              target="_blank"
                              rel="noopener noreferrer"
                              onClick={(e) => {
                                if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return;
                                const meta = inspectJumpMetaForRow(row);
                                if (meta) registerPendingInspectForHistory(meta);
                              }}
                              className={
                                jobTimeOutlier
                                  ? 'font-medium text-inherit underline decoration-current hover:opacity-90'
                                  : 'text-blue-600 underline hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300'
                              }
                            >
                              {formatCell(row[key])}
                            </Link>
                          ) : key === 'Customer' ? (
                            formatCell(row['Customer'] ?? row['customer'])
                          ) : (
                            formatCell(row[key])
                          )}
                        </td>
                      );
                      })}
                      {jobClientView && (
                        <td className="whitespace-nowrap px-2 py-1.5 text-zinc-600 dark:text-zinc-400">
                          {formatDateDDMM(row.actual_start_time ?? row.step_1_actual_time ?? row.step_1_gps_completed_at ?? row.step_1_completed_at)}
                        </td>
                      )}
                      {!jobClientView && (() => {
                        const limitsRow = getMatchingLimitsRow(row);
                        return STEP_NUMS.map((n) => {
                          const actualKey = `step_${n}_actual_time`;
                          const viaKey = `step_${n}_via`;
                          const actualVal = row[actualKey] ?? row[`step_${n}_gps_completed_at`] ?? row[`step_${n}_completed_at`];
                          const viaVal = row[viaKey];
                          const viaStr = formatCell(viaVal);
                          const isGps = viaStr.toLowerCase() === 'gps';
                          const isGpsStar = viaStr === 'GPS*' || viaStr.toLowerCase() === 'gps*';
                          const viaLower = viaStr.toLowerCase();
                          const isVineFencePlus = viaLower === 'vinefence+';
                          const isVineFenceVPlus = viaLower === 'vinefencev+';
                          const isVineSr1 = viaLower === 'vinesr1';
                          const isOride = viaStr.toUpperCase() === 'ORIDE';
                          const mins = n >= 2 ? minsBetween(row, n - 1, n) : null;
                          const minsOver = n >= 2 && isOverLimitStepWithRow(mins, limitsRow, n);
                          // Step 1 cells should not have a filled background; Via should read as font-only (GPS/VW/etc).
                          const groupBg = n === 1 ? '' : STEP_GROUP_BG[n];
                          const styleKey = n === 2 ? 'step2' : n === 3 ? 'step3' : n === 4 ? 'step4' : n === 5 ? 'step5' : null;
                          const viaDisplay = isVineFencePlus ? 'GPS+' : isVineFenceVPlus ? 'GPS+V' : viaStr;
                          const viaGreen =
                            isGps || isVineFencePlus || isVineFenceVPlus || isGpsStar || isVineSr1;
                          return (
                            <React.Fragment key={n}>
                              {n >= 2 && (
                                <td
                                  style={styleKey ? bgFor(styleKey) : undefined}
                                  className={`w-10 whitespace-nowrap px-1 py-1.5 text-right tabular-nums ${minsOver ? 'text-red-700 dark:text-red-300' : `${groupBg} text-zinc-500 dark:text-zinc-400`}`}
                                >
                                  {mins != null ? formatIntNz(mins) : '—'}
                                </td>
                              )}
                            <td
                              style={styleKey ? bgFor(styleKey) : undefined}
                              className={`whitespace-nowrap px-2 py-1.5 text-zinc-600 dark:text-zinc-400 ${groupBg}`}
                            >
                              {formatDateDDMM(actualVal)}
                            </td>
                            <td
                              style={styleKey ? bgFor(styleKey) : undefined}
                              className={`whitespace-nowrap px-2 py-1.5 font-medium ${isOride ? 'text-red-700 dark:text-red-300' : viaGreen ? 'text-emerald-700 dark:text-emerald-300' : `${groupBg} text-zinc-700 dark:text-zinc-300`}`}
                            >
                              {viaDisplay}
                            </td>
                          </React.Fragment>
                          );
                        });
                      })()}
                      {(function travelSummaryCells() {
                        const limitsRow = getMatchingLimitsRow(row);
                        const travel = travelMins(row);
                        const inVineyard = minsBetween(row, 2, 3);
                        const inWinery = minsBetween(row, 4, 5);
                        const total = totalMins(row);
                        return (
                          <React.Fragment>
                            <td
                              style={bgFor('travel')}
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isOverLimitWithRow(travel, limitsRow, 'travel') ? 'text-red-700 dark:text-red-300' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {travel != null ? formatIntNz(travel) : '—'}
                            </td>
                            <td
                              style={bgFor('inVineyard')}
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isOverLimitWithRow(inVineyard, limitsRow, 'in_vineyard') ? 'text-red-700 dark:text-red-300' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {inVineyard != null ? formatIntNz(inVineyard) : '—'}
                            </td>
                            <td
                              style={bgFor('inWinery')}
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isOverLimitWithRow(inWinery, limitsRow, 'in_winery') ? 'text-red-700 dark:text-red-300' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {inWinery != null ? formatIntNz(inWinery) : '—'}
                            </td>
                            <td
                              style={bgFor('total')}
                              className={`whitespace-nowrap px-3 py-2 tabular-nums ${isOverLimitWithRow(total, limitsRow, 'total') ? 'text-red-700 dark:text-red-300' : 'text-zinc-500 dark:text-zinc-400'}`}
                            >
                              {total != null ? formatIntNz(total) : '—'}
                            </td>
                          </React.Fragment>
                        );
                      })()}
                    </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
          )}
          {summaryTab === 'data_audit' && (
            <div className="max-h-[78vh] overflow-auto rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
              <p className="mb-3 text-sm text-zinc-600 dark:text-zinc-400">
                Pivot job counts for the same filters as Season / Daily / By Job. Drag dimensions to match how you want to reconcile totals.
              </p>
              <SummaryDataAuditPivot jobs={filteredRows} loading={loading} />
            </div>
          )}
          {summaryTab === 'vineyards' && (
            <div className="max-h-[70vh] overflow-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
              <table className="w-full max-w-3xl text-left text-xs">
                <thead
                  className={`${SUMMARY_THEAD_TH_ALIGNMENT} sticky top-0 z-10 bg-zinc-100 shadow-[0_1px_0_0_rgba(0,0,0,0.1)] dark:bg-zinc-800 dark:shadow-[0_1px_0_0_rgba(255,255,255,0.08)]`}
                >
                  <tr className="border-b border-zinc-200 dark:border-zinc-700">
                    <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                      Vineyard
                    </th>
                    <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700">Jobs</th>
                    {viewMode !== 'client' && (
                      <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700">
                        Total mins
                      </th>
                    )}
                    <th className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-700">KMs</th>
                  </tr>
                </thead>
                <tbody>
                  {vineyardSummaryList.length === 0 ? (
                    <tr>
                      <td
                        colSpan={viewMode === 'client' ? 3 : 4}
                        className="px-3 py-6 text-center text-zinc-500 dark:text-zinc-400"
                      >
                        {filteredRows.length === 0
                          ? 'No jobs match the current filters.'
                          : 'No rollup jobs (all may be marked excluded).'}
                      </td>
                    </tr>
                  ) : (
                    vineyardSummaryList.map((v) => (
                      <tr key={v.vineyardLabel} className="border-b border-zinc-100 dark:border-zinc-800">
                        <td className="min-w-[9rem] whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-800 dark:border-zinc-700 dark:text-zinc-200">
                          {v.vineyardLabel}
                        </td>
                        <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
                          {formatIntNz(v.jobCount)}
                        </td>
                        {viewMode !== 'client' && (
                          <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
                            {formatIntNz(v.totalMinsSum)}
                          </td>
                        )}
                        <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right tabular-nums text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
                          {formatKmNz2(v.kmsRoundTrip)}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
                {vineyardSummaryList.length > 0 && (
                  <tfoot className="border-t-2 border-zinc-300 bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-800/80">
                    <tr>
                      <td className="border-r border-zinc-200 px-2 py-2 text-xs font-medium text-zinc-600 dark:border-zinc-700 dark:text-zinc-400">
                        Total
                      </td>
                      <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right text-xs font-medium tabular-nums text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                        {formatIntNz(vineyardSummaryList.reduce((s, v) => s + v.jobCount, 0))}
                      </td>
                      {viewMode !== 'client' && (
                        <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right text-xs font-medium tabular-nums text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                          {formatIntNz(vineyardSummaryList.reduce((s, v) => s + v.totalMinsSum, 0))}
                        </td>
                      )}
                      <td className="whitespace-nowrap border-r border-zinc-200 px-2 py-2 text-right text-xs font-medium tabular-nums text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
                        {formatKmNz2(vineyardSummaryList.reduce((s, v) => s + v.kmsRoundTrip, 0))}
                      </td>
                    </tr>
                  </tfoot>
                )}
              </table>
            </div>
          )}
          <div className="mt-3 flex flex-wrap items-center gap-3 text-zinc-500">
            <span>
              {summaryTab === 'by_job'
                ? (() => {
                    const n = sortedRows.length;
                    const start = n === 0 ? 0 : jobsPage * jobsPageSize + 1;
                    const end = Math.min((jobsPage + 1) * jobsPageSize, n);
                    return n > jobsPageSize
                      ? `${formatIntNz(start)}–${formatIntNz(end)} of ${formatIntNz(n)} jobs`
                      : `${formatIntNz(n)} row${n !== 1 ? 's' : ''}`;
                  })()
                : summaryTab === 'data_audit'
                  ? `Jobs (rollups): ${formatIntNz(rollupJobsCount)}${rollupJobsCount !== filteredRows.length ? ` · ${formatIntNz(filteredRows.length)} loaded` : ''}${filteredRows.length !== totalJobsFromApi ? ` (client filter ${formatIntNz(filteredRows.length)} vs API ${formatIntNz(totalJobsFromApi)})` : ''}`
                  : summaryTab === 'vineyards'
                    ? `${formatIntNz(vineyardSummaryList.length)} vineyard${vineyardSummaryList.length !== 1 ? 's' : ''} · Jobs (rollups): ${formatIntNz(rollupJobsCount)}${rollupJobsCount !== filteredRows.length ? ` · ${formatIntNz(filteredRows.length)} loaded` : ''}${filteredRows.length !== totalJobsFromApi ? ` (client filter ${formatIntNz(filteredRows.length)} vs API ${formatIntNz(totalJobsFromApi)})` : ''}`
                    : `Total harvest days: ${formatIntNz(summaryHarvestDayCount)} · Jobs (rollups): ${formatIntNz(rollupJobsCount)}${rollupJobsCount !== filteredRows.length ? ` · ${formatIntNz(filteredRows.length)} loaded` : ''}${filteredRows.length !== totalJobsFromApi ? ` (client filter ${formatIntNz(filteredRows.length)} vs API ${formatIntNz(totalJobsFromApi)})` : ''}`}
            </span>
            {summaryTab === 'by_job' && sortedRows.length > jobsPageSize && (
              <span className="flex items-center gap-2">
                <button
                  type="button"
                  disabled={jobsPage === 0 || loading}
                  onClick={() => setJobsPage((p) => Math.max(0, p - 1))}
                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800"
                >
                  Prev
                </button>
                <span className="text-xs">
                  Page {formatIntNz(jobsPage + 1)} of {formatIntNz(jobsTotalPages)}
                </span>
                <button
                  type="button"
                  disabled={jobsPage >= jobsTotalPages - 1 || loading}
                  onClick={() => setJobsPage((p) => p + 1)}
                  className="rounded border border-zinc-300 bg-white px-2 py-1 text-sm disabled:opacity-50 dark:border-zinc-600 dark:bg-zinc-800"
                >
                  Next
                </button>
              </span>
            )}
          </div>
          {jobsQueryDebug &&
            viewMode !== 'client' &&
            (summaryTab === 'season' || summaryTab === 'by_day' || summaryTab === 'data_audit' || summaryTab === 'vineyards') && (
            <details className="mt-4 rounded border border-zinc-200 bg-zinc-50 p-3 text-xs dark:border-zinc-600 dark:bg-zinc-900/50">
              <summary className="cursor-pointer font-medium text-zinc-700 dark:text-zinc-300">
                Debug: job count SQL (criteria for this summary)
              </summary>
              <pre className="mt-2 max-h-48 overflow-auto whitespace-pre-wrap break-all font-mono text-zinc-600 dark:text-zinc-400">
                {jobsQueryDebug.debugSqlLiteral}
              </pre>
              <p className="mt-2 font-mono text-[10px] text-zinc-500 dark:text-zinc-500">
                Params JSON: {JSON.stringify(jobsQueryDebug.debugSqlParams)}
              </p>
            </details>
          )}
          </>
          )}
        </>
      )}
    </div>
  );
}

export default function SummaryPage() {
  return (
    <Suspense
      fallback={
        <div className="flex min-h-[200px] items-center justify-center p-6 text-zinc-500 dark:text-zinc-400">
          Loading…
        </div>
      }
    >
      <SummaryPageInner />
    </Suspense>
  );
}

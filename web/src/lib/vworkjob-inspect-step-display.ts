/**
 * Build per-step VWork / GPS / Manual / Final values to match Query → Inspect “Step details”.
 * Final = manual (oride) if set, else `step_N_actual_time`, else VWork `step_N_completed_at` — same as DB after steps write-back.
 */

import { dateToLiteral } from '@/lib/utils';

export type InspectStyleStepRow = {
  n: number;
  vwork: string | null;
  gps: string | null;
  manual: string | null;
  final: string | null;
  via: string | null;
};

function col(row: Record<string, unknown>, logical: string): unknown {
  const want = logical.toLowerCase();
  for (const [k, v] of Object.entries(row)) {
    if (k.toLowerCase() === want) return v;
  }
  return undefined;
}

/** Normalize timestamp to YYYY-MM-DD HH:mm:ss for comparison (e.g. minutes-between). */
function normalizeForCompare(v: unknown): string | null {
  if (v == null || v === '') return null;
  const raw = cellToTimestampString(v);
  if (raw == null) return null;
  const s = raw.replace(/\s+(?:GMT|UTC)[+-]\d{3,4}.*$/i, '').trim();
  if (!s) return null;
  const dmy = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2}):(\d{2})/);
  if (dmy) {
    const yy = dmy[3].length === 2 ? `20${dmy[3]}` : dmy[3];
    return `${yy}-${dmy[2].padStart(2, '0')}-${dmy[1].padStart(2, '0')} ${dmy[4].padStart(2, '0')}:${dmy[5]}:${dmy[6]}`;
  }
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2})?:?(\d{2})?:?(\d{2})?/);
  if (iso) {
    const h = iso[4]?.padStart(2, '0') ?? '00';
    const m = iso[5]?.padStart(2, '0') ?? '00';
    const sec = iso[6]?.padStart(2, '0') ?? '00';
    return `${iso[1]}-${iso[2]}-${iso[3]} ${h}:${m}:${sec}`;
  }
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(s)) return s.slice(0, 19);
  return null;
}

/** pg returns `Date` for timestamps — never use `String(date)` (locale garbage). */
function cellToTimestampString(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === 'object' && v instanceof Date) {
    if (Number.isNaN(v.getTime())) return null;
    return dateToLiteral(v);
  }
  const s = String(v).trim();
  return s === '' ? null : s;
}

function asTrimmedSlice(v: unknown, maxLen: number): string | null {
  const s = cellToTimestampString(v);
  if (s == null) return null;
  return s.length > maxLen ? s.slice(0, maxLen) : s;
}

/** Same GPS column precedence as Inspect `readInspectStepGps`. */
function readStepGps(row: Record<string, unknown>, n: number): string | null {
  const snake = `step_${n}_gps_completed_at`;
  const v =
    row[`step_${n}_gps_completed_at`] ??
    row[`Step_${n}_GPS_completed_at`] ??
    row[`Step_${n}_GPS_Completed_At`] ??
    col(row, snake);
  if (v == null || v === '') return null;
  return asTrimmedSlice(v, 32);
}

/**
 * @param row — one `tbl_vworkjobs` row (`SELECT *` or API-shaped object)
 */
export function buildInspectStyleStepsFromJobRow(row: Record<string, unknown>): InspectStyleStepRow[] {
  const out: InspectStyleStepRow[] = [];
  for (let n = 1; n <= 5; n++) {
    const vworkRaw =
      col(row, `step_${n}_completed_at`) ??
      row[`Step_${n}_completed_at`] ??
      row[`Step_${n}_Completed_At`];
    const vworkStr = asTrimmedSlice(vworkRaw, 32);

    const gpsStr = readStepGps(row, n);

    const orideLogical = `step${n}oride`;
    const manualRaw = col(row, orideLogical) ?? row[`Step${n}oride`];
    const manualStr = asTrimmedSlice(manualRaw, 32);

    const actualRaw =
      col(row, `step_${n}_actual_time`) ?? row[`Step_${n}_actual_time`] ?? row[`Step_${n}_Actual_Time`];
    const actualStr = asTrimmedSlice(actualRaw, 32);

    const finalStr: string | null = manualStr ? manualStr : actualStr ?? vworkStr;

    const viaRaw = col(row, `step_${n}_via`) ?? row[`Step_${n}_Via`];
    const viaStr = viaRaw != null && String(viaRaw).trim() !== '' ? String(viaRaw).trim() : null;

    out.push({ n, vwork: vworkStr, gps: gpsStr, manual: manualStr, final: finalStr, via: viaStr });
  }
  return out;
}

/** Minutes from previous step Final to this step Final (Inspect-style; naive on YYYY-MM-DD HH:mm:ss). */
export function minutesBetweenFinalColumns(prevFinal: string | null, currFinal: string | null): number | null {
  const a = normalizeForCompare(prevFinal);
  const b = normalizeForCompare(currFinal);
  if (!a || !b) return null;
  const ma = a.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  const mb = b.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  if (!ma || !mb) return null;
  const msA = Date.UTC(Number(ma[1]), Number(ma[2]) - 1, Number(ma[3]), Number(ma[4]), Number(ma[5]), Number(ma[6]));
  const msB = Date.UTC(Number(mb[1]), Number(mb[2]) - 1, Number(mb[3]), Number(mb[4]), Number(mb[5]), Number(mb[6]));
  return Math.round((msB - msA) / 60000);
}

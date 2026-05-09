/** Format Date to YYYY-MM-DD HH:mm:ss — no toISOString. Uses local components. */
export function dateToLiteral(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

/** Serialize Date as YYYY-MM-DD HH:mm:ss using UTC components. Do NOT use for NZ-stored timestamps — use dateToLiteral or return strings via to_char in SQL. */
export function dateToLiteralUTC(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

/** Format date string to dd/mm/yy hh:mm:ss — pure formatting, no Date/timezone, just extract and reorder. */
export function formatDateNZ(s: string): string {
  const m = String(s).trim().match(/^(\d{4})-(\d{2})-(\d{2})[T\s]?(\d{2})?:?(\d{2})?:?(\d{2})?/);
  if (!m) return s;
  const [, y, mo, d, h = '00', min = '00', sec = '00'] = m;
  return `${d}/${mo}/${y.slice(-2)} ${h}:${min}:${sec}`;
}

/** Narrow display for step grids: `dd/mm hh:mm` from a DB/API timestamp string (pattern match only, no Date parsing). */
export function formatDateDdMmHhMm(s: string | null | undefined): string {
  if (s == null) return '—';
  const t = String(s).trim();
  if (t === '') return '—';
  // ISO: 2025-03-10T12:34:56 or ...Z or with fractional seconds
  let m = t.match(/^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?(?:Z|[+-]\d{2}(?::?\d{2})?)?$/i);
  if (m) {
    const [, , mo, d, h, min] = m;
    return `${d}/${mo} ${h}:${min}`;
  }
  // Space form from to_char / dateToLiteral
  m = t.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?/);
  if (m) {
    const [, , mo, d, h, min] = m;
    return `${d}/${mo} ${h}:${min}`;
  }
  // d/m/y ... (optional time)
  const dmy = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})[T\s]?(\d{1,2})?:?(\d{2})?:?(\d{2})?/);
  if (dmy) {
    const day = dmy[1].padStart(2, '0');
    const mo = dmy[2].padStart(2, '0');
    const h = (dmy[4] ?? '0').padStart(2, '0');
    const min = (dmy[5] ?? '00').padStart(2, '0');
    return `${day}/${mo} ${h}:${min}`;
  }
  return '—';
}

/** Format snake_case column name for display: actual_start_time → "Actual Start Time" */
export function formatColumnLabel(col: string): string {
  return col
    .split('_')
    .map((part) => (part ? part.charAt(0).toUpperCase() + part.slice(1).toLowerCase() : part))
    .join(' ');
}

let measureCanvas: HTMLCanvasElement | null = null;

function getMeasureContext(): CanvasRenderingContext2D | null {
  if (typeof document === 'undefined') return null;
  if (!measureCanvas) measureCanvas = document.createElement('canvas');
  return measureCanvas.getContext('2d');
}

/** Measure text width in px using table font (text-sm = 14px) */
function measureText(text: string): number {
  const ctx = getMeasureContext();
  if (!ctx) return 0;
  ctx.font = '14px ui-sans-serif, system-ui, sans-serif';
  const m = ctx.measureText(String(text ?? ''));
  return Math.ceil(m.width);
}

/** Compute column widths from header + cell content (max per column). Padding px-3 = 24px. */
export function computeColumnWidths(
  columns: string[],
  rows: Record<string, unknown>[],
  formatCell: (v: unknown) => string,
  formatColumnLabel: (c: string) => string,
  paddingPx = 28
): Record<string, number> {
  const out: Record<string, number> = {};
  for (const col of columns) {
    let max = measureText(formatColumnLabel(col));
    for (const row of rows) {
      const w = measureText(formatCell(row[col]));
      if (w > max) max = w;
    }
    out[col] = max + paddingPx;
  }
  return out;
}

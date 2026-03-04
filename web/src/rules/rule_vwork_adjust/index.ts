/**
 * rule_vwork_adjust: Add 1 hour to a datetime.
 *
 * Logic: 23:50 becomes 00:50 with day +1. Pure arithmetic, no timezone.
 * Use in UI and batch execution over DB rows.
 */

const DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] as const;

function isLeapYear(y: number): boolean {
  return (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0;
}

function lastDayOfMonth(y: number, mo: number): number {
  if (mo === 2) return isLeapYear(y) ? 29 : 28;
  return DAYS_IN_MONTH[mo - 1];
}

const pad = (n: number) => String(n).padStart(2, '0');

export type InputFormat = 'db' | 'display';

/**
 * Add 1 hour to datetime. No timezone conversion.
 *
 * @param value - Datetime string
 * @param format - 'db' = YYYY-MM-DD HH:mm:ss, 'display' = DD/MM/YY HH:mm:ss
 * @returns Adjusted datetime in same format, or null if parse fails
 *
 * Logic: 23:50 → 00:50, day +1 (with month/year rollover)
 */
export function addOneHour(
  value: string,
  format: InputFormat = 'db'
): string | null {
  const s = String(value ?? '').trim();
  if (!s) return null;

  let y: number, mo: number, d: number, h: number, min: number, sec: number;

  if (format === 'display') {
    // DD/MM/YY HH:mm:ss
    const m = s.match(/^(\d{2})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
    if (!m) return null;
    [, d, mo, y] = m.slice(1, 4).map(Number);
    [h, min, sec] = m.slice(4, 7).map(Number);
    y = 2000 + y;
  } else {
    // YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss (ignores trailing .000Z etc)
    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})/);
    if (!m) return null;
    [, y, mo, d, h, min, sec] = m.slice(1, 7).map(Number);
  }

  h += 1;
  if (h >= 24) {
    h -= 24;
    d += 1;
    const lastDay = lastDayOfMonth(y, mo);
    if (d > lastDay) {
      d = 1;
      mo += 1;
      if (mo > 12) {
        mo = 1;
        y += 1;
      }
    }
  }

  if (format === 'display') {
    const yy = y % 100;
    return `${pad(d)}/${pad(mo)}/${pad(yy)} ${pad(h)}:${pad(min)}:${pad(sec)}`;
  }
  return `${y}-${pad(mo)}-${pad(d)} ${pad(h)}:${pad(min)}:${pad(sec)}`;
}

/**
 * Date window helpers for entry/exit tagging.
 * Uses position_time_nz as stored (YYYY-MM-DD HH:mm:ss) — no timezone conversion.
 */

/** Parse YYYY-MM-DD and add days; return YYYY-MM-DD. */
function addDays(dateStr: string, days: number): string {
  const d = new Date(dateStr + 'T12:00:00Z');
  d.setUTCDate(d.getUTCDate() + days);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/** Format a Date to YYYY-MM-DD HH:mm:ss (UTC). */
function formatDateTime(d: Date): string {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  const h = String(d.getUTCHours()).padStart(2, '0');
  const min = String(d.getUTCMinutes()).padStart(2, '0');
  const s = String(d.getUTCSeconds()).padStart(2, '0');
  return `${y}-${m}-${day} ${h}:${min}:${s}`;
}

/**
 * Tagging range: from 00:00 on fromDate to midnight on toDate (i.e. end of toDate), plus buffer at each end.
 * So no valid ENTER/EXIT at the boundary can slip through. Returns [start, end) for SQL (position_time_nz >= start AND position_time_nz < end).
 * bufferHours: e.g. 1 = one hour before fromDate 00:00 and one hour after end of toDate.
 */
export function taggingRange(fromDate: string, toDate: string, bufferHours: number): { start: string; end: string } {
  const startDay = new Date(fromDate + 'T00:00:00Z');
  const endDayExclusive = new Date(addDays(toDate, 1) + 'T00:00:00Z');
  const start = new Date(startDay.getTime() - bufferHours * 60 * 60 * 1000);
  const end = new Date(endDayExclusive.getTime() + bufferHours * 60 * 60 * 1000);
  return { start: formatDateTime(start), end: formatDateTime(end) };
}

/** Write window: [dateX 00:00:00, dateX+1 00:00:00) — legacy single-day; prefer taggingRange. */
export function writeWindow(dateX: string): { start: string; end: string } {
  return {
    start: `${dateX} 00:00:00`,
    end: `${addDays(dateX, 1)} 00:00:00`,
  };
}

/** Scan window: [dateX-1 00:00:00, dateX+2 00:00:00) — legacy; prefer taggingRange. */
export function scanWindow(dateX: string): { start: string; end: string } {
  return {
    start: `${addDays(dateX, -1)} 00:00:00`,
    end: `${addDays(dateX, 2)} 00:00:00`,
  };
}

/** Return true if timestamp (position_time_nz) is in [start, end) (string comparison). */
export function inWindow(ts: string, start: string, end: string): boolean {
  return ts >= start && ts < end;
}

/** Parse position_time_nz to ms for duration checks (interpret as UTC). */
export function parseTimestampToMs(ts: string): number {
  const normalized = ts.trim().replace(' ', 'T') + 'Z';
  return new Date(normalized).getTime();
}

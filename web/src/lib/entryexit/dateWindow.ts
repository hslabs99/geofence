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

/** Write window: [dateX 00:00:00, dateX+1 00:00:00) — only rows in this range get ENTER/EXIT. */
export function writeWindow(dateX: string): { start: string; end: string } {
  return {
    start: `${dateX} 00:00:00`,
    end: `${addDays(dateX, 1)} 00:00:00`,
  };
}

/** Scan window: [dateX-1 00:00:00, dateX+2 00:00:00) — for state machine context. */
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

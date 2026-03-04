/**
 * ENTER/EXIT tagging: ENTER on first fence row, EXIT on last fence row.
 * - null → fence: ENTER on current row (first fence row) — only if row is in writeWindow.
 * - fence → null: EXIT on previous row (last fence row) — emit EXIT when stay duration >= graceSeconds
 *   so we always close the stay when the device leaves, even if the last fence row is just outside writeWindow
 *   (e.g. device exits at 00:34 and writeWindow ended at 00:00).
 * - fence → fence: EXIT on previous row (last of old fence), ENTER on current row (first of new fence).
 * Short-stay (noise) suppression: if the stay in a fence is shorter than graceSeconds, we do not tag
 * ENTER or EXIT for that bump (touch-and-go); the fence change did not persist.
 */

import { inWindow, parseTimestampToMs } from './dateWindow';

/** DB may return position_time_nz as Date or string; geofence_id as number or string. */
export interface ScanRow {
  id: number | bigint;
  position_time_nz: string | Date;
  geofence_id?: string | number | null;
}

export interface TagUpdate {
  id: number | bigint;
  geofence_type: 'ENTER' | 'EXIT';
}

function normalizeFenceId(raw: string | number | null | undefined): string | null {
  if (raw == null || raw === '') return null;
  const s = typeof raw === 'string' ? raw : String(raw);
  const t = s.trim();
  return t === '' ? null : t;
}

/** position_time_nz from DB may be Date or string; normalize to YYYY-MM-DD HH:mm:ss for window comparison. */
function normalizePositionTimeNz(raw: string | Date | null | undefined): string {
  if (raw == null) return '';
  if (typeof raw === 'string') return raw.trim();
  if (raw instanceof Date) {
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${raw.getUTCFullYear()}-${pad(raw.getUTCMonth() + 1)}-${pad(raw.getUTCDate())} ${pad(raw.getUTCHours())}:${pad(raw.getUTCMinutes())}:${pad(raw.getUTCSeconds())}`;
  }
  return String(raw);
}

function idEq(a: number | bigint | null, b: number | bigint | null): boolean {
  if (a == null || b == null) return false;
  return Number(a) === Number(b);
}

export function runStateMachine(
  rows: ScanRow[],
  writeStart: string,
  writeEnd: string,
  graceSeconds: number
): TagUpdate[] {
  const updates: TagUpdate[] = [];
  let prevFenceId: string | null | undefined = undefined;
  let prevRowId: number | bigint | null = null;
  let prevRowInWrite = false;
  let prevTs = '';
  let lastEnterRowId: number | bigint | null = null;
  let lastEnterTsMs: number | null = null;
  const graceMs = graceSeconds * 1000;

  for (const row of rows) {
    const currentFenceId = normalizeFenceId(row.geofence_id);
    const rowId = typeof row.id === 'bigint' ? Number(row.id) : row.id;
    const ts = normalizePositionTimeNz(row.position_time_nz);
    const inWrite = inWindow(ts, writeStart, writeEnd);

    if (prevFenceId !== undefined && currentFenceId !== prevFenceId) {
      // Exiting previous fence (fence→null or fence→fence): apply EXIT or suppress short-stay noise.
      // Emit EXIT on last fence row when duration >= graceMs even if that row is outside writeWindow,
      // so a drop-off at end (fence then persistent null) still gets an EXIT and the stay is closed.
      if (prevFenceId !== null && prevRowId !== null && prevTs && lastEnterTsMs != null && lastEnterRowId != null) {
        const durationMs = parseTimestampToMs(prevTs) - lastEnterTsMs;
        if (durationMs >= graceMs) {
          updates.push({ id: prevRowId, geofence_type: 'EXIT' });
        } else {
          const idx = updates.findIndex((u) => idEq(u.id, lastEnterRowId) && u.geofence_type === 'ENTER');
          if (idx >= 0) updates.splice(idx, 1);
        }
      }
      // Entering new fence (null→fence or fence→fence): ENTER on current row
      if (currentFenceId !== null && inWrite) {
        updates.push({ id: rowId, geofence_type: 'ENTER' });
        lastEnterRowId = row.id;
        lastEnterTsMs = parseTimestampToMs(ts);
      }
    }

    prevFenceId = currentFenceId;
    prevRowId = rowId;
    prevRowInWrite = inWrite;
    prevTs = ts;
  }

  return updates;
}

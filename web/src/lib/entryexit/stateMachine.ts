/**
 * ENTER/EXIT tagging:
 * - A fence visit is "real" only if the device stayed on that fence for >= graceSeconds.
 * - EXIT (and the paired ENTER on the first row of that visit) is applied only when we leave a real
 *   fence visit **and** the next state (null or another fence) persists for >= graceSeconds in one
 *   contiguous run. If that outside state changes before grace (e.g. null → other fence), the timer
 *   resets to the new outside state. Returning to the same fence before any outside state reaches
 *   grace cancels the pending exit (same visit).
 * - If the stream ends while still inside a fence and that segment persisted >= grace, tag ENTER only
 *   on the first row (no EXIT).
 */

import { parseTimestampToMs } from './dateWindow';

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

/** position_time_nz from DB may be Date or string; normalize to YYYY-MM-DD HH:mm:ss (local, no UTC). */
function normalizePositionTimeNz(raw: string | Date | null | undefined): string {
  if (raw == null) return '';
  if (typeof raw === 'string') return raw.trim();
  if (raw instanceof Date) {
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${raw.getFullYear()}-${pad(raw.getMonth() + 1)}-${pad(raw.getDate())} ${pad(raw.getHours())}:${pad(raw.getMinutes())}:${pad(raw.getSeconds())}`;
  }
  return String(raw);
}

interface PendingLeave {
  leftFenceId: string | null;
  fenceSegStartRowId: number | bigint;
  fenceSegStartTsMs: number;
  lastFenceRowId: number | bigint;
  outsideStartTsMs: number;
  outsideStartRowId: number | bigint;
  outsideKey: string | null;
}

/**
 * Run state machine: EXIT only after the post-fence state persists >= graceSeconds.
 */
export function runStateMachine(rows: ScanRow[], graceSeconds: number): TagUpdate[] {
  const updates: TagUpdate[] = [];
  const graceMs = graceSeconds * 1000;

  let prevFenceId: string | null | undefined = undefined;
  let prevRowId: number | bigint | null = null;
  let prevTs = '';
  let segmentStartTsMs: number = 0;
  let segmentStartRowId: number | bigint | null = null;

  let pendingLeave: PendingLeave | null = null;

  for (const row of rows) {
    const currentFenceId = normalizeFenceId(row.geofence_id);
    const rowIdRaw = row.id;
    const ts = normalizePositionTimeNz(row.position_time_nz);
    const tsMs = parseTimestampToMs(ts);

    if (pendingLeave) {
      if (currentFenceId === pendingLeave.leftFenceId) {
        const p: PendingLeave = pendingLeave;
        pendingLeave = null;
        const mergedStartTs = p.fenceSegStartTsMs;
        const mergedStartId = p.fenceSegStartRowId;
        segmentStartTsMs = mergedStartTs;
        segmentStartRowId = mergedStartId;
        prevFenceId = currentFenceId;
        prevRowId = rowIdRaw;
        prevTs = ts;
        continue;
      }

      if (currentFenceId !== pendingLeave.outsideKey) {
        pendingLeave.outsideKey = currentFenceId;
        pendingLeave.outsideStartTsMs = tsMs;
        pendingLeave.outsideStartRowId = rowIdRaw;
      }

      if (tsMs - pendingLeave.outsideStartTsMs >= graceMs) {
        updates.push({ id: pendingLeave.fenceSegStartRowId, geofence_type: 'ENTER' });
        updates.push({ id: pendingLeave.lastFenceRowId, geofence_type: 'EXIT' });
        const outStartTs = pendingLeave.outsideStartTsMs;
        const outStartId: number | bigint = pendingLeave.outsideStartRowId;
        pendingLeave = null;
        segmentStartTsMs = outStartTs;
        segmentStartRowId = outStartId;
        prevFenceId = currentFenceId;
        prevRowId = rowIdRaw;
        prevTs = ts;
        continue;
      }

      prevFenceId = currentFenceId;
      prevRowId = rowIdRaw;
      prevTs = ts;
      continue;
    }

    if (prevFenceId === undefined) {
      segmentStartTsMs = tsMs;
      segmentStartRowId = rowIdRaw;
    } else if (currentFenceId !== prevFenceId) {
      const segmentDurationMs = prevTs ? parseTimestampToMs(prevTs) - segmentStartTsMs : 0;
      const segmentPersisted = segmentDurationMs >= graceMs;

      if (prevFenceId !== null && prevRowId !== null && segmentStartRowId !== null && segmentPersisted) {
        pendingLeave = {
          leftFenceId: prevFenceId,
          fenceSegStartRowId: segmentStartRowId,
          fenceSegStartTsMs: segmentStartTsMs,
          lastFenceRowId: prevRowId,
          outsideStartTsMs: tsMs,
          outsideStartRowId: rowIdRaw,
          outsideKey: currentFenceId,
        };
      }
      segmentStartTsMs = tsMs;
      segmentStartRowId = rowIdRaw;
    }

    prevFenceId = currentFenceId;
    prevRowId = rowIdRaw;
    prevTs = ts;
  }

  if (pendingLeave) {
    if (prevTs && parseTimestampToMs(prevTs) - pendingLeave.outsideStartTsMs >= graceMs) {
      updates.push({ id: pendingLeave.fenceSegStartRowId, geofence_type: 'ENTER' });
      updates.push({ id: pendingLeave.lastFenceRowId, geofence_type: 'EXIT' });
    }
  } else if (prevFenceId !== null && prevTs && segmentStartRowId !== null) {
    const segmentDurationMs = parseTimestampToMs(prevTs) - segmentStartTsMs;
    if (segmentDurationMs >= graceMs) {
      updates.push({ id: segmentStartRowId, geofence_type: 'ENTER' });
    }
  }

  return updates;
}

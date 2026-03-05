/**
 * ENTER/EXIT tagging — simple rule:
 * If a state (fence or null) persists >= graceSeconds (e.g. 300 s), it's real; else it's noise.
 * We tag EXIT when leaving a fence whose stay was real (duration >= grace).
 * We tag ENTER only when we have confirmed the fence stay was real: i.e. when we leave that
 * fence (or end stream) and the fence segment lasted >= grace. So ENTER is applied to the
 * first row of that segment. This avoids putting ENTER on a blip (brief in-and-out) and
 * blocking the real ENTER when the device later enters and stays.
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

/** position_time_nz from DB may be Date or string; normalize to YYYY-MM-DD HH:mm:ss. */
function normalizePositionTimeNz(raw: string | Date | null | undefined): string {
  if (raw == null) return '';
  if (typeof raw === 'string') return raw.trim();
  if (raw instanceof Date) {
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${raw.getUTCFullYear()}-${pad(raw.getUTCMonth() + 1)}-${pad(raw.getUTCDate())} ${pad(raw.getUTCHours())}:${pad(raw.getUTCMinutes())}:${pad(raw.getUTCSeconds())}`;
  }
  return String(raw);
}

/**
 * Run state machine: only treat a segment as real if it persisted >= graceSeconds.
 * ENTER is tagged only when we leave a fence (or end in a fence) and that fence segment
 * lasted >= grace — so we tag ENTER on the first row of that segment. This avoids
 * putting ENTER on a blip (brief in-and-out) which would block the real ENTER later.
 */
export function runStateMachine(rows: ScanRow[], graceSeconds: number): TagUpdate[] {
  const updates: TagUpdate[] = [];
  const graceMs = graceSeconds * 1000;

  let prevFenceId: string | null | undefined = undefined;
  let prevRowId: number | bigint | null = null;
  let prevTs = '';
  let segmentStartTsMs: number = 0;
  let segmentStartRowId: number | bigint | null = null;

  for (const row of rows) {
    const currentFenceId = normalizeFenceId(row.geofence_id);
    const rowIdRaw = row.id;
    const ts = normalizePositionTimeNz(row.position_time_nz);
    const tsMs = parseTimestampToMs(ts);

    if (prevFenceId === undefined) {
      segmentStartTsMs = tsMs;
      segmentStartRowId = rowIdRaw;
    } else if (currentFenceId !== prevFenceId) {
      const segmentDurationMs = prevTs ? parseTimestampToMs(prevTs) - segmentStartTsMs : 0;
      const segmentPersisted = segmentDurationMs >= graceMs;

      if (segmentPersisted) {
        // Leaving a segment that was real: if it was a fence, tag ENTER on first row of that segment and EXIT on last
        if (prevFenceId !== null && prevRowId !== null && segmentStartRowId !== null) {
          updates.push({ id: segmentStartRowId, geofence_type: 'ENTER' });
          updates.push({ id: prevRowId, geofence_type: 'EXIT' });
        }
      }
      // Always advance segment start on transition so the next segment is measured from here; when segment was short we still move state (no tags)
      segmentStartTsMs = tsMs;
      segmentStartRowId = rowIdRaw;
    }

    prevFenceId = currentFenceId;
    prevRowId = rowIdRaw;
    prevTs = ts;
  }

  // If we ended while inside a fence and that segment persisted >= grace, tag ENTER on first row (no EXIT — we didn't leave)
  if (prevFenceId !== null && prevTs && segmentStartRowId !== null) {
    const segmentDurationMs = parseTimestampToMs(prevTs) - segmentStartTsMs;
    if (segmentDurationMs >= graceMs) {
      updates.push({ id: segmentStartRowId, geofence_type: 'ENTER' });
    }
  }

  return updates;
}

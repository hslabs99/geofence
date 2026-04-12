/**
 * GPS window for Query → Inspect tracking grid (must stay in sync with inspect/page.tsx).
 * Not the same as buildTrackingWindowForJob (harvest/24h fallback) — Inspect uses no upper bound when actual_end is missing.
 */

import { addMinutesToTimestampAsNZ } from '@/lib/fetch-steps';

export type InspectGpsWindowOptions = {
  startLessMinutes: number;
  endPlusMinutes: number;
  /** Inspect-only extra minutes before start (default 0). */
  displayExpandBefore?: number;
  /** Inspect-only extra minutes after end (default 0). */
  displayExpandAfter?: number;
};

export type InspectGpsWindowResult = {
  positionAfter: string;
  /** Null when job has no actual_end_time — matches /api/tracking (no positionBefore). */
  positionBefore: string | null;
  device: string;
  error: string | null;
};

/**
 * Same logic as Inspect’s default GPS grid (no gpsWindowOverride):
 * - positionAfter = actual_start − (startLess + displayExpandBefore)
 * - positionBefore = actual_end + (endPlus + displayExpandAfter), or null if no actual_end
 * Uses worker/truck and actual_start_time / actual_end_time only (no gps_end_time).
 */
export function buildInspectGpsWindowForJob(
  raw: Record<string, unknown>,
  opts: InspectGpsWindowOptions
): InspectGpsWindowResult {
  const startLess = opts.startLessMinutes;
  const endPlus = opts.endPlusMinutes;
  const expB = opts.displayExpandBefore ?? 0;
  const expA = opts.displayExpandAfter ?? 0;
  const totalStartLess = startLess + expB;
  const totalEndPlus = endPlus + expA;

  const get = (j: Record<string, unknown>, ...keys: string[]): string => {
    for (const k of keys) {
      const v = j[k];
      if (v != null && String(v).trim() !== '') return String(v).trim();
    }
    return '';
  };

  const device = get(raw, 'worker', 'Worker') || get(raw, 'truck_id', 'Truck_ID');
  const actualStart = get(raw, 'actual_start_time', 'Actual_Start_Time');
  /** Same composite as Inspect `actualEndTime`: DB actual_end, else legacy gps_end on row object. */
  const actualEnd =
    get(raw, 'actual_end_time', 'Actual_End_Time') || get(raw, 'gps_end_time', 'Gps_End_Time');

  if (!device || !actualStart) {
    return {
      positionAfter: '',
      positionBefore: null,
      device: device || '',
      error: !device ? 'missing worker (device)' : 'missing actual_start_time',
    };
  }

  const positionAfter = addMinutesToTimestampAsNZ(actualStart, -totalStartLess);
  const positionBefore =
    actualEnd.trim() !== '' ? addMinutesToTimestampAsNZ(actualEnd, totalEndPlus) : null;

  return { positionAfter, positionBefore, device, error: null };
}

/**
 * Diverted-job signal: completed a GPS vineyard visit (ENTER+EXIT on job vineyard fence set),
 * then ENTERed a different mapped vineyard (std ENTER/EXIT tags) before first winery ENTER.
 * Drivers may pass through another vineyard without a real diversion; callers can filter further
 * (e.g. next job on the same truck still at this vineyard → unlikely diversion).
 */

import { query } from '@/lib/db';
import {
  getVineyardFenceIdsForVworkName,
  getWineryFenceIdsForVworkName,
  listFenceEnterExitEventsInWindow,
  normalizeTimestampString,
} from '@/lib/derived-steps';
import { buildInspectGpsWindowForJob, type InspectGpsWindowOptions } from '@/lib/inspect-gps-window';

/** Distinct tbl_geofences.fence_id values referenced by tbl_gpsmappings type Vineyard (vwname/gpsname). */
export async function loadAllMappedVineyardFenceIds(): Promise<number[]> {
  const rows = await query<{ fence_id: unknown }>(
    `SELECT DISTINCT g.fence_id
     FROM tbl_gpsmappings m
     INNER JOIN tbl_geofences g ON (
       LOWER(TRIM(COALESCE(g.fence_name, ''))) = LOWER(TRIM(COALESCE(m.vwname, '')))
       OR LOWER(TRIM(COALESCE(g.fence_name, ''))) = LOWER(TRIM(COALESCE(m.gpsname, '')))
     )
     WHERE TRIM(COALESCE(m.type, '')) ILIKE 'vineyard'
       AND (TRIM(COALESCE(m.vwname, '')) <> '' OR TRIM(COALESCE(m.gpsname, '')) <> '')`
  );
  return rows
    .map((r) => Number(r.fence_id))
    .filter((n) => Number.isFinite(n) && n > 0);
}

function jobVineyardSet(fenceIds: number[]): Set<number> {
  return new Set(fenceIds.map((n) => Number(n)));
}

/**
 * True when: (a) first job-vineyard ENTER then EXIT on that fence set (std tags),
 * (b) before first delivery-winery ENTER after that EXIT, an ENTER on a mapped vineyard fence not in the job set.
 */
export async function jobHasDetourVineyardBeforeWinery(
  rawJob: Record<string, unknown>,
  windowOpts: InspectGpsWindowOptions,
  allVineyardFenceIds: number[]
): Promise<boolean> {
  const win = buildInspectGpsWindowForJob(rawJob, windowOpts);
  if (win.error || !win.device || !win.positionAfter) return false;

  const vineyardName =
    String(rawJob.vineyard_name ?? rawJob.Vineyard_Name ?? '')
      .trim();
  const wineryName =
    String(rawJob.delivery_winery ?? rawJob.Delivery_Winery ?? '')
      .trim();
  if (!vineyardName || !wineryName) return false;

  const [jobVineIds, wineryIds] = await Promise.all([
    getVineyardFenceIdsForVworkName(vineyardName),
    getWineryFenceIdsForVworkName(wineryName),
  ]);
  if (jobVineIds.length === 0 || wineryIds.length === 0) return false;

  const jSet = jobVineyardSet(jobVineIds);
  const allVSet = new Set(allVineyardFenceIds.map((n) => Number(n)));
  const wSet = new Set(wineryIds.map((n) => Number(n)));

  const events = await listFenceEnterExitEventsInWindow(win.device, win.positionAfter, win.positionBefore);

  let vEnterIdx = -1;
  for (let i = 0; i < events.length; i++) {
    const e = events[i];
    if (e.geofenceType === 'ENTER' && jSet.has(e.geofenceId)) {
      vEnterIdx = i;
      break;
    }
  }
  if (vEnterIdx < 0) return false;

  let vExitTime: string | null = null;
  for (let j = vEnterIdx + 1; j < events.length; j++) {
    const e = events[j];
    if (e.geofenceType === 'EXIT' && jSet.has(e.geofenceId)) {
      vExitTime = e.timeNorm;
      break;
    }
  }
  if (!vExitTime) return false;

  let wineryEnterTime: string | null = null;
  for (const e of events) {
    if (e.timeNorm <= vExitTime) continue;
    if (e.geofenceType === 'ENTER' && wSet.has(e.geofenceId)) {
      wineryEnterTime = e.timeNorm;
      break;
    }
  }
  if (!wineryEnterTime) return false;

  const vExitN = normalizeTimestampString(vExitTime);
  const wEnterN = normalizeTimestampString(wineryEnterTime);
  if (!vExitN || !wEnterN || vExitN >= wEnterN) return false;

  for (const e of events) {
    if (e.geofenceType !== 'ENTER') continue;
    if (e.timeNorm <= vExitN) continue;
    if (e.timeNorm >= wEnterN) break;
    if (jSet.has(e.geofenceId)) continue;
    if (allVSet.has(e.geofenceId)) return true;
  }

  return false;
}

/**
 * If the next scheduled job on the same truck is still at this job's vineyard, a GPS detour through
 * another mapped vineyard is more likely transit than a diversion to pick elsewhere.
 * @returns true when the detour hit should be excluded from the diverted-jobs list.
 */
export async function excludeDetourHitBecauseNextTruckJobSameVineyard(
  rawJob: Record<string, unknown>
): Promise<boolean> {
  const jobId = String(rawJob.job_id ?? rawJob.Job_ID ?? '').trim();
  const truckId = String(rawJob.truck_id ?? rawJob.Truck_ID ?? '').trim();
  const vineyardName = String(rawJob.vineyard_name ?? rawJob.Vineyard_Name ?? '').trim();
  if (!jobId || !truckId || !vineyardName) return false;

  const actualStartRaw = rawJob.actual_start_time ?? rawJob.Actual_Start_Time;
  if (actualStartRaw == null || String(actualStartRaw).trim() === '') return false;
  const actualStartNorm = normalizeTimestampString(actualStartRaw as string | Date);
  if (!actualStartNorm) return false;

  const rows = await query<{ vn: string | null }>(
    `SELECT TRIM(COALESCE(vineyard_name::text, '')) AS vn
     FROM tbl_vworkjobs t
     WHERE TRIM(COALESCE(t.truck_id::text, '')) = TRIM($1::text)
       AND TRIM(COALESCE(t.truck_id::text, '')) <> ''
       AND t.actual_start_time IS NOT NULL
       AND t.actual_start_time > $2::timestamp
       AND TRIM(COALESCE(t.job_id::text, '')) <> TRIM($3::text)
     ORDER BY t.actual_start_time ASC NULLS LAST, TRIM(t.job_id::text) ASC
     LIMIT 1`,
    [truckId, actualStartNorm, jobId]
  );
  const nextVn = rows[0]?.vn?.trim() ?? '';
  if (!nextVn) return false;
  return nextVn.toLowerCase() === vineyardName.toLowerCase();
}

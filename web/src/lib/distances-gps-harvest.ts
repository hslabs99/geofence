/**
 * GPS harvest: winery EXIT → vineyard ENTER path in the same tbl_tracking window as derived steps.
 * Meters = sum of haversine segments between consecutive points (position_time_nz order).
 * Minutes = vineyard ENTER position_time_nz − winery EXIT position_time_nz.
 * Failed attempts can be persisted with outcome=failed and full debug_json.
 */

import { haversineMeters } from '@/lib/haversine-meters';
import { query, execute } from '@/lib/db';
import { addMinutesToTimestampAsNZ } from '@/lib/fetch-steps';
import {
  aggregateStepsPlusBufferedSegments,
  fetchOutboundLegAnchors,
  getVineyardFenceIdsForVworkName,
  type GpsStepCandidate,
  type JobForDerivedSteps,
  normalizeTimestampString,
  vworkStep1TimeForCleanup,
} from '@/lib/derived-steps';
import { getJobEndCeilingBufferMinutes } from '@/lib/job-end-ceiling-buffer-setting';
import { getStep5ExtendWineryExitMinutes } from '@/lib/step5-winery-exit-extend-setting';
import { runStepsPlusQuery } from '@/lib/steps-plus-query';
import { getStepsPlusSettings } from '@/lib/steps-plus-settings';
import {
  DEFAULT_HARVEST_END_PLUS_MINUTES,
  DEFAULT_HARVEST_START_LESS_MINUTES,
  DEFAULT_HARVEST_WINDOW_MINUTES,
  GPS_HARVEST_DEFAULT_MAX_JOBS,
  GPS_HARVEST_DEFAULT_MAX_SUCCESSES,
  GPS_HARVEST_SQL_JOB_LIMIT,
  GPS_PLUS_VINEYARD_BUFFER_METERS,
} from '@/lib/gps-harvest-constants';
export {
  DEFAULT_HARVEST_END_PLUS_MINUTES,
  DEFAULT_HARVEST_START_LESS_MINUTES,
  DEFAULT_HARVEST_WINDOW_MINUTES,
  GPS_HARVEST_DEFAULT_MAX_JOBS,
  GPS_HARVEST_DEFAULT_MAX_SUCCESSES,
  GPS_HARVEST_SQL_JOB_LIMIT,
} from '@/lib/gps-harvest-constants';

export { haversineMeters };

export function minutesBetweenNz(tsEarlier: string, tsLater: string): number | null {
  const a = normalizeTimestampString(tsEarlier);
  const b = normalizeTimestampString(tsLater);
  if (!a || !b) return null;
  const ma = a.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  const mb = b.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  if (!ma || !mb) return null;
  const msA = Date.UTC(Number(ma[1]), Number(ma[2]) - 1, Number(ma[3]), Number(ma[4]), Number(ma[5]), Number(ma[6]));
  const msB = Date.UTC(Number(mb[1]), Number(mb[2]) - 1, Number(mb[3]), Number(mb[4]), Number(mb[5]), Number(mb[6]));
  return (msB - msA) / (60 * 1000);
}

function pickJobField(row: Record<string, unknown>, ...keys: string[]): unknown {
  for (const k of keys) {
    if (row[k] !== undefined && row[k] !== null && String(row[k]).trim() !== '') return row[k];
  }
  return null;
}

export function vworkRowToJobForDerivedSteps(raw: Record<string, unknown>): JobForDerivedSteps {
  return {
    job_id: pickJobField(raw, 'job_id', 'Job_ID'),
    vineyard_name: (pickJobField(raw, 'vineyard_name', 'Vineyard_Name') as string | null) ?? undefined,
    delivery_winery: (pickJobField(raw, 'delivery_winery', 'Delivery_Winery') as string | null) ?? undefined,
    truck_id: (pickJobField(raw, 'truck_id', 'Truck_ID') as string | null) ?? undefined,
    worker: (pickJobField(raw, 'worker', 'Worker') as string | null) ?? undefined,
    actual_start_time: (pickJobField(raw, 'actual_start_time', 'Actual_Start_Time') as string | null) ?? undefined,
    actual_end_time: (pickJobField(raw, 'actual_end_time', 'Actual_End_Time') as string | null) ?? undefined,
    step_5_completed_at: (pickJobField(raw, 'step_5_completed_at', 'Step_5_Completed_At') as string | null) ?? undefined,
    step_1_completed_at: (pickJobField(raw, 'step_1_completed_at', 'Step_1_Completed_At') as string | null) ?? undefined,
    step_2_completed_at: (pickJobField(raw, 'step_2_completed_at', 'Step_2_Completed_At') as string | null) ?? undefined,
    step_3_completed_at: (pickJobField(raw, 'step_3_completed_at', 'Step_3_Completed_At') as string | null) ?? undefined,
    step_4_completed_at: (pickJobField(raw, 'step_4_completed_at', 'Step_4_Completed_At') as string | null) ?? undefined,
  };
}

export function buildTrackingWindowForJob(
  raw: Record<string, unknown>,
  startLessMinutes: number,
  endPlusMinutes: number
): { positionAfter: string; positionBefore: string | null; device: string; error: string | null } {
  const get = (j: Record<string, unknown>, ...keys: string[]): string => {
    for (const k of keys) {
      const v = j[k];
      if (v != null && String(v).trim() !== '') return String(v).trim();
    }
    return '';
  };
  const device = get(raw, 'worker', 'Worker') || get(raw, 'truck_id', 'Truck_ID');
  const actualStart = get(raw, 'actual_start_time', 'Actual_Start_Time');
  const actualEnd = get(raw, 'actual_end_time', 'Actual_End_Time') || get(raw, 'gps_end_time', 'Gps_End_Time');
  if (!device || !actualStart) {
    return {
      positionAfter: '',
      positionBefore: null,
      device: device || '',
      error: !device ? 'missing worker (device)' : 'missing actual_start_time',
    };
  }
  const positionAfter = addMinutesToTimestampAsNZ(actualStart, -startLessMinutes);
  let positionBefore: string | null = null;
  if (actualEnd) {
    positionBefore = addMinutesToTimestampAsNZ(actualEnd, endPlusMinutes);
  } else {
    positionBefore = addMinutesToTimestampAsNZ(actualStart, 24 * 60 + endPlusMinutes);
  }
  return { positionAfter, positionBefore, device, error: null };
}

/** Count tbl_tracking rows in the same open interval as derived-steps lookups (position_time_nz bounds). */
export async function countTrackingRowsInWindow(
  device: string,
  positionAfter: string,
  positionBefore: string | null
): Promise<number> {
  const rawAfter = normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19);
  const params: unknown[] = [device, rawAfter];
  let cond = 't.position_time_nz > $2';
  if (positionBefore) {
    const rawBefore = normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19);
    params.push(rawBefore);
    cond += ' AND t.position_time_nz < $3';
  }
  const rows = await query<{ c: string }>(
    `SELECT COUNT(*)::text AS c FROM tbl_tracking t WHERE t.device_name = $1 AND ${cond}`,
    params
  );
  return parseInt(rows[0]?.c ?? '0', 10) || 0;
}

/** GPS+: winery anchor = EXIT tag, else first track outside winery union after being inside, else step1; vineyard = first track within buffered vineyard union. */
export type GpsPlusWineryAnchorSource =
  | 'fence_EXIT'
  | 'geom_first_outside_after_inside'
  | 'vwork_step1_or_actual_start';

export type GpsPlusAttemptSummary = {
  ok: boolean;
  meters?: number;
  minutes?: number;
  skipReason?: string;
  winery_anchor_source?: GpsPlusWineryAnchorSource;
  vineyard_hit_tracking_id?: number | null;
  segment_point_count?: number;
  /** Anchors and buffer size for dry-run / inspect UI. */
  detail?: Record<string, unknown>;
};

export type SingleHarvestAttempt = {
  job_id: string;
  ok: boolean;
  outcome: 'success' | 'failed';
  skipReason?: string;
  meters?: number;
  minutes?: number;
  segment_point_count?: number;
  /** Null when winery EXIT is inferred from VWork step 1 / actual start (no tbl_tracking EXIT row). */
  winery_tracking_id?: number | null;
  /** Null when vineyard ENTER came from Steps+ buffered stay (no tbl_tracking ENTER row). */
  vineyard_tracking_id?: number | null;
  /** Independent path: vineyard target = first point within 100m of fence union (no ENTER tag required). */
  gps_plus?: GpsPlusAttemptSummary;
  debug: Record<string, unknown>;
};

function jsonSafeForDb(obj: unknown): string {
  return JSON.stringify(obj, (_, v) => (typeof v === 'bigint' ? v.toString() : v));
}

function enrichDebugBase(
  rawJobRow: Record<string, unknown>,
  win: { positionAfter: string; positionBefore: string | null; device: string; error: string | null },
  opts: { startLessMinutes: number; endPlusMinutes: number; windowMinutes: number },
  trackingRowsInWindow: number
): Record<string, unknown> {
  const get = (j: Record<string, unknown>, ...keys: string[]): string | null => {
    for (const k of keys) {
      const v = j[k];
      if (v != null && String(v).trim() !== '') return String(v).trim();
    }
    return null;
  };
  return {
    vwork_job_id: rawJobRow.job_id != null ? String(rawJobRow.job_id).trim() : '',
    delivery_winery: get(rawJobRow, 'delivery_winery', 'Delivery_Winery'),
    vineyard_name: get(rawJobRow, 'vineyard_name', 'Vineyard_Name'),
    worker_device: win.device,
    window: {
      startLessMinutes: opts.startLessMinutes,
      endPlusMinutes: opts.endPlusMinutes,
      windowMinutesParam: opts.windowMinutes,
      actual_start_raw: get(rawJobRow, 'actual_start_time', 'Actual_Start_Time'),
      actual_end_raw: get(rawJobRow, 'actual_end_time', 'Actual_End_Time') ?? get(rawJobRow, 'gps_end_time', 'Gps_End_Time'),
      positionAfter: win.positionAfter,
      positionBefore: win.positionBefore,
    },
    tracking_rows_in_window_position_time_nz: trackingRowsInWindow,
  };
}

async function listTrackingPointsBetween(
  deviceName: string,
  tAfterInclusive: string,
  tBeforeInclusive: string
): Promise<{ id: number; lat: number; lon: number; position_time_nz: string }[]> {
  const rawAfter = normalizeTimestampString(tAfterInclusive) ?? tAfterInclusive.slice(0, 19);
  const rawBefore = normalizeTimestampString(tBeforeInclusive) ?? tBeforeInclusive.slice(0, 19);
  const rows = await query<{ id: unknown; lat: unknown; lon: unknown; position_time_nz: unknown }>(
    `SELECT t.id, t.lat, t.lon,
            to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz
     FROM tbl_tracking t
     WHERE t.device_name = $1
       AND t.position_time_nz IS NOT NULL
       AND t.position_time_nz >= $2::timestamp
       AND t.position_time_nz <= $3::timestamp
     ORDER BY t.position_time_nz ASC, t.id ASC`,
    [deviceName, rawAfter, rawBefore]
  );
  const out: { id: number; lat: number; lon: number; position_time_nz: string }[] = [];
  for (const r of rows) {
    const id = Number(r.id);
    if (!Number.isFinite(id)) continue;
    const lat = Number(r.lat);
    const lon = Number(r.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    const ptnz =
      normalizeTimestampString(String(r.position_time_nz ?? '')) ?? String(r.position_time_nz).slice(0, 19);
    out.push({ id, lat, lon, position_time_nz: ptnz });
  }
  return out;
}

/** First tracking row outside winery fence union after an earlier row inside (same window as harvest). */
async function findFirstWineryOutsideAfterInside(
  device: string,
  positionAfter: string,
  positionBefore: string | null,
  wineryFenceIds: number[]
): Promise<{ id: number; position_time_nz: string } | null> {
  if (wineryFenceIds.length === 0) return null;
  const rawAfter = normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19);
  const rawBefore = positionBefore
    ? (normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19))
    : null;
  const params: unknown[] = [device, rawAfter, wineryFenceIds];
  let beforeSql = '';
  if (rawBefore) {
    params.push(rawBefore);
    beforeSql = `AND t.position_time_nz < $4::timestamp`;
  }
  const rows = await query<{ id: unknown; position_time_nz: unknown }>(
    `WITH pts AS (
       SELECT t.id, t.position_time_nz,
         ST_Transform(ST_SetSRID(ST_MakePoint(t.lon::float8, t.lat::float8), 4326), 3857) AS pt3857,
         ROW_NUMBER() OVER (ORDER BY t.position_time_nz ASC, t.id ASC) AS rn
       FROM tbl_tracking t
       WHERE t.device_name = $1
         AND t.position_time_nz > $2::timestamp
         ${beforeSql}
         AND t.lon IS NOT NULL AND t.lat IS NOT NULL
     ),
     wu AS (
       SELECT ST_UnaryUnion(ST_Collect(ST_Transform(ST_Force2D(g.geom), 3857))) AS u
       FROM tbl_geofences g
       WHERE g.geom IS NOT NULL AND g.fence_id = ANY($3::int[])
     )
     SELECT p.id, to_char(p.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz
     FROM pts p
     CROSS JOIN wu
     WHERE wu.u IS NOT NULL
       AND NOT ST_Within(p.pt3857, wu.u)
       AND EXISTS (
         SELECT 1 FROM pts p2
         WHERE p2.rn < p.rn AND ST_Within(p2.pt3857, wu.u)
       )
     ORDER BY p.position_time_nz ASC, p.id ASC
     LIMIT 1`,
    params
  );
  const r0 = rows[0];
  if (!r0) return null;
  const id = Number(r0.id);
  if (!Number.isFinite(id)) return null;
  const ptnz =
    normalizeTimestampString(String(r0.position_time_nz ?? '')) ?? String(r0.position_time_nz).slice(0, 19);
  return { id, position_time_nz: ptnz };
}

/** First tracking row at/after winery anchor inside ST_Buffer(vineyard union, bufferM) in Web Mercator (same pattern as Steps+). */
async function findFirstVineyardInsideBufferAfter(
  device: string,
  wineryAnchorTimeInclusive: string,
  positionBefore: string | null,
  vineyardFenceIds: number[],
  bufferMeters: number
): Promise<{ id: number; position_time_nz: string } | null> {
  if (vineyardFenceIds.length === 0) return null;
  const rawAfter = normalizeTimestampString(wineryAnchorTimeInclusive) ?? wineryAnchorTimeInclusive.slice(0, 19);
  const rawBefore = positionBefore
    ? (normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19))
    : null;
  let beforeSql = '';
  if (rawBefore) {
    beforeSql = `AND t.position_time_nz <= $5::timestamp`;
  }
  const rows = await query<{ id: unknown; position_time_nz: unknown }>(
    `WITH bu AS (
       SELECT ST_Buffer(ST_UnaryUnion(ST_Collect(ST_Transform(ST_Force2D(g.geom), 3857))), $4::numeric) AS buffered
       FROM tbl_geofences g
       WHERE g.geom IS NOT NULL AND g.fence_id = ANY($3::int[])
     )
     SELECT t.id, to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz
     FROM tbl_tracking t
     CROSS JOIN bu
     WHERE t.device_name = $1
       AND bu.buffered IS NOT NULL
       AND t.position_time_nz >= $2::timestamp
       ${beforeSql}
       AND t.lon IS NOT NULL AND t.lat IS NOT NULL
       AND ST_Within(
         ST_Transform(ST_SetSRID(ST_MakePoint(t.lon::float8, t.lat::float8), 4326), 3857),
         bu.buffered
       )
     ORDER BY t.position_time_nz ASC, t.id ASC
     LIMIT 1`,
    rawBefore ? [device, rawAfter, vineyardFenceIds, bufferMeters, rawBefore] : [device, rawAfter, vineyardFenceIds, bufferMeters]
  );
  const r0 = rows[0];
  if (!r0) return null;
  const id = Number(r0.id);
  if (!Number.isFinite(id)) return null;
  const ptnz =
    normalizeTimestampString(String(r0.position_time_nz ?? '')) ?? String(r0.position_time_nz).slice(0, 19);
  return { id, position_time_nz: ptnz };
}

async function computeGpsPlusForJob(
  job: JobForDerivedSteps,
  win: { positionAfter: string; positionBefore: string | null; device: string; error: string | null },
  anchors: Awaited<ReturnType<typeof fetchOutboundLegAnchors>>,
  opts: { startLessMinutes: number; endPlusMinutes: number; windowMinutes: number },
  trackingRowsInWindow: number
): Promise<GpsPlusAttemptSummary> {
  const dbgBase = {
    buffer_m_vineyard: GPS_PLUS_VINEYARD_BUFFER_METERS,
    tracking_rows_in_window: trackingRowsInWindow,
    windowMinutesParam: opts.windowMinutes,
  };
  const dd = anchors.debug as {
    winery?: { fenceIds?: number[] };
    vineyard?: { fenceIds?: number[] };
  };
  const wineryFenceIds = (dd.winery?.fenceIds ?? []).filter((n) => Number.isFinite(n));
  const vineyardFenceIds = (dd.vineyard?.fenceIds ?? []).filter((n) => Number.isFinite(n));

  let winerySource: GpsPlusWineryAnchorSource | undefined;
  let tExit: string | null = null;
  let wineryExitTrackingId: number | null = null;

  if (anchors.wineryExit?.value) {
    const n = normalizeTimestampString(anchors.wineryExit.value);
    if (n) {
      tExit = n;
      wineryExitTrackingId = anchors.wineryExit.trackingId ?? null;
      winerySource = 'fence_EXIT';
    }
  }
  if (!tExit) {
    const geomHit = await findFirstWineryOutsideAfterInside(
      win.device,
      win.positionAfter,
      win.positionBefore,
      wineryFenceIds
    );
    if (geomHit) {
      tExit = normalizeTimestampString(geomHit.position_time_nz) ?? geomHit.position_time_nz.slice(0, 19);
      wineryExitTrackingId = geomHit.id;
      winerySource = 'geom_first_outside_after_inside';
    }
  }
  if (!tExit) {
    const fb = vworkStep1TimeForCleanup(job);
    if (fb) {
      tExit = normalizeTimestampString(fb) ?? fb.slice(0, 19);
      winerySource = 'vwork_step1_or_actual_start';
    }
  }

  if (!tExit) {
    return {
      ok: false,
      skipReason: 'no winery anchor (EXIT tag, first GPS outside winery fence after inside, or step1/actual start)',
      detail: { ...dbgBase, winery_fence_ids: wineryFenceIds, vineyard_fence_ids: vineyardFenceIds },
    };
  }

  if (vineyardFenceIds.length === 0) {
    return {
      ok: false,
      skipReason: 'no vineyard fences resolved for GPS+',
      winery_anchor_source: winerySource,
      detail: { ...dbgBase, t_winery_anchor: tExit, winery_exit_tracking_id: wineryExitTrackingId },
    };
  }

  const vHit = await findFirstVineyardInsideBufferAfter(
    win.device,
    tExit,
    win.positionBefore,
    vineyardFenceIds,
    GPS_PLUS_VINEYARD_BUFFER_METERS
  );

  if (!vHit) {
    return {
      ok: false,
      skipReason: `no tracking within ${GPS_PLUS_VINEYARD_BUFFER_METERS}m of vineyard fence after winery anchor`,
      winery_anchor_source: winerySource,
      detail: {
        ...dbgBase,
        t_winery_anchor: tExit,
        winery_exit_tracking_id: wineryExitTrackingId,
        vineyard_fence_ids: vineyardFenceIds,
      },
    };
  }

  const tVineyard =
    normalizeTimestampString(vHit.position_time_nz) ?? vHit.position_time_nz.slice(0, 19);
  if (tExit > tVineyard) {
    return {
      ok: false,
      skipReason: 'vineyard buffer hit before winery anchor time',
      winery_anchor_source: winerySource,
      detail: {
        ...dbgBase,
        t_winery_anchor: tExit,
        t_vineyard_buffer: tVineyard,
        winery_exit_tracking_id: wineryExitTrackingId,
      },
    };
  }

  const minutes = minutesBetweenNz(tExit, tVineyard);
  if (minutes == null || minutes < 0) {
    return {
      ok: false,
      skipReason: 'could not compute GPS+ minutes between anchors',
      winery_anchor_source: winerySource,
      detail: { ...dbgBase, t_winery_anchor: tExit, t_vineyard_buffer: tVineyard },
    };
  }

  const points = await listTrackingPointsBetween(win.device, tExit, tVineyard);
  if (points.length < 2) {
    return {
      ok: false,
      skipReason: 'fewer than 2 GPS points on GPS+ path',
      winery_anchor_source: winerySource,
      detail: {
        ...dbgBase,
        t_winery_anchor: tExit,
        t_vineyard_buffer: tVineyard,
        path_point_count: points.length,
      },
    };
  }

  let meters = 0;
  for (let i = 1; i < points.length; i++) {
    const a = points[i - 1]!;
    const b = points[i]!;
    meters += haversineMeters(a.lat, a.lon, b.lat, b.lon);
  }

  return {
    ok: true,
    meters: Math.round(meters * 100) / 100,
    minutes,
    winery_anchor_source: winerySource,
    vineyard_hit_tracking_id: vHit.id,
    segment_point_count: points.length,
    detail: {
      ...dbgBase,
      t_winery_anchor: tExit,
      t_vineyard_buffer: tVineyard,
      winery_exit_tracking_id: wineryExitTrackingId,
      vineyard_hit_tracking_id: vHit.id,
      pathFirstId: points[0]?.id,
      pathLastId: points[points.length - 1]?.id,
    },
  };
}

/**
 * Steps+ buffered vineyard fence (same as /api/tracking/derived-steps): used when there is no ENTER tag,
 * or to replace polygon ENTER with an earlier buffer enter when the merged stay is at or before the fence ENTER.
 */
async function tryVineyardEnterViaStepsPlus(
  job: JobForDerivedSteps,
  device: string,
  positionAfter: string,
  positionBefore: string | null
): Promise<{ candidate: GpsStepCandidate | null; debug: Record<string, unknown> }> {
  const debug: Record<string, unknown> = { applied: false };
  const vineyardName = job.vineyard_name ? String(job.vineyard_name).trim() : '';
  if (!vineyardName || !device || !positionAfter) {
    return { candidate: null, debug: { ...debug, reason: 'missing vineyard name or window' } };
  }

  const mappings = await query<{ vwname: string | null; gpsname: string | null }>(
    `SELECT vwname, gpsname FROM tbl_gpsmappings WHERE type = 'Vineyard'
     AND (
       LOWER(TRIM(COALESCE(vwname,''))) = LOWER(TRIM($1::text))
       OR LOWER(TRIM(COALESCE(gpsname,''))) = LOWER(TRIM($1::text))
     )`,
    [vineyardName]
  );
  const fenceNames: string[] = [vineyardName];
  for (const m of mappings) {
    const gps = (m.gpsname ?? '').trim();
    if (gps && !fenceNames.includes(gps)) fenceNames.push(gps);
  }
  if (fenceNames.length === 0) {
    return { candidate: null, debug: { ...debug, reason: 'no fence names' } };
  }

  const { bufferMeters: stepsPlusBufferM, minDurationSeconds: stepsPlusMinSec } =
    await getStepsPlusSettings();
  const stepsPlusEnd =
    positionBefore ?? addMinutesToTimestampAsNZ(positionAfter.trim(), 24 * 60);

  const stepsPlusRows = await runStepsPlusQuery(
    device,
    positionAfter.trim(),
    stepsPlusEnd,
    fenceNames,
    stepsPlusBufferM
  );
  const stays = stepsPlusRows.filter((r) => Number(r.duration_seconds) >= stepsPlusMinSec);

  const vworkEndRaw = job.step_5_completed_at ?? job.actual_end_time;
  const vworkEnd =
    vworkEndRaw != null && String(vworkEndRaw).trim() !== ''
      ? normalizeTimestampString(String(vworkEndRaw))
      : null;

  const staysInJob =
    vworkEnd == null
      ? stays
      : stays.filter((r) => {
          const ent = normalizeTimestampString(r.enter_time);
          const ext = normalizeTimestampString(r.exit_time);
          return ent != null && ext != null && ent < vworkEnd && ext < vworkEnd;
        });

  Object.assign(debug, {
    raw_segment_count: stepsPlusRows.length,
    stays_after_min_duration: stays.length,
    stays_in_job: staysInJob.length,
    buffer_m: stepsPlusBufferM,
    min_duration_sec: stepsPlusMinSec,
    fence_names: fenceNames,
  });

  if (staysInJob.length < 1) {
    return {
      candidate: null,
      debug: { ...debug, reason: 'no Steps+ stay met min duration in job window', applied: true },
    };
  }

  const vineyardFenceIds = await getVineyardFenceIdsForVworkName(vineyardName);
  const merged = await aggregateStepsPlusBufferedSegments(
    staysInJob.map((r) => ({
      enter_time: r.enter_time,
      exit_time: r.exit_time,
      fence_name: r.fence_name,
    })),
    device,
    positionAfter.trim(),
    positionBefore,
    vineyardFenceIds
  );

  Object.assign(debug, {
    merged_enter: merged.enter,
    merged_exit: merged.exit,
    used_gps_star_merge: merged.usedGpsStarMerge,
  });

  const enterNorm = normalizeTimestampString(merged.enter);
  if (!enterNorm) {
    return { candidate: null, debug: { ...debug, reason: 'Steps+ merged enter empty', applied: true } };
  }

  return {
    candidate: { value: enterNorm, trackingId: null },
    debug: { ...debug, applied: true, via: 'steps_plus_buffered' },
  };
}

export async function harvestWineryToVineyardForJob(
  rawJobRow: Record<string, unknown>,
  opts: {
    startLessMinutes: number;
    endPlusMinutes: number;
    windowMinutes: number;
  }
): Promise<SingleHarvestAttempt> {
  const jobId = rawJobRow.job_id != null ? String(rawJobRow.job_id).trim() : '';
  const job = vworkRowToJobForDerivedSteps(rawJobRow);
  const win = buildTrackingWindowForJob(rawJobRow, opts.startLessMinutes, opts.endPlusMinutes);

  let trackingRows = 0;
  if (!win.error && win.positionAfter) {
    trackingRows = await countTrackingRowsInWindow(win.device, win.positionAfter, win.positionBefore);
  }

  if (win.error) {
    return {
      job_id: jobId,
      ok: false,
      outcome: 'failed',
      skipReason: win.error,
      gps_plus: { ok: false, skipReason: win.error ?? 'no tracking window' },
      debug: {
        ...enrichDebugBase(rawJobRow, win, opts, trackingRows),
        window_error: win.error,
      },
    };
  }

  const jobEndCeilingBufferMinutes = await getJobEndCeilingBufferMinutes();
  const step5ExtendWineryExitMinutes = await getStep5ExtendWineryExitMinutes();
  const anchors = await fetchOutboundLegAnchors(job, {
    windowMinutes: opts.windowMinutes,
    device: win.device,
    positionAfter: win.positionAfter,
    positionBefore: win.positionBefore,
    jobEndCeilingBufferMinutes,
    step5ExtendWineryExitMinutes,
  });

  const gpsPlusSummary = await computeGpsPlusForJob(job, win, anchors, opts, trackingRows);

  const dd = anchors.debug as {
    winery?: { step1?: { sqlHint?: string } };
    vineyard?: { step2?: { sqlHint?: string } };
  };
  const sqlHints = {
    winery_exit_lookup_sql_hint: dd?.winery?.step1?.sqlHint ?? null,
    vineyard_enter_lookup_sql_hint: dd?.vineyard?.step2?.sqlHint ?? null,
  };

  let wExit = anchors.wineryExit;
  let vEnter = anchors.vineyardEnter;
  let stepsPlusDebug: Record<string, unknown> | null = null;

  if (job.vineyard_name?.trim()) {
    const sp = await tryVineyardEnterViaStepsPlus(job, win.device, win.positionAfter, win.positionBefore);
    stepsPlusDebug = sp.debug;
    if (sp.candidate?.value) {
      const poly = anchors.vineyardEnter;
      const m = normalizeTimestampString(sp.candidate.value);
      const pv = poly?.value ? normalizeTimestampString(poly.value) : null;
      if (pv == null) {
        vEnter = sp.candidate;
      } else if (m != null && m <= pv) {
        vEnter = sp.candidate;
        if (poly?.trackingId != null) {
          Object.assign(sp.debug, { replaced_polygon_enter_with_buffer: true });
        }
      }
    }
  }

  /**
   * Inspect / derived-steps can show VineFence+ for vineyard with no matching Winery EXIT in tbl_tracking
   * (guardrails, missing tags, or EXIT outside window). Path meters still need a start time: same as cleanup —
   * step_1_completed_at or actual_start_time.
   */
  let wineryExitSource: 'fence_EXIT' | 'vwork_step1_or_actual_start' = 'fence_EXIT';
  if (!wExit?.value) {
    const fb = vworkStep1TimeForCleanup(job);
    if (fb) {
      wExit = { value: fb, trackingId: null };
      wineryExitSource = 'vwork_step1_or_actual_start';
    }
  }

  if (!wExit?.value) {
    return {
      job_id: jobId,
      ok: false,
      outcome: 'failed',
      skipReason: 'no winery EXIT (GPS) and no step_1/actual_start for path start',
      gps_plus: gpsPlusSummary,
      debug: {
        ...enrichDebugBase(rawJobRow, win, opts, trackingRows),
        sql_hints: sqlHints,
        anchors: { wineryExit: anchors.wineryExit, vineyardEnter: vEnter },
        derived_debug: anchors.debug,
        steps_plus: stepsPlusDebug,
      },
    };
  }

  if (!vEnter?.value) {
    return {
      job_id: jobId,
      ok: false,
      outcome: 'failed',
      skipReason: 'no vineyard ENTER (fence tag or Steps+)',
      gps_plus: gpsPlusSummary,
      debug: {
        ...enrichDebugBase(rawJobRow, win, opts, trackingRows),
        sql_hints: sqlHints,
        winery_exit_source: wineryExitSource,
        anchors: { wineryExit: wExit, vineyardEnter: vEnter },
        derived_debug: anchors.debug,
        steps_plus: stepsPlusDebug,
      },
    };
  }

  const tExit = normalizeTimestampString(wExit.value) ?? wExit.value.slice(0, 19);
  const tEnter = normalizeTimestampString(vEnter.value) ?? vEnter.value.slice(0, 19);
  if (tExit >= tEnter) {
    return {
      job_id: jobId,
      ok: false,
      outcome: 'failed',
      skipReason: 'winery exit not strictly before vineyard enter',
      gps_plus: gpsPlusSummary,
      debug: {
        ...enrichDebugBase(rawJobRow, win, opts, trackingRows),
        sql_hints: sqlHints,
        winery_exit_source: wineryExitSource,
        tExit,
        tEnter,
        anchors: { wineryExit: wExit, vineyardEnter: vEnter },
        derived_debug: anchors.debug,
        steps_plus: stepsPlusDebug,
      },
    };
  }

  const minutes = minutesBetweenNz(tExit, tEnter);
  if (minutes == null || minutes < 0) {
    return {
      job_id: jobId,
      ok: false,
      outcome: 'failed',
      skipReason: 'could not compute minutes between anchors',
      gps_plus: gpsPlusSummary,
      debug: {
        ...enrichDebugBase(rawJobRow, win, opts, trackingRows),
        sql_hints: sqlHints,
        winery_exit_source: wineryExitSource,
        tExit,
        tEnter,
        derived_debug: anchors.debug,
        steps_plus: stepsPlusDebug,
      },
    };
  }

  const points = await listTrackingPointsBetween(win.device, tExit, tEnter);
  if (points.length < 2) {
    return {
      job_id: jobId,
      ok: false,
      outcome: 'failed',
      skipReason: 'fewer than 2 GPS points between anchors',
      gps_plus: gpsPlusSummary,
      debug: {
        ...enrichDebugBase(rawJobRow, win, opts, trackingRows),
        sql_hints: sqlHints,
        winery_exit_source: wineryExitSource,
        path_point_count: points.length,
        anchor_times: { tExit, tEnter },
        derived_debug: anchors.debug,
        steps_plus: stepsPlusDebug,
      },
    };
  }

  let meters = 0;
  for (let i = 1; i < points.length; i++) {
    const a = points[i - 1]!;
    const b = points[i]!;
    meters += haversineMeters(a.lat, a.lon, b.lat, b.lon);
  }

  return {
    job_id: jobId,
    ok: true,
    outcome: 'success',
    meters: Math.round(meters * 100) / 100,
    minutes,
    segment_point_count: points.length,
    winery_tracking_id: wExit.trackingId ?? null,
    vineyard_tracking_id: vEnter.trackingId ?? null,
    gps_plus: gpsPlusSummary,
    debug: {
      ...enrichDebugBase(rawJobRow, win, opts, trackingRows),
      sql_hints: sqlHints,
      anchors: { wineryExit: wExit, vineyardEnter: vEnter },
      winery_exit_source: wineryExitSource,
      vineyard_enter_source:
        vEnter.trackingId != null
          ? 'fence_ENTER'
          : (stepsPlusDebug && (stepsPlusDebug as { replaced_polygon_enter_with_buffer?: boolean }).replaced_polygon_enter_with_buffer
              ? 'steps_plus_wider'
              : 'steps_plus_buffered'),
      steps_plus: stepsPlusDebug,
      derived_debug: anchors.debug,
      pathFirstId: points[0]?.id,
      pathLastId: points[points.length - 1]?.id,
      path_point_count: points.length,
    },
  };
}

/** Same aggregates as rollup SQL over samples, computed from in-memory harvest attempts (dry run). */
export function computeRollupStatsFromAttempts(attempts: SingleHarvestAttempt[]): {
  cnt: number;
  avgM: number | null;
  avgMin: number | null;
  gpCnt: number;
  avgGpM: number | null;
  avgGpMin: number | null;
} {
  const primaryOk = attempts.filter(
    (a) => a.ok && a.outcome === 'success' && a.meters != null && Number.isFinite(Number(a.meters))
  );
  const cnt = primaryOk.length;
  const avgM =
    cnt === 0 ? null : Math.round(primaryOk.reduce((s, a) => s + Number(a.meters), 0) / cnt);
  const withMin = primaryOk.filter((a) => typeof a.minutes === 'number' && Number.isFinite(a.minutes));
  const avgMin =
    withMin.length === 0 ? null : withMin.reduce((s, a) => s + (a.minutes as number), 0) / withMin.length;

  const gpOk = attempts.filter(
    (a) =>
      a.gps_plus?.ok === true &&
      a.gps_plus.meters != null &&
      Number.isFinite(Number(a.gps_plus.meters))
  );
  const gpCnt = gpOk.length;
  const avgGpM =
    gpCnt === 0 ? null : Math.round(gpOk.reduce((s, a) => s + Number(a.gps_plus!.meters), 0) / gpCnt);
  const withGpMin = gpOk.filter(
    (a) => typeof a.gps_plus?.minutes === 'number' && Number.isFinite(a.gps_plus!.minutes)
  );
  const avgGpMin =
    withGpMin.length === 0
      ? null
      : withGpMin.reduce((s, a) => s + (a.gps_plus!.minutes as number), 0) / withGpMin.length;

  return { cnt, avgM, avgMin, gpCnt, avgGpM, avgGpMin };
}

async function writeDistanceRollupAggregates(
  distanceId: number,
  cnt: number,
  avgM: number | null,
  avgMin: number | null,
  gpCnt: number,
  avgGpM: number | null,
  avgGpMin: number | null
): Promise<void> {
  await execute(
    `UPDATE tbl_distances
     SET gps_sample_count = $2::int,
         gps_avg_distance_m = CASE WHEN $2::int = 0 THEN NULL ELSE $3::int END,
         gps_avg_duration_min = CASE WHEN $2::int = 0 THEN NULL ELSE $4::numeric END,
         gps_averaged_at = now(),
         gps_plus_sample_count = $5::int,
         gps_plus_avg_distance_m = CASE WHEN $5::int = 0 THEN NULL ELSE $6::int END,
         gps_plus_avg_duration_min = CASE WHEN $5::int = 0 THEN NULL ELSE $7::numeric END,
         gps_plus_averaged_at = now(),
         distance_via = CASE
           WHEN $2::int > 0 THEN 'GPSTAGS'
           WHEN $5::int > 0 THEN 'GPS+'
           ELSE 'FAIL'
         END,
         distance_m = CASE
           WHEN $2::int > 0 THEN $3::int
           WHEN $5::int > 0 THEN $6::int
           ELSE NULL
         END,
         duration_min = CASE
           WHEN $2::int > 0 THEN $4::numeric
           WHEN $5::int > 0 THEN $7::numeric
           ELSE NULL
         END,
         updated_at = now()
     WHERE id = $1`,
    [distanceId, cnt, avgM, avgMin, gpCnt, avgGpM, avgGpMin]
  );
}

async function recomputeDistanceRollup(distanceId: number): Promise<void> {
  const agg = await query<{
    cnt: string;
    avg_m: string | null;
    avg_min: string | null;
    gp_cnt: string;
    gp_avg_m: string | null;
    gp_avg_min: string | null;
  }>(
    `SELECT COUNT(*) FILTER (WHERE outcome = 'success' AND meters IS NOT NULL)::text AS cnt,
            AVG(meters) FILTER (WHERE outcome = 'success' AND meters IS NOT NULL)::text AS avg_m,
            AVG(minutes) FILTER (WHERE outcome = 'success' AND minutes IS NOT NULL)::text AS avg_min,
            COUNT(*) FILTER (WHERE gps_plus_outcome = 'success' AND gps_plus_meters IS NOT NULL)::text AS gp_cnt,
            AVG(gps_plus_meters) FILTER (WHERE gps_plus_outcome = 'success' AND gps_plus_meters IS NOT NULL)::text AS gp_avg_m,
            AVG(gps_plus_minutes) FILTER (WHERE gps_plus_outcome = 'success' AND gps_plus_minutes IS NOT NULL)::text AS gp_avg_min
     FROM tbl_distances_gps_samples
     WHERE distance_id = $1`,
    [distanceId]
  );
  const row = agg[0];
  const cnt = parseInt(row?.cnt ?? '0', 10) || 0;
  const avgM = row?.avg_m != null ? Math.round(Number(row.avg_m)) : null;
  const avgMin = row?.avg_min != null ? Number(row.avg_min) : null;
  const gpCnt = parseInt(row?.gp_cnt ?? '0', 10) || 0;
  const avgGpM = row?.gp_avg_m != null ? Math.round(Number(row.gp_avg_m)) : null;
  const avgGpMin = row?.gp_avg_min != null ? Number(row.gp_avg_min) : null;

  await writeDistanceRollupAggregates(distanceId, cnt, avgM, avgMin, gpCnt, avgGpM, avgGpMin);
}

async function loadRollupSummaryFromDb(distanceId: number): Promise<HarvestRunResult['rollup']> {
  const rollRows = await query<{
    distance_via: string | null;
    gps_sample_count: number | string | null;
    gps_plus_sample_count: number | string | null;
    distance_m: number | string | null;
    gps_avg_distance_m: number | string | null;
    gps_plus_avg_distance_m: number | string | null;
  }>(
    `SELECT distance_via,
            gps_sample_count,
            gps_plus_sample_count,
            distance_m,
            gps_avg_distance_m,
            gps_plus_avg_distance_m
     FROM tbl_distances WHERE id = $1`,
    [distanceId]
  );
  const rr = rollRows[0];
  if (!rr) return undefined;
  return {
    distance_via: rr.distance_via != null ? String(rr.distance_via).trim() : null,
    gps_sample_count:
      rr.gps_sample_count != null ? parseInt(String(rr.gps_sample_count), 10) || 0 : null,
    gps_plus_sample_count:
      rr.gps_plus_sample_count != null ? parseInt(String(rr.gps_plus_sample_count), 10) || 0 : null,
    distance_m: rr.distance_m != null ? Math.round(Number(rr.distance_m)) : null,
    gps_avg_distance_m: rr.gps_avg_distance_m != null ? Math.round(Number(rr.gps_avg_distance_m)) : null,
    gps_plus_avg_distance_m:
      rr.gps_plus_avg_distance_m != null ? Math.round(Number(rr.gps_plus_avg_distance_m)) : null,
  };
}

async function upsertHarvestSample(
  distanceId: number,
  attempt: SingleHarvestAttempt,
  runIndex: number
): Promise<void> {
  const success = attempt.ok && attempt.outcome === 'success';
  const outcome = success ? 'success' : 'failed';
  const failureReason = success ? null : (attempt.skipReason ?? 'failed');
  const gp = attempt.gps_plus;
  const gpsPlusOk = gp?.ok === true;
  const gpsPlusOutcome = gp == null ? null : gpsPlusOk ? 'success' : 'failed';
  const gpsPlusMeters = gpsPlusOk ? (gp.meters ?? null) : null;
  const gpsPlusMinutes = gpsPlusOk ? (gp.minutes ?? null) : null;
  const gpsPlusVineyardTrackingId = gpsPlusOk ? (gp.vineyard_hit_tracking_id ?? null) : null;
  const debugForDb = { ...attempt.debug, gps_plus: attempt.gps_plus };
  await execute(
    `INSERT INTO tbl_distances_gps_samples
      (distance_id, job_id, outcome, failure_reason, winery_tracking_id, vineyard_tracking_id,
       meters, minutes, segment_point_count, debug_json, run_index, updated_at,
       gps_plus_meters, gps_plus_minutes, gps_plus_outcome, gps_plus_vineyard_tracking_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, now(),
             $12, $13, $14, $15)
     ON CONFLICT (distance_id, job_id) DO UPDATE SET
       outcome = EXCLUDED.outcome,
       failure_reason = EXCLUDED.failure_reason,
       winery_tracking_id = EXCLUDED.winery_tracking_id,
       vineyard_tracking_id = EXCLUDED.vineyard_tracking_id,
       meters = EXCLUDED.meters,
       minutes = EXCLUDED.minutes,
       segment_point_count = EXCLUDED.segment_point_count,
       debug_json = EXCLUDED.debug_json,
       run_index = EXCLUDED.run_index,
       gps_plus_meters = EXCLUDED.gps_plus_meters,
       gps_plus_minutes = EXCLUDED.gps_plus_minutes,
       gps_plus_outcome = EXCLUDED.gps_plus_outcome,
       gps_plus_vineyard_tracking_id = EXCLUDED.gps_plus_vineyard_tracking_id,
       updated_at = now()`,
    [
      distanceId,
      attempt.job_id,
      outcome,
      failureReason,
      attempt.winery_tracking_id ?? null,
      attempt.vineyard_tracking_id ?? null,
      attempt.meters ?? null,
      attempt.minutes ?? null,
      attempt.segment_point_count ?? null,
      jsonSafeForDb(debugForDb),
      runIndex,
      gpsPlusMeters,
      gpsPlusMinutes,
      gpsPlusOutcome,
      gpsPlusVineyardTrackingId,
    ]
  );
}

export type HarvestCandidateJob = {
  job_id: string;
  actual_start_time: string | null;
  worker: string | null;
};

export type HarvestRunResult = {
  distanceId: number;
  persist: boolean;
  attempts: SingleHarvestAttempt[];
  insertedOrUpdated: number;
  jobsPolled: number;
  successCount: number;
  failedCount: number;
  jobMatchDebug?: {
    countSql: string;
    listSql: string;
    params: unknown[];
    vworkJobsMatchCount: number;
    candidateJobIds: string[];
  };
  /** Caps used for this run (UI: successes vs target, jobs tried vs max). */
  maxJobsCap: number;
  maxSuccessesCap: number;
  /** Pair from tbl_distances (what we match in tbl_vworkjobs). */
  distancePair: { delivery_winery: string; vineyard_name: string };
  /** Rows in tbl_vworkjobs matching the pair (case-insensitive trim). */
  vworkJobsMatchCount: number;
  /** Jobs we would consider (same order as harvest); harvest polls the first maxJobs. */
  candidateJobs: HarvestCandidateJob[];
  message?: string;
  /** After persist + rollup: how distance_m was chosen (null if dry-run or no rows written). */
  rollup?: {
    distance_via: string | null;
    gps_sample_count: number | null;
    gps_plus_sample_count: number | null;
    distance_m: number | null;
    gps_avg_distance_m: number | null;
    gps_plus_avg_distance_m: number | null;
  };
  /** Dry run with dryRunUpdateDistanceRow: tbl_distances row was updated from attempt aggregates (samples not written). */
  dryDistanceRowUpdated?: boolean;
};

export async function runGpsHarvestForDistanceId(
  distanceId: number,
  options: {
    persist: boolean;
    /**
     * When persisting, clear existing samples for this distance_id first.
     * This is important when ENTER/EXIT tagging improves and we want fresh rollups.
     */
    resetExistingSamples?: boolean;
    startLessMinutes?: number;
    endPlusMinutes?: number;
    windowMinutes?: number;
    maxJobs?: number;
    maxSuccesses?: number;
    /**
     * When persist is false (dry run): if true, still UPDATE tbl_distances rollups + distance_m / duration_min / distance_via
     * from this run's attempts only (does not write tbl_distances_gps_samples).
     */
    dryRunUpdateDistanceRow?: boolean;
  }
): Promise<HarvestRunResult> {
  const startLess = options.startLessMinutes ?? DEFAULT_HARVEST_START_LESS_MINUTES;
  const endPlus = options.endPlusMinutes ?? DEFAULT_HARVEST_END_PLUS_MINUTES;
  const windowMinutes = options.windowMinutes ?? DEFAULT_HARVEST_WINDOW_MINUTES;
  const maxJobsCap = Math.min(
    GPS_HARVEST_SQL_JOB_LIMIT,
    Math.max(1, options.maxJobs ?? GPS_HARVEST_DEFAULT_MAX_JOBS)
  );
  const maxSuccessesCap = Math.min(
    GPS_HARVEST_SQL_JOB_LIMIT,
    Math.max(1, options.maxSuccesses ?? GPS_HARVEST_DEFAULT_MAX_SUCCESSES)
  );

  const distRows = await query<{ delivery_winery: string; vineyard_name: string }>(
    `SELECT delivery_winery, vineyard_name FROM tbl_distances WHERE id = $1`,
    [distanceId]
  );
  const d0 = distRows[0];
  if (!d0) {
    return {
      distanceId,
      persist: options.persist,
      attempts: [],
      insertedOrUpdated: 0,
      jobsPolled: 0,
      successCount: 0,
      failedCount: 0,
      maxJobsCap,
      maxSuccessesCap,
      distancePair: { delivery_winery: '', vineyard_name: '' },
      vworkJobsMatchCount: 0,
      candidateJobs: [],
      message: 'distance_id not found',
    };
  }

  const jobMatchSql = `EXISTS (
    SELECT 1 FROM tbl_distances d
    WHERE d.id = $1
      AND lower(trim(COALESCE(j.delivery_winery, ''))) = lower(trim(COALESCE(d.delivery_winery, '')))
      AND lower(trim(COALESCE(j.vineyard_name, ''))) = lower(trim(COALESCE(d.vineyard_name, '')))
  )`;

  const countSql = `SELECT COUNT(*)::text AS c FROM tbl_vworkjobs j WHERE ${jobMatchSql}`;
  const listSql = `SELECT j.* FROM tbl_vworkjobs j WHERE ${jobMatchSql} ORDER BY j.job_id ASC NULLS LAST LIMIT ${GPS_HARVEST_SQL_JOB_LIMIT}`;

  const [countRow, jobs] = await Promise.all([
    query<{ c: string }>(
      countSql,
      [distanceId]
    ),
    query<Record<string, unknown>>(
      listSql,
      [distanceId]
    ),
  ]);

  const vworkJobsMatchCount = parseInt(countRow[0]?.c ?? '0', 10) || 0;

  const candidateJobs: HarvestCandidateJob[] = jobs.map((raw) => {
    const jid = raw.job_id != null ? String(raw.job_id).trim() : '';
    const w = pickJobField(raw, 'worker', 'Worker');
    const ast = pickJobField(raw, 'actual_start_time', 'Actual_Start_Time');
    return {
      job_id: jid,
      worker: w != null ? String(w) : null,
      actual_start_time: ast != null ? String(ast) : null,
    };
  });

  const attempts: SingleHarvestAttempt[] = [];
  let insertedOrUpdated = 0;
  let jobsTried = 0;
  let successCount = 0;
  let failedCount = 0;
  let dynamicSuccessCap = maxSuccessesCap;
  let extendedOnce = false;

  if (options.persist && options.resetExistingSamples) {
    await execute(`DELETE FROM tbl_distances_gps_samples WHERE distance_id = $1`, [distanceId]);
  }

  for (const raw of jobs) {
    if (jobsTried >= maxJobsCap) break;
    if (successCount >= dynamicSuccessCap) {
      if (!extendedOnce) {
        const okMeters = attempts
          .filter((a) => a.ok && a.outcome === 'success' && typeof a.meters === 'number' && Number.isFinite(a.meters))
          .map((a) => a.meters as number);
        if (okMeters.length >= maxSuccessesCap) {
          const min = Math.min(...okMeters);
          const max = Math.max(...okMeters);
          const avg = okMeters.reduce((s, x) => s + x, 0) / okMeters.length;
          const spreadRatio = avg > 0 ? (max - min) / avg : 0;
          if (spreadRatio >= 0.2) {
            dynamicSuccessCap = Math.min(GPS_HARVEST_SQL_JOB_LIMIT, dynamicSuccessCap + 3);
            extendedOnce = true;
          }
        }
      }
      if (successCount >= dynamicSuccessCap) break;
    }
    jobsTried++;

    const jid = raw.job_id != null ? String(raw.job_id).trim() : '';
    if (!jid) {
      attempts.push({
        job_id: '(no job_id)',
        ok: false,
        outcome: 'failed',
        skipReason: 'missing job_id on vwork row',
        gps_plus: { ok: false, skipReason: 'missing job_id on vwork row' },
        debug: { raw_keys: Object.keys(raw) },
      });
      failedCount++;
      continue;
    }

    const attempt = await harvestWineryToVineyardForJob(raw, {
      startLessMinutes: startLess,
      endPlusMinutes: endPlus,
      windowMinutes,
    });
    attempts.push(attempt);
    if (attempt.ok) successCount++;
    else failedCount++;

    if (options.persist) {
      await upsertHarvestSample(distanceId, attempt, jobsTried);
      insertedOrUpdated++;
    }
  }

  let rollup: HarvestRunResult['rollup'] = undefined;
  let dryDistanceRowUpdated = false;

  if (options.persist && insertedOrUpdated > 0) {
    await recomputeDistanceRollup(distanceId);
    rollup = await loadRollupSummaryFromDb(distanceId);
    if (rollup) {
      console.log(
        `[gps-harvest] distance_id=${distanceId} ${d0.delivery_winery} → ${d0.vineyard_name} ` +
          `distance_via=${rollup.distance_via ?? 'null'} ` +
          `tag_samples=${rollup.gps_sample_count ?? 0} gps+_samples=${rollup.gps_plus_sample_count ?? 0} ` +
          `distance_m=${rollup.distance_m ?? 'null'} ` +
          `avg_tag_m=${rollup.gps_avg_distance_m ?? 'null'} avg_gps+_m=${rollup.gps_plus_avg_distance_m ?? 'null'}`
      );
    }
  } else if (!options.persist && options.dryRunUpdateDistanceRow === true && attempts.length > 0) {
    const s = computeRollupStatsFromAttempts(attempts);
    await writeDistanceRollupAggregates(distanceId, s.cnt, s.avgM, s.avgMin, s.gpCnt, s.avgGpM, s.avgGpMin);
    rollup = await loadRollupSummaryFromDb(distanceId);
    dryDistanceRowUpdated = true;
    if (rollup) {
      console.log(
        `[gps-harvest] dry→row distance_id=${distanceId} ${d0.delivery_winery} → ${d0.vineyard_name} ` +
          `distance_via=${rollup.distance_via ?? 'null'} ` +
          `tag_samples=${rollup.gps_sample_count ?? 0} gps+_samples=${rollup.gps_plus_sample_count ?? 0} ` +
          `distance_m=${rollup.distance_m ?? 'null'}`
      );
    }
  }

  let message: string | undefined;
  if (vworkJobsMatchCount === 0) {
    message =
      'No tbl_vworkjobs rows match this winery+vineyard pair (case-insensitive trim, same rule as the grid). ' +
      'tbl_distances pairs must match job delivery_winery and vineyard_name — re-run Seed or fix names. ' +
      'tbl_gpsmappings expands fence names for GPS paths only, not vWork job matching.';
  } else if (jobsTried === 0 && vworkJobsMatchCount > 0) {
    message = 'Unexpected: jobs matched but none polled; report as bug.';
  }

  return {
    distanceId,
    persist: options.persist,
    attempts,
    insertedOrUpdated,
    jobsPolled: jobsTried,
    successCount,
    failedCount,
    jobMatchDebug: {
      countSql,
      listSql,
      params: [distanceId],
      vworkJobsMatchCount,
      candidateJobIds: candidateJobs.map((c) => c.job_id).filter(Boolean),
    },
    maxJobsCap,
    maxSuccessesCap,
    distancePair: { delivery_winery: d0.delivery_winery, vineyard_name: d0.vineyard_name },
    vworkJobsMatchCount,
    candidateJobs,
    message,
    rollup,
    dryDistanceRowUpdated: dryDistanceRowUpdated || undefined,
  };
}

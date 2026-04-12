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
import { runStepsPlusQuery } from '@/lib/steps-plus-query';
import { getStepsPlusSettings } from '@/lib/steps-plus-settings';
import {
  DEFAULT_HARVEST_END_PLUS_MINUTES,
  DEFAULT_HARVEST_START_LESS_MINUTES,
  DEFAULT_HARVEST_WINDOW_MINUTES,
  GPS_HARVEST_DEFAULT_MAX_JOBS,
  GPS_HARVEST_DEFAULT_MAX_SUCCESSES,
  GPS_HARVEST_SQL_JOB_LIMIT,
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
      debug: {
        ...enrichDebugBase(rawJobRow, win, opts, trackingRows),
        window_error: win.error,
      },
    };
  }

  const anchors = await fetchOutboundLegAnchors(job, {
    windowMinutes: opts.windowMinutes,
    device: win.device,
    positionAfter: win.positionAfter,
    positionBefore: win.positionBefore,
  });

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

async function recomputeDistanceRollup(distanceId: number): Promise<void> {
  const agg = await query<{ cnt: string; avg_m: string | null; avg_min: string | null }>(
    `SELECT COUNT(*)::text AS cnt,
            AVG(meters)::text AS avg_m,
            AVG(minutes)::text AS avg_min
     FROM tbl_distances_gps_samples
     WHERE distance_id = $1
       AND outcome = 'success'
       AND meters IS NOT NULL`,
    [distanceId]
  );
  const row = agg[0];
  const cnt = parseInt(row?.cnt ?? '0', 10) || 0;
  const avgM = row?.avg_m != null ? Math.round(Number(row.avg_m)) : null;
  const avgMin = row?.avg_min != null ? Number(row.avg_min) : null;
  if (cnt === 0) {
    await execute(
      `UPDATE tbl_distances
       SET gps_sample_count = 0,
           gps_avg_distance_m = NULL,
           gps_avg_duration_min = NULL,
           gps_averaged_at = now(),
           updated_at = now()
       WHERE id = $1`,
      [distanceId]
    );
    return;
  }
  await execute(
    `UPDATE tbl_distances
     SET gps_sample_count = $2,
         gps_avg_distance_m = $3,
         gps_avg_duration_min = $4,
         gps_averaged_at = now(),
         distance_m = $3,
         duration_min = $4,
         updated_at = now()
     WHERE id = $1`,
    [distanceId, cnt, avgM, avgMin]
  );
}

async function upsertHarvestSample(
  distanceId: number,
  attempt: SingleHarvestAttempt,
  runIndex: number
): Promise<void> {
  const success = attempt.ok && attempt.outcome === 'success';
  const outcome = success ? 'success' : 'failed';
  const failureReason = success ? null : (attempt.skipReason ?? 'failed');
  await execute(
    `INSERT INTO tbl_distances_gps_samples
      (distance_id, job_id, outcome, failure_reason, winery_tracking_id, vineyard_tracking_id,
       meters, minutes, segment_point_count, debug_json, run_index, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, now())
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
      jsonSafeForDb(attempt.debug),
      runIndex,
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

  const pairW = String(d0.delivery_winery ?? '').trim();
  const pairV = String(d0.vineyard_name ?? '').trim();

  const [countRow, jobs] = await Promise.all([
    query<{ c: string }>(
      `SELECT COUNT(*)::text AS c
       FROM tbl_vworkjobs
       WHERE lower(trim(COALESCE(delivery_winery, ''))) = lower($1::text)
         AND lower(trim(COALESCE(vineyard_name, ''))) = lower($2::text)`,
      [pairW, pairV]
    ),
    query<Record<string, unknown>>(
      `SELECT *
       FROM tbl_vworkjobs
       WHERE lower(trim(COALESCE(delivery_winery, ''))) = lower($1::text)
         AND lower(trim(COALESCE(vineyard_name, ''))) = lower($2::text)
       ORDER BY job_id ASC NULLS LAST
       LIMIT ${GPS_HARVEST_SQL_JOB_LIMIT}`,
      [pairW, pairV]
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

  if (options.persist && options.resetExistingSamples) {
    await execute(`DELETE FROM tbl_distances_gps_samples WHERE distance_id = $1`, [distanceId]);
  }

  for (const raw of jobs) {
    if (jobsTried >= maxJobsCap) break;
    if (successCount >= maxSuccessesCap) break;
    jobsTried++;

    const jid = raw.job_id != null ? String(raw.job_id).trim() : '';
    if (!jid) {
      attempts.push({
        job_id: '(no job_id)',
        ok: false,
        outcome: 'failed',
        skipReason: 'missing job_id on vwork row',
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

  if (options.persist && insertedOrUpdated > 0) {
    await recomputeDistanceRollup(distanceId);
  }

  let message: string | undefined;
  if (vworkJobsMatchCount === 0) {
    message =
      'No tbl_vworkjobs rows match this winery+vineyard pair (case-insensitive trim). ' +
      'tbl_distances pairs must exist on at least one vWork job — re-run Seed or fix names.';
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
    maxJobsCap,
    maxSuccessesCap,
    distancePair: { delivery_winery: d0.delivery_winery, vineyard_name: d0.vineyard_name },
    vworkJobsMatchCount,
    candidateJobs,
    message,
  };
}

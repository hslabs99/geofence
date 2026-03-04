import type { PrismaClient } from '@prisma/client';

/** Same as tracking API: parse to YYYY-MM-DD HH:mm:ss only — no timezone in output. Handles Date (e.g. from DB) and strings; strips GMT+1300 etc. so PostgreSQL never sees them. */
function normalizeTimestampString(s: string | Date | null): string | null {
  if (s == null) return null;
  let t: string = typeof s === 'string' ? s.trim() : s.toString().trim();
  // Strip timezone suffix so we never pass "GMT+1300" etc. to PostgreSQL (not recognized)
  t = t.replace(/\s+(?:GMT|UTC)[+-]\d{3,4}.*$/i, '').trim();
  if (!t) return null;

  const dmy = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2}):(\d{2})/);
  if (dmy) {
    const yy = dmy[3].length === 2 ? `20${dmy[3]}` : dmy[3];
    return `${yy}-${dmy[2].padStart(2, '0')}-${dmy[1].padStart(2, '0')} ${dmy[4].padStart(2, '0')}:${dmy[5]}:${dmy[6]}`;
  }
  const iso = t.match(/^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2})?:?(\d{2})?:?(\d{2})?/);
  if (iso) {
    const h = iso[4]?.padStart(2, '0') ?? '00';
    const m = iso[5]?.padStart(2, '0') ?? '00';
    const sec = iso[6]?.padStart(2, '0') ?? '00';
    return `${iso[1]}-${iso[2]}-${iso[3]} ${h}:${m}:${sec}`;
  }
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(t)) return t.slice(0, 19);
  // Date.toString() style e.g. "Tue Feb 19 2026 21:08:34" (after stripping GMT+1300)
  const d = new Date(t);
  if (!Number.isNaN(d.getTime())) {
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }
  return t.slice(0, 19);
}

export type FenceResolutionDebug = {
  type: 'Vineyard' | 'Winery';
  vworkName: string;
  /** Rows from tbl_gpsmappings WHERE type = ... AND (vwname = vworkName OR gpsname = vworkName) */
  mappingsFound: { vwname: string | null; gpsname: string | null }[];
  /** List used in IN (...): [original vwork name, ...gpsnames from mappings] */
  fenceNamesInList: string[];
  /** All fence_ids from tbl_geofences WHERE fence_name IN (...); we query tracking for any of these */
  fenceIds: number[];
  /** Human-readable: fence_name per fence_id (for debug) */
  resolvedFenceNames: { fence_id: number; fence_name: string | null }[];
};

/** Get all fence_ids from tbl_geofences for names list (original vwork name + mapped gpsnames). Used so we match any fence in the list. */
async function getFenceIdsForVworkNameWithDebug(
  prisma: PrismaClient,
  type: 'Vineyard' | 'Winery',
  vworkName: string
): Promise<{ fenceIds: number[]; debug: FenceResolutionDebug }> {
  const debug: FenceResolutionDebug = {
    type,
    vworkName: vworkName.trim(),
    mappingsFound: [],
    fenceNamesInList: [],
    fenceIds: [],
    resolvedFenceNames: [],
  };

  if (!vworkName.trim()) {
    return { fenceIds: [], debug };
  }

  const mappings = await prisma.$queryRawUnsafe<
    { type: string; vwname: string | null; gpsname: string | null }[]
  >(
    `SELECT type, vwname, gpsname FROM tbl_gpsmappings WHERE type = $1 AND (TRIM(vwname) = $2 OR TRIM(gpsname) = $2)`,
    type,
    vworkName.trim()
  );

  debug.mappingsFound = mappings.map((m) => ({ vwname: m.vwname, gpsname: m.gpsname }));

  const names: string[] = [vworkName.trim()];
  for (const m of mappings) {
    const gps = (m.gpsname ?? '').trim();
    if (gps && !names.includes(gps)) names.push(gps);
  }
  debug.fenceNamesInList = [...names];

  const escaped = names.map((n) => `'${String(n).replace(/'/g, "''")}'`);
  const rows = await prisma.$queryRawUnsafe<{ fence_id: number | bigint; fence_name: string | null }[]>(
    `SELECT fence_id, fence_name FROM tbl_geofences WHERE fence_name IN (${escaped.join(', ')})`
  );
  debug.resolvedFenceNames = rows.map((r) => ({ fence_id: Number(r.fence_id), fence_name: r.fence_name }));
  debug.fenceIds = rows.map((r) => Number(r.fence_id));

  return { fenceIds: debug.fenceIds, debug };
}

export type TrackingLookupDebug = {
  device: string;
  positionAfter: string;
  positionBefore: string | null;
  fenceIds: number[];
  geofenceType: 'ENTER' | 'EXIT';
  found: boolean;
  position_time_nz: string | null;
  /** tbl_tracking row id for the matched row */
  trackingId: number | null;
  /** Approximate SQL used for traceability */
  sqlHint: string;
};

/** First (or last if orderDesc) tracking row in window at any of the given fences with given geofence_type (ENTER or EXIT). */
async function getFirstTrackingInWindowWithDebug(
  prisma: PrismaClient,
  device: string,
  positionAfter: string,
  positionBefore: string | null,
  fenceIds: number[],
  geofenceType: 'ENTER' | 'EXIT',
  orderDesc = false
): Promise<{ value: string | null; trackingId: number | null; debug: TrackingLookupDebug }> {
  const escapedDevice = String(device).replace(/'/g, "''");
  const rawAfter = normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19);
  const escapedAfter = String(rawAfter).replace(/'/g, "''");
  const rawBefore = positionBefore ? (normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19)) : null;
  const escapedBefore = rawBefore ? String(rawBefore).replace(/'/g, "''") : null;
  // Same condition as tbl_tracking API: literal comparison, no cast, no timezone
  const timeCondition = escapedBefore
    ? `t.position_time_nz > '${escapedAfter}' AND t.position_time_nz < '${escapedBefore}'`
    : `t.position_time_nz > '${escapedAfter}'`;

  if (fenceIds.length === 0) {
    return {
      value: null,
      trackingId: null,
      debug: {
        device,
        positionAfter,
        positionBefore,
        fenceIds: [],
        geofenceType,
        found: false,
        position_time_nz: null,
        trackingId: null,
        sqlHint: `(no fence_ids)`,
      },
    };
  }

  const orderClause = orderDesc ? 'DESC' : 'ASC';
  const fenceIdList = fenceIds.map((id) => Number(id)).join(', ');
  const sqlHint = `SELECT t.id, t.position_time_nz FROM tbl_tracking t WHERE t.device_name='...' AND t.geofence_id IN (${fenceIdList}) AND t.geofence_type='${geofenceType}' AND ${timeCondition} ORDER BY t.position_time_nz ${orderClause} LIMIT 1`;

  const rows = await prisma.$queryRawUnsafe<{ id: unknown; position_time_nz: unknown }[]>(
    `SELECT t.id, t.position_time_nz FROM tbl_tracking t
     WHERE t.device_name = '${escapedDevice}' AND t.geofence_id IN (${fenceIdList}) AND t.geofence_type = '${geofenceType}'
     AND ${timeCondition}
     ORDER BY t.position_time_nz ${orderClause} LIMIT 1`
  );

  const val = rows[0]?.position_time_nz;
  let value: string | null = null;
  if (val != null) {
    if (typeof val === 'string') value = normalizeTimestampString(val) ?? val.slice(0, 19);
    else if (val instanceof Date) value = val.toISOString().replace('T', ' ').slice(0, 19);
    else value = String(val).slice(0, 19);
  }
  const rawId = rows[0]?.id;
  const trackingId = rawId != null && typeof rawId === 'number' ? rawId : (rawId != null && (typeof rawId === 'string' || typeof rawId === 'bigint') ? Number(rawId) : null);
  const trackingIdSafe = Number.isFinite(trackingId) ? (trackingId as number) : null;

  const debug: TrackingLookupDebug = {
    device,
    positionAfter,
    positionBefore,
    fenceIds: [...fenceIds],
    geofenceType,
    found: value != null,
    position_time_nz: value,
    trackingId: trackingIdSafe,
    sqlHint,
  };

  return { value, trackingId: trackingIdSafe, debug };
}

export type JobForDerivedSteps = {
  job_id: unknown;
  vineyard_name?: string | null;
  delivery_winery?: string | null;
  truck_id?: string | null;
  /** Worker = device_name for tbl_tracking (GPS records keyed by worker, not truck_id). */
  worker?: string | null;
  actual_start_time?: string | null;
  actual_end_time?: string | null;
  /** VWork step 5 (job completed in system). Use this for step 5 rule: only use GPS when Winery EXIT < this value. */
  step_5_completed_at?: string | null;
};

/** Part 1: A single fetched GPS candidate (value + tracking id). No decision applied. */
export type GpsStepCandidate = { value: string; trackingId: number | null };

/** Part 1 output: Raw GPS candidates per step (winery/vineyard, ENTER/EXIT). Steps 1–4: use if present. Step 5: use only if value < VWork step 5 (decided in Part 2). */
export type FetchedGpsCandidates = {
  step1: GpsStepCandidate | null;
  step2: GpsStepCandidate | null;
  step3: GpsStepCandidate | null;
  step4: GpsStepCandidate | null;
  step5: GpsStepCandidate | null;
};

export type DerivedStepsResult = {
  step1: string | null; // Job start by GPS: first Winery EXIT before Vineyard ENTER (step 2); may be null if job started after leaving fence
  step2: string | null; // Vineyard ENTER (Arrive Vineyard)
  step3: string | null; // Vineyard EXIT (Depart Vineyard)
  step4: string | null; // Winery ENTER (Arrive Winery)
  step5: string | null; // Job end by GPS: first Winery EXIT after step 4, within data window, and < VWork step 5 (forgot to end); in most jobs null
  step1TrackingId: number | null;
  step2TrackingId: number | null;
  step3TrackingId: number | null;
  step4TrackingId: number | null;
  step5TrackingId: number | null;
};

export type DerivedStepsDebug = {
  jobId: string;
  windowMinutes: number;
  truckId: string;
  actualStartTime: string;
  actualEndTime: string | null;
  positionAfter: string;
  positionBefore: string | null;
  vineyard: FenceResolutionDebug & {
    step2?: TrackingLookupDebug;
    step3?: TrackingLookupDebug;
  };
  winery: FenceResolutionDebug & {
    step1?: TrackingLookupDebug; // Winery EXIT before Vineyard ENTER
    step4?: TrackingLookupDebug;
    step5?: TrackingLookupDebug; // Winery EXIT after step 4, before job end = GPS job end
  };
};

export type DerivedStepsResultWithDebug = DerivedStepsResult & { debug: DerivedStepsDebug };

export type DerivedStepsOptions = {
  windowMinutes: number;
  /** Same as tbl_tracking API: device = tbl_vworkjobs.worker (device_name for tbl_tracking). */
  device: string;
  positionAfter: string;
  positionBefore: string | null;
};

/**
 * STEP RULES (GPS-derived steps)
 * ------------------------------
 * Part 1 — Fetch: Get a valid (or probably valid) GPS entry (Winery/Vineyard, ENTER/EXIT). Part 2 — Decide: Steps 1–4 use GPS if exists else VWork; Step 5 use GPS only if GPS < VWork.
 * Step 1 — Job start by GPS: First Winery EXIT that occurs BEFORE Vineyard ENTER (step 2).
 *   - Meaning: job start = when driver left the winery (before arriving at vineyard).
 *   - May be absent if the job started after the driver had already left the winery fence.
 * Step 2 — Arrive vineyard: First Vineyard ENTER in window.
 * Step 3 — Leave vineyard: First Vineyard EXIT in window.
 * Step 4 — Arrive winery: First Winery ENTER that (a) is in the data window and (b) is after max(step2, step3) if both exist; else only (a). Only step 5 (Winery EXIT) is subject to step 5 times.
 * Step 5 — Job end by GPS: VWork step 5 = step_5_completed_at (job completed in system). Only use GPS when Winery EXIT is strictly < VWork step 5 (gpsNorm < vworkStep5). IF VWork step 5 > step 4, AND we have a Winery EXIT that is > step 4, within the data window, and < VWork step 5, we use that Winery EXIT (driver left winery after returning from vineyard and forgot to end job). We take the FIRST such Winery EXIT (not the last), so we get the “forgot to end” exit, not the exit at job end. Search window capped at job end and at positionBefore (data window). In most jobs this does not apply (step5 null).
 *
 * Derive GPS step timestamps for a job using these rules. Window is passed in (same as tbl_tracking UI)
 * — no server-side timezone or date logic. Uses tbl_gpsmappings + original vwork name → tbl_geofences
 * → fence_ids; then scans tbl_tracking in window.
 */

/** Part 1: Fetch GPS candidates for each step (winery/vineyard, ENTER/EXIT). No decision — just lookups. */
async function fetchGpsStepCandidates(
  prisma: PrismaClient,
  job: JobForDerivedSteps,
  options: DerivedStepsOptions,
  debug: DerivedStepsDebug
): Promise<FetchedGpsCandidates> {
  const candidates: FetchedGpsCandidates = { step1: null, step2: null, step3: null, step4: null, step5: null };
  const { device: truckId, positionAfter, positionBefore } = options;
  const vineyardName = job.vineyard_name ? String(job.vineyard_name).trim() : '';
  const deliveryWinery = job.delivery_winery ? String(job.delivery_winery).trim() : '';
  let step2Value: string | null = null;
  let step3Value: string | null = null;
  let step4Value: string | null = null;

  if (vineyardName) {
    const { fenceIds: vineyardFenceIds, debug: vineyardDebug } = await getFenceIdsForVworkNameWithDebug(prisma, 'Vineyard', vineyardName);
    Object.assign(debug.vineyard, vineyardDebug);
    if (vineyardFenceIds.length > 0) {
      const step2Result = await getFirstTrackingInWindowWithDebug(prisma, truckId, positionAfter, positionBefore, vineyardFenceIds, 'ENTER');
      step2Value = step2Result.value;
      debug.vineyard.step2 = step2Result.debug;
      if (step2Result.value != null) candidates.step2 = { value: step2Result.value, trackingId: step2Result.trackingId };
      const step3Result = await getFirstTrackingInWindowWithDebug(prisma, truckId, positionAfter, positionBefore, vineyardFenceIds, 'EXIT');
      step3Value = step3Result.value;
      debug.vineyard.step3 = step3Result.debug;
      if (step3Result.value != null) candidates.step3 = { value: step3Result.value, trackingId: step3Result.trackingId };
    }
  }

  if (deliveryWinery) {
    const { fenceIds: wineryFenceIds, debug: wineryDebug } = await getFenceIdsForVworkNameWithDebug(prisma, 'Winery', deliveryWinery);
    Object.assign(debug.winery, wineryDebug);
    if (wineryFenceIds.length > 0) {
      const step1Before = step2Value ?? positionBefore;
      const step1Result = await getFirstTrackingInWindowWithDebug(prisma, truckId, positionAfter, step1Before, wineryFenceIds, 'EXIT');
      debug.winery.step1 = step1Result.debug;
      if (step1Result.value != null) candidates.step1 = { value: step1Result.value, trackingId: step1Result.trackingId };
      /** First Winery ENTER in window; if both step2 and step3 exist, must be after max(step2, step3). */
      const step4After =
        step2Value != null && step3Value != null
          ? (step2Value > step3Value ? step2Value : step3Value)
          : positionAfter;
      const step4Result = await getFirstTrackingInWindowWithDebug(prisma, truckId, step4After, positionBefore, wineryFenceIds, 'ENTER');
      step4Value = step4Result.value;
      debug.winery.step4 = step4Result.debug;
      if (step4Result.value != null) candidates.step4 = { value: step4Result.value, trackingId: step4Result.trackingId };
      const vworkStep5 = (job.step_5_completed_at ?? job.actual_end_time) != null
        ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
        : null;
      const step5WindowEnd = vworkStep5 != null ? (positionBefore != null && positionBefore < vworkStep5 ? positionBefore : vworkStep5) : positionBefore;
      if (step4Value != null && vworkStep5 != null && step5WindowEnd != null && vworkStep5 > step4Value) {
        const step5Result = await getFirstTrackingInWindowWithDebug(prisma, truckId, step4Value, step5WindowEnd, wineryFenceIds, 'EXIT', false);
        debug.winery.step5 = step5Result.debug;
        if (step5Result.value != null) candidates.step5 = { value: step5Result.value, trackingId: step5Result.trackingId };
      }
    }
  }
  return candidates;
}

/** Part 2: Decide which date (VWork or GPS) gets used in final. Steps 1–4: if GPS exists use GPS else VWork. Step 5: use GPS only if GPS < VWork step 5. */
function decideFinalSteps(candidates: FetchedGpsCandidates, job: JobForDerivedSteps): DerivedStepsResult {
  const result: DerivedStepsResult = {
    step1: null, step2: null, step3: null, step4: null, step5: null,
    step1TrackingId: null, step2TrackingId: null, step3TrackingId: null, step4TrackingId: null, step5TrackingId: null,
  };
  const vworkStep5 = (job.step_5_completed_at ?? job.actual_end_time) != null
    ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
    : null;
  if (candidates.step1) { result.step1 = candidates.step1.value; result.step1TrackingId = candidates.step1.trackingId; }
  if (candidates.step2) { result.step2 = candidates.step2.value; result.step2TrackingId = candidates.step2.trackingId; }
  if (candidates.step3) { result.step3 = candidates.step3.value; result.step3TrackingId = candidates.step3.trackingId; }
  if (candidates.step4) { result.step4 = candidates.step4.value; result.step4TrackingId = candidates.step4.trackingId; }
  if (candidates.step5 && vworkStep5 != null) {
    const gpsNorm = normalizeTimestampString(candidates.step5.value);
    if (gpsNorm != null && gpsNorm < vworkStep5) {
      result.step5 = candidates.step5.value;
      result.step5TrackingId = candidates.step5.trackingId;
    }
  }
  return result;
}

export async function deriveGpsStepsForJob(
  prisma: PrismaClient,
  job: JobForDerivedSteps,
  options: DerivedStepsOptions
): Promise<DerivedStepsResultWithDebug> {
  const emptyResult: DerivedStepsResult = {
    step1: null, step2: null, step3: null, step4: null, step5: null,
    step1TrackingId: null, step2TrackingId: null, step3TrackingId: null, step4TrackingId: null, step5TrackingId: null,
  };
  const debug: DerivedStepsDebug = {
    jobId: String(job.job_id ?? ''),
    windowMinutes: options.windowMinutes,
    truckId: options.device,
    actualStartTime: '',
    actualEndTime: null,
    positionAfter: options.positionAfter,
    positionBefore: options.positionBefore,
    vineyard: { type: 'Vineyard', vworkName: '', mappingsFound: [], fenceNamesInList: [], fenceIds: [], resolvedFenceNames: [] },
    winery: { type: 'Winery', vworkName: '', mappingsFound: [], fenceNamesInList: [], fenceIds: [], resolvedFenceNames: [] },
  };
  if (!options.device || !options.positionAfter) return { ...emptyResult, debug };
  const candidates = await fetchGpsStepCandidates(prisma, job, options, debug);
  const result = decideFinalSteps(candidates, job);
  return { ...result, debug };
}

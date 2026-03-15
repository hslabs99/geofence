import { query } from '@/lib/db';
import { dateToLiteral } from '@/lib/utils';

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

  const mappings = await query<{ type: string; vwname: string | null; gpsname: string | null }>(
    'SELECT type, vwname, gpsname FROM tbl_gpsmappings WHERE type = $1 AND (TRIM(vwname) = $2 OR TRIM(gpsname) = $2)',
    [type, vworkName.trim()]
  );

  debug.mappingsFound = mappings.map((m) => ({ vwname: m.vwname, gpsname: m.gpsname }));

  const names: string[] = [vworkName.trim()];
  for (const m of mappings) {
    const gps = (m.gpsname ?? '').trim();
    if (gps && !names.includes(gps)) names.push(gps);
  }
  debug.fenceNamesInList = [...names];

  if (names.length === 0) return { fenceIds: [], debug };
  const rows = await query<{ fence_id: number | string; fence_name: string | null }>(
    'SELECT fence_id, fence_name FROM tbl_geofences WHERE fence_name = ANY($1::text[])',
    [names]
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
  device: string,
  positionAfter: string,
  positionBefore: string | null,
  fenceIds: number[],
  geofenceType: 'ENTER' | 'EXIT',
  orderDesc = false
): Promise<{ value: string | null; trackingId: number | null; debug: TrackingLookupDebug }> {
  const rawAfter = normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19);
  const rawBefore = positionBefore ? (normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19)) : null;

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
  const sqlHint = `SELECT t.id, t.position_time_nz FROM tbl_tracking t WHERE t.device_name=$1 AND t.geofence_id = ANY($2) AND t.geofence_type=$3 AND position_time_nz > $4 ... ORDER BY t.position_time_nz ${orderClause} LIMIT 1`;

  const params: unknown[] = [device, fenceIds, geofenceType, rawAfter];
  let timeCondition = 't.position_time_nz > $4';
  if (rawBefore) {
    params.push(rawBefore);
    timeCondition += ' AND t.position_time_nz < $5';
  }

  const rows = await query<{ id: unknown; position_time_nz: unknown }>(
    `SELECT t.id, to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz FROM tbl_tracking t
     WHERE t.device_name = $1 AND t.geofence_id = ANY($2::int[]) AND t.geofence_type = $3 AND ${timeCondition}
     ORDER BY t.position_time_nz ${orderClause} LIMIT 1`,
    params
  );

  const val = rows[0]?.position_time_nz;
  let value: string | null = null;
  if (val != null) {
    if (typeof val === 'string') value = normalizeTimestampString(val) ?? val.slice(0, 19);
    else if (val instanceof Date) value = dateToLiteral(val);
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
  /** VWork-reported step 1 (job start). Used in cleanup: if step_1_completed_at > step2_gps we apply step1 = step2_gps - 20 min. */
  step_1_completed_at?: string | null;
  /** Manual overrides (Part 3): if set, trump GPS/VWork for that step. */
  step1oride?: string | null;
  step2oride?: string | null;
  step3oride?: string | null;
  step4oride?: string | null;
  step5oride?: string | null;
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

/** After cleanup: when step1 vwork > step2 gps we set step1_actual = step2_gps - travel_min (step4−step3); Via = RULE. */
export type Step1CleanupOverride = string | null;

export type StepVia = 'GPS' | 'VW' | 'RULE' | 'ORIDE' | 'VineFence+';

export type DerivedStepsResultWithDebug = DerivedStepsResult & {
  debug: DerivedStepsDebug;
  /** Set when cleanup rule applied: step1_actual_time should be this value (step2_gps - 20 min). */
  step1ActualOverride?: Step1CleanupOverride;
  /** Per-step source (set after Part 3 apply orides). */
  step1Via?: StepVia;
  step2Via?: StepVia;
  step3Via?: StepVia;
  step4Via?: StepVia;
  step5Via?: StepVia;
};

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
 * Step 3 — Leave vineyard: First Vineyard EXIT after step 2 (so we don't pick an earlier exit before the enter).
 * Step 4 — Arrive winery: First Winery ENTER that (a) is in the data window and (b) is after max(step2, step3) if both exist; else only (a). Only step 5 (Winery EXIT) is subject to step 5 times.
 * Step 5 — Job end by GPS: VWork step 5 = step_5_completed_at (job completed in system). Only use GPS when Winery EXIT is strictly < VWork step 5 (gpsNorm < vworkStep5). IF VWork step 5 > step 4, AND we have a Winery EXIT that is > step 4, within the data window, and < VWork step 5, we use that Winery EXIT (driver left winery after returning from vineyard and forgot to end job). We take the FIRST such Winery EXIT (not the last), so we get the “forgot to end” exit, not the exit at job end. Search window capped at job end and at positionBefore (data window). In most jobs this does not apply (step5 null).
 *
 * Derive GPS step timestamps for a job using these rules. Window is passed in (same as tbl_tracking UI)
 * — no server-side timezone or date logic. Uses tbl_gpsmappings + original vwork name → tbl_geofences
 * → fence_ids; then scans tbl_tracking in window.
 */

/** Part 1: Fetch GPS candidates for each step (winery/vineyard, ENTER/EXIT). No decision — just lookups. */
async function fetchGpsStepCandidates(
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
    const { fenceIds: vineyardFenceIds, debug: vineyardDebug } = await getFenceIdsForVworkNameWithDebug('Vineyard', vineyardName);
    Object.assign(debug.vineyard, vineyardDebug);
    if (vineyardFenceIds.length > 0) {
      const step2Result = await getFirstTrackingInWindowWithDebug(truckId, positionAfter, positionBefore, vineyardFenceIds, 'ENTER');
      step2Value = step2Result.value;
      debug.vineyard.step2 = step2Result.debug;
      if (step2Result.value != null) candidates.step2 = { value: step2Result.value, trackingId: step2Result.trackingId };
      // Step 3 (Depart Vineyard): first Vineyard EXIT after step 2 (Enter Vineyard), so we don't pick an earlier exit
      const step3After = step2Value ?? positionAfter;
      const step3Result = await getFirstTrackingInWindowWithDebug(truckId, step3After, positionBefore, vineyardFenceIds, 'EXIT');
      step3Value = step3Result.value;
      debug.vineyard.step3 = step3Result.debug;
      if (step3Result.value != null) candidates.step3 = { value: step3Result.value, trackingId: step3Result.trackingId };
    }
  }

  if (deliveryWinery) {
    const { fenceIds: wineryFenceIds, debug: wineryDebug } = await getFenceIdsForVworkNameWithDebug('Winery', deliveryWinery);
    Object.assign(debug.winery, wineryDebug);
    if (wineryFenceIds.length > 0) {
      const step1Before = step2Value ?? positionBefore;
      const step1Result = await getFirstTrackingInWindowWithDebug(truckId, positionAfter, step1Before, wineryFenceIds, 'EXIT');
      debug.winery.step1 = step1Result.debug;
      if (step1Result.value != null) candidates.step1 = { value: step1Result.value, trackingId: step1Result.trackingId };
      const step4After =
        step2Value != null && step3Value != null
          ? (step2Value > step3Value ? step2Value : step3Value)
          : positionAfter;
      const step4Result = await getFirstTrackingInWindowWithDebug(truckId, step4After, positionBefore, wineryFenceIds, 'ENTER');
      step4Value = step4Result.value;
      debug.winery.step4 = step4Result.debug;
      if (step4Result.value != null) candidates.step4 = { value: step4Result.value, trackingId: step4Result.trackingId };
      const vworkStep5 = (job.step_5_completed_at ?? job.actual_end_time) != null
        ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
        : null;
      const step5WindowEnd = vworkStep5 != null ? (positionBefore != null && positionBefore < vworkStep5 ? positionBefore : vworkStep5) : positionBefore;
      if (step4Value != null && vworkStep5 != null && step5WindowEnd != null && vworkStep5 > step4Value) {
        const step5Result = await getFirstTrackingInWindowWithDebug(truckId, step4Value, step5WindowEnd, wineryFenceIds, 'EXIT', false);
        debug.winery.step5 = step5Result.debug;
        if (step5Result.value != null) candidates.step5 = { value: step5Result.value, trackingId: step5Result.trackingId };
      }
    }
  }
  return candidates;
}

/**
 * Subtract minutes from a timestamp string (YYYY-MM-DD HH:mm:ss). In-memory only; naive date math.
 * Used for cleanup rules (e.g. step1_actual = step2_gps - travel minutes).
 */
function subtractMinutesFromTimestamp(ts: string, minutes: number): string {
  const normalized = normalizeTimestampString(ts);
  if (!normalized) return ts;
  const m = normalized.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  if (!m) return ts;
  const [, y, mo, d, h, min, s] = m.map(Number);
  const ms = Date.UTC(y, mo - 1, d, h, min, s) - minutes * 60 * 1000;
  const date = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`;
}

/** Minutes from tsEarlier to tsLater (tsLater - tsEarlier). Naive date math. */
function minutesBetween(tsEarlier: string, tsLater: string): number {
  const a = normalizeTimestampString(tsEarlier);
  const b = normalizeTimestampString(tsLater);
  if (!a || !b) return 0;
  const ma = a.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  const mb = b.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  if (!ma || !mb) return 0;
  const msA = Date.UTC(Number(ma[1]), Number(ma[2]) - 1, Number(ma[3]), Number(ma[4]), Number(ma[5]), Number(ma[6]));
  const msB = Date.UTC(Number(mb[1]), Number(mb[2]) - 1, Number(mb[3]), Number(mb[4]), Number(mb[5]), Number(mb[6]));
  return Math.round((msB - msA) / (60 * 1000));
}

/**
 * Cleanup rules run after GPS phase (after we have decided GPS vs VWork and populated final step values).
 * Rule 1: If step1 VWork start > step2 GPS (vineyard arrive), we have impossible negative travel time.
 *         Then set step1_actual = step2_gps - travel_minutes, where travel_minutes = step4_final - step3_final
 *         (return leg: vineyard → winery). If step3/step4 missing, fall back to 20 minutes. Summary shows Via = RULE.
 */
function applyCleanupRules(
  result: DerivedStepsResult,
  job: JobForDerivedSteps
): { result: DerivedStepsResult; step1ActualOverride: Step1CleanupOverride } {
  let step1ActualOverride: Step1CleanupOverride = null;
  const step1Vwork = job.step_1_completed_at != null ? normalizeTimestampString(job.step_1_completed_at as string) : null;
  const step2Gps = result.step2 != null ? normalizeTimestampString(result.step2) : null;
  if (step1Vwork != null && step2Gps != null && step1Vwork > step2Gps) {
    const travelMinutes =
      result.step3 != null && result.step4 != null
        ? minutesBetween(result.step3, result.step4)
        : 20;
    const safeMinutes = Math.max(1, Math.min(120, travelMinutes));
    step1ActualOverride = subtractMinutesFromTimestamp(step2Gps, safeMinutes);
  }
  return { result, step1ActualOverride };
}

/** Part 3: Apply manual overrides (orides). If job has stepNoride set, use it for that step and set via = ORIDE; otherwise keep result and set via from GPS/VW/RULE. */
function applyOrides(
  result: DerivedStepsResult,
  job: JobForDerivedSteps,
  step1ActualOverride: Step1CleanupOverride
): { result: DerivedStepsResult; step1Via: StepVia; step2Via: StepVia; step3Via: StepVia; step4Via: StepVia; step5Via: StepVia } {
  const orides = [
    job.step1oride != null && String(job.step1oride).trim() !== '' ? normalizeTimestampString(String(job.step1oride).trim()) : null,
    job.step2oride != null && String(job.step2oride).trim() !== '' ? normalizeTimestampString(String(job.step2oride).trim()) : null,
    job.step3oride != null && String(job.step3oride).trim() !== '' ? normalizeTimestampString(String(job.step3oride).trim()) : null,
    job.step4oride != null && String(job.step4oride).trim() !== '' ? normalizeTimestampString(String(job.step4oride).trim()) : null,
    job.step5oride != null && String(job.step5oride).trim() !== '' ? normalizeTimestampString(String(job.step5oride).trim()) : null,
  ];
  const step1Final = orides[0] ?? step1ActualOverride ?? result.step1;
  const step2Final = orides[1] ?? result.step2;
  const step3Final = orides[2] ?? result.step3;
  const step4Final = orides[3] ?? result.step4;
  const step5Final = orides[4] ?? result.step5;
  const out: DerivedStepsResult = {
    ...result,
    step1: step1Final,
    step2: step2Final,
    step3: step3Final,
    step4: step4Final,
    step5: step5Final,
  };
  const step1Via: StepVia = orides[0] != null ? 'ORIDE' : (step1ActualOverride != null ? 'RULE' : (result.step1 != null ? 'GPS' : 'VW'));
  const step2Via: StepVia = orides[1] != null ? 'ORIDE' : (result.step2 != null ? 'GPS' : 'VW');
  const step3Via: StepVia = orides[2] != null ? 'ORIDE' : (result.step3 != null ? 'GPS' : 'VW');
  const step4Via: StepVia = orides[3] != null ? 'ORIDE' : (result.step4 != null ? 'GPS' : 'VW');
  const step5Via: StepVia = orides[4] != null ? 'ORIDE' : (result.step5 != null ? 'GPS' : 'VW');
  return { result: out, step1Via, step2Via, step3Via, step4Via, step5Via };
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
  if (!options.device || !options.positionAfter) return { ...emptyResult, debug, step1ActualOverride: null };
  const candidates = await fetchGpsStepCandidates(job, options, debug);
  const result = decideFinalSteps(candidates, job);
  const { result: afterCleanup, step1ActualOverride } = applyCleanupRules(result, job);
  const { result: afterOrides, step1Via, step2Via, step3Via, step4Via, step5Via } = applyOrides(afterCleanup, job, step1ActualOverride ?? null);
  return {
    ...afterOrides,
    debug,
    step1ActualOverride: step1ActualOverride ?? undefined,
    step1Via,
    step2Via,
    step3Via,
    step4Via,
    step5Via,
  };
}

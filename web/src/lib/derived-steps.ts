/**
 * Derive VWork **job steps 1–5** from tbl_tracking ENTER/EXIT and business rules.
 * **Steps+** (buffered vineyard polygon fallback for missing step 2/3) runs in `/api/tracking/derived-steps`, not in this module.
 */
import { query } from '@/lib/db';
import { dateToLiteral } from '@/lib/utils';

/** Same as tracking API: parse to YYYY-MM-DD HH:mm:ss only — no timezone in output. Handles Date (e.g. from DB) and strings; strips GMT+1300 etc. so PostgreSQL never sees them. */
export function normalizeTimestampString(s: string | Date | null): string | null {
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
  /** Rows from tbl_gpsmappings matched case-insensitively on trimmed vwname/gpsname vs vwork name */
  mappingsFound: { vwname: string | null; gpsname: string | null }[];
  /** List passed to geofence resolve: [original vwork name, ...gpsnames from mappings] */
  fenceNamesInList: string[];
  /** All fence_ids from tbl_geofences whose fence_name matches any list entry (case-insensitive, trimmed) */
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
    `SELECT type, vwname, gpsname FROM tbl_gpsmappings WHERE type = $1
     AND (
       LOWER(TRIM(COALESCE(vwname,''))) = LOWER(TRIM($2::text))
       OR LOWER(TRIM(COALESCE(gpsname,''))) = LOWER(TRIM($2::text))
     )`,
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
    `SELECT fence_id, fence_name FROM tbl_geofences g
     WHERE EXISTS (
       SELECT 1 FROM unnest($1::text[]) AS n(nm)
       WHERE nm IS NOT NULL AND TRIM(nm) <> ''
         AND LOWER(TRIM(COALESCE(g.fence_name,''))) = LOWER(TRIM(nm))
     )`,
    [names]
  );
  debug.resolvedFenceNames = rows.map((r) => ({ fence_id: Number(r.fence_id), fence_name: r.fence_name }));
  debug.fenceIds = rows.map((r) => Number(r.fence_id));

  return { fenceIds: debug.fenceIds, debug };
}

/** Job vineyard fence_ids (tbl_geofences) for GPS* / Steps+ alien-fence checks. */
export async function getVineyardFenceIdsForVworkName(vineyardName: string): Promise<number[]> {
  const { fenceIds } = await getFenceIdsForVworkNameWithDebug('Vineyard', vineyardName.trim());
  return fenceIds;
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

/** Max vineyard EXIT→ENTER same-fence aggregations for step 3 (GPS*); further same-vineyard pairs are ignored. */
const MAX_GPS_STAR_LOOPS = 3;

type FenceEventRow = {
  id: number;
  geofenceId: number;
  geofenceType: 'ENTER' | 'EXIT';
  timeNorm: string;
};

/** All ENTER/EXIT rows in the tracking window, ordered by time (for GPS* step-3 aggregation only). */
async function listFenceEnterExitEventsInWindow(
  device: string,
  positionAfter: string,
  positionBefore: string | null
): Promise<FenceEventRow[]> {
  const rawAfter = normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19);
  const rawBefore = positionBefore ? (normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19)) : null;
  const params: unknown[] = [device, rawAfter];
  let cond = 't.position_time_nz > $2';
  if (rawBefore) {
    params.push(rawBefore);
    cond += ' AND t.position_time_nz < $3';
  }
  const rows = await query<{ id: unknown; geofence_id: unknown; geofence_type: string | null; position_time_nz: unknown }>(
    `SELECT t.id, t.geofence_id, t.geofence_type,
            to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz
     FROM tbl_tracking t
     WHERE t.device_name = $1 AND t.geofence_type IN ('ENTER','EXIT') AND ${cond}
     ORDER BY t.position_time_nz ASC, t.id ASC`,
    params
  );
  const out: FenceEventRow[] = [];
  for (const r of rows) {
    const gt = (r.geofence_type ?? '').trim().toUpperCase();
    if (gt !== 'ENTER' && gt !== 'EXIT') continue;
    const timeNorm = normalizeTimestampString(String(r.position_time_nz ?? ''));
    if (!timeNorm) continue;
    const rawId = r.id;
    const id =
      rawId != null && typeof rawId === 'number'
        ? rawId
        : rawId != null && (typeof rawId === 'string' || typeof rawId === 'bigint')
          ? Number(rawId)
          : NaN;
    if (!Number.isFinite(id)) continue;
    const gid = Number(r.geofence_id);
    if (!Number.isFinite(gid)) continue;
    out.push({ id, geofenceId: gid, geofenceType: gt as 'ENTER' | 'EXIT', timeNorm });
  }
  return out;
}

/** True if any ENTER/EXIT on a geofence outside `vineyardSet` lies strictly between the two times. */
function hasNonVineyardEnterExitBetween(
  events: FenceEventRow[],
  vineyardSet: Set<number>,
  timeAfterExclusive: string,
  timeBeforeExclusive: string
): boolean {
  const tLo = normalizeTimestampString(timeAfterExclusive);
  const tHi = normalizeTimestampString(timeBeforeExclusive);
  if (!tLo || !tHi || tLo >= tHi) return false;
  for (const e of events) {
    if (e.timeNorm <= tLo) continue;
    if (e.timeNorm >= tHi) break;
    if (!vineyardSet.has(e.geofenceId)) return true;
  }
  return false;
}

/**
 * Optionally move step 3 from first vineyard EXIT to a later EXIT after up to MAX_GPS_STAR_LOOPS
 * same-vineyard re-entries. Any non-vineyard ENTER/EXIT between consecutive anchors voids GPS*.
 */
function tryGpsStarVineyardExit(
  events: FenceEventRow[],
  vineyardFenceIds: number[],
  step2: GpsStepCandidate,
  step3: GpsStepCandidate,
  positionBefore: string | null
): { step3: GpsStepCandidate; usedGpsStar: boolean } {
  const vineyardSet = new Set(vineyardFenceIds.map((n) => Number(n)));
  const e1Norm = normalizeTimestampString(step2.value);
  const x1Norm = normalizeTimestampString(step3.value);
  if (!e1Norm || !x1Norm) return { step3, usedGpsStar: false };

  const step2Idx = events.findIndex((e) => e.id === step2.trackingId);
  if (step2Idx < 0) return { step3, usedGpsStar: false };

  if (hasNonVineyardEnterExitBetween(events, vineyardSet, e1Norm, x1Norm)) {
    return { step3, usedGpsStar: false };
  }

  let candTime = x1Norm;
  let candId: number | null = step3.trackingId;
  let candIdx = events.findIndex(
    (e) =>
      e.id === step3.trackingId &&
      e.geofenceType === 'EXIT' &&
      vineyardSet.has(e.geofenceId) &&
      e.timeNorm === x1Norm
  );
  if (candIdx < 0) {
    candIdx = events.findIndex(
      (e) =>
        e.geofenceType === 'EXIT' &&
        vineyardSet.has(e.geofenceId) &&
        e.timeNorm === x1Norm &&
        e.timeNorm > e1Norm
    );
  }
  if (candIdx < 0) return { step3, usedGpsStar: false };

  const hiBound = positionBefore ? normalizeTimestampString(positionBefore) : null;
  let usedGpsStar = false;

  for (let loop = 0; loop < MAX_GPS_STAR_LOOPS; loop++) {
    let enterJ = -1;
    let j = candIdx + 1;
    for (; j < events.length; j++) {
      const e = events[j];
      if (hiBound != null && e.timeNorm >= hiBound) {
        enterJ = -1;
        break;
      }
      if (!vineyardSet.has(e.geofenceId)) {
        enterJ = -2;
        break;
      }
      if (e.geofenceType === 'ENTER') {
        enterJ = j;
        break;
      }
      enterJ = -3;
      break;
    }
    if (enterJ < 0) break;

    if (hasNonVineyardEnterExitBetween(events, vineyardSet, candTime, events[enterJ].timeNorm)) break;

    let exitJ = -1;
    for (let k = enterJ + 1; k < events.length; k++) {
      const e = events[k];
      if (hiBound != null && e.timeNorm >= hiBound) break;
      if (!vineyardSet.has(e.geofenceId)) {
        exitJ = -2;
        break;
      }
      if (e.geofenceType === 'EXIT') {
        exitJ = k;
        break;
      }
      exitJ = -3;
      break;
    }
    if (exitJ < 0) break;

    if (hasNonVineyardEnterExitBetween(events, vineyardSet, events[enterJ].timeNorm, events[exitJ].timeNorm)) break;

    candTime = events[exitJ].timeNorm;
    candId = events[exitJ].id;
    candIdx = exitJ;
    usedGpsStar = true;
  }

  if (!usedGpsStar) return { step3, usedGpsStar: false };
  return {
    step3: { value: candTime, trackingId: Number.isFinite(candId as number) ? (candId as number) : null },
    usedGpsStar: true,
  };
}

/** Steps+ buffered segment shape (from runStepsPlusQuery); fence_name optional for sorting only. */
export type StepsPlusBufferedSegment = {
  enter_time: string;
  exit_time: string;
  fence_name?: string;
};

/**
 * Merge multiple Steps+ inside-segments (driver just outside buffer, re-enters same vineyard buffer):
 * first segment's enter_time, last merged segment's exit_time, up to MAX_GPS_STAR_LOOPS re-entries.
 * Same alien-fence rule as GPS*: any ENTER/EXIT on a geofence not in `vineyardFenceIds` strictly between
 * a segment exit and the next segment enter voids further merging.
 */
export async function aggregateStepsPlusBufferedSegments(
  segments: StepsPlusBufferedSegment[],
  device: string,
  positionAfter: string,
  positionBefore: string | null,
  vineyardFenceIds: number[]
): Promise<{ enter: string; exit: string; usedGpsStarMerge: boolean }> {
  if (segments.length === 0) {
    return { enter: '', exit: '', usedGpsStarMerge: false };
  }
  const sorted = [...segments].sort((a, b) => {
    const ea = normalizeTimestampString(a.enter_time) ?? '';
    const eb = normalizeTimestampString(b.enter_time) ?? '';
    const c = ea.localeCompare(eb);
    if (c !== 0) return c;
    const xa = normalizeTimestampString(a.exit_time) ?? '';
    const xb = normalizeTimestampString(b.exit_time) ?? '';
    return xa.localeCompare(xb);
  });
  const e0 = normalizeTimestampString(sorted[0].enter_time);
  const x0 = normalizeTimestampString(sorted[0].exit_time);
  if (!e0 || !x0) {
    return {
      enter: sorted[0].enter_time,
      exit: sorted[0].exit_time,
      usedGpsStarMerge: false,
    };
  }
  if (vineyardFenceIds.length === 0 || sorted.length === 1) {
    return { enter: e0, exit: x0, usedGpsStarMerge: false };
  }

  const events = await listFenceEnterExitEventsInWindow(device, positionAfter, positionBefore);
  const vineyardSet = new Set(vineyardFenceIds.map((n) => Number(n)));
  let candidateExit = x0;
  let segIdx = 0;
  let usedGpsStarMerge = false;

  for (let loop = 0; loop < MAX_GPS_STAR_LOOPS; loop++) {
    if (segIdx + 1 >= sorted.length) break;
    const nextEnter = normalizeTimestampString(sorted[segIdx + 1].enter_time);
    if (!nextEnter) break;
    if (candidateExit >= nextEnter) break;
    if (hasNonVineyardEnterExitBetween(events, vineyardSet, candidateExit, nextEnter)) break;
    segIdx += 1;
    const xn = normalizeTimestampString(sorted[segIdx].exit_time);
    if (!xn) break;
    candidateExit = xn;
    usedGpsStarMerge = true;
  }

  return { enter: e0, exit: candidateExit, usedGpsStarMerge };
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
  /** VWork-reported step 1 (job start). Cleanup also falls back to actual_start_time when this is empty. */
  step_1_completed_at?: string | null;
  /** Manual overrides (Part 3): if set, trump GPS/VWork for that step. */
  step1oride?: string | null;
  step2oride?: string | null;
  step3oride?: string | null;
  step4oride?: string | null;
  step5oride?: string | null;
  step_2_completed_at?: string | null;
  step_3_completed_at?: string | null;
  step_4_completed_at?: string | null;
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
  /** Step 3 used GPS* vineyard re-exit aggregation (tbl_vworkjobs.step_3_via / calcnotes). */
  step3GpsStar?: boolean;
};

export type StepVia = 'GPS' | 'VW' | 'RULE' | 'ORIDE' | 'VineFence+' | 'GPS*';

export type DerivedStepsResult = {
  /** Raw GPS times from tbl_tracking (Step_N_GPS_completed_at). */
  step1Gps: string | null;
  step2Gps: string | null;
  step3Gps: string | null;
  step4Gps: string | null;
  step5Gps: string | null;
  /** Final times for step_N_actual_time: GPS∨VWork base, then cleanup, then orides. */
  step1: string | null;
  step2: string | null;
  step3: string | null;
  step4: string | null;
  step5: string | null;
  step1TrackingId: number | null;
  step2TrackingId: number | null;
  step3TrackingId: number | null;
  step4TrackingId: number | null;
  step5TrackingId: number | null;
  /** Steps+ may set before finalizeDerivedSteps so applyOrides keeps VineFence+. */
  step2Via?: StepVia;
  step3Via?: StepVia;
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
  /** Step 3 extended via same-vineyard re-entry smoothing (GPS*). */
  step3GpsStar?: boolean;
};

/** After cleanup: when step1 vwork > step2 gps we set step1_actual = step2_gps - travel_min (step4−step3); Via = RULE. */
export type Step1CleanupOverride = string | null;

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
 *   GPS* (optional): If the driver briefly exits and re-enters the same vineyard fence set with no other fence
 *   ENTER/EXIT between, aggregate up to 3 such loops; step 3 becomes the last EXIT in the chain. Any alien fence
 *   event voids GPS* for that job (revert to first exit only). Marked step3Via = GPS* and calcnotes GPS*:.
 * Step 4 — Arrive winery (return leg): First mapped Winery ENTER with position_time_nz strictly &gt; max(GPS step1, step2, step3) among non-null steps (first re-entry after vineyard steps 2–3; step 1 included so we do not pick an ENTER before the morning exit), and strictly &lt; data window end (positionBefore). If steps 1–3 are all absent, lower bound is positionAfter.
 * Step 5 — Job end by GPS: VWork step 5 = step_5_completed_at (job completed in system). Only use GPS when Winery EXIT is strictly < VWork step 5 (gpsNorm < vworkStep5). IF VWork step 5 > step 4, AND we have a Winery EXIT that is > step 4, within the data window, and < VWork step 5, we use that Winery EXIT (driver left winery after returning from vineyard and forgot to end job). We take the FIRST such Winery EXIT (not the last), so we get the “forgot to end” exit, not the exit at job end. Search window capped at job end and at positionBefore (data window). In most jobs this does not apply (step5 null).
 *
 * Derive GPS step timestamps for a job using these rules. Window is passed in (same as tbl_tracking UI)
 * — no server-side timezone or date logic. Uses tbl_gpsmappings + original vwork name → tbl_geofences
 * → fence_ids; then scans tbl_tracking in window.
 *
 * Guardrail (after fetch): If GPS step 1 exists, steps 2–3 and 5 must be strictly &gt; step 1 time (step 4 uses max(step1–3) floor — see below). Each tbl_tracking id may appear at most once. Step 5 cleared if step 4 cleared.
 * Guardrail — step 4: must be strictly &gt; max(GPS step1, step2, step3) among non-null steps so “arrive winery” cannot precede leaving the vineyard (or an earlier winery exit).
 * Guardrail — VWork job end: GPS steps 1–3 must be strictly **before** VWork step 5 (job completed). Stops picking the next job’s winery exit as step 1, or vineyard enter/exit from after job end.
 */

/** Lexicographic max for YYYY-MM-DD HH:mm:ss strings; ignores nulls. */
function maxTimestampString(...vals: (string | null | undefined)[]): string | null {
  let best: string | null = null;
  for (const v of vals) {
    if (v == null || v === '') continue;
    const n = normalizeTimestampString(v);
    if (n == null) continue;
    if (best == null || n > best) best = n;
  }
  return best;
}

/**
 * If GPS step 1 exists, drop steps 2–3 and 5 with time &lt;= step 1 (step 4 excluded — uses max(step1–3) floor below).
 * Step 4: drop if &lt;= max(GPS step1, step2, step3) when that max exists.
 * VWork ceiling: steps 1–3 must be &lt; VWork job end (step_5_completed_at / actual_end) — no winery exit / vineyard events on the way to the next job.
 */
function applyGpsGuardrails(candidates: FetchedGpsCandidates, job: JobForDerivedSteps): void {
  const vworkEnd =
    job.step_5_completed_at != null || job.actual_end_time != null
      ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
      : null;
  if (vworkEnd != null && candidates.step1?.value != null) {
    const s1c = normalizeTimestampString(candidates.step1.value);
    if (s1c != null && s1c >= vworkEnd) {
      candidates.step1 = null;
    }
  }
  const s1 = candidates.step1;
  if (s1?.value != null) {
    const floor = normalizeTimestampString(s1.value);
    if (floor) {
      for (const key of ['step2', 'step3', 'step5'] as const) {
        const c = candidates[key];
        if (c?.value == null) continue;
        const n = normalizeTimestampString(c.value);
        if (n != null && n <= floor) candidates[key] = null;
      }
    }
  }
  if (vworkEnd != null) {
    const s2 = candidates.step2?.value != null ? normalizeTimestampString(candidates.step2!.value) : null;
    if (s2 != null && s2 >= vworkEnd) {
      candidates.step2 = null;
      candidates.step3 = null;
    } else {
      const s3 = candidates.step3?.value != null ? normalizeTimestampString(candidates.step3!.value) : null;
      if (s3 != null && s3 >= vworkEnd) {
        candidates.step3 = null;
      }
    }
  }
  const step4Floor = maxTimestampString(
    candidates.step1?.value,
    candidates.step2?.value,
    candidates.step3?.value
  );
  if (step4Floor != null && candidates.step4?.value != null) {
    const s4 = normalizeTimestampString(candidates.step4.value);
    if (s4 != null && s4 <= step4Floor) {
      candidates.step4 = null;
    }
  }
  const seen = new Set<number>();
  for (const key of ['step1', 'step2', 'step3', 'step4', 'step5'] as const) {
    const c = candidates[key];
    const id = c?.trackingId;
    if (id == null || !Number.isFinite(id)) continue;
    if (seen.has(id)) {
      candidates[key] = null;
    } else {
      seen.add(id);
    }
  }
  if (candidates.step4 == null && candidates.step5 != null) {
    candidates.step5 = null;
  }
  if (candidates.step3 == null) {
    delete candidates.step3GpsStar;
  }
}

/**
 * Winery steps 4–5: first mapped Winery ENTER after max(step1..3), then first Winery EXIT before VWork job end
 * (same SQL rules as fetchGpsStepCandidates). Updates debug.winery.step4 / step5.
 */
async function fetchWineryStep4And5ForValues(
  job: JobForDerivedSteps,
  options: DerivedStepsOptions,
  debug: DerivedStepsDebug,
  truckId: string,
  wineryFenceIds: number[],
  step1Value: string | null,
  step2Value: string | null,
  step3Value: string | null
): Promise<{ step4: GpsStepCandidate | null; step5: GpsStepCandidate | null }> {
  const { positionAfter, positionBefore } = options;
  if (wineryFenceIds.length === 0) {
    return { step4: null, step5: null };
  }
  const step4LowerBound = maxTimestampString(step1Value, step2Value, step3Value) ?? positionAfter;
  const step4Result = await getFirstTrackingInWindowWithDebug(truckId, step4LowerBound, positionBefore, wineryFenceIds, 'ENTER');
  debug.winery.step4 = step4Result.debug;
  let step4: GpsStepCandidate | null = null;
  if (step4Result.value != null) {
    step4 = { value: step4Result.value, trackingId: step4Result.trackingId };
  }
  const step4Value = step4?.value ?? null;
  const vworkStep5 =
    (job.step_5_completed_at ?? job.actual_end_time) != null
      ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
      : null;
  const step5WindowEnd =
    vworkStep5 != null ? (positionBefore != null && positionBefore < vworkStep5 ? positionBefore : vworkStep5) : positionBefore;
  let step5: GpsStepCandidate | null = null;
  if (step4Value != null && vworkStep5 != null && step5WindowEnd != null && vworkStep5 > step4Value) {
    const step5Result = await getFirstTrackingInWindowWithDebug(truckId, step4Value, step5WindowEnd, wineryFenceIds, 'EXIT', false);
    debug.winery.step5 = step5Result.debug;
    if (step5Result.value != null) {
      step5 = { value: step5Result.value, trackingId: step5Result.trackingId };
    }
  }
  return { step4, step5 };
}

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
      if (candidates.step2 != null && candidates.step3 != null) {
        const eventRows = await listFenceEnterExitEventsInWindow(truckId, positionAfter, positionBefore);
        const star = tryGpsStarVineyardExit(eventRows, vineyardFenceIds, candidates.step2, candidates.step3, positionBefore);
        candidates.step3 = star.step3;
        if (star.usedGpsStar) {
          candidates.step3GpsStar = true;
          debug.step3GpsStar = true;
        }
      }
    }
  }

  if (deliveryWinery) {
    const { fenceIds: wineryFenceIds, debug: wineryDebug } = await getFenceIdsForVworkNameWithDebug('Winery', deliveryWinery);
    Object.assign(debug.winery, wineryDebug);
    if (wineryFenceIds.length > 0) {
      const step1Before = step2Value ?? positionBefore;
      const step1Result = await getFirstTrackingInWindowWithDebug(truckId, positionAfter, step1Before, wineryFenceIds, 'EXIT');
      debug.winery.step1 = step1Result.debug;
      let step1Value: string | null = null;
      if (step1Result.value != null) {
        candidates.step1 = { value: step1Result.value, trackingId: step1Result.trackingId };
        step1Value = step1Result.value;
      }
      const { step4, step5 } = await fetchWineryStep4And5ForValues(
        job,
        options,
        debug,
        truckId,
        wineryFenceIds,
        step1Value,
        step2Value,
        step3Value
      );
      if (step4 != null) candidates.step4 = step4;
      if (step5 != null) candidates.step5 = step5;
    }
  }
  applyGpsGuardrails(candidates, job);
  if (!candidates.step3GpsStar) {
    delete debug.step3GpsStar;
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
 * VWork job-start time for cleanup rules: `step_1_completed_at` when set, otherwise `actual_start_time`
 * (many jobs only have actual start populated; without this, cleanup_start / travel rule never run).
 */
function vworkStep1TimeForCleanup(job: JobForDerivedSteps): string | null {
  const s1 =
    job.step_1_completed_at != null && String(job.step_1_completed_at).trim() !== ''
      ? job.step_1_completed_at
      : job.actual_start_time != null && String(job.actual_start_time).trim() !== ''
        ? job.actual_start_time
        : null;
  return s1 != null ? normalizeTimestampString(s1 as string) : null;
}

/** VWork step N completed time for merging into actuals (steps 2–5). */
function vworkStepTime(job: JobForDerivedSteps, which: 2 | 3 | 4 | 5): string | null {
  const raw =
    which === 2
      ? job.step_2_completed_at
      : which === 3
        ? job.step_3_completed_at
        : which === 4
          ? job.step_4_completed_at
          : job.step_5_completed_at ?? job.actual_end_time;
  if (raw == null || String(raw).trim() === '') return null;
  return normalizeTimestampString(raw as string);
}

/**
 * After GPS decisions: fill step1..step5 with GPS time or VWork step completed time (same scale as stored job fields).
 */
function resolveActualFromGpsAndVwork(gps: DerivedStepsResult, job: JobForDerivedSteps): DerivedStepsResult {
  const v1 = vworkStep1TimeForCleanup(job);
  const v2 = vworkStepTime(job, 2);
  const v3 = vworkStepTime(job, 3);
  const v4 = vworkStepTime(job, 4);
  const v5 = vworkStepTime(job, 5);
  return {
    ...gps,
    step1: gps.step1Gps ?? v1 ?? null,
    step2: gps.step2Gps ?? v2 ?? null,
    step3: gps.step3Gps ?? v3 ?? null,
    step4: gps.step4Gps ?? v4 ?? null,
    step5: gps.step5Gps ?? v5 ?? null,
  };
}

/**
 * Cleanup runs only on **actual** times (`step1`..`step5` after GPS ∨ VWork merge). Does not read `stepNGps`.
 * Runs once at end of finalize after actuals are fully populated.
 *
 * cleanup_start: VWork start is after actual vineyard arrive, but **actual step 1 is missing** → step1 = actual.step2 − 10 min.
 * travel: VWork start is after actual step 2, and **actual step 1 is missing or not strictly before step 2** →
 *   step1 = actual.step2 − travel (actual.step3→step4 leg, else 20 min).
 */
function applyCleanupRules(actual: DerivedStepsResult, job: JobForDerivedSteps): void {
  const step1Vwork = vworkStep1TimeForCleanup(job);
  const a1 = actual.step1 != null ? normalizeTimestampString(actual.step1) : null;
  const a2 = actual.step2 != null ? normalizeTimestampString(actual.step2) : null;

  if (a1 == null && a2 != null && step1Vwork != null && a2 < step1Vwork) {
    actual.step1 = subtractMinutesFromTimestamp(a2, 10);
  } else if (step1Vwork != null && a2 != null && step1Vwork > a2 && (a1 == null || a1 >= a2)) {
    const travelMinutes =
      actual.step3 != null && actual.step4 != null
        ? minutesBetween(actual.step3, actual.step4)
        : 20;
    const safeMinutes = Math.max(1, Math.min(120, travelMinutes));
    actual.step1 = subtractMinutesFromTimestamp(a2, safeMinutes);
  }
}

/** Part 3: Manual overrides on top of resolved actuals. `gps` retains raw GPS for Via labels. */
function applyOrides(
  actual: DerivedStepsResult,
  job: JobForDerivedSteps,
  gps: DerivedStepsResult,
  step1CleanupApplied: boolean
): { result: DerivedStepsResult; step1Via: StepVia; step2Via: StepVia; step3Via: StepVia; step4Via: StepVia; step5Via: StepVia } {
  const orides = [
    job.step1oride != null && String(job.step1oride).trim() !== '' ? normalizeTimestampString(String(job.step1oride).trim()) : null,
    job.step2oride != null && String(job.step2oride).trim() !== '' ? normalizeTimestampString(String(job.step2oride).trim()) : null,
    job.step3oride != null && String(job.step3oride).trim() !== '' ? normalizeTimestampString(String(job.step3oride).trim()) : null,
    job.step4oride != null && String(job.step4oride).trim() !== '' ? normalizeTimestampString(String(job.step4oride).trim()) : null,
    job.step5oride != null && String(job.step5oride).trim() !== '' ? normalizeTimestampString(String(job.step5oride).trim()) : null,
  ];
  const step1Final = orides[0] ?? actual.step1;
  const step2Final = orides[1] ?? actual.step2;
  const step3Final = orides[2] ?? actual.step3;
  const step4Final = orides[3] ?? actual.step4;
  const step5Final = orides[4] ?? actual.step5;
  const out: DerivedStepsResult = {
    ...gps,
    step1: step1Final,
    step2: step2Final,
    step3: step3Final,
    step4: step4Final,
    step5: step5Final,
  };
  const step1Via: StepVia = orides[0] != null ? 'ORIDE' : step1CleanupApplied ? 'RULE' : (gps.step1Gps != null ? 'GPS' : 'VW');
  const step2Via: StepVia =
    orides[1] != null ? 'ORIDE' : gps.step2Via === 'VineFence+' ? 'VineFence+' : (gps.step2Gps != null ? 'GPS' : 'VW');
  const step3Via: StepVia =
    orides[2] != null
      ? 'ORIDE'
      : gps.step3Via === 'VineFence+'
        ? 'VineFence+'
        : gps.step3Via === 'GPS*'
          ? 'GPS*'
          : gps.step3Gps != null
            ? 'GPS'
            : 'VW';
  const step4Via: StepVia = orides[3] != null ? 'ORIDE' : (gps.step4Gps != null ? 'GPS' : 'VW');
  const step5Via: StepVia = orides[4] != null ? 'ORIDE' : (gps.step5Gps != null ? 'GPS' : 'VW');
  return { result: out, step1Via, step2Via, step3Via, step4Via, step5Via };
}

/** Part 2: Raw GPS times only (`stepNGps`). VWork merge, cleanup, orides happen in finalizeDerivedSteps. */
function decideFinalSteps(candidates: FetchedGpsCandidates, job: JobForDerivedSteps): DerivedStepsResult {
  const result: DerivedStepsResult = {
    step1Gps: null,
    step2Gps: null,
    step3Gps: null,
    step4Gps: null,
    step5Gps: null,
    step1: null,
    step2: null,
    step3: null,
    step4: null,
    step5: null,
    step1TrackingId: null,
    step2TrackingId: null,
    step3TrackingId: null,
    step4TrackingId: null,
    step5TrackingId: null,
  };
  const vworkStep5 = (job.step_5_completed_at ?? job.actual_end_time) != null
    ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
    : null;
  if (candidates.step1) {
    result.step1Gps = candidates.step1.value;
    result.step1TrackingId = candidates.step1.trackingId;
  }
  if (candidates.step2) {
    result.step2Gps = candidates.step2.value;
    result.step2TrackingId = candidates.step2.trackingId;
  }
  if (candidates.step3) {
    result.step3Gps = candidates.step3.value;
    result.step3TrackingId = candidates.step3.trackingId;
  }
  if (candidates.step3GpsStar) {
    result.step3Via = 'GPS*';
  }
  if (candidates.step4) {
    result.step4Gps = candidates.step4.value;
    result.step4TrackingId = candidates.step4.trackingId;
  }
  if (candidates.step5 && vworkStep5 != null) {
    const gpsNorm = normalizeTimestampString(candidates.step5.value);
    if (gpsNorm != null && gpsNorm < vworkStep5) {
      result.step5Gps = candidates.step5.value;
      result.step5TrackingId = candidates.step5.trackingId;
    }
  }
  return result;
}

/**
 * After VineFence+ (Steps+) sets vineyard enter/exit, re-query winery steps 4–5 so the max(step1–3) floor uses
 * those times (not step 4/5 from the first pass with missing or fence-only step 2/3).
 */
export async function deriveGpsLayerAfterVineFencePlus(
  job: JobForDerivedSteps,
  options: DerivedStepsOptions,
  step123: {
    step1: GpsStepCandidate | null;
    step2: GpsStepCandidate | null;
    step3: GpsStepCandidate | null;
  },
  debug: DerivedStepsDebug
): Promise<DerivedStepsResult> {
  const truckId = options.device;
  const deliveryWinery = job.delivery_winery ? String(job.delivery_winery).trim() : '';
  const candidates: FetchedGpsCandidates = {
    step1: step123.step1,
    step2: step123.step2,
    step3: step123.step3,
    step4: null,
    step5: null,
  };
  if (deliveryWinery) {
    const { fenceIds: wineryFenceIds, debug: wineryDebug } = await getFenceIdsForVworkNameWithDebug('Winery', deliveryWinery);
    Object.assign(debug.winery, wineryDebug);
    if (wineryFenceIds.length > 0) {
      const { step4, step5 } = await fetchWineryStep4And5ForValues(
        job,
        options,
        debug,
        truckId,
        wineryFenceIds,
        step123.step1?.value ?? null,
        step123.step2?.value ?? null,
        step123.step3?.value ?? null
      );
      if (step4 != null) candidates.step4 = step4;
      if (step5 != null) candidates.step5 = step5;
    }
  }
  applyGpsGuardrails(candidates, job);
  return decideFinalSteps(candidates, job);
}

/** Strip merged actuals so finalize re-runs resolve + cleanup (e.g. after Steps+). */
export function toGpsLayerForFinalize(r: DerivedStepsResult): DerivedStepsResult {
  return {
    step1Gps: r.step1Gps,
    step2Gps: r.step2Gps,
    step3Gps: r.step3Gps,
    step4Gps: r.step4Gps,
    step5Gps: r.step5Gps,
    step1: null,
    step2: null,
    step3: null,
    step4: null,
    step5: null,
    step1TrackingId: r.step1TrackingId,
    step2TrackingId: r.step2TrackingId,
    step3TrackingId: r.step3TrackingId,
    step4TrackingId: r.step4TrackingId,
    step5TrackingId: r.step5TrackingId,
    step2Via: r.step2Via,
    step3Via: r.step3Via,
  };
}

/**
 * GPS + VWork merge, cleanup on actuals, orides. Call again after Steps+ updates `step2Gps`/`step3Gps`.
 */
export function finalizeDerivedSteps(gps: DerivedStepsResult, job: JobForDerivedSteps): DerivedStepsResult & {
  step1Via: StepVia;
  step2Via: StepVia;
  step3Via: StepVia;
  step4Via: StepVia;
  step5Via: StepVia;
  step1ActualOverride?: Step1CleanupOverride;
} {
  const actual = resolveActualFromGpsAndVwork(gps, job);
  const beforeCleanup = actual.step1;
  applyCleanupRules(actual, job);
  const step1CleanupApplied =
    normalizeTimestampString(beforeCleanup) !== normalizeTimestampString(actual.step1);
  const step1ActualOverride: Step1CleanupOverride = step1CleanupApplied ? actual.step1 : null;
  const { result, step1Via, step2Via, step3Via, step4Via, step5Via } = applyOrides(actual, job, gps, step1CleanupApplied);
  return {
    ...result,
    step1Via,
    step2Via,
    step3Via,
    step4Via,
    step5Via,
    step1ActualOverride: step1ActualOverride ?? undefined,
  };
}

export async function deriveGpsStepsForJob(
  job: JobForDerivedSteps,
  options: DerivedStepsOptions
): Promise<DerivedStepsResultWithDebug> {
  const emptyResult: DerivedStepsResult = {
    step1Gps: null,
    step2Gps: null,
    step3Gps: null,
    step4Gps: null,
    step5Gps: null,
    step1: null,
    step2: null,
    step3: null,
    step4: null,
    step5: null,
    step1TrackingId: null,
    step2TrackingId: null,
    step3TrackingId: null,
    step4TrackingId: null,
    step5TrackingId: null,
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
  const gpsOnly = decideFinalSteps(candidates, job);
  const finalized = finalizeDerivedSteps(gpsOnly, job);
  const { step1Via, step2Via, step3Via, step4Via, step5Via, step1ActualOverride, ...rest } = finalized;
  return {
    ...rest,
    debug,
    step1ActualOverride: step1ActualOverride ?? undefined,
    step1Via,
    step2Via,
    step3Via,
    step4Via,
    step5Via,
  };
}

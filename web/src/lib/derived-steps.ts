/**
 * Derive VWork **job steps 1–5** from tbl_tracking ENTER/EXIT and business rules.
 * **Steps+** (buffered vineyard polygon fallback for missing step 2/3): snapshot merge runs once in
 * `/api/tracking/derived-steps` via `@/lib/steps-plus-merge-snapshot` before Part 1; same snapshot feeds
 * `tentativeVineyardEnterForStep1Bracket` and VineFence+ apply (no second `runStepsPlusQuery`).
 */
import { query } from '@/lib/db';
import { addMinutesToTimestampAsNZ } from '@/lib/fetch-steps';
import { JOB_END_CEILING_BUFFER_DEFAULT_MINUTES } from '@/lib/job-end-ceiling-buffer-setting-names';
import { normalizeTimestampString } from '@/lib/normalize-timestamp-string';
import { STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES } from '@/lib/step5-winery-exit-extend-setting-names';
import { dateToLiteral } from '@/lib/utils';

export { normalizeTimestampString };

/** Vineyard special rule 1: Bankhouse South — if step 2/3 missing for South fences, use Bankhouse fences (tag VineSR1). */
const VINE_SR1_SOUTH_NAME = 'Bankhouse South';
const VINE_SR1_FALLBACK_VINEYARD_NAME = 'Bankhouse';

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

/** Appended to vineyard polygon trace lines: each tbl_geofences row as name + numeric id (matches GPS grid / ANY query). */
function formatResolvedFenceNamesForGpsTrace(resolved: FenceResolutionDebug['resolvedFenceNames']): string {
  if (!resolved?.length) return '';
  return resolved
    .map((r) => {
      const name =
        r.fence_name != null && String(r.fence_name).trim() !== '' ? String(r.fence_name).trim() : '(unnamed)';
      return `${name} geofence_id=${r.fence_id}`;
    })
    .join('; ');
}

/** Job vineyard fence_ids (tbl_geofences) for GPS* / Steps+ alien-fence checks. */
export async function getVineyardFenceIdsForVworkName(vineyardName: string): Promise<number[]> {
  const { fenceIds } = await getFenceIdsForVworkNameWithDebug('Vineyard', vineyardName.trim());
  return fenceIds;
}

export async function getWineryFenceIdsForVworkName(wineryName: string): Promise<number[]> {
  const { fenceIds } = await getFenceIdsForVworkNameWithDebug('Winery', wineryName.trim());
  return fenceIds;
}

/** Dedupe + sort for stable SQL `ANY` arrays and audits. */
function sortedUniqueFenceIds(ids: number[]): number[] {
  const set = new Set<number>();
  for (const n of ids) {
    if (typeof n === 'number' && Number.isFinite(n)) set.add(n);
  }
  return [...set].sort((a, b) => a - b);
}

export type Step1MorningWineryFenceUnionDebug = {
  /** Chronologically prior same-day job (same worker), if any. */
  previousJobId: string | null;
  /** Previous job `delivery_winery` when loaded (trimmed). */
  previousDeliveryWinery: string | null;
  mergedFenceIdCount: number;
  /** True when fence ids from the previous job’s winery were merged into the Step 1 morning EXIT search set. */
  unionedPreviousWinery: boolean;
};

/**
 * GPS Step 1 morning winery EXIT: fence_ids for **this** job’s `delivery_winery` plus, when the chained previous
 * same-day job has a **different** delivery winery, union that job’s mapped winery fences too — so the physical
 * EXIT leaving the prior stop still qualifies as “start this leg” before vineyard ENTER. Steps 4–5 remain this job’s winery only.
 */
async function mergeWineryFenceIdsForStep1MorningExit(
  job: JobForDerivedSteps,
  deviceForTracking: string,
  currentFenceIds: number[]
): Promise<{
  merged: number[];
  previousJobId: string | null;
  previousDeliveryWinery: string | null;
}> {
  const base = sortedUniqueFenceIds(currentFenceIds);
  const currentDelivery = job.delivery_winery != null ? String(job.delivery_winery).trim() : '';
  const workerTrim =
    job.worker != null && String(job.worker).trim() !== ''
      ? String(job.worker).trim()
      : deviceForTracking.trim();
  const emptyMeta = {
    merged: base,
    previousJobId: null as string | null,
    previousDeliveryWinery: null as string | null,
  };
  if (!workerTrim) return emptyMeta;
  const actualStartRaw = job.actual_start_time;
  if (actualStartRaw == null || String(actualStartRaw).trim() === '') return emptyMeta;
  const actualStartNorm = normalizeTimestampString(actualStartRaw as string | Date);
  if (!actualStartNorm) return emptyMeta;

  const prevRows = await query<{ job_id: string; delivery_winery: string | null }>(
    `SELECT trim(job_id::text) AS job_id,
            TRIM(COALESCE(delivery_winery::text, '')) AS delivery_winery
     FROM tbl_vworkjobs
     WHERE LOWER(TRIM(COALESCE(worker::text, ''))) = LOWER(TRIM($1::text))
       AND actual_start_time IS NOT NULL
       AND (actual_start_time::date) = ($2::timestamp)::date
       AND actual_start_time < $2::timestamp
     ORDER BY actual_start_time DESC, trim(job_id::text) DESC
     LIMIT 1`,
    [workerTrim, actualStartNorm]
  );
  const prev = prevRows[0];
  if (!prev) return emptyMeta;
  const prevJobId = prev.job_id != null ? String(prev.job_id).trim() : null;
  const prevDelivery = (prev.delivery_winery ?? '').trim();
  if (!prevDelivery) {
    return { merged: base, previousJobId: prevJobId, previousDeliveryWinery: null };
  }
  if (currentDelivery && prevDelivery.toLowerCase() === currentDelivery.toLowerCase()) {
    return { merged: base, previousJobId: prevJobId, previousDeliveryWinery: prevDelivery };
  }
  const { fenceIds: prevFenceIds } = await getFenceIdsForVworkNameWithDebug('Winery', prevDelivery);
  const merged = sortedUniqueFenceIds([...base, ...prevFenceIds]);
  return {
    merged,
    previousJobId: prevJobId,
    previousDeliveryWinery: prevDelivery,
  };
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
  /** Geofence id on the matched tbl_tracking row (which fence fired). */
  matchedGeofenceId: number | null;
  /** tbl_geofences.fence_name for matchedGeofenceId (Inspect: which mapped fence won). */
  matchedFenceName: string | null;
  /** Approximate SQL used for traceability */
  sqlHint: string;
  /** Inspect: ordered plain-English window + inherited bounds for this lookup. */
  tracePlain?: string;
  /** Inspect: all rows matching the same WHERE as LIMIT 1 (ordered); guardrails may still drop the winner later. */
  matchingRowsOrdered?: Array<{
    id: number;
    position_time_nz: string;
    geofence_id: number | null;
    fence_name: string | null;
  }>;
  matchingRowsTruncated?: boolean;
  matchingRowsCaption?: string;
  /**
   * Step 4 audit only: rows with position_time_nz strictly after max(positionAfter option, step2, step3) — no step1 leg.
   * When job step-1 anchor (oride∨VWork) is after that relaxed bound, lists winery ENTERs excluded from Part 1 only because the step1 leg raised the fetch floor.
   */
  auditLowerExclusive?: string | null;
  auditMatchingRowsOrdered?: Array<{
    id: number;
    position_time_nz: string;
    geofence_id: number | null;
    fence_name: string | null;
  }>;
  auditMatchingRowsTruncated?: boolean;
  auditMatchingRowsCaption?: string;
};

const VINEYARD_WINDOW_MATCH_LIST_CAP = 100;

async function fetchOrderedMatchListForSameWindow(
  device: string,
  rawAfter: string,
  rawBefore: string | null,
  fenceIds: number[],
  geofenceType: 'ENTER' | 'EXIT',
  orderDesc: boolean,
  maxRows: number
): Promise<{
  rows: NonNullable<TrackingLookupDebug['matchingRowsOrdered']>;
  truncated: boolean;
}> {
  const cap = Math.min(200, Math.max(1, maxRows));
  const fetchLimit = cap + 1;
  const params: unknown[] = [device, fenceIds, geofenceType, rawAfter];
  let timeCondition = 't.position_time_nz > $4';
  if (rawBefore) {
    params.push(rawBefore);
    timeCondition += ' AND t.position_time_nz < $5';
  }
  params.push(fetchLimit);
  const limIdx = params.length;
  const orderClause = orderDesc ? 'DESC' : 'ASC';
  const listRows = await query<{ id: unknown; geofence_id: unknown; position_time_nz: unknown; fence_name: unknown }>(
    `SELECT t.id, t.geofence_id,
            to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz,
            g.fence_name
     FROM tbl_tracking t
     LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
     WHERE t.device_name = $1 AND t.geofence_id = ANY($2::int[]) AND t.geofence_type = $3 AND ${timeCondition}
     ORDER BY t.position_time_nz ${orderClause} LIMIT $${limIdx}`,
    params
  );
  const truncated = listRows.length > cap;
  const slice = truncated ? listRows.slice(0, cap) : listRows;
  const out: NonNullable<TrackingLookupDebug['matchingRowsOrdered']> = [];
  for (const row of slice) {
    const pid = row.id;
    const idNum =
      pid != null && typeof pid === 'number'
        ? pid
        : pid != null && (typeof pid === 'string' || typeof pid === 'bigint')
          ? Number(pid)
          : null;
    if (idNum == null || !Number.isFinite(idNum)) continue;
    const val = row.position_time_nz;
    let tnz = '';
    if (val != null) {
      if (typeof val === 'string') tnz = normalizeTimestampString(val) ?? val.slice(0, 19);
      else if (val instanceof Date) tnz = dateToLiteral(val);
      else tnz = String(val).slice(0, 19);
    }
    const rawGf = row.geofence_id;
    const gid =
      rawGf != null && typeof rawGf === 'number'
        ? rawGf
        : rawGf != null && (typeof rawGf === 'string' || typeof rawGf === 'bigint')
          ? Number(rawGf)
          : null;
    const fn = row.fence_name != null && String(row.fence_name).trim() !== '' ? String(row.fence_name).trim() : null;
    out.push({
      id: idNum,
      position_time_nz: tnz,
      geofence_id: gid != null && Number.isFinite(gid) ? gid : null,
      fence_name: fn,
    });
  }
  return { rows: out, truncated };
}

/** First (or last if orderDesc) tracking row in window at any of the given fences with given geofence_type (ENTER or EXIT). */
async function getFirstTrackingInWindowWithDebug(
  device: string,
  positionAfter: string,
  positionBefore: string | null,
  fenceIds: number[],
  geofenceType: 'ENTER' | 'EXIT',
  orderDesc = false,
  tracePlain?: string,
  /** When > 0, also fetch ordered rows (same WHERE) for Inspect — vineyard step 2/3 only at call sites. */
  orderedMatchListMax?: number
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
        matchedGeofenceId: null,
        matchedFenceName: null,
        sqlHint: `(no fence_ids)`,
        ...(tracePlain != null && tracePlain.trim() !== '' ? { tracePlain: tracePlain.trim() } : {}),
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

  const rows = await query<{ id: unknown; geofence_id: unknown; position_time_nz: unknown; fence_name: unknown }>(
    `SELECT t.id, t.geofence_id,
            to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz,
            g.fence_name
     FROM tbl_tracking t
     LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
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

  const rawGf = rows[0]?.geofence_id;
  const matchedGeofenceId =
    rawGf != null && typeof rawGf === 'number'
      ? rawGf
      : rawGf != null && (typeof rawGf === 'string' || typeof rawGf === 'bigint')
        ? Number(rawGf)
        : null;
  const matchedGeofenceIdSafe = matchedGeofenceId != null && Number.isFinite(matchedGeofenceId) ? matchedGeofenceId : null;

  const fn = rows[0]?.fence_name;
  const matchedFenceName =
    fn != null && String(fn).trim() !== '' ? String(fn).trim() : null;

  const debug: TrackingLookupDebug = {
    device,
    positionAfter,
    positionBefore,
    fenceIds: [...fenceIds],
    geofenceType,
    found: value != null,
    position_time_nz: value,
    trackingId: trackingIdSafe,
    matchedGeofenceId: matchedGeofenceIdSafe,
    matchedFenceName,
    sqlHint,
    ...(tracePlain != null && tracePlain.trim() !== '' ? { tracePlain: tracePlain.trim() } : {}),
  };

  if (orderedMatchListMax != null && orderedMatchListMax > 0) {
    const { rows, truncated } = await fetchOrderedMatchListForSameWindow(
      device,
      rawAfter,
      rawBefore,
      fenceIds,
      geofenceType,
      orderDesc,
      Math.min(VINEYARD_WINDOW_MATCH_LIST_CAP, orderedMatchListMax)
    );
    debug.matchingRowsOrdered = rows;
    debug.matchingRowsTruncated = truncated;
    debug.matchingRowsCaption = `Rows matching this lookup’s WHERE (device_name, geofence_id ANY mapped set, ${geofenceType}, strict position_time_nz window — same predicates as the LIMIT 1 query). Ordered ${orderDesc ? 'DESC' : 'ASC'}; up to ${Math.min(VINEYARD_WINDOW_MATCH_LIST_CAP, orderedMatchListMax)} listed; later guardrails may still exclude the chosen row.`;
  }

  return { value, trackingId: trackingIdSafe, debug };
}

/**
 * Morning winery EXIT (step 1): first mapped winery EXIT in the window.
 * Disqualification is handled by the *upper bound* passed in by caller:
 * when a mapped vineyard ENTER exists, caller caps step 1 search to be strictly before that ENTER
 * (so we don't treat a return-leg winery EXIT as "start job").
 */
async function getFirstWineryMorningExitInWindowWithDebug(
  device: string,
  positionAfter: string,
  positionBefore: string | null,
  wineryFenceIds: number[]
): Promise<{ value: string | null; trackingId: number | null; debug: TrackingLookupDebug }> {
  const rawAfter = normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19);
  const rawBefore = positionBefore ? (normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19)) : null;

  if (wineryFenceIds.length === 0) {
    return {
      value: null,
      trackingId: null,
      debug: {
        device,
        positionAfter,
        positionBefore,
        fenceIds: [],
        geofenceType: 'EXIT',
        found: false,
        position_time_nz: null,
        trackingId: null,
        matchedGeofenceId: null,
        matchedFenceName: null,
        sqlHint: `(no fence_ids)`,
      },
    };
  }

  const sqlHint = `first winery EXIT in (positionAfter, positionBefore) — step 1 cap handled by upper bound`;

  let timeCondition = 't.position_time_nz > $3';
  const params: unknown[] = [device, wineryFenceIds, rawAfter];
  if (rawBefore) {
    params.push(rawBefore);
    timeCondition += ' AND t.position_time_nz < $4';
  }

  const rows = await query<{ id: unknown; geofence_id: unknown; position_time_nz: unknown; fence_name: unknown }>(
    `SELECT t.id, t.geofence_id,
            to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz,
            g.fence_name
     FROM tbl_tracking t
     LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
     WHERE t.device_name = $1
       AND t.geofence_id = ANY($2::int[])
       AND t.geofence_type = 'EXIT'
       AND ${timeCondition}
     ORDER BY t.position_time_nz ASC
     LIMIT 1`,
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

  const rawGf = rows[0]?.geofence_id;
  const matchedGeofenceId =
    rawGf != null && typeof rawGf === 'number'
      ? rawGf
      : rawGf != null && (typeof rawGf === 'string' || typeof rawGf === 'bigint')
        ? Number(rawGf)
        : null;
  const matchedGeofenceIdSafe = matchedGeofenceId != null && Number.isFinite(matchedGeofenceId) ? matchedGeofenceId : null;

  const fn = rows[0]?.fence_name;
  const matchedFenceName =
    fn != null && String(fn).trim() !== '' ? String(fn).trim() : null;

  const debug: TrackingLookupDebug = {
    device,
    positionAfter,
    positionBefore,
    fenceIds: [...wineryFenceIds],
    geofenceType: 'EXIT',
    found: value != null,
    position_time_nz: value,
    trackingId: trackingIdSafe,
    matchedGeofenceId: matchedGeofenceIdSafe,
    matchedFenceName,
    sqlHint,
  };

  return { value, trackingId: trackingIdSafe, debug };
}

const STEP1_MORNING_EXIT_RULE_ENGLISH =
  'GPS step 1 is the first mapped winery EXIT (fence_ids = this job delivery winery ∪ previous same-day job delivery winery when different) with lowerExclusive < t_exit < upperExclusive (strict). When a mapped vineyard ENTER exists, upperExclusive is capped to be strictly before that vineyard ENTER (so Step 1 cannot land after arriving at vineyard). Winery ENTER rows do not disqualify the EXIT.';

function parseLatLonForAudit(v: unknown): number | null {
  if (v == null) return null;
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  const n = parseFloat(String(v).trim());
  return Number.isFinite(n) ? n : null;
}

function step1MorningExitSnapshotFromRow(
  row: {
    id: unknown;
    device_name?: unknown;
    geofence_id?: unknown;
    geofence_type?: unknown;
    position_time_nz: unknown;
    position_time?: unknown;
    lat?: unknown;
    lon?: unknown;
    fence_name?: unknown;
  },
  fallbackGeofenceType: 'ENTER' | 'EXIT'
): Step1MorningExitTrackingRowSnapshot {
  const tNz =
    typeof row.position_time_nz === 'string'
      ? normalizeTimestampString(row.position_time_nz) ?? String(row.position_time_nz).slice(0, 19)
      : row.position_time_nz instanceof Date
        ? dateToLiteral(row.position_time_nz)
        : String(row.position_time_nz ?? '').slice(0, 19);
  const tStore =
    row.position_time != null
      ? typeof row.position_time === 'string'
        ? normalizeTimestampString(row.position_time) ?? String(row.position_time).slice(0, 19)
        : row.position_time instanceof Date
          ? dateToLiteral(row.position_time)
          : null
      : null;
  const rawId = row.id;
  const tid =
    rawId != null && typeof rawId === 'number'
      ? rawId
      : rawId != null && (typeof rawId === 'string' || typeof rawId === 'bigint')
        ? Number(rawId)
        : null;
  const gidRaw = row.geofence_id;
  const gid =
    gidRaw != null && typeof gidRaw === 'number'
      ? gidRaw
      : gidRaw != null && (typeof gidRaw === 'string' || typeof gidRaw === 'bigint')
        ? Number(gidRaw)
        : null;
  const gtRaw = (row.geofence_type != null ? String(row.geofence_type).trim().toUpperCase() : '') as string;
  const geofenceType: 'ENTER' | 'EXIT' =
    gtRaw === 'ENTER' || gtRaw === 'EXIT' ? gtRaw : fallbackGeofenceType;
  const dev = row.device_name != null && String(row.device_name).trim() !== '' ? String(row.device_name).trim() : null;
  const fn = row.fence_name != null && String(row.fence_name).trim() !== '' ? String(row.fence_name).trim() : null;
  const lat = parseLatLonForAudit(row.lat);
  const lon = parseLatLonForAudit(row.lon);
  const latLon =
    lat != null && lon != null ? `${lat}, ${lon}` : lat != null ? String(lat) : lon != null ? String(lon) : '—';
  const idStr = tid != null && Number.isFinite(tid) ? String(tid) : '—';
  const gidStr = gid != null && Number.isFinite(gid) ? String(gid) : '—';
  const asGridRow = `${dev ?? '—'} | ${fn ?? '—'} | geofence_id=${gidStr} | ${geofenceType} | ${tNz} | ${latLon} | tbl_tracking.id=${idStr}`;
  return {
    tblTrackingId: tid != null && Number.isFinite(tid) ? tid : null,
    deviceName: dev,
    geofenceId: gid != null && Number.isFinite(gid) ? gid : null,
    geofenceType,
    fenceName: fn,
    positionTimeNz: tNz,
    positionTimeStore: tStore,
    lat,
    lon,
    asGridRow,
  };
}

/**
 * Read-only diagnostics for Inspect: naive first winery EXIT in the same (X,Y) window (ignores NOT EXISTS),
 * then first winery ENTER between X and that EXIT (shows why e.g. Delegat EXIT after Delegat ENTER is ignored).
 */
async function attachWineryStep1MorningExitAudit(
  debug: DerivedStepsDebug,
  args: {
    device: string;
    wineryFenceIds: number[];
    trackingWindowAfter: string;
    step1MorningUpperExclusive: string | null;
    positionAfter: string;
    anchor: string | null;
    positionBefore: string | null;
    step1UpperResolved: string | null;
    step2PolygonEnter: string | null;
    vworkStep2Cap: string | null;
    rerunAfterLeaveWineryRefine: boolean;
  }
): Promise<void> {
  if (args.wineryFenceIds.length === 0) {
    debug.winery.step1MorningExitSearch = {
      lowerExclusive: null,
      upperExclusive: null,
      positionAfterFromOptions: normalizeTimestampString(args.positionAfter),
      jobStartAnchor: args.anchor != null ? normalizeTimestampString(args.anchor) : null,
      trackingWindowAfterUsed: null,
      step1UpperResolved: args.step1UpperResolved != null ? normalizeTimestampString(args.step1UpperResolved) : null,
      polygonGpsStep2EnterAtAudit:
        args.step2PolygonEnter != null ? normalizeTimestampString(args.step2PolygonEnter) : null,
      vworkStep2Cap: args.vworkStep2Cap != null ? normalizeTimestampString(args.vworkStep2Cap) : null,
      rerunAfterLeaveWineryRefine: args.rerunAfterLeaveWineryRefine,
      wineryFenceIdsUsed: [],
      rulePlainEnglish: STEP1_MORNING_EXIT_RULE_ENGLISH,
      notExistsCheckPlain: null,
      comparisons: ['No mapped winery geofence_id list — nothing to compare.'],
      naiveFirstWineryExit: null,
      blockingWineryEnterBeforeNaiveExit: null,
      morningExitOutcome: 'no_fence_ids',
      summaryLine: 'No mapped winery fence ids — morning winery EXIT (step 1) query was not run.',
    };
    return;
  }

  const lowerRaw =
    normalizeTimestampString(args.trackingWindowAfter) ?? String(args.trackingWindowAfter).trim().slice(0, 19);
  const upperRaw =
    args.step1MorningUpperExclusive != null && String(args.step1MorningUpperExclusive).trim() !== ''
      ? normalizeTimestampString(args.step1MorningUpperExclusive) ??
        String(args.step1MorningUpperExclusive).trim().slice(0, 19)
      : null;

  let naiveFirst: WineryStep1MorningExitSearchDebug['naiveFirstWineryExit'] = null;
  // Legacy audit field (winery ENTER “re-enter” blocker) — rule no longer uses this.
  let blocking: WineryStep1MorningExitSearchDebug['blockingWineryEnterBeforeNaiveExit'] = null;

  const paramsNaive: unknown[] = [args.device, args.wineryFenceIds, lowerRaw];
  let condNaive = `t.position_time_nz > $3`;
  if (upperRaw) {
    paramsNaive.push(upperRaw);
    condNaive += ` AND t.position_time_nz < $4`;
  }
  type AuditTrackRow = {
    id: unknown;
    device_name: unknown;
    geofence_id: unknown;
    geofence_type: unknown;
    position_time_nz: unknown;
    position_time: unknown;
    lat: unknown;
    lon: unknown;
    fence_name: unknown;
  };
  const naiveRows = await query<AuditTrackRow>(
    `SELECT t.id, t.device_name, t.geofence_id, t.geofence_type,
            to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz,
            to_char(t.position_time, 'YYYY-MM-DD HH24:MI:SS') AS position_time,
            t.lat, t.lon,
            g.fence_name
     FROM tbl_tracking t
     LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
     WHERE t.device_name = $1
       AND t.geofence_id = ANY($2::int[])
       AND t.geofence_type = 'EXIT'
       AND ${condNaive}
     ORDER BY t.position_time_nz ASC
     LIMIT 1`,
    paramsNaive
  );
  const n0 = naiveRows[0];
  let naiveExitTimeForNotExists: string | null = null;
  if (n0?.position_time_nz != null) {
    naiveFirst = step1MorningExitSnapshotFromRow(n0, 'EXIT');
    naiveExitTimeForNotExists = naiveFirst.positionTimeNz;

    const paramsBlock: unknown[] = [args.device, args.wineryFenceIds, lowerRaw, naiveExitTimeForNotExists];
    const blockRows = await query<AuditTrackRow>(
      `SELECT t.id, t.device_name, t.geofence_id, t.geofence_type,
              to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz,
              to_char(t.position_time, 'YYYY-MM-DD HH24:MI:SS') AS position_time,
              t.lat, t.lon,
              g.fence_name
       FROM tbl_tracking t
       LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
       WHERE t.device_name = $1
         AND t.geofence_id = ANY($2::int[])
         AND t.geofence_type = 'ENTER'
         AND t.position_time_nz > $3::timestamp
         AND t.position_time_nz < $4::timestamp
       ORDER BY t.position_time_nz ASC
       LIMIT 1`,
      paramsBlock
    );
    const b0 = blockRows[0];
    if (b0?.position_time_nz != null) {
      blocking = step1MorningExitSnapshotFromRow(b0, 'ENTER');
    }
  }

  const mainFound =
    debug.winery.step1?.found === true &&
    debug.winery.step1?.position_time_nz != null &&
    String(debug.winery.step1.position_time_nz).trim() !== '';

  const fenceIdList = [...args.wineryFenceIds];
  const comparisons: string[] = [];
  comparisons.push(
    `Candidate winery EXIT must satisfy (strict): X < t_exit < Y, with X=${lowerRaw}, Y=${upperRaw ?? '(no upper — open ended)'}.`
  );
  comparisons.push(
    `X is trackingWindowAfter (= min(job window positionAfter, job-start anchor)); Y is the final step1Before cap (see step1UpperResolved / polygon step 2 / VWork step 2).`
  );
  comparisons.push(`device_name for all queries: ${args.device}.`);
  comparisons.push(`Mapped winery geofence_id list (ENTER and EXIT on these ids only): [${fenceIdList.join(', ')}].`);
  comparisons.push(
    `Ordered Part 1 context: polygon vineyard ENTER (tentative Step 2) is fetched first; this Step 1 morning-EXIT audit uses that tentative ENTER to cap the exclusive upper when present (see polygonGpsStep2EnterAtAudit).`
  );
  comparisons.push(
    `Inherited lower X = trackingWindowAfter = min(options.positionAfter=${normalizeTimestampString(args.positionAfter) ?? String(args.positionAfter).trim().slice(0, 19)}, jobStartAnchor=${args.anchor != null ? normalizeTimestampString(args.anchor) ?? String(args.anchor).slice(0, 19) : '—'}) → ${lowerRaw}.`
  );
  comparisons.push(
    `Tentative polygon Step 2 ENTER (pre-refine, caps step-1 upper when set): ${args.step2PolygonEnter != null ? (normalizeTimestampString(args.step2PolygonEnter) ?? String(args.step2PolygonEnter).slice(0, 19)) : '(null — upper may use VWork step 2 or window end)'}. VWork step_2_completed_at cap: ${args.vworkStep2Cap != null ? normalizeTimestampString(args.vworkStep2Cap) ?? String(args.vworkStep2Cap).slice(0, 19) : '(null)'}.`
  );

  let notExistsCheckPlain: string | null = null;
  if (naiveFirst != null && naiveExitTimeForNotExists != null) {
    comparisons.push('Naive first winery EXIT in (X,Y) — first row if we IGNORE the re-enter NOT EXISTS rule (debug only):');
    comparisons.push(`  ${naiveFirst.asGridRow}`);
    notExistsCheckPlain =
      'Winery ENTER rows are ignored for step 1 selection. Disqualification is instead via the upper bound: when a mapped vineyard ENTER exists, upperExclusive is that vineyard ENTER time, so any winery EXIT at/after vineyard ENTER is outside the search window.';
  } else {
    comparisons.push('Naive first winery EXIT: (none in interval) — no row to compare for NOT EXISTS.');
  }

  if (naiveFirst != null && args.step2PolygonEnter != null) {
    const vineEnter = normalizeTimestampString(args.step2PolygonEnter) ?? String(args.step2PolygonEnter).slice(0, 19);
    comparisons.push(`Vineyard ENTER (Step 2 polygon) used as cap when present: vineyard_enter=${vineEnter}.`);
    comparisons.push(
      `This step-1 query upperExclusive is capped to be strictly before that vineyard ENTER, so any winery EXIT with t_exit ≥ ${vineEnter} is ignored for step 1.`
    );
    comparisons.push(`Naive winery EXIT time: t_exit=${naiveFirst.positionTimeNz}. Check: ${naiveFirst.positionTimeNz} < ${vineEnter}.`);
  }

  if (mainFound && debug.winery.step1) {
    const s1 = debug.winery.step1;
    const s1t =
      s1.position_time_nz != null
        ? (normalizeTimestampString(String(s1.position_time_nz)) ?? String(s1.position_time_nz).trim().slice(0, 19))
        : '—';
    comparisons.push('Main morning-EXIT query result (after NOT EXISTS):');
    comparisons.push(
      `  id=${s1.trackingId ?? '—'} · ${s1t} · ${s1.matchedFenceName ?? '—'} · geofence_id=${s1.matchedGeofenceId ?? '—'}`
    );
  } else {
    comparisons.push('Main morning-EXIT query result: (no row accepted).');
  }

  let morningExitOutcome: WineryStep1MorningExitSearchDebug['morningExitOutcome'];
  let summaryLine: string;
  if (!upperRaw) {
    morningExitOutcome = 'no_upper_bound';
    summaryLine =
      'No exclusive upper bound for morning winery EXIT (step1Before was null) — same open-ended behaviour as main query.';
  } else if (naiveFirst == null) {
    morningExitOutcome = 'no_exit_in_xy_window';
    summaryLine = `No mapped winery EXIT in (strict) ${lowerRaw} < t_exit < ${upperRaw}. Rows outside this open interval are ignored for step 1 fetch.`;
  } else if (naiveFirst != null && args.step2PolygonEnter != null) {
    // With vineyard ENTER present, the upper bound is already capped so the selected EXIT must be before vineyard ENTER.
    morningExitOutcome = mainFound ? 'main_query_found_exit' : 'no_exit_in_xy_window';
    const vineEnter = normalizeTimestampString(args.step2PolygonEnter) ?? String(args.step2PolygonEnter).slice(0, 19);
    summaryLine = mainFound
      ? `Morning winery EXIT accepted at ${String(debug.winery.step1?.position_time_nz).slice(0, 19)} — and it is strictly before vineyard ENTER ${vineEnter}.`
      : `No winery EXIT found in (strict) ${lowerRaw} < t_exit < ${upperRaw} (upper bound is vineyard ENTER ${vineEnter} when present).`;
  } else {
    morningExitOutcome = mainFound ? 'main_query_found_exit' : 'no_exit_in_xy_window';
    summaryLine = mainFound
      ? `Morning winery EXIT accepted at ${String(debug.winery.step1?.position_time_nz).slice(0, 19)} — no blocking winery ENTER between lowerExclusive ${lowerRaw} and that EXIT.`
      : `Unexpected: naive first EXIT ${naiveFirst.positionTimeNz} has no blocking ENTER but main query did not return it — check timestamp normalization.`;
  }

  debug.winery.step1MorningExitSearch = {
    lowerExclusive: lowerRaw,
    upperExclusive: upperRaw,
    positionAfterFromOptions: normalizeTimestampString(args.positionAfter),
    jobStartAnchor: args.anchor != null ? normalizeTimestampString(args.anchor) : null,
    trackingWindowAfterUsed: lowerRaw,
    step1UpperResolved: args.step1UpperResolved != null ? normalizeTimestampString(args.step1UpperResolved) : null,
    polygonGpsStep2EnterAtAudit:
      args.step2PolygonEnter != null ? normalizeTimestampString(args.step2PolygonEnter) : null,
    vworkStep2Cap: args.vworkStep2Cap != null ? normalizeTimestampString(args.vworkStep2Cap) : null,
    rerunAfterLeaveWineryRefine: args.rerunAfterLeaveWineryRefine,
    wineryFenceIdsUsed: fenceIdList,
    rulePlainEnglish: STEP1_MORNING_EXIT_RULE_ENGLISH,
    notExistsCheckPlain,
    comparisons,
    naiveFirstWineryExit: naiveFirst,
    blockingWineryEnterBeforeNaiveExit: blocking,
    morningExitOutcome,
    summaryLine,
  };
}

/** Max vineyard EXIT→ENTER same-fence aggregations for step 3 (GPS*); further same-vineyard pairs are ignored. */
const MAX_GPS_STAR_LOOPS = 3;

type FenceEventRow = {
  id: number;
  geofenceId: number;
  geofenceType: 'ENTER' | 'EXIT';
  timeNorm: string;
};

/** All ENTER/EXIT rows in the tracking window, ordered by time (for GPS* step-3 aggregation and admin detour checks). */
export async function listFenceEnterExitEventsInWindow(
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
  /** VWork step 5 (job completed in system). Step 5 GPS uses winery EXIT before this time, or before this + step-5 extend buffer. */
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

/** Part 1 output: Raw GPS candidates per step (winery/vineyard, ENTER/EXIT). Steps 1–4: use if present. Step 5: Part 2 keeps GPS if &lt; job end or within step-5 extend minutes after job end. */
export type FetchedGpsCandidates = {
  step1: GpsStepCandidate | null;
  step2: GpsStepCandidate | null;
  step3: GpsStepCandidate | null;
  step4: GpsStepCandidate | null;
  step5: GpsStepCandidate | null;
  /** Step 3 used GPS* vineyard re-exit aggregation (tbl_vworkjobs.step_3_via / calcnotes). */
  step3GpsStar?: boolean;
  /** Step 2/3 resolved via Bankhouse fallback (VineSR1) for Bankhouse South. */
  vineSr1Fallback?: boolean;
};

export type StepVia =
  | 'GPS'
  | 'VW'
  | 'RULE'
  | 'ORIDE'
  | 'VineFence+'
  | 'VineFenceV+'
  | 'GPS*'
  | 'VineSR1'
  /** Cleanup: VWork step 3 after GPS step 4 — wind step 3 back from GPS step 4 (or midpoint fallback). */
  | 'Step3windback';

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
  /** Steps+ may set before finalizeDerivedSteps so applyOrides keeps VineFence+ / VineFenceV+. */
  step2Via?: StepVia;
  step3Via?: StepVia;
};

/**
 * Inspect / JSON: auditable bounds for the Part 1 query “first winery EXIT after GPS step 4 ENTER”.
 * SQL uses `position_time_nz > lowerExclusive` AND `position_time_nz < upperExclusive` (both strict).
 */
export type WineryStep5SearchWindowDebug = {
  /** If non-null, the EXIT row query was not run for this reason. */
  fetchSkippedReason:
    | null
    | 'no_winery_fence_ids'
    | 'no_mapped_winery_fences'
    | 'no_delivery_winery_on_job'
    | 'no_gps_step4_enter'
    | 'no_vwork_job_end_for_step5_rule'
    | 'no_step5_upper_bound';
  /** Lower bound (exclusive) passed to tbl_tracking for step 5 EXIT — same instant as chosen winery ENTER for step 4. */
  lowerExclusive: string | null;
  /** Upper bound (exclusive) — `max(positionBefore from options, anchor + Step5ExtendWineryExit)` when both exist. */
  upperExclusive: string | null;
  /** `step_5_completed_at ?? actual_end_time` (normalized), used for extend cap — not `gps_end_time`. */
  jobEndForStep5Rule: string | null;
  step5ExtendWineryExitMinutes: number;
  /** max(tap, GPS step 4 ENTER); extend is added to this (not to tap alone when GPS 4 is later). */
  step5ExtendAnchor: string | null;
  /** `step5ExtendAnchor + extend` when extend &gt; 0; else same as anchor; null if no anchor. */
  jobEndPlusExtend: string | null;
  /** `options.positionBefore` passed into derivation (Inspect / tagging job window end). */
  positionBeforeFromOptions: string | null;
  /**
   * How `upperExclusive` was chosen from `positionBefore` vs `anchor + Step5Extend`:
   * `anchor_plus_extend_wider_than_position_before` = extend band extends past job window end (max used);
   * `position_before_wider_than_anchor_plus_extend` = job window end is later than extend cap;
   * `position_before_equals_job_end_plus_extend` = position before, anchor+extend, and upperExclusive all the same instant.
   */
  upperExclusiveSource:
    | 'not_computed'
    | 'only_position_before'
    | 'only_job_end_plus_extend'
    | 'only_vwork_step5_no_extend_zero'
    | 'anchor_plus_extend_wider_than_position_before'
    | 'position_before_wider_than_anchor_plus_extend'
    | 'position_before_equals_job_end_plus_extend';
  /** True when `getFirstTrackingInWindowWithDebug` ran for winery EXIT after step 4. */
  step5ExitQueryRan: boolean;
  /** One line for UI: e.g. “EXIT must satisfy step4 &lt; t &lt; upper (exclusive).” */
  summaryLine: string;
};

/** One tbl_tracking row as shown in Inspect GPS grid — for step 1 morning EXIT audit only. */
export type Step1MorningExitTrackingRowSnapshot = {
  tblTrackingId: number | null;
  deviceName: string | null;
  geofenceId: number | null;
  geofenceType: 'ENTER' | 'EXIT';
  fenceName: string | null;
  positionTimeNz: string;
  /** Raw `position_time` when present (may differ from NZ column in edge cases). */
  positionTimeStore: string | null;
  lat: number | null;
  lon: number | null;
  /** device | fence | ENTER/EXIT | position_time_nz | lat/lon | id=… — scan line for operators */
  asGridRow: string;
};

/**
 * Inspect / JSON: morning winery EXIT (GPS step 1) — same bounds as {@link getFirstWineryMorningExitInWindowWithDebug},
 * plus a naive “first EXIT in window” row to show re-entry / NOT EXISTS disqualification.
 */
export type WineryStep1MorningExitSearchDebug = {
  /** SQL lower (exclusive): same instant passed as `positionAfter` to morning EXIT query (= trackingWindowAfter). */
  lowerExclusive: string | null;
  /** SQL upper (exclusive): final `step1Before` cap (min of job window end vs polygon step 2 or VWork step 2). */
  upperExclusive: string | null;
  positionAfterFromOptions: string | null;
  /** Anchor = step1oride or VWork step 1 — used with positionAfter to form trackingWindowAfter. */
  jobStartAnchor: string | null;
  trackingWindowAfterUsed: string | null;
  /** Resolved min(positionBefore, step2 polygon | VWork step2 cap) before ?? chain. */
  step1UpperResolved: string | null;
  polygonGpsStep2EnterAtAudit: string | null;
  vworkStep2Cap: string | null;
  /** Morning EXIT query was re-run after “leave winery → arrive vineyard” refine (updated step 2). */
  rerunAfterLeaveWineryRefine: boolean;
  /** `geofence_id` set used for morning winery ENTER/EXIT (same as Part 1 query). */
  wineryFenceIdsUsed: number[];
  /** Main rule (NOT EXISTS winery ENTER between lower and candidate EXIT). */
  rulePlainEnglish: string;
  /** What the NOT EXISTS subquery enforces (plain language with this job’s X and naive EXIT time when known). */
  notExistsCheckPlain: string | null;
  /** Ordered lines: what is compared to what (intervals, row vs row). */
  comparisons: string[];
  /** First winery EXIT in (lower, upper) ignoring NOT EXISTS — read-only diagnostic. */
  naiveFirstWineryExit: Step1MorningExitTrackingRowSnapshot | null;
  /** Legacy (no longer used): winery ENTER blocker snapshot. Always null under the current rule. */
  blockingWineryEnterBeforeNaiveExit: Step1MorningExitTrackingRowSnapshot | null;
  morningExitOutcome:
    | 'main_query_found_exit'
    | 'no_exit_in_xy_window'
    | 'no_fence_ids'
    | 'no_upper_bound';
  summaryLine: string;
};

/**
 * Part 1: why GPS step 1 (morning winery EXIT) was kept or cleared in {@link applyGpsGuardrails}.
 * GPS2 values are snapshots at the anchor-vs-G2 bracket check (before floor/dedup mutates step2).
 */
export type GpsStep1GuardrailDebug = {
  vworkJobEndForStep12Ceiling: string | null;
  droppedGps1AtOrAfterVworkJobEnd: boolean;
  gps1BeforeJobEndCheck: string | null;
  gps1TrackingIdBeforeJobEndCheck: number | null;
  jobStartAnchor: string | null;
  /** True when anchor and GPS1 existed so the G1&gt;anchor vs GPS2 rule was evaluated. */
  anchorBracketEvaluated: boolean;
  gps1AtAnchorBracket: string | null;
  gps1TrackingIdAtAnchorBracket: number | null;
  /** GPS step 2 at bracket check (same moment as `keepG1AfterV1` in code). */
  gps2AtAnchorBracketCheck: string | null;
  gps2TrackingIdAtAnchorBracketCheck: number | null;
  gps1StrictlyAfterAnchor: boolean | null;
  /** `g2 != null && g1 < g2` — required to keep GPS1 when `g1 > anchor`. */
  keepG1AfterAnchorConditionMet: boolean | null;
  droppedGps1ByAfterAnchorBracket: boolean;
  /** `min(positionAfter, job-start anchor)` — tentative G2 must be strictly after this (same as tracking window lower). */
  step1BracketTrackingFloor: string | null;
  /** Caller-supplied VineFence+ merged enter (read-only for bracket). */
  tentativeVineyardEnterFromOptions: string | null;
  /** Tentative time after anchor and floor checks; null if absent or rejected. */
  tentativeG2QualifiedForBracket: string | null;
  /** True when bracket used Steps+ tentative enter because polygon GPS2 was null. */
  bracketUsedTentativeG2: boolean;
  /** Effective G2 instant for strict `g1 < g2` (committed polygon GPS2, else qualified tentative). */
  g2EffectiveForAnchorBracket: string | null;
  anchorBracketOutcome:
    | 'not_evaluated_no_anchor'
    | 'not_evaluated_no_gps1'
    | 'not_evaluated_gps1_cleared_by_job_end_first'
    | 'evaluated_gps1_not_after_anchor_unchanged'
    | 'kept_g1_after_anchor_g2_exists_and_g1_before_g2'
    | 'dropped_g1_after_anchor_no_gps2'
    | 'dropped_g1_after_anchor_g1_not_strictly_before_gps2';
  summaryLine: string;
};

/** Why Part 2 kept or dropped GPS step 5 after guardrails (same rules as `decideFinalSteps`). */
export type Step5DecideDebug = {
  vworkStep5: string | null;
  step5ExtendWineryExitMinutes: number;
  /** Candidate entering `decideFinalSteps` (after `applyGpsGuardrails`). */
  fetchCandidateTime: string | null;
  fetchCandidateTrackingId: number | null;
  /** Exclusive upper for “after tap” acceptance: max(tap, GPS step 4 ENTER) + extend (Part 2). */
  acceptAfterJobEndExclusiveUpper: string | null;
  /** Same anchor as Part 1 step-5 window: max(VWork tap, GPS winery ENTER step 4). */
  step5ExtendAnchor: string | null;
  step5GpsAccepted: boolean;
  outcome:
    | 'no_candidate_after_guardrails'
    | 'accepted_exit_strictly_before_job_end'
    | 'accepted_exit_after_job_end_within_extend'
    | 'rejected_exit_at_or_after_job_end_outside_extend'
    | 'rejected_extend_disabled_and_exit_not_before_job_end'
    | 'skipped_no_vwork_job_end';
  summaryLine: string;
};

export type VineyardGpsOrderingFloorRule =
  | 'oride_only'
  | 'max_oride_and_gps1'
  | 'min_tap_and_gps1_no_oride'
  | 'gps1_only'
  | 'tap_only';

export type VineyardGpsOrderingFloorDebug = {
  orideNorm: string | null;
  gps1NormAfterBracket: string | null;
  tapOnlyNorm: string | null;
  floorFor235: string | null;
  rule: VineyardGpsOrderingFloorRule;
  summaryLine: string;
};

/** Snapshot of one vineyard/winery GPS step before post-fetch guardrails (Inspect: why a grid row was not kept). */
export type VineyardStepPreclearSnapshot = {
  positionTimeNz: string | null;
  trackingId: number | null;
  matchedGeofenceId: number | null;
  matchedFenceName: string | null;
  device: string | null;
};

/**
 * After Part 1 fetch, `applyGpsGuardrails` may clear steps 2/3/5. Inspect uses this to separate
 * “SQL never picked this row” vs “picked then dropped by floor / job end / duplicate tracking id”.
 */
export type VineyardPart1FetchGuardrailDebug = {
  orderingFloorExclusive: string | null;
  preclearStep2: VineyardStepPreclearSnapshot | null;
  preclearStep3: VineyardStepPreclearSnapshot | null;
  preclearStep5: VineyardStepPreclearSnapshot | null;
  /** GPS winery ENTER after fetch, before max(vineyard ordering floor, step2, step3) guardrail on step 4. */
  preclearStep4: VineyardStepPreclearSnapshot | null;
  clearedByOrderingFloorStep2: boolean;
  clearedByOrderingFloorStep3: boolean;
  clearedByOrderingFloorStep5: boolean;
  /** Step 4 cleared because winery ENTER was not strictly after max(ordering floor, step2, step3). */
  clearedByStep4OrderingFloor: boolean;
  /** Floor used with step 4 (same components as fetch lower when both vineyard GPS steps exist; else see code). */
  step4OrderingFloorExclusive: string | null;
  clearedVworkJobEndStep2And3: boolean;
  clearedStep3OnlyJobEndCeiling: boolean;
  duplicateTrackingIdClears: string[];
  /** One paragraph for polygon Step 2 fail / audit. */
  summaryLineStep2Polygon: string;
};

/** Inspect: how Part 1 computed the strict lower bound for winery ENTER (step 4) vs relaxed audit lower. */
export type Step4FetchLowerBreakdownDebug = {
  anchor: string | null;
  gpsMorningExit: string | null;
  /** Value in max() for the step-1 leg: anchor when set, else GPS morning winery EXIT. */
  step1LegUsedForFetch: string | null;
  step2: string | null;
  step3: string | null;
  positionAfterOption: string | null;
  fetchLowerExclusive: string | null;
  /** max(positionAfter, step2, step3) — winery ENTER audit window when this is strictly before fetch lower. */
  auditRelaxedLowerExclusive: string | null;
  bothVineyardGpsStepsMissing: boolean;
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
    /** Part 1 fetch → guardrails: which candidate existed and whether floor / job end / dedupe cleared it. */
    part1FetchGuardrail?: VineyardPart1FetchGuardrailDebug;
  };
  winery: FenceResolutionDebug & {
    step1?: TrackingLookupDebug; // Winery EXIT before Vineyard ENTER
    step4?: TrackingLookupDebug;
    step5?: TrackingLookupDebug; // Winery EXIT after step 4, before job end = GPS job end
    /** Auditable morning winery EXIT (step 1) window + re-enter rule; Inspect Explanation surfaces this. */
    step1MorningExitSearch?: WineryStep1MorningExitSearchDebug;
    /** Step 1 morning EXIT fence_id union: this job delivery winery ∪ previous same-day job winery when chained. */
    step1MorningFenceUnion?: Step1MorningWineryFenceUnionDebug;
    /** Auditable (X,Y) window for step 5 EXIT fetch; Inspect Explanation surfaces this. */
    step5SearchWindow?: WineryStep5SearchWindowDebug;
    /** Step 4: anchor vs GPS1 vs step2/3 → strict fetch lower vs relaxed audit lower (Inspect Explanation). */
    step4FetchLowerBreakdown?: Step4FetchLowerBreakdownDebug;
  };
  /** Part 2: why GPS step 5 was kept or dropped vs VWork job end + extend. */
  step5Decide?: Step5DecideDebug;
  /** Part 1: auditable floor used to drop vineyard GPS steps 2/3/5 when t ≤ floor (and step‑4 winery lower bound). */
  vineyardGpsOrderingFloor?: VineyardGpsOrderingFloorDebug;
  /** Part 1: why GPS step 1 was kept or dropped in `applyGpsGuardrails` (anchor vs GPS2 bracket). */
  gpsStep1Guardrail?: GpsStep1GuardrailDebug;
  /** Step 3 extended via same-vineyard re-entry smoothing (GPS*). */
  step3GpsStar?: boolean;
  /** True when VineSR1 fallback (Bankhouse South → Bankhouse) produced step 2/3. */
  vineSr1?: boolean;
};

/** After cleanup: when step1 vwork > step2 gps we set step1_actual = step2_gps - travel_min (step4−step3); Via = RULE. */
export type Step1CleanupOverride = string | null;

/** Auditable snapshot from `applyCleanupRules` for Inspect / API JSON (Part 3b). */
export type CleanupRulesReport = {
  step1: {
    applied: boolean;
    rule?: 'cleanup_start' | 'travel';
    step1Before?: string | null;
    step1After?: string | null;
  };
  step3Windback:
    | null
    | {
        path: 'wind' | 'midpoint';
        mergedStep3Before: string;
        step4Gps: string;
        outboundMinutes: number;
        step3After: string;
      };
  step4Order:
    | null
    | {
        mergedStep4Before: string;
        step3At: string;
        step4After: string;
        outboundMinutes: number;
      };
  /** No merged step 4 (GPS/VWork); step 3 and 5 present — midpoint on return leg (e.g. unmapped winery fences). */
  step4Mid35:
    | null
    | {
        step3At: string;
        step5At: string;
        step4After: string;
      };
};

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
  /** Part 3b cleanup audit (Step3windback, step4_order, step 1 travel/start). */
  cleanupRulesReport?: CleanupRulesReport;
};

export type DerivedStepsOptions = {
  windowMinutes: number;
  /** Same as tbl_tracking API: device = tbl_vworkjobs.worker (device_name for tbl_tracking). */
  device: string;
  positionAfter: string;
  positionBefore: string | null;
  /**
   * Minutes past VWork job end (`step_5_completed_at` / `actual_end_time`) that GPS step 3 (vineyard EXIT) may still use.
   * Defaults to `JOB_END_CEILING_BUFFER_DEFAULT_MINUTES` when omitted (caller should load from tbl_settings when possible).
   */
  jobEndCeilingBufferMinutes?: number;
  /**
   * Minutes after max(VWork tap, GPS winery ENTER step 4) to search for winery EXIT and accept it as GPS step 5
   * (early “job complete” before physical leave, or tap before GPS return to winery).
   * Defaults to `STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES` when omitted (tbl_settings Step5ExtendWineryExit).
   */
  step5ExtendWineryExitMinutes?: number;
  /**
   * VineFence+ merged vineyard ENTER from the same Steps+ snapshot as `/api/tracking/derived-steps`
   * (computed once before Part 1). Used only inside {@link applyGpsGuardrails} to qualify morning GPS step 1
   * when polygon `candidates.step2` is missing — never written to `candidates.step2`.
   */
  tentativeVineyardEnterForStep1Bracket?: string | null;
};

/**
 * STEP RULES (GPS-derived steps)
 * ------------------------------
 * Part 1 — Fetch: Get a valid (or probably valid) GPS entry (Winery/Vineyard, ENTER/EXIT). Part 2 — Decide: Steps 1–4 use GPS if exists else VWork; Step 5 use GPS if winery EXIT is before job end or within step-5 extend after max(tap, GPS step 4 ENTER).
 * Step 1 — Job start by GPS: First Winery EXIT strictly before arrive vineyard (mapped winery fences for **this**
 *   job’s delivery_winery **∪** when chained, the same-day **previous** job’s delivery_winery — same worker ordering as lastjobendstep1limiter).
 *   - Upper bound: if polygon vineyard ENTER exists, min(data window end, polygon ENTER) only — VWork step 2 is not used (can be early vs GPS). If polygon step 2 is missing, min(data window end, VWork step 2) so we do not take a return-leg EXIT as “start job”.
 *   - May be absent if the job started after the driver had already left the winery fence.
 * Step 2 — Arrive vineyard: First Vineyard ENTER in window (SQL lowerExclusive is max(geometry/window lower, vineyard ordering floor) so LIMIT 1 matches the same strictly-after-floor rule as Part 1 guardrails).
 * Step 3 — Leave vineyard: First Vineyard EXIT after step 2 (so we don't pick an earlier exit before the enter).
 *   GPS* (optional): If the driver briefly exits and re-enters the same vineyard fence set with no other fence
 *   ENTER/EXIT between, aggregate up to 3 such loops; step 3 becomes the last EXIT in the chain. Any alien fence
 *   event voids GPS* for that job (revert to first exit only). Marked step3Via = GPS* and calcnotes GPS*:.
 * Step 4 — Arrive winery (return leg): First mapped Winery ENTER strictly &lt; data window end (positionBefore).
 * Lower bound: **if both GPS step 2 and step 3 are present**, strictly &gt; max(**job step-1 anchor (oride∨VWork) ?? GPS morning winery EXIT**, step2, step3) (anchor replaces GPS step 1 in that max when set — early winery ENTER before anchor is excluded from Part 1).
 * **If both GPS step 2 and step 3 are missing**, do not use vineyard times in the max — use max(GPS step1 leg, positionAfter) only so a partial or absent vineyard GPS leg does not block a valid winery ENTER/EXIT pair in the window.
 * Step 5 — Job end by GPS: VWork step 5 = step_5_completed_at (job completed in system). Use GPS when Winery EXIT is strictly &lt; VWork step 5 (forgot to end job), OR when EXIT is ≥ job end and strictly &lt; max(tap, GPS step 4 ENTER) + `step5ExtendWineryExitMinutes` (tapped complete before leaving, or tap before physical return to winery). Search first EXIT after step 4 with upper bound max(positionBefore, that anchor + extend). FIRST such EXIT wins.
 *
 * Derive GPS step timestamps for a job using these rules. Window is passed in (same as tbl_tracking UI)
 * — no server-side timezone or date logic. Uses tbl_gpsmappings + original vwork name → tbl_geofences
 * → fence_ids; then scans tbl_tracking in window.
 *
 * Guardrail (after fetch): Steps 2–3 and 5 must be strictly after the **vineyard ordering floor** from {@link computeVineyardGpsOrderingFloorDebug} (with oride: max(oride, GPS1); without oride: min(tap, GPS1) when both exist). Step 4 uses the same step‑1 leg in max(step1 leg, step2, step3). Each tbl_tracking id may appear at most once. Step 5 cleared if step 4 cleared.
 * Guardrail — step 4: when GPS step 2 and 3 both present, max(step1 leg, step2, step3); when both missing, max(step1 leg, positionAfter) only.
 * Guardrail — VWork job end: GPS steps 1–2 must be strictly **before** VWork step 5. Step 3 may be before VWork step 5 plus `jobEndCeilingBufferMinutes` (Job End Ceiling Buffer). Step 5 fetch/accept may extend past job end by `step5ExtendWineryExitMinutes` from max(tap, GPS step 4 ENTER) (tbl_settings Step5ExtendWineryExit).
 */

/**
 * Upper bound for vineyard fence queries: extend a tight `positionBefore` to at least job end + buffer so late vineyard EXIT rows are visible to Part 1.
 * When `positionBefore` is null (open-ended window), leave null.
 */
function vineyardFetchPositionBefore(
  positionBefore: string | null,
  job: JobForDerivedSteps,
  bufferMinutes: number
): string | null {
  if (bufferMinutes <= 0) return positionBefore;
  const vworkSrc = job.step_5_completed_at ?? job.actual_end_time;
  if (vworkSrc == null || String(vworkSrc).trim() === '') return positionBefore;
  const vEnd = normalizeTimestampString(vworkSrc as string | Date);
  if (vEnd == null) return positionBefore;
  const ceiling = normalizeTimestampString(addMinutesToTimestampAsNZ(vEnd, bufferMinutes));
  if (ceiling == null) return positionBefore;
  if (positionBefore == null) return ceiling;
  const pb = normalizeTimestampString(positionBefore);
  if (pb == null) return positionBefore;
  return pb < ceiling ? ceiling : pb;
}

/**
 * VineFenceV+ only applies when buffered enter is **more than** this many minutes before polygon ENTER
 * (avoids false positives when the truck is briefly beside the fence on the road; real queues are longer).
 */
export const VINE_FENCE_V_PLUS_MIN_ENTER_DELTA_MINUTES = 5;

/**
 * VineFenceV+ may only pull **arrive vineyard** earlier than polygon ENTER (queue outside gate inside buffer).
 * Polygon EXIT is always kept verbatim when both polygon steps exist — we never widen the exit with the buffer.
 * Returns true when merged buffered enter is strictly before polygon ENTER, before polygon EXIT, and the
 * enter delta exceeds {@link VINE_FENCE_V_PLUS_MIN_ENTER_DELTA_MINUTES} minutes.
 */
export function vineyardBufferWidensPolygonEnter(
  mergedEnter: string,
  polygonEnter: string,
  polygonExit: string
): boolean {
  const e = normalizeTimestampString(mergedEnter);
  const pe = normalizeTimestampString(polygonEnter);
  const px = normalizeTimestampString(polygonExit);
  if (!e || !pe || !px) return false;
  if (e >= pe) return false;
  if (e >= px) return false;
  const deltaMin = minutesBetween(e, pe);
  if (deltaMin <= VINE_FENCE_V_PLUS_MIN_ENTER_DELTA_MINUTES) return false;
  return true;
}

/**
 * Minutes polygon ENTER is after merged buffered enter (positive = buffer pulled arrive earlier).
 * Zero if merged is not strictly before polygon ENTER.
 */
export function vineyardEnterMinutesEarlierThanPolygon(
  mergedEnter: string,
  polygonEnter: string
): number {
  const e = normalizeTimestampString(mergedEnter);
  const pe = normalizeTimestampString(polygonEnter);
  if (!e || !pe || e >= pe) return 0;
  return minutesBetween(e, pe);
}

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
 * Step 5 extend anchor: max(VWork tap, GPS winery ENTER step 4). Extend minutes are added to this instant
 * (not to tap alone) so when tap is before physical return to winery, the EXIT search upper bound is still wide enough.
 */
function step5ExtendAnchorMaxTapAndGps4(
  vworkTap5: string | null | undefined,
  gpsStep4Enter: string | null | undefined
): string | null {
  const tap =
    vworkTap5 != null && String(vworkTap5).trim() !== ''
      ? normalizeTimestampString(vworkTap5 as string | Date) ?? String(vworkTap5).trim().slice(0, 19)
      : null;
  if (tap == null) return null;
  const e4 =
    gpsStep4Enter != null && String(gpsStep4Enter).trim() !== ''
      ? normalizeTimestampString(gpsStep4Enter as string | Date) ?? String(gpsStep4Enter).trim().slice(0, 19)
      : null;
  if (e4 == null) return tap;
  return maxTimestampString(tap, e4) ?? tap;
}

/**
 * Step 5 EXIT fetch upper (exclusive): max(job `positionBefore`, anchor + Step5Extend) when both exist,
 * so a tight job/Inspect window does not clip the extend band after a late GPS step 4.
 */
function step5ExitExclusiveUpper(
  positionBefore: string | null | undefined,
  anchorPlusExtend: string | null | undefined
): string | null {
  const ext =
    anchorPlusExtend != null && String(anchorPlusExtend).trim() !== ''
      ? normalizeTimestampString(anchorPlusExtend as string | Date) ??
        String(anchorPlusExtend).trim().slice(0, 19)
      : null;
  const pb =
    positionBefore != null && String(positionBefore).trim() !== ''
      ? normalizeTimestampString(positionBefore as string | Date) ??
        String(positionBefore).trim().slice(0, 19)
      : null;
  if (ext == null) return pb ?? null;
  if (pb == null) return ext;
  return maxTimestampString(pb, ext) ?? ext;
}

/** Lexicographic min (earliest instant); ignores nulls. */
function minTimestampString(...vals: (string | null | undefined)[]): string | null {
  let best: string | null = null;
  for (const v of vals) {
    if (v == null || v === '') continue;
    const n = normalizeTimestampString(v);
    if (n == null) continue;
    if (best == null || n < best) best = n;
  }
  return best;
}

/**
 * Same vineyard times vs VWork job end as {@link applyGpsGuardrails} (step 2 ≥ job end → clear both 2+3; step 3 ≥ ceiling → clear 3).
 * Used **before** computing winery step 4–5 lower bound so a vineyard ENTER after “job complete” does not inflate the step 4 floor
 * while those rows are about to be dropped by guardrails anyway.
 */
function pruneVineyardGpsForJobEnd(
  step2Value: string | null,
  step3Value: string | null,
  job: JobForDerivedSteps,
  jobEndCeilingBufferMinutes: number
): { step2: string | null; step3: string | null } {
  const vworkEnd =
    job.step_5_completed_at != null || job.actual_end_time != null
      ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
      : null;
  if (vworkEnd == null) {
    return { step2: step2Value, step3: step3Value };
  }
  const step3Ceiling =
    vworkEnd != null && jobEndCeilingBufferMinutes > 0
      ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkEnd, jobEndCeilingBufferMinutes))
      : vworkEnd;
  const s2 = step2Value != null ? normalizeTimestampString(step2Value) : null;
  if (s2 != null && s2 >= vworkEnd) {
    return { step2: null, step3: null };
  }
  const s3 = step3Value != null ? normalizeTimestampString(step3Value) : null;
  const ceiling = step3Ceiling ?? vworkEnd;
  if (s3 != null && s3 >= ceiling) {
    return { step2: step2Value, step3: null };
  }
  return { step2: step2Value, step3: step3Value };
}

function vineyardPreclearSnapshot(
  cand: GpsStepCandidate | null | undefined,
  cell: TrackingLookupDebug | undefined
): VineyardStepPreclearSnapshot | null {
  if (cand?.value == null || String(cand.value).trim() === '') return null;
  const positionTimeNz =
    cell?.position_time_nz != null && String(cell.position_time_nz).trim() !== ''
      ? normalizeTimestampString(String(cell.position_time_nz)) ?? String(cell.position_time_nz).trim().slice(0, 19)
      : normalizeTimestampString(cand.value) ?? String(cand.value).trim().slice(0, 19);
  return {
    positionTimeNz,
    trackingId:
      cand.trackingId != null && Number.isFinite(cand.trackingId)
        ? cand.trackingId
        : cell?.trackingId != null && Number.isFinite(cell.trackingId)
          ? cell.trackingId
          : null,
    matchedGeofenceId: cell?.matchedGeofenceId ?? null,
    matchedFenceName: cell?.matchedFenceName ?? null,
    device: cell?.device ?? null,
  };
}

function buildSummaryLineStep2Polygon(g: VineyardPart1FetchGuardrailDebug, fetchFoundStep2: boolean): string {
  const floor = g.orderingFloorExclusive;
  if (!fetchFoundStep2 || g.preclearStep2 == null) {
    return `Part 1 did not return a vineyard ENTER for this device in the strict polygon window on the mapped geofence_id set — there was no LIMIT 1 row for decideFinalSteps. A row you see in Inspect (same fence name and geofence_id) is not necessarily that answer: check device_name, ENTER type, exclusive time bounds vs position_time_nz, and whether an earlier ENTER on the same fence set wins ORDER BY ASC LIMIT 1.`;
  }
  const p = g.preclearStep2;
  const head = `Part 1 selected ENTER position_time_nz=${p.positionTimeNz ?? '—'} · tbl_tracking.id=${p.trackingId ?? '—'} · geofence_id=${p.matchedGeofenceId ?? '—'} (${p.matchedFenceName ?? '—'}) · device=${p.device ?? '—'}.`;
  const reasons: string[] = [];
  if (g.clearedByOrderingFloorStep2) {
    reasons.push(
      `Dropped by vineyard ordering floor — need strictly position_time_nz > ${floor ?? '—'} (candidate was not strictly after that instant).`
    );
  }
  if (g.clearedVworkJobEndStep2And3) {
    reasons.push('Dropped: vineyard ENTER at or after VWork job end — steps 2 and 3 cleared together.');
  }
  if (g.duplicateTrackingIdClears.includes('step2')) {
    reasons.push(
      'Dropped: duplicate tbl_tracking.id — same id already used by an earlier step in guardrail pass order (steps 1→5).'
    );
  }
  if (reasons.length === 0) {
    if (fetchFoundStep2 && g.preclearStep2 != null) {
      reasons.push(
        'This pass did not log floor, job-end, or duplicate-id removal for the Part-1 vineyard ENTER above — if merged GPS step 2 is still empty, compare API snapshot timing or refetch steps after deploy.'
      );
    } else {
      reasons.push(
        'Guardrails did not record floor, job-end, or duplicate-id removal for this Part-1 step 2 candidate — if final GPS step 2 is still empty, check merge / orides / buffer path.'
      );
    }
  }
  return `${head} ${reasons.join(' ')}`;
}

/**
 * Drop steps 2–3 and 5 with time &lt;= **vineyard GPS ordering floor** from {@link computeVineyardGpsOrderingFloorDebug} (step 4 uses same leg in max with step2/step3).
 * **G1 vs jobStep1Anchor (tap∨oride):** if G1 &gt; anchor, keep G1 only when effective G2 exists and G1 &lt; G2 (tap before physical exit, but left before vineyard). Effective G2 = polygon GPS step 2 if present, else optional VineFence+ merged enter from {@link DerivedStepsOptions.tentativeVineyardEnterForStep1Bracket} when it is strictly after anchor and after `step1BracketTrackingFloor` — never written to `candidates.step2`.
 * Step 4: drop if &lt;= floor (when GPS step 2+3 both present: max(step1 leg from same floor helper, step2, step3); when both missing: max(same step1 leg, positionAfter) only).
 * VWork ceiling: steps 1–2 must be &lt; VWork job end. Step 3 must be &lt; job end + buffer (see `jobEndCeilingBufferMinutes`).
 */
function applyGpsGuardrails(
  candidates: FetchedGpsCandidates,
  job: JobForDerivedSteps,
  jobEndCeilingBufferMinutes: number = JOB_END_CEILING_BUFFER_DEFAULT_MINUTES,
  /** Data window start; used for step 4 floor when GPS step 2 or 3 is missing. */
  positionAfter?: string | null,
  debug?: DerivedStepsDebug | null,
  step1Bracket?: {
    tentativeVineyardEnterForStep1Bracket: string | null;
    step1BracketTrackingFloor: string | null;
  } | null
): void {
  const vworkEnd =
    job.step_5_completed_at != null || job.actual_end_time != null
      ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
      : null;
  const step3Ceiling =
    vworkEnd != null && jobEndCeilingBufferMinutes > 0
      ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkEnd, jobEndCeilingBufferMinutes))
      : vworkEnd;

  const gps1BeforeJobEndCheck =
    candidates.step1?.value != null ? normalizeTimestampString(candidates.step1.value) : null;
  const gps1TrackingIdBeforeJobEndCheck =
    candidates.step1?.trackingId != null && Number.isFinite(candidates.step1.trackingId)
      ? candidates.step1.trackingId
      : null;

  let droppedGps1AtOrAfterVworkJobEnd = false;
  if (vworkEnd != null && candidates.step1?.value != null) {
    const s1c = normalizeTimestampString(candidates.step1.value);
    if (s1c != null && s1c >= vworkEnd) {
      candidates.step1 = null;
      droppedGps1AtOrAfterVworkJobEnd = true;
    }
  }

  const anchor = jobStep1Anchor(job);
  const s1 = candidates.step1;

  const tentativeRawForDebug =
    step1Bracket?.tentativeVineyardEnterForStep1Bracket != null &&
    String(step1Bracket.tentativeVineyardEnterForStep1Bracket).trim() !== ''
      ? String(step1Bracket.tentativeVineyardEnterForStep1Bracket).trim().slice(0, 19)
      : null;
  const tentativeFromOpt =
    tentativeRawForDebug != null ? normalizeTimestampString(tentativeRawForDebug) ?? tentativeRawForDebug : null;
  const step1BracketTrackingFloorNorm =
    step1Bracket?.step1BracketTrackingFloor != null &&
    String(step1Bracket.step1BracketTrackingFloor).trim() !== ''
      ? normalizeTimestampString(step1Bracket.step1BracketTrackingFloor) ??
        String(step1Bracket.step1BracketTrackingFloor).trim().slice(0, 19)
      : null;

  let anchorBracketEvaluated = false;
  let gps1AtAnchorBracket: string | null = null;
  let gps1TrackingIdAtAnchorBracket: number | null = null;
  let gps2AtAnchorBracketCheck: string | null = null;
  let gps2TrackingIdAtAnchorBracketCheck: number | null = null;
  let gps1StrictlyAfterAnchor: boolean | null = null;
  let keepG1AfterAnchorConditionMet: boolean | null = null;
  let droppedGps1ByAfterAnchorBracket = false;
  let anchorBracketOutcome: GpsStep1GuardrailDebug['anchorBracketOutcome'] = 'not_evaluated_no_gps1';
  let tentativeG2QualifiedForBracket: string | null = null;
  let bracketUsedTentativeG2 = false;
  let g2EffectiveForAnchorBracket: string | null = null;

  if (anchor == null) {
    anchorBracketOutcome = 'not_evaluated_no_anchor';
  } else if (s1?.value == null) {
    anchorBracketOutcome = droppedGps1AtOrAfterVworkJobEnd
      ? 'not_evaluated_gps1_cleared_by_job_end_first'
      : 'not_evaluated_no_gps1';
  } else {
    anchorBracketEvaluated = true;
    const g1 = normalizeTimestampString(s1.value);
    gps1AtAnchorBracket = g1;
    gps1TrackingIdAtAnchorBracket =
      s1.trackingId != null && Number.isFinite(s1.trackingId) ? s1.trackingId : null;
    const g2Cell = candidates.step2;
    const g2Committed =
      g2Cell?.value != null && String(g2Cell.value).trim() !== ''
        ? normalizeTimestampString(g2Cell.value)
        : null;
    gps2AtAnchorBracketCheck = g2Committed;
    gps2TrackingIdAtAnchorBracketCheck =
      g2Cell?.trackingId != null && Number.isFinite(g2Cell.trackingId) ? g2Cell.trackingId : null;

    let g2Eff = g2Committed;
    if (g2Eff == null && tentativeFromOpt != null) {
      const tNorm = normalizeTimestampString(tentativeFromOpt) ?? tentativeFromOpt;
      const passesAnchorOnly = tNorm > anchor;
      const passesFloorBound =
        step1BracketTrackingFloorNorm == null || tNorm > step1BracketTrackingFloorNorm;
      if (passesAnchorOnly && passesFloorBound) {
        g2Eff = tNorm;
        tentativeG2QualifiedForBracket = tNorm;
        bracketUsedTentativeG2 = true;
      }
    }
    g2EffectiveForAnchorBracket = g2Eff;

    if (g1 != null && g1 > anchor) {
      gps1StrictlyAfterAnchor = true;
      const keepG1AfterV1 = g2Eff != null && g1 < g2Eff;
      keepG1AfterAnchorConditionMet = keepG1AfterV1;
      if (!keepG1AfterV1) {
        candidates.step1 = null;
        droppedGps1ByAfterAnchorBracket = true;
        anchorBracketOutcome =
          g2Eff == null ? 'dropped_g1_after_anchor_no_gps2' : 'dropped_g1_after_anchor_g1_not_strictly_before_gps2';
      } else {
        anchorBracketOutcome = 'kept_g1_after_anchor_g2_exists_and_g1_before_g2';
      }
    } else {
      gps1StrictlyAfterAnchor = g1 != null ? g1 > anchor : false;
      anchorBracketOutcome = 'evaluated_gps1_not_after_anchor_unchanged';
    }
  }

  const s1AfterDrop = candidates.step1;
  const s1Norm = s1AfterDrop?.value != null ? normalizeTimestampString(s1AfterDrop.value) : null;
  const vFloorDbg = computeVineyardGpsOrderingFloorDebug(job, s1Norm);
  const floorFor235 = vFloorDbg.floorFor235;
  const vineStep2Cell = debug?.vineyard?.step2;
  const vineStep3Cell = debug?.vineyard?.step3;
  const wineryStep5Cell = debug?.winery?.step5;
  const wineryStep4Cell = debug?.winery?.step4;
  const preclearStep2 = vineyardPreclearSnapshot(candidates.step2, vineStep2Cell);
  const preclearStep3 = vineyardPreclearSnapshot(candidates.step3, vineStep3Cell);
  const preclearStep5 = vineyardPreclearSnapshot(candidates.step5, wineryStep5Cell);
  const preclearStep4 = vineyardPreclearSnapshot(candidates.step4, wineryStep4Cell);
  let clearedByOrderingFloorStep2 = false;
  let clearedByOrderingFloorStep3 = false;
  let clearedByOrderingFloorStep5 = false;

  if (debug != null) {
    debug.vineyardGpsOrderingFloor = vFloorDbg;
  }
  if (floorFor235 != null) {
    for (const key of ['step2', 'step3', 'step5'] as const) {
      const c = candidates[key];
      if (c?.value == null) continue;
      const n = normalizeTimestampString(c.value);
      if (n != null && n <= floorFor235) {
        if (key === 'step2') clearedByOrderingFloorStep2 = true;
        if (key === 'step3') clearedByOrderingFloorStep3 = true;
        if (key === 'step5') clearedByOrderingFloorStep5 = true;
        candidates[key] = null;
      }
    }
  }
  const hadStep2BeforeVworkClear = candidates.step2 != null;
  const hadStep3BeforeVworkClear = candidates.step3 != null;
  let clearedVworkJobEndStep2And3 = false;
  let clearedStep3OnlyJobEndCeiling = false;
  if (vworkEnd != null) {
    const s2 = candidates.step2?.value != null ? normalizeTimestampString(candidates.step2!.value) : null;
    if (s2 != null && s2 >= vworkEnd) {
      clearedVworkJobEndStep2And3 = hadStep2BeforeVworkClear || hadStep3BeforeVworkClear;
      candidates.step2 = null;
      candidates.step3 = null;
    } else {
      const s3 = candidates.step3?.value != null ? normalizeTimestampString(candidates.step3!.value) : null;
      const ceiling = step3Ceiling ?? vworkEnd;
      if (s3 != null && s3 >= ceiling) {
        if (candidates.step3 != null) clearedStep3OnlyJobEndCeiling = true;
        candidates.step3 = null;
      }
    }
  }
  const s1ForMax = candidates.step1?.value != null ? normalizeTimestampString(candidates.step1.value) : null;
  const step1ForStep4Max = vFloorDbg.floorFor235;
  const rawPosAfter =
    positionAfter != null && String(positionAfter).trim() !== ''
      ? normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19)
      : null;
  const step4Floor =
    candidates.step2 == null && candidates.step3 == null
      ? maxTimestampString(step1ForStep4Max, rawPosAfter)
      : maxTimestampString(step1ForStep4Max, candidates.step2?.value, candidates.step3?.value);
  const step4OrderingFloorExclusiveNorm =
    step4Floor != null ? normalizeTimestampString(step4Floor) ?? String(step4Floor).trim().slice(0, 19) : null;
  let clearedByStep4OrderingFloor = false;
  if (step4Floor != null && candidates.step4?.value != null) {
    const s4 = normalizeTimestampString(candidates.step4.value);
    if (s4 != null && s4 <= step4Floor) {
      clearedByStep4OrderingFloor = true;
      candidates.step4 = null;
    }
  }
  const duplicateTrackingIdClears: string[] = [];
  const seen = new Set<number>();
  for (const key of ['step1', 'step2', 'step3', 'step4', 'step5'] as const) {
    const c = candidates[key];
    const id = c?.trackingId;
    if (id == null || !Number.isFinite(id)) continue;
    if (seen.has(id)) {
      duplicateTrackingIdClears.push(key);
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

  if (debug != null) {
    const fetchFoundStep2 = debug.vineyard.step2?.found === true;
    const part1FetchGuardrail: VineyardPart1FetchGuardrailDebug = {
      orderingFloorExclusive: floorFor235,
      preclearStep2,
      preclearStep3,
      preclearStep5,
      preclearStep4,
      clearedByOrderingFloorStep2,
      clearedByOrderingFloorStep3,
      clearedByOrderingFloorStep5,
      clearedByStep4OrderingFloor,
      step4OrderingFloorExclusive: step4OrderingFloorExclusiveNorm,
      clearedVworkJobEndStep2And3,
      clearedStep3OnlyJobEndCeiling,
      duplicateTrackingIdClears: [...duplicateTrackingIdClears],
      summaryLineStep2Polygon: '',
    };
    part1FetchGuardrail.summaryLineStep2Polygon = buildSummaryLineStep2Polygon(
      part1FetchGuardrail,
      fetchFoundStep2
    );
    debug.vineyard.part1FetchGuardrail = part1FetchGuardrail;
  }

  if (debug != null) {
    const parts: string[] = [];
    if (droppedGps1AtOrAfterVworkJobEnd) {
      parts.push(
        `GPS1 cleared at/after VWork job end ${vworkEnd ?? '—'} (candidate was ${gps1BeforeJobEndCheck ?? '—'} id ${gps1TrackingIdBeforeJobEndCheck ?? '—'}).`
      );
    }
    if (droppedGps1ByAfterAnchorBracket) {
      parts.push(
        `GPS1 cleared after anchor ${anchor ?? '—'}: need effective G2 and strict GPS1<G2eff; GPS1@bracket=${gps1AtAnchorBracket ?? '—'} polygon GPS2=${gps2AtAnchorBracketCheck ?? '—'} tentative merged enter=${tentativeRawForDebug ?? '—'} qualified tentative=${tentativeG2QualifiedForBracket ?? '—'} G2eff=${g2EffectiveForAnchorBracket ?? '—'} floor=${step1BracketTrackingFloorNorm ?? '—'}.`
      );
    }
    if (
      !droppedGps1AtOrAfterVworkJobEnd &&
      !droppedGps1ByAfterAnchorBracket &&
      anchorBracketOutcome === 'kept_g1_after_anchor_g2_exists_and_g1_before_g2'
    ) {
      parts.push(
        `GPS1 kept: after anchor ${anchor ?? '—'}, G2eff=${g2EffectiveForAnchorBracket ?? '—'} strictly after GPS1=${gps1AtAnchorBracket ?? '—'}${bracketUsedTentativeG2 ? ' (VineFence+ tentative enter used for bracket only; polygon GPS2 unchanged).' : ''}`
      );
    }
    if (
      !droppedGps1AtOrAfterVworkJobEnd &&
      !droppedGps1ByAfterAnchorBracket &&
      anchorBracketOutcome === 'evaluated_gps1_not_after_anchor_unchanged' &&
      gps1BeforeJobEndCheck != null
    ) {
      parts.push(
        `GPS1 not strictly after anchor (${anchor ?? '—'}) — morning EXIT left as fetched (GPS1=${gps1BeforeJobEndCheck}).`
      );
    }
    if (gps1BeforeJobEndCheck == null && !droppedGps1AtOrAfterVworkJobEnd) {
      parts.push('No GPS step 1 from Part 1 fetch — anchor bracket not applied to a candidate.');
    }
    if (anchorBracketOutcome === 'not_evaluated_no_anchor') {
      parts.push('Job step-1 anchor missing — anchor vs GPS2 bracket skipped.');
    }
    if (
      anchorBracketOutcome === 'not_evaluated_no_gps1' &&
      gps1BeforeJobEndCheck == null &&
      !droppedGps1AtOrAfterVworkJobEnd
    ) {
      parts.push('No GPS1 time to evaluate (fetch miss).');
    }
    const summaryLine =
      parts.length > 0 ? parts.join(' ') : `GPS step 1 guardrail: ${anchorBracketOutcome.replace(/_/g, ' ')}.`;

    debug.gpsStep1Guardrail = {
      vworkJobEndForStep12Ceiling: vworkEnd,
      droppedGps1AtOrAfterVworkJobEnd,
      gps1BeforeJobEndCheck,
      gps1TrackingIdBeforeJobEndCheck,
      jobStartAnchor: anchor,
      anchorBracketEvaluated,
      gps1AtAnchorBracket,
      gps1TrackingIdAtAnchorBracket,
      gps2AtAnchorBracketCheck,
      gps2TrackingIdAtAnchorBracketCheck,
      gps1StrictlyAfterAnchor,
      keepG1AfterAnchorConditionMet,
      droppedGps1ByAfterAnchorBracket,
      step1BracketTrackingFloor: step1BracketTrackingFloorNorm,
      tentativeVineyardEnterFromOptions: tentativeRawForDebug,
      tentativeG2QualifiedForBracket,
      bracketUsedTentativeG2,
      g2EffectiveForAnchorBracket,
      anchorBracketOutcome,
      summaryLine,
    };
  }
}

/**
 * Winery steps 4–5: first mapped Winery ENTER after a lower bound, then first Winery EXIT after step 4.
 * Lower bound: if **both** step2 and step3 values are non-null, max(step1, step2, step3); if **both** are null,
 * max(step1, positionAfter) only. Callers should pass step2/step3 **after** {@link pruneVineyardGpsForJobEnd} so times that
 * guardrails will drop (e.g. vineyard ENTER after VWork job end) do not raise the floor.
 * Step 5: upper bound max(positionBefore, max(tap, GPS step 4 ENTER) + step5ExtendWineryExitMinutes) when applicable.
 * Updates debug.winery.step4 / step5 and debug.winery.step5SearchWindow (Inspect-only audit).
 */
function assignWineryStep5SearchWindowDebug(
  debug: DerivedStepsDebug,
  args: {
    wineryFenceIds: number[];
    step4Value: string | null;
    vworkStep5: string | null;
    /** max(tap, GPS step 4); extend added here. */
    step5ExtendAnchor: string | null;
    step5ExtMin: number;
    vworkStep5SearchEnd: string | null;
    step5WindowEnd: string | null;
    positionBefore: string | null;
    step5ExitQueryRan: boolean;
    fetchSkippedReason: WineryStep5SearchWindowDebug['fetchSkippedReason'];
  }
): void {
  const pbNorm =
    args.positionBefore != null && String(args.positionBefore).trim() !== ''
      ? normalizeTimestampString(args.positionBefore) ?? String(args.positionBefore).trim().slice(0, 19)
      : null;
  const jPlus = args.vworkStep5SearchEnd;
  const winEnd = args.step5WindowEnd != null ? normalizeTimestampString(args.step5WindowEnd) : null;
  let upperExclusiveSource: WineryStep5SearchWindowDebug['upperExclusiveSource'] = 'not_computed';
  if (args.fetchSkippedReason != null) {
    upperExclusiveSource = 'not_computed';
  } else if (winEnd != null && jPlus != null && pbNorm != null) {
    if (winEnd === pbNorm && pbNorm === jPlus) upperExclusiveSource = 'position_before_equals_job_end_plus_extend';
    else if (winEnd === jPlus && jPlus > pbNorm) upperExclusiveSource = 'anchor_plus_extend_wider_than_position_before';
    else if (winEnd === pbNorm && pbNorm > jPlus) upperExclusiveSource = 'position_before_wider_than_anchor_plus_extend';
    else upperExclusiveSource = 'not_computed';
  } else if (winEnd != null && jPlus != null && pbNorm == null) {
    upperExclusiveSource = args.step5ExtMin > 0 ? 'only_job_end_plus_extend' : 'only_vwork_step5_no_extend_zero';
  } else if (winEnd != null && pbNorm != null) {
    upperExclusiveSource = 'only_position_before';
  }

  const lowerEx = args.step4Value != null ? normalizeTimestampString(args.step4Value) : null;
  const anchorEx =
    args.step5ExtendAnchor != null ? normalizeTimestampString(args.step5ExtendAnchor) : null;
  const lines: string[] = [];
  lines.push(
    'Step 5 GPS fetch: first winery EXIT on mapped delivery_winery fences where (strict) step4_ENTER < t < upperExclusive.'
  );
  if (args.fetchSkippedReason != null) {
    lines.push(`Fetch skipped: ${args.fetchSkippedReason.replace(/_/g, ' ')}.`);
  } else {
    lines.push(`X (lowerExclusive) = GPS step 4 winery ENTER: ${lowerEx ?? '—'}.`);
    lines.push(`Y (upperExclusive) = max(positionBefore, max(tap, GPS4) + Step5Extend): ${winEnd ?? '—'}.`);
    lines.push(`Step5 extend anchor max(tap, GPS step 4 ENTER): ${anchorEx ?? '—'}.`);
    lines.push(`Job end (tap) for step-5 rule (step_5_completed_at ?? actual_end_time): ${args.vworkStep5 ?? '—'}.`);
    lines.push(`positionBefore from request/options: ${pbNorm ?? '—'}.`);
    lines.push(`Step5ExtendWineryExit minutes: ${args.step5ExtMin}.`);
    lines.push(`anchor + extend (input to max with position before): ${jPlus ?? '—'}.`);
    lines.push(`upperExclusiveSource: ${upperExclusiveSource.replace(/_/g, ' ')}.`);
    lines.push(
      args.step5ExitQueryRan
        ? 'EXIT query ran: SQL requires step4 < t < upperExclusive — a winery EXIT at or after Y is excluded from step 5 fetch (e.g. late Delegat Marlborough EXIT).'
        : 'EXIT query not run — see fetchSkippedReason or preconditions above.'
    );
  }

  debug.winery.step5SearchWindow = {
    fetchSkippedReason: args.fetchSkippedReason,
    lowerExclusive: lowerEx,
    upperExclusive: winEnd,
    jobEndForStep5Rule: args.vworkStep5,
    step5ExtendWineryExitMinutes: args.step5ExtMin,
    step5ExtendAnchor: anchorEx,
    jobEndPlusExtend: jPlus,
    positionBeforeFromOptions: pbNorm,
    upperExclusiveSource,
    step5ExitQueryRan: args.step5ExitQueryRan,
    summaryLine: lines.join(' '),
  };
}

async function fetchWineryStep4And5ForValues(
  job: JobForDerivedSteps,
  options: DerivedStepsOptions,
  debug: DerivedStepsDebug,
  truckId: string,
  wineryFenceIds: number[],
  step1Value: string | null,
  step2Value: string | null,
  step3Value: string | null,
  step4AuditInputs: { anchor: string | null; gpsMorningExit: string | null }
): Promise<{ step4: GpsStepCandidate | null; step5: GpsStepCandidate | null }> {
  const { positionAfter, positionBefore } = options;
  const vworkStep5Early =
    (job.step_5_completed_at ?? job.actual_end_time) != null
      ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
      : null;
  const step5ExtMinEarly = options.step5ExtendWineryExitMinutes ?? STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;
  const vworkStep5SearchEndEarly =
    vworkStep5Early != null && step5ExtMinEarly > 0
      ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkStep5Early, step5ExtMinEarly))
      : vworkStep5Early;
  const step5WindowEndEarly = step5ExitExclusiveUpper(positionBefore, vworkStep5SearchEndEarly);

  if (wineryFenceIds.length === 0) {
    const anchorEarly = step5ExtendAnchorMaxTapAndGps4(vworkStep5Early, null);
    assignWineryStep5SearchWindowDebug(debug, {
      wineryFenceIds,
      step4Value: null,
      vworkStep5: vworkStep5Early,
      step5ExtendAnchor: anchorEarly,
      step5ExtMin: step5ExtMinEarly,
      vworkStep5SearchEnd: vworkStep5SearchEndEarly,
      step5WindowEnd: step5WindowEndEarly != null ? normalizeTimestampString(step5WindowEndEarly) : null,
      positionBefore,
      step5ExitQueryRan: false,
      fetchSkippedReason: 'no_winery_fence_ids',
    });
    return { step4: null, step5: null };
  }
  const rawPosAfter =
    positionAfter != null && String(positionAfter).trim() !== ''
      ? normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19)
      : null;
  const step4LowerBound =
    step2Value == null && step3Value == null
      ? maxTimestampString(step1Value, rawPosAfter) ?? positionAfter
      : maxTimestampString(step1Value, step2Value, step3Value) ?? positionAfter;
  const fetchLowerNorm =
    normalizeTimestampString(step4LowerBound) ?? String(step4LowerBound).trim().slice(0, 19);
  const auditRelaxedLower =
    step2Value == null && step3Value == null
      ? rawPosAfter
      : maxTimestampString(rawPosAfter, step2Value, step3Value);
  const auditRelaxedNorm =
    auditRelaxedLower != null && String(auditRelaxedLower).trim() !== ''
      ? normalizeTimestampString(auditRelaxedLower) ?? String(auditRelaxedLower).trim().slice(0, 19)
      : null;
  const step1LegNorm =
    step1Value != null && String(step1Value).trim() !== ''
      ? normalizeTimestampString(step1Value) ?? String(step1Value).trim().slice(0, 19)
      : null;
  const anchorNorm =
    step4AuditInputs.anchor != null && String(step4AuditInputs.anchor).trim() !== ''
      ? normalizeTimestampString(step4AuditInputs.anchor) ?? String(step4AuditInputs.anchor).trim().slice(0, 19)
      : null;
  const gps1Norm =
    step4AuditInputs.gpsMorningExit != null && String(step4AuditInputs.gpsMorningExit).trim() !== ''
      ? normalizeTimestampString(step4AuditInputs.gpsMorningExit) ??
        String(step4AuditInputs.gpsMorningExit).trim().slice(0, 19)
      : null;
  const bothVineMissing = step2Value == null && step3Value == null;
  const tracePlain =
    `Step 4 (winery return ENTER) — first mapped delivery_winery ENTER with strict position_time_nz > fetchLowerExclusive AND ` +
    `(if set) position_time_nz < positionBefore. fetchLowerExclusive = max(step1 leg, ${bothVineMissing ? 'positionAfter only when both vineyard GPS steps missing' : 'step2, step3'}) where step1 leg = anchor(step1 oride∨VWork) ?? GPS morning winery EXIT = ${step1LegNorm ?? '—'}. ` +
    `Parts: anchor=${anchorNorm ?? '—'} · GPS morning EXIT=${gps1Norm ?? '—'} · step2=${step2Value != null ? (normalizeTimestampString(step2Value) ?? String(step2Value).slice(0, 19)) : '—'} · step3=${step3Value != null ? (normalizeTimestampString(step3Value) ?? String(step3Value).slice(0, 19)) : '—'} · positionAfter(options)=${rawPosAfter ?? '—'} · fetchLowerExclusive=${fetchLowerNorm}. ` +
    (auditRelaxedNorm != null && auditRelaxedNorm < fetchLowerNorm
      ? `Audit relaxed lower (max(positionAfter, step2, step3) only) = ${auditRelaxedNorm} — strictly before fetch lower, so ENTER rows between these bounds are excluded from Part 1 only because the step1 leg raised the floor (e.g. early re-entry before contractual job start).`
      : `Audit relaxed lower matches fetch lower (no anchor-only gap) — same WHERE as audit list would duplicate Part 1 list.`);
  const step4Result = await getFirstTrackingInWindowWithDebug(
    truckId,
    step4LowerBound,
    positionBefore,
    wineryFenceIds,
    'ENTER',
    false,
    tracePlain,
    VINEYARD_WINDOW_MATCH_LIST_CAP
  );
  const breakdown: Step4FetchLowerBreakdownDebug = {
    anchor: anchorNorm,
    gpsMorningExit: gps1Norm,
    step1LegUsedForFetch: step1LegNorm,
    step2:
      step2Value != null && String(step2Value).trim() !== ''
        ? normalizeTimestampString(step2Value) ?? String(step2Value).trim().slice(0, 19)
        : null,
    step3:
      step3Value != null && String(step3Value).trim() !== ''
        ? normalizeTimestampString(step3Value) ?? String(step3Value).trim().slice(0, 19)
        : null,
    positionAfterOption: rawPosAfter,
    fetchLowerExclusive: fetchLowerNorm,
    auditRelaxedLowerExclusive: auditRelaxedNorm,
    bothVineyardGpsStepsMissing: bothVineMissing,
  };
  debug.winery.step4FetchLowerBreakdown = breakdown;

  const mergedDebug: TrackingLookupDebug = { ...step4Result.debug };
  if (auditRelaxedNorm != null && auditRelaxedNorm < fetchLowerNorm) {
    const { rows, truncated } = await fetchOrderedMatchListForSameWindow(
      truckId,
      auditRelaxedNorm,
      positionBefore != null && String(positionBefore).trim() !== ''
        ? normalizeTimestampString(positionBefore) ?? String(positionBefore).trim().slice(0, 19)
        : null,
      wineryFenceIds,
      'ENTER',
      false,
      VINEYARD_WINDOW_MATCH_LIST_CAP
    );
    mergedDebug.auditLowerExclusive = auditRelaxedNorm;
    mergedDebug.auditMatchingRowsOrdered = rows;
    mergedDebug.auditMatchingRowsTruncated = truncated;
    mergedDebug.auditMatchingRowsCaption =
      'Winery ENTER rows with position_time_nz strictly after max(positionAfter, step2, step3) — no step1 leg. ' +
      'Rows at or before fetchLowerExclusive (max(step1 leg, step2, step3)) are excluded from Part 1 only because anchor ?? GPS step 1 raised the floor; ORDER BY ASC; capped for payload.';
  }
  debug.winery.step4 = mergedDebug;
  let step4: GpsStepCandidate | null = null;
  if (step4Result.value != null) {
    step4 = { value: step4Result.value, trackingId: step4Result.trackingId };
  }
  const step4Value = step4?.value ?? null;
  const vworkStep5 =
    (job.step_5_completed_at ?? job.actual_end_time) != null
      ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
      : null;
  const step5ExtMin = options.step5ExtendWineryExitMinutes ?? STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;
  const step5ExtendAnchor = vworkStep5 != null ? step5ExtendAnchorMaxTapAndGps4(vworkStep5, step4Value) : null;
  const vworkStep5SearchEnd =
    step5ExtendAnchor != null && step5ExtMin > 0
      ? normalizeTimestampString(addMinutesToTimestampAsNZ(step5ExtendAnchor, step5ExtMin))
      : step5ExtendAnchor;
  const step5WindowEnd = step5ExitExclusiveUpper(positionBefore, vworkStep5SearchEnd);
  let step5: GpsStepCandidate | null = null;
  let fetchSkip: WineryStep5SearchWindowDebug['fetchSkippedReason'] = null;
  let exitRan = false;

  if (step4Value == null) {
    fetchSkip = 'no_gps_step4_enter';
  } else if (vworkStep5 == null) {
    fetchSkip = 'no_vwork_job_end_for_step5_rule';
  } else if (step5WindowEnd == null) {
    fetchSkip = 'no_step5_upper_bound';
  } else {
    exitRan = true;
    const step5Result = await getFirstTrackingInWindowWithDebug(truckId, step4Value, step5WindowEnd, wineryFenceIds, 'EXIT', false);
    debug.winery.step5 = step5Result.debug;
    if (step5Result.value != null) {
      step5 = { value: step5Result.value, trackingId: step5Result.trackingId };
    }
  }

  const winEndNorm = step5WindowEnd != null ? normalizeTimestampString(step5WindowEnd) : null;
  assignWineryStep5SearchWindowDebug(debug, {
    wineryFenceIds,
    step4Value,
    vworkStep5,
    step5ExtendAnchor,
    step5ExtMin,
    vworkStep5SearchEnd,
    step5WindowEnd: winEndNorm,
    positionBefore,
    step5ExitQueryRan: exitRan,
    fetchSkippedReason: fetchSkip,
  });
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
  const jobEndCeilingBufferMinutes =
    options.jobEndCeilingBufferMinutes ?? JOB_END_CEILING_BUFFER_DEFAULT_MINUTES;
  const anchor = jobStep1Anchor(job);
  const trackingWindowAfter =
    anchor != null && positionAfter != null && String(positionAfter).trim() !== ''
      ? minTimestampString(positionAfter, anchor) ?? positionAfter
      : positionAfter;
  const vineyardBefore = vineyardFetchPositionBefore(positionBefore, job, jobEndCeilingBufferMinutes);
  const vineyardUpperTrace =
    vineyardBefore != null && String(vineyardBefore).trim() !== ''
      ? `t < ${String(vineyardBefore).trim()} (exclusive)`
      : 'no exclusive upper on vineyard window end';
  const vineyardName = job.vineyard_name ? String(job.vineyard_name).trim() : '';
  const deliveryWinery = job.delivery_winery ? String(job.delivery_winery).trim() : '';
  let step2Value: string | null = null;
  let step3Value: string | null = null;
  /** Fence set used for step 2/3 (primary vineyard or VineSR1 fallback) — for refine-after-exit. */
  let vineyardFenceIdsForRefine: number[] | null = null;

  if (vineyardName) {
    const twaForVineyardFloor =
      trackingWindowAfter != null && String(trackingWindowAfter).trim() !== ''
        ? String(trackingWindowAfter).trim()
        : '';
    const pass1FloorMerge = mergeVineyardPolygonLowerWithOrderingFloor(twaForVineyardFloor, job, null);
    const step2SqlLowerPass1 = pass1FloorMerge.merged;

    const { fenceIds: vineyardFenceIds, debug: vineyardDebug } = await getFenceIdsForVworkNameWithDebug('Vineyard', vineyardName);
    Object.assign(debug.vineyard, vineyardDebug);
    if (vineyardFenceIds.length > 0) {
      vineyardFenceIdsForRefine = vineyardFenceIds;
      const mappedNames = vineyardDebug.fenceNamesInList.join('; ') || '—';
      const resolvedFenceTrace = formatResolvedFenceNamesForGpsTrace(vineyardDebug.resolvedFenceNames);
      const pass1FloorHint =
        pass1FloorMerge.floor != null
          ? ` Ordering-floor merge (same rule as Part 1 guardrail): strictly > ${pass1FloorMerge.floor} (${String(pass1FloorMerge.rule).replace(/_/g, ' ')}); SQL lowerExclusive = max(X, floor) = ${step2SqlLowerPass1}.`
          : '';
      const step2Trace =
        `Step 2 (polygon pass 1) — Vineyard ENTER on mapped geofence_id ANY([${vineyardFenceIds.join(', ')}]) for vWork vineyard "${vineyardName}". Names: [${mappedNames}].` +
        (resolvedFenceTrace ? ` Fences (name + id): ${resolvedFenceTrace}.` : '') +
        ` Strict: position_time_nz > ${step2SqlLowerPass1} AND ${vineyardUpperTrace}. Lower inherits X = min(options.positionAfter, jobStep1Anchor): positionAfter=${String(positionAfter ?? '')}; anchor(step1oride∨tap)=${anchor ?? '—'}; X=${trackingWindowAfter}.${pass1FloorHint}`;
      const step2Result = await getFirstTrackingInWindowWithDebug(
        truckId,
        step2SqlLowerPass1,
        vineyardBefore,
        vineyardFenceIds,
        'ENTER',
        false,
        step2Trace,
        VINEYARD_WINDOW_MATCH_LIST_CAP
      );
      step2Value = step2Result.value;
      debug.vineyard.step2 = step2Result.debug;
      if (step2Result.value != null) candidates.step2 = { value: step2Result.value, trackingId: step2Result.trackingId };
      // Step 3 (Depart Vineyard): first Vineyard EXIT after step 2 (Enter Vineyard), so we don't pick an earlier exit
      const step3After = step2Value ?? step2SqlLowerPass1;
      const step3Trace =
        `Step 3 (polygon pass 1) — Vineyard EXIT after step-2 ENTER; lowerExclusive = prior ENTER time or X if no step2: position_time_nz > ${step3After}. Upper: ${vineyardUpperTrace}. Same fence set as step 2.` +
        (resolvedFenceTrace ? ` Fences (name + id): ${resolvedFenceTrace}.` : '');
      const step3Result = await getFirstTrackingInWindowWithDebug(
        truckId,
        step3After,
        vineyardBefore,
        vineyardFenceIds,
        'EXIT',
        false,
        step3Trace,
        VINEYARD_WINDOW_MATCH_LIST_CAP
      );
      step3Value = step3Result.value;
      debug.vineyard.step3 = step3Result.debug;
      if (step3Result.value != null) candidates.step3 = { value: step3Result.value, trackingId: step3Result.trackingId };
      if (candidates.step2 != null && candidates.step3 != null) {
        const eventRows = await listFenceEnterExitEventsInWindow(truckId, trackingWindowAfter, vineyardBefore);
        const star = tryGpsStarVineyardExit(eventRows, vineyardFenceIds, candidates.step2, candidates.step3, vineyardBefore);
        candidates.step3 = star.step3;
        if (star.usedGpsStar) {
          candidates.step3GpsStar = true;
          debug.step3GpsStar = true;
        }
      }
    }

    // VineSR1: Bankhouse South — if polygon step 2/3 not both found for South, try "Bankhouse" vineyard fences only.
    if (
      vineyardName === VINE_SR1_SOUTH_NAME &&
      (candidates.step2 == null || candidates.step3 == null)
    ) {
      candidates.step2 = null;
      candidates.step3 = null;
      delete candidates.step3GpsStar;
      delete debug.step3GpsStar;
      step2Value = null;
      step3Value = null;
      const { fenceIds: sr1FenceIds, debug: sr1VineyardDebug } = await getFenceIdsForVworkNameWithDebug(
        'Vineyard',
        VINE_SR1_FALLBACK_VINEYARD_NAME
      );
      Object.assign(debug.vineyard, sr1VineyardDebug);
      if (sr1FenceIds.length > 0) {
        const sr1Names = sr1VineyardDebug.fenceNamesInList.join('; ') || '—';
        const sr1ResolvedTrace = formatResolvedFenceNamesForGpsTrace(sr1VineyardDebug.resolvedFenceNames);
        const sr2Trace =
          `Step 2 (VineSR1 fallback, ${VINE_SR1_FALLBACK_VINEYARD_NAME}) — Vineyard ENTER. Fences: [${sr1Names}].` +
          (sr1ResolvedTrace ? ` Fences (name + id): ${sr1ResolvedTrace}.` : '') +
          ` Strict: t > ${step2SqlLowerPass1}; ${vineyardUpperTrace}.` +
          (pass1FloorMerge.floor != null
            ? ` Same ordering-floor merge as pass 1: max(X, floor) = ${step2SqlLowerPass1}.`
            : '');
        const sr1Step2 = await getFirstTrackingInWindowWithDebug(
          truckId,
          step2SqlLowerPass1,
          vineyardBefore,
          sr1FenceIds,
          'ENTER',
          false,
          sr2Trace,
          VINEYARD_WINDOW_MATCH_LIST_CAP
        );
        step2Value = sr1Step2.value;
        debug.vineyard.step2 = sr1Step2.debug;
        if (sr1Step2.value != null) {
          candidates.step2 = { value: sr1Step2.value, trackingId: sr1Step2.trackingId };
        }
        const sr1Step3After = step2Value ?? step2SqlLowerPass1;
        const sr3Trace =
          `Step 3 (VineSR1) — Vineyard EXIT; lowerExclusive t > ${sr1Step3After}. ${vineyardUpperTrace}.` +
          (sr1ResolvedTrace ? ` Fences (name + id): ${sr1ResolvedTrace}.` : '');
        const sr1Step3 = await getFirstTrackingInWindowWithDebug(
          truckId,
          sr1Step3After,
          vineyardBefore,
          sr1FenceIds,
          'EXIT',
          false,
          sr3Trace,
          VINEYARD_WINDOW_MATCH_LIST_CAP
        );
        step3Value = sr1Step3.value;
        debug.vineyard.step3 = sr1Step3.debug;
        if (sr1Step3.value != null) {
          candidates.step3 = { value: sr1Step3.value, trackingId: sr1Step3.trackingId };
        }
        if (candidates.step2 != null && candidates.step3 != null) {
          const eventRowsSr1 = await listFenceEnterExitEventsInWindow(truckId, trackingWindowAfter, vineyardBefore);
          const starSr1 = tryGpsStarVineyardExit(
            eventRowsSr1,
            sr1FenceIds,
            candidates.step2,
            candidates.step3,
            vineyardBefore
          );
          candidates.step3 = starSr1.step3;
          if (starSr1.usedGpsStar) {
            candidates.step3GpsStar = true;
            debug.step3GpsStar = true;
          }
          candidates.vineSr1Fallback = true;
          debug.vineSr1 = true;
        }
        vineyardFenceIdsForRefine = sr1FenceIds;
      }
    }
  }

  if (deliveryWinery) {
    const { fenceIds: wineryFenceIds, debug: wineryDebug } = await getFenceIdsForVworkNameWithDebug('Winery', deliveryWinery);
    Object.assign(debug.winery, wineryDebug);
    const step1Union = await mergeWineryFenceIdsForStep1MorningExit(job, truckId, wineryFenceIds);
    const step1MorningFenceIds = step1Union.merged;
    const baseFenceCount = sortedUniqueFenceIds(wineryFenceIds).length;
    debug.winery.step1MorningFenceUnion = {
      previousJobId: step1Union.previousJobId,
      previousDeliveryWinery: step1Union.previousDeliveryWinery,
      mergedFenceIdCount: step1MorningFenceIds.length,
      unionedPreviousWinery:
        step1MorningFenceIds.length > baseFenceCount && step1Union.previousDeliveryWinery != null,
    };
    if (wineryFenceIds.length > 0 || step1MorningFenceIds.length > 0) {
      /**
       * Morning winery EXIT must be strictly before arrive vineyard.
       * When polygon vineyard ENTER (step2Value) exists, cap only with min(window end, polygon ENTER).
       * VWork step_2_completed_at can be earlier than GPS (driver tap) and must NOT clip the search — it would
       * exclude a valid winery EXIT that still lies before the real vineyard ENTER (e.g. EXIT 11:06:09 vs ENTER 11:06:20).
       * When polygon step 2 is missing, include VWork step 2 in the cap (legacy behaviour).
       * EXIT is chosen only if no mapped winery ENTER lies strictly between the window lower bound and that EXIT
       * (so a re-ENTER before a later EXIT is not treated as "depart winery").
       */
      const vworkStep2Cap = vworkStepTime(job, 2);
      const step1Upper =
        step2Value != null
          ? minTimestampString(positionBefore, step2Value)
          : minTimestampString(positionBefore, vworkStep2Cap);
      const step1Before = step1Upper ?? positionBefore ?? step2Value ?? vworkStep2Cap ?? null;
      let step1MorningUpperForAudit: string | null = step1Before;
      let step1UpperForAudit: string | null = step1Upper ?? null;
      let vworkStep2CapForAudit: string | null = vworkStep2Cap;
      let rerunMorningExitAfterRefine = false;
      const step1Result = await getFirstWineryMorningExitInWindowWithDebug(
        truckId,
        trackingWindowAfter,
        step1Before,
        step1MorningFenceIds
      );
      debug.winery.step1 = step1Result.debug;
      let step1Value: string | null = null;
      if (step1Result.value != null) {
        candidates.step1 = { value: step1Result.value, trackingId: step1Result.trackingId };
        step1Value = step1Result.value;
      }
      /**
       * First vineyard ENTER can be before the real outbound (e.g. earlier drive-by). Morning winery EXIT (step 1)
       * is chosen using that ENTER as upper bound, but the true "arrive vineyard" for this job is the first ENTER
       * **after** leaving the winery. Re-pick step 2/3 from that lower bound, then re-query morning EXIT before new step 2.
       */
      if (
        step1Value != null &&
        vineyardFenceIdsForRefine != null &&
        vineyardFenceIdsForRefine.length > 0
      ) {
        const exitNorm = normalizeTimestampString(step1Value);
        const lowerEnter = maxTimestampString(trackingWindowAfter, step1Value);
        if (exitNorm != null && lowerEnter != null) {
          const refineFloorMerge = mergeVineyardPolygonLowerWithOrderingFloor(lowerEnter, job, exitNorm);
          const step2RefineSqlLower = refineFloorMerge.merged;
          const refineFloorHint =
            refineFloorMerge.floor != null
              ? ` Vineyard ordering floor (Part 1 guardrail): strictly > ${refineFloorMerge.floor} (${String(refineFloorMerge.rule).replace(/_/g, ' ')}); SQL lowerExclusive = max(max(trackingWindowAfter, GPS winery EXIT), floor) = ${step2RefineSqlLower}.`
              : '';
          const refineResolvedTrace = formatResolvedFenceNamesForGpsTrace(debug.vineyard.resolvedFenceNames);
          const r2Trace =
            `Step 2 (refine after morning winery EXIT) — Vineyard ENTER. Base lowerExclusive = max(trackingWindowAfter, GPS winery EXIT) = max(${trackingWindowAfter}, ${step1Value}) = ${lowerEnter}.${refineFloorHint} ${vineyardUpperTrace}. Replaces pass-1 if first ENTER after exit is strictly after exit time.` +
            (refineResolvedTrace ? ` Fences (name + id): ${refineResolvedTrace}.` : '');
          const r2AfterExit = await getFirstTrackingInWindowWithDebug(
            truckId,
            step2RefineSqlLower,
            vineyardBefore,
            vineyardFenceIdsForRefine,
            'ENTER',
            false,
            r2Trace,
            VINEYARD_WINDOW_MATCH_LIST_CAP
          );
          const entNorm =
            r2AfterExit.value != null ? normalizeTimestampString(r2AfterExit.value) : null;
          if (entNorm != null && entNorm > exitNorm && r2AfterExit.value != null) {
            candidates.step2 = { value: r2AfterExit.value, trackingId: r2AfterExit.trackingId };
            step2Value = r2AfterExit.value;
            debug.vineyard.step2 = r2AfterExit.debug;
            const step3AfterRefine = step2Value ?? lowerEnter;
            const r3Trace =
              `Step 3 (refine) — Vineyard EXIT; lowerExclusive t > ${step3AfterRefine} (step-2 ENTER or lowerEnter). ${vineyardUpperTrace}.` +
              (refineResolvedTrace ? ` Fences (name + id): ${refineResolvedTrace}.` : '');
            const r3After = await getFirstTrackingInWindowWithDebug(
              truckId,
              step3AfterRefine,
              vineyardBefore,
              vineyardFenceIdsForRefine,
              'EXIT',
              false,
              r3Trace,
              VINEYARD_WINDOW_MATCH_LIST_CAP
            );
            step3Value = r3After.value;
            debug.vineyard.step3 = r3After.debug;
            if (r3After.value != null) {
              candidates.step3 = { value: r3After.value, trackingId: r3After.trackingId };
            } else {
              candidates.step3 = null;
            }
            delete candidates.step3GpsStar;
            delete debug.step3GpsStar;
            if (candidates.step2 != null && candidates.step3 != null) {
              const eventRowsRef = await listFenceEnterExitEventsInWindow(
                truckId,
                trackingWindowAfter,
                vineyardBefore
              );
              const starRef = tryGpsStarVineyardExit(
                eventRowsRef,
                vineyardFenceIdsForRefine,
                candidates.step2,
                candidates.step3,
                vineyardBefore
              );
              candidates.step3 = starRef.step3;
              if (starRef.usedGpsStar) {
                candidates.step3GpsStar = true;
                debug.step3GpsStar = true;
              }
            }
            const vworkStep2CapRef = vworkStepTime(job, 2);
            const step1UpperRef =
              step2Value != null
                ? minTimestampString(positionBefore, step2Value)
                : minTimestampString(positionBefore, vworkStep2CapRef);
            const step1BeforeRef =
              step1UpperRef ?? positionBefore ?? step2Value ?? vworkStep2CapRef ?? null;
            const step1Refined = await getFirstWineryMorningExitInWindowWithDebug(
              truckId,
              trackingWindowAfter,
              step1BeforeRef,
              step1MorningFenceIds
            );
            debug.winery.step1 = step1Refined.debug;
            if (step1Refined.value != null) {
              candidates.step1 = { value: step1Refined.value, trackingId: step1Refined.trackingId };
              step1Value = step1Refined.value;
            }
            step1MorningUpperForAudit = step1BeforeRef;
            step1UpperForAudit = step1UpperRef ?? null;
            vworkStep2CapForAudit = vworkStep2CapRef;
            rerunMorningExitAfterRefine = true;
          }
        }
      }
      await attachWineryStep1MorningExitAudit(debug, {
        device: truckId,
        wineryFenceIds: step1MorningFenceIds,
        trackingWindowAfter,
        step1MorningUpperExclusive: step1MorningUpperForAudit,
        positionAfter,
        anchor,
        positionBefore,
        step1UpperResolved: step1UpperForAudit,
        step2PolygonEnter: step2Value,
        vworkStep2Cap: vworkStep2CapForAudit,
        rerunAfterLeaveWineryRefine: rerunMorningExitAfterRefine,
      });
      const step1ValueForStep4 = anchor ?? step1Value;
      const step23ForStep4Floor = pruneVineyardGpsForJobEnd(
        step2Value,
        step3Value,
        job,
        jobEndCeilingBufferMinutes
      );
      if (wineryFenceIds.length > 0) {
        const { step4, step5 } = await fetchWineryStep4And5ForValues(
          job,
          options,
          debug,
          truckId,
          wineryFenceIds,
          step1ValueForStep4,
          step23ForStep4Floor.step2,
          step23ForStep4Floor.step3,
          { anchor, gpsMorningExit: step1Value }
        );
        if (step4 != null) candidates.step4 = step4;
        if (step5 != null) candidates.step5 = step5;
      } else {
        const vworkStep5NoFence =
          (job.step_5_completed_at ?? job.actual_end_time) != null
            ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
            : null;
        const step5ExtNoFence = options.step5ExtendWineryExitMinutes ?? STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;
        const anchorNoFence = step5ExtendAnchorMaxTapAndGps4(vworkStep5NoFence, null);
        const vworkSearchEndNoFence =
          anchorNoFence != null && step5ExtNoFence > 0
            ? normalizeTimestampString(addMinutesToTimestampAsNZ(anchorNoFence, step5ExtNoFence))
            : anchorNoFence;
        const step5WinEndNoFence = step5ExitExclusiveUpper(positionBefore, vworkSearchEndNoFence);
        assignWineryStep5SearchWindowDebug(debug, {
          wineryFenceIds,
          step4Value: null,
          vworkStep5: vworkStep5NoFence,
          step5ExtendAnchor: anchorNoFence,
          step5ExtMin: step5ExtNoFence,
          vworkStep5SearchEnd: vworkSearchEndNoFence,
          step5WindowEnd:
            step5WinEndNoFence != null ? normalizeTimestampString(step5WinEndNoFence) : null,
          positionBefore,
          step5ExitQueryRan: false,
          fetchSkippedReason: 'no_mapped_winery_fences',
        });
      }
    } else {
      const vworkStep5NoFence =
        (job.step_5_completed_at ?? job.actual_end_time) != null
          ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
          : null;
      const step5ExtNoFence = options.step5ExtendWineryExitMinutes ?? STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;
      const anchorNoFence2 = step5ExtendAnchorMaxTapAndGps4(vworkStep5NoFence, null);
      const vworkSearchEndNoFence =
        anchorNoFence2 != null && step5ExtNoFence > 0
          ? normalizeTimestampString(addMinutesToTimestampAsNZ(anchorNoFence2, step5ExtNoFence))
          : anchorNoFence2;
      const step5WinEndNoFence = step5ExitExclusiveUpper(positionBefore, vworkSearchEndNoFence);
      assignWineryStep5SearchWindowDebug(debug, {
        wineryFenceIds,
        step4Value: null,
        vworkStep5: vworkStep5NoFence,
        step5ExtendAnchor: anchorNoFence2,
        step5ExtMin: step5ExtNoFence,
        vworkStep5SearchEnd: vworkSearchEndNoFence,
        step5WindowEnd:
          step5WinEndNoFence != null ? normalizeTimestampString(step5WinEndNoFence) : null,
        positionBefore,
        step5ExitQueryRan: false,
        fetchSkippedReason: 'no_mapped_winery_fences',
      });
    }
  } else {
    const vworkStep5NoDel =
      (job.step_5_completed_at ?? job.actual_end_time) != null
        ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
        : null;
    const step5ExtNd = options.step5ExtendWineryExitMinutes ?? STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;
    const anchorNd = step5ExtendAnchorMaxTapAndGps4(vworkStep5NoDel, null);
    const vworkSearchNd =
      anchorNd != null && step5ExtNd > 0
        ? normalizeTimestampString(addMinutesToTimestampAsNZ(anchorNd, step5ExtNd))
        : anchorNd;
    const step5WinNd = step5ExitExclusiveUpper(positionBefore, vworkSearchNd);
    assignWineryStep5SearchWindowDebug(debug, {
      wineryFenceIds: [],
      step4Value: null,
      vworkStep5: vworkStep5NoDel,
      step5ExtendAnchor: anchorNd,
      step5ExtMin: step5ExtNd,
      vworkStep5SearchEnd: vworkSearchNd,
      step5WindowEnd: step5WinNd != null ? normalizeTimestampString(step5WinNd) : null,
      positionBefore,
      step5ExitQueryRan: false,
      fetchSkippedReason: 'no_delivery_winery_on_job',
    });
  }
  const step1BracketFloor =
    trackingWindowAfter != null && String(trackingWindowAfter).trim() !== ''
      ? normalizeTimestampString(trackingWindowAfter) ?? String(trackingWindowAfter).trim().slice(0, 19)
      : null;
  applyGpsGuardrails(candidates, job, jobEndCeilingBufferMinutes, positionAfter, debug, {
    tentativeVineyardEnterForStep1Bracket: options.tentativeVineyardEnterForStep1Bracket ?? null,
    step1BracketTrackingFloor: step1BracketFloor,
  });
  if (candidates.vineSr1Fallback && (candidates.step2 == null || candidates.step3 == null)) {
    delete candidates.vineSr1Fallback;
    delete debug.vineSr1;
  }
  if (!candidates.step3GpsStar) {
    delete debug.step3GpsStar;
  }
  return candidates;
}

/**
 * Outbound leg only: after the same Part 1 fetch + guardrails as full derived steps, return winery EXIT (step 1),
 * vineyard ENTER (step 2), and vineyard EXIT (step 3) anchors for distance-on-map / Step1(lastJobEnd). Same window + mappings as steps.
 */
export async function fetchOutboundLegAnchors(
  job: JobForDerivedSteps,
  options: DerivedStepsOptions
): Promise<{
  wineryExit: GpsStepCandidate | null;
  vineyardEnter: GpsStepCandidate | null;
  vineyardExit: GpsStepCandidate | null;
  debug: DerivedStepsDebug;
}> {
  const debug: DerivedStepsDebug = {
    jobId: String(job.job_id ?? ''),
    windowMinutes: options.windowMinutes,
    truckId: options.device,
    actualStartTime: job.actual_start_time != null ? String(job.actual_start_time).trim() : '',
    actualEndTime: job.actual_end_time != null ? String(job.actual_end_time).trim() : null,
    positionAfter: options.positionAfter,
    positionBefore: options.positionBefore,
    vineyard: {
      type: 'Vineyard',
      vworkName: job.vineyard_name ? String(job.vineyard_name).trim() : '',
      mappingsFound: [],
      fenceNamesInList: [],
      fenceIds: [],
      resolvedFenceNames: [],
    },
    winery: {
      type: 'Winery',
      vworkName: job.delivery_winery ? String(job.delivery_winery).trim() : '',
      mappingsFound: [],
      fenceNamesInList: [],
      fenceIds: [],
      resolvedFenceNames: [],
    },
  };
  if (!options.device || !options.positionAfter) {
    return { wineryExit: null, vineyardEnter: null, vineyardExit: null, debug };
  }
  const candidates = await fetchGpsStepCandidates(job, options, debug);
  return {
    wineryExit: candidates.step1,
    vineyardEnter: candidates.step2,
    vineyardExit: candidates.step3,
    debug,
  };
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

/** Midpoint instant between two naive timestamps (50/50 time split). Same UTC arithmetic as `minutesBetween`. */
function midpointBetweenTimestamps(tsA: string, tsB: string): string | null {
  const a = normalizeTimestampString(tsA);
  const b = normalizeTimestampString(tsB);
  if (!a || !b) return null;
  const ma = a.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  const mb = b.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  if (!ma || !mb) return null;
  const msA = Date.UTC(Number(ma[1]), Number(ma[2]) - 1, Number(ma[3]), Number(ma[4]), Number(ma[5]), Number(ma[6]));
  const msB = Date.UTC(Number(mb[1]), Number(mb[2]) - 1, Number(mb[3]), Number(mb[4]), Number(mb[5]), Number(mb[6]));
  const mid = Math.round((msA + msB) / 2);
  const date = new Date(mid);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`;
}

/** `step_1_completed_at` / `actual_start_time` only — no override (for merge order when no `step1oride`). */
function vworkStep1FromJobFields(job: JobForDerivedSteps): string | null {
  const s1 =
    job.step_1_completed_at != null && String(job.step_1_completed_at).trim() !== ''
      ? job.step_1_completed_at
      : job.actual_start_time != null && String(job.actual_start_time).trim() !== ''
        ? job.actual_start_time
        : null;
  return s1 != null ? normalizeTimestampString(s1 as string) : null;
}

/** Normalized `step1oride` when non-empty. */
function normalizedStep1Oride(job: JobForDerivedSteps): string | null {
  if (job.step1oride == null || String(job.step1oride).trim() === '') return null;
  return normalizeTimestampString(String(job.step1oride).trim());
}

/**
 * Floor for dropping vineyard GPS steps 2/3/5 when t ≤ floor (and winery step‑4 max lower bound).
 * With **step1oride:** use **max(oride, GPS1)** when GPS1 survived brackets (cannot arrive before physical exit).
 * Without oride: **min(tap, GPS1)** when both exist (late tap must not kill ENTER after morning EXIT).
 * Else: GPS1 only, else tap only.
 */
function computeVineyardGpsOrderingFloorDebug(
  job: JobForDerivedSteps,
  gps1NormAfterBracket: string | null
): VineyardGpsOrderingFloorDebug {
  const orideNorm = normalizedStep1Oride(job);
  const tapOnlyNorm = vworkStep1FromJobFields(job);
  let rule: VineyardGpsOrderingFloorRule;
  let floorFor235: string | null;

  if (orideNorm != null) {
    if (gps1NormAfterBracket != null) {
      floorFor235 = maxTimestampString(orideNorm, gps1NormAfterBracket) ?? orideNorm;
      rule = 'max_oride_and_gps1';
    } else {
      floorFor235 = orideNorm;
      rule = 'oride_only';
    }
  } else if (gps1NormAfterBracket != null && tapOnlyNorm != null) {
    floorFor235 = minTimestampString(tapOnlyNorm, gps1NormAfterBracket) ?? gps1NormAfterBracket;
    rule = 'min_tap_and_gps1_no_oride';
  } else if (gps1NormAfterBracket != null) {
    floorFor235 = gps1NormAfterBracket;
    rule = 'gps1_only';
  } else {
    floorFor235 = tapOnlyNorm;
    rule = 'tap_only';
  }

  const summaryLine = `Vineyard GPS steps 2/3/5: drop when t≤${floorFor235 ?? '—'} (need strictly t>${floorFor235 ?? '—'}). Rule=${rule}; parts: step1oride=${orideNorm ?? '—'}, GPS1(after bracket)=${gps1NormAfterBracket ?? '—'}, tap=${tapOnlyNorm ?? '—'}.`;

  return { orideNorm, gps1NormAfterBracket, tapOnlyNorm, floorFor235, rule, summaryLine };
}

/**
 * Raise vineyard Step 2 polygon SQL `lowerExclusive` so LIMIT 1 matches the same “strictly after floor”
 * rule as {@link applyGpsGuardrails} ({@link computeVineyardGpsOrderingFloorDebug}: oride / max(oride, GPS1) /
 * min(tap, GPS1) / …). Pass **morning winery EXIT** norm for `gps1MorningExitNorm` once known so the floor
 * can be max(oride, GPS1); use `null` before GPS1 exists (oride∨tap-only branch). Without this merge, the
 * query can return the first ENTER after winery EXIT that is still on or before the floor and is later
 * dropped, while a later ENTER would qualify but never wins LIMIT 1.
 */
function mergeVineyardPolygonLowerWithOrderingFloor(
  polygonLowerExclusive: string,
  job: JobForDerivedSteps,
  gps1MorningExitNorm: string | null
): { merged: string; floor: string | null; rule: VineyardGpsOrderingFloorRule } {
  const raw = polygonLowerExclusive != null ? String(polygonLowerExclusive).trim() : '';
  const lo = raw !== '' ? normalizeTimestampString(raw) ?? raw.slice(0, 19) : '';
  const floorDbg = computeVineyardGpsOrderingFloorDebug(job, gps1MorningExitNorm);
  const floor = floorDbg.floorFor235;
  if (floor == null) {
    return { merged: lo || raw, floor: null, rule: floorDbg.rule };
  }
  const f = normalizeTimestampString(floor) ?? floor.slice(0, 19);
  if (!lo) {
    return { merged: f, floor, rule: floorDbg.rule };
  }
  const merged = maxTimestampString(lo, f) ?? lo;
  return { merged, floor, rule: floorDbg.rule };
}

/**
 * Single job-start anchor: **override if set**, else VWork step 1 from the job row.
 * Used for fetch window, guardrails, merge (when oride: anchor wins over GPS step 1), and cleanup.
 */
export function jobStep1Anchor(job: JobForDerivedSteps): string | null {
  const o = normalizedStep1Oride(job);
  if (o != null) return o;
  return vworkStep1FromJobFields(job);
}

/** Same as {@link jobStep1Anchor}. Exported for GPS distance harvest and existing call sites. */
export function vworkStep1TimeForCleanup(job: JobForDerivedSteps): string | null {
  return jobStep1Anchor(job);
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
  const anchor = jobStep1Anchor(job);
  const o1 = normalizedStep1Oride(job);
  const v2 = vworkStepTime(job, 2);
  const v3 = vworkStepTime(job, 3);
  const v4 = vworkStepTime(job, 4);
  const v5 = vworkStepTime(job, 5);
  return {
    ...gps,
    step1: o1 != null ? o1 : (gps.step1Gps ?? anchor ?? null),
    step2: gps.step2Gps ?? v2 ?? null,
    step3: gps.step3Gps ?? v3 ?? null,
    step4: gps.step4Gps ?? v4 ?? null,
    step5: gps.step5Gps ?? v5 ?? null,
  };
}

/**
 * Cleanup runs on **merged actual** times (`step1`..`step5`). **Step3windback** also reads the GPS layer
 * (`step3Gps` / `step4Gps`) only to decide if step 3 vs step 4 came from GPS.
 * Runs once at end of finalize after actuals are fully populated.
 *
 * cleanup_start: VWork start is after actual vineyard arrive, but **actual step 1 is missing** → step1 = actual.step2 − 10 min.
 * travel: VWork start is after actual step 2, and **actual step 1 is missing or not strictly before step 2** →
 *   step1 = actual.step2 − travel (actual.step3→step4 leg, else 20 min).
 * Step3windback: **GPS step 4** is truth, **step 3 is not GPS**, but merged step 3 (VWork) is **after** step 4 →
 *   step 3 = step 4 − outbound (step2 − step1), capped like step4_order; **if** that is not strictly after step 2,
 *   step 3 = midpoint between step 2 and step 4. Does **not** move step 4.
 * step4_order (else): **Arrive winery** (step 4) is before **leave vineyard** (step 3) → step4 = step3 + outbound minutes
 *   (same duration as morning leg: step2 − step1). Skipped when Step3windback runs.
 * step4_mid_35: merged step 4 still missing but step 3 and step 5 exist with step 3 &lt; step 5 → step4 = time midpoint
 *   (e.g. no winery ENTER from GPS and no VWork step 4).
 */
function applyCleanupRules(
  actual: DerivedStepsResult,
  job: JobForDerivedSteps,
  gps: DerivedStepsResult
): { step4RuleApplied: boolean; step3WindbackApplied: boolean; report: CleanupRulesReport } {
  const report: CleanupRulesReport = {
    step1: { applied: false },
    step3Windback: null,
    step4Order: null,
    step4Mid35: null,
  };

  const step1BeforeCleanup = actual.step1 != null ? normalizeTimestampString(actual.step1) : null;
  const step1Vwork = vworkStep1TimeForCleanup(job);
  const a1 = actual.step1 != null ? normalizeTimestampString(actual.step1) : null;
  const a2 = actual.step2 != null ? normalizeTimestampString(actual.step2) : null;

  let step1RuleKind: 'cleanup_start' | 'travel' | undefined;
  if (a1 == null && a2 != null && step1Vwork != null && a2 < step1Vwork) {
    actual.step1 = subtractMinutesFromTimestamp(a2, 10);
    step1RuleKind = 'cleanup_start';
  } else if (step1Vwork != null && a2 != null && step1Vwork > a2 && (a1 == null || a1 >= a2)) {
    const travelMinutes =
      actual.step3 != null && actual.step4 != null
        ? minutesBetween(actual.step3, actual.step4)
        : 20;
    const safeMinutes = Math.max(1, Math.min(120, travelMinutes));
    actual.step1 = subtractMinutesFromTimestamp(a2, safeMinutes);
    step1RuleKind = 'travel';
  }
  const step1AfterCleanup = actual.step1 != null ? normalizeTimestampString(actual.step1) : null;
  if (step1RuleKind != null && step1BeforeCleanup !== step1AfterCleanup) {
    report.step1 = {
      applied: true,
      rule: step1RuleKind,
      step1Before: step1BeforeCleanup,
      step1After: step1AfterCleanup,
    };
  }

  let step4RuleApplied = false;
  let step3WindbackApplied = false;
  const s1 = actual.step1 != null ? normalizeTimestampString(actual.step1) : null;
  const s2 = actual.step2 != null ? normalizeTimestampString(actual.step2) : null;
  const s3 = actual.step3 != null ? normalizeTimestampString(actual.step3) : null;
  const s4 = actual.step4 != null ? normalizeTimestampString(actual.step4) : null;

  const step3FromGps = gps.step3Gps != null && String(gps.step3Gps).trim() !== '';
  const step4FromGps = gps.step4Gps != null && String(gps.step4Gps).trim() !== '';

  if (
    !step3FromGps &&
    step4FromGps &&
    s1 != null &&
    s2 != null &&
    s3 != null &&
    s4 != null &&
    s4 < s3 &&
    actual.step1 &&
    actual.step2 &&
    actual.step3 &&
    actual.step4
  ) {
    const outboundMin = minutesBetween(actual.step1, actual.step2);
    if (outboundMin >= 1) {
      const capped = Math.min(outboundMin, 24 * 60);
      const rawWind = subtractMinutesFromTimestamp(actual.step4, capped);
      const windNorm = normalizeTimestampString(rawWind);
      let next3: string | null = null;
      let windbackPath: 'wind' | 'midpoint' | null = null;
      if (windNorm != null && windNorm > s2) {
        next3 = windNorm;
        windbackPath = 'wind';
      } else {
        next3 = midpointBetweenTimestamps(actual.step2, actual.step4);
        next3 = next3 != null ? normalizeTimestampString(next3) : null;
        windbackPath = 'midpoint';
      }
      if (next3 != null && windbackPath != null) {
        actual.step3 = next3;
        step3WindbackApplied = true;
        report.step3Windback = {
          path: windbackPath,
          mergedStep3Before: s3,
          step4Gps: s4,
          outboundMinutes: capped,
          step3After: next3,
        };
      }
    }
  }

  if (
    !step3WindbackApplied &&
    s1 != null &&
    s2 != null &&
    s3 != null &&
    s4 != null &&
    s4 < s3 &&
    actual.step1 &&
    actual.step2 &&
    actual.step3
  ) {
    const outboundMin = minutesBetween(actual.step1, actual.step2);
    if (outboundMin >= 1) {
      const capped = Math.min(outboundMin, 24 * 60);
      const rawAdded = addMinutesToTimestampAsNZ(actual.step3, capped);
      const adjusted = normalizeTimestampString(rawAdded);
      if (adjusted != null && adjusted >= s3) {
        report.step4Order = {
          mergedStep4Before: s4,
          step3At: s3,
          step4After: adjusted,
          outboundMinutes: capped,
        };
        actual.step4 = adjusted;
        step4RuleApplied = true;
      }
    }
  }

  const s3Final = actual.step3 != null ? normalizeTimestampString(actual.step3) : null;
  const s5Final = actual.step5 != null ? normalizeTimestampString(actual.step5) : null;
  const s4Final = actual.step4 != null ? normalizeTimestampString(actual.step4) : null;
  if (
    s4Final == null &&
    s3Final != null &&
    s5Final != null &&
    s3Final < s5Final &&
    actual.step3 &&
    actual.step5
  ) {
    const midRaw = midpointBetweenTimestamps(actual.step3, actual.step5);
    const midNorm = midRaw != null ? normalizeTimestampString(midRaw) : null;
    if (midNorm != null && midNorm > s3Final && midNorm < s5Final) {
      actual.step4 = midNorm;
      step4RuleApplied = true;
      report.step4Mid35 = {
        step3At: s3Final,
        step5At: s5Final,
        step4After: midNorm,
      };
    }
  }

  return { step4RuleApplied, step3WindbackApplied, report };
}

/** Part 3: Manual overrides on top of resolved actuals. `gps` retains raw GPS for Via labels. */
function applyOrides(
  actual: DerivedStepsResult,
  job: JobForDerivedSteps,
  gps: DerivedStepsResult,
  step1CleanupApplied: boolean,
  step4RuleApplied: boolean,
  step3WindbackApplied: boolean
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
    orides[1] != null
      ? 'ORIDE'
      : gps.step2Via === 'VineFence+' || gps.step2Via === 'VineFenceV+' || gps.step2Via === 'VineSR1'
        ? gps.step2Via
        : gps.step2Gps != null
          ? 'GPS'
          : 'VW';
  const step3Via: StepVia =
    orides[2] != null
      ? 'ORIDE'
      : step3WindbackApplied
        ? 'Step3windback'
        : gps.step3Via === 'VineFence+' || gps.step3Via === 'VineFenceV+' || gps.step3Via === 'VineSR1'
          ? gps.step3Via
          : gps.step3Via === 'GPS*'
            ? 'GPS*'
            : gps.step3Gps != null
              ? 'GPS'
              : 'VW';
  const step4Via: StepVia =
    orides[3] != null ? 'ORIDE' : step4RuleApplied ? 'RULE' : gps.step4Gps != null ? 'GPS' : 'VW';
  const step5Via: StepVia = orides[4] != null ? 'ORIDE' : (gps.step5Gps != null ? 'GPS' : 'VW');
  return { result: out, step1Via, step2Via, step3Via, step4Via, step5Via };
}

/** Part 2: Raw GPS times only (`stepNGps`). VWork merge, cleanup, orides happen in finalizeDerivedSteps. */
function decideFinalSteps(
  candidates: FetchedGpsCandidates,
  job: JobForDerivedSteps,
  step5ExtendWineryExitMinutes: number = STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES
): DerivedStepsResult {
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
  if (candidates.step1 && normalizedStep1Oride(job) == null) {
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
  if (candidates.vineSr1Fallback && candidates.step2 && candidates.step3) {
    result.step2Via = 'VineSR1';
    if (!candidates.step3GpsStar) {
      result.step3Via = 'VineSR1';
    }
  }
  if (candidates.step4) {
    result.step4Gps = candidates.step4.value;
    result.step4TrackingId = candidates.step4.trackingId;
  }
  if (candidates.step5 && vworkStep5 != null) {
    const gpsNorm = normalizeTimestampString(candidates.step5.value);
    const anchor = step5ExtendAnchorMaxTapAndGps4(vworkStep5, candidates.step4?.value ?? null);
    if (gpsNorm != null) {
      if (gpsNorm < vworkStep5) {
        result.step5Gps = candidates.step5.value;
        result.step5TrackingId = candidates.step5.trackingId;
      } else if (step5ExtendWineryExitMinutes > 0 && anchor != null) {
        const upper = normalizeTimestampString(addMinutesToTimestampAsNZ(anchor, step5ExtendWineryExitMinutes));
        if (upper != null && gpsNorm >= vworkStep5 && gpsNorm < upper) {
          result.step5Gps = candidates.step5.value;
          result.step5TrackingId = candidates.step5.trackingId;
        }
      }
    }
  }
  return result;
}

/** Part 2 audit for Inspect JSON / Explanation — does not change step outcomes. */
function attachStep5DecideDebug(
  debug: DerivedStepsDebug,
  step5Candidate: GpsStepCandidate | null | undefined,
  job: JobForDerivedSteps,
  step5ExtendWineryExitMinutes: number,
  acceptedStep5Gps: string | null,
  step4GpsEnter: string | null | undefined
): void {
  const vworkStep5 =
    (job.step_5_completed_at ?? job.actual_end_time) != null
      ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
      : null;
  const candNorm =
    step5Candidate?.value != null ? normalizeTimestampString(step5Candidate.value) : null;
  const step5ExtendAnchor =
    vworkStep5 != null ? step5ExtendAnchorMaxTapAndGps4(vworkStep5, step4GpsEnter) : null;
  const upperExclusive =
    step5ExtendAnchor != null && step5ExtendWineryExitMinutes > 0
      ? normalizeTimestampString(addMinutesToTimestampAsNZ(step5ExtendAnchor, step5ExtendWineryExitMinutes))
      : null;

  let outcome: Step5DecideDebug['outcome'];
  let summaryLine: string;

  if (vworkStep5 == null) {
    outcome = 'skipped_no_vwork_job_end';
    summaryLine = 'No step_5_completed_at / actual_end_time — Part 2 does not set GPS step 5.';
  } else if (candNorm == null) {
    outcome = 'no_candidate_after_guardrails';
    summaryLine =
      'No winery EXIT candidate reached decideFinalSteps (fetch miss, fetch skip, or guardrails cleared step 5).';
  } else if (acceptedStep5Gps != null) {
    if (candNorm < vworkStep5) {
      outcome = 'accepted_exit_strictly_before_job_end';
      summaryLine = `GPS step 5 kept: EXIT ${candNorm} is strictly before VWork job end ${vworkStep5}.`;
    } else {
      outcome = 'accepted_exit_after_job_end_within_extend';
      summaryLine = `GPS step 5 kept: EXIT ${candNorm} is on/after job end ${vworkStep5} but strictly before exclusive upper ${upperExclusive ?? 'n/a'} (max(tap, GPS step 4) + Step5Extend ${step5ExtendWineryExitMinutes} min).`;
    }
  } else if (candNorm < vworkStep5) {
    outcome = 'no_candidate_after_guardrails';
    summaryLine = 'Inconsistent: candidate before job end was not promoted — check logic.';
  } else if (step5ExtendWineryExitMinutes <= 0) {
    outcome = 'rejected_extend_disabled_and_exit_not_before_job_end';
    summaryLine = `EXIT ${candNorm} is at/after job end ${vworkStep5} and Step5ExtendWineryExit is ${step5ExtendWineryExitMinutes} — Part 2 rejects.`;
  } else if (upperExclusive != null && candNorm >= upperExclusive) {
    outcome = 'rejected_exit_at_or_after_job_end_outside_extend';
    summaryLine = `EXIT ${candNorm} is at/after exclusive upper ${upperExclusive} (max(tap, GPS4) + Step5Extend) — Part 2 rejects (outside “complete before leave” band).`;
  } else {
    outcome = 'rejected_exit_at_or_after_job_end_outside_extend';
    summaryLine = `EXIT ${candNorm} not accepted as GPS step 5 vs job end ${vworkStep5} / anchor ${step5ExtendAnchor ?? 'n/a'} / extend ${step5ExtendWineryExitMinutes} min.`;
  }

  debug.step5Decide = {
    vworkStep5,
    step5ExtendWineryExitMinutes,
    fetchCandidateTime: candNorm,
    fetchCandidateTrackingId:
      step5Candidate?.trackingId != null && Number.isFinite(step5Candidate.trackingId)
        ? step5Candidate.trackingId
        : null,
    acceptAfterJobEndExclusiveUpper: upperExclusive,
    step5ExtendAnchor: step5ExtendAnchor != null ? normalizeTimestampString(step5ExtendAnchor) : null,
    step5GpsAccepted: acceptedStep5Gps != null,
    outcome,
    summaryLine,
  };
}

/**
 * After VineFence+ / VineFenceV+ (Steps+) sets vineyard enter/exit, re-query winery steps 4–5 so the max(step1–3) floor uses
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
  const step5Ext = options.step5ExtendWineryExitMinutes ?? STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;

  if (deliveryWinery) {
    const { fenceIds: wineryFenceIds, debug: wineryDebug } = await getFenceIdsForVworkNameWithDebug('Winery', deliveryWinery);
    Object.assign(debug.winery, wineryDebug);
    if (wineryFenceIds.length > 0) {
      const anchor = jobStep1Anchor(job);
      const step1ForStep4 = anchor ?? step123.step1?.value ?? null;
      const buf = options.jobEndCeilingBufferMinutes ?? JOB_END_CEILING_BUFFER_DEFAULT_MINUTES;
      const step23ForStep4Floor = pruneVineyardGpsForJobEnd(
        step123.step2?.value ?? null,
        step123.step3?.value ?? null,
        job,
        buf
      );
      const { step4, step5 } = await fetchWineryStep4And5ForValues(
        job,
        options,
        debug,
        truckId,
        wineryFenceIds,
        step1ForStep4,
        step23ForStep4Floor.step2,
        step23ForStep4Floor.step3,
        { anchor, gpsMorningExit: step123.step1?.value ?? null }
      );
      if (step4 != null) candidates.step4 = step4;
      if (step5 != null) candidates.step5 = step5;
    } else {
      const vworkStep5NoFence =
        (job.step_5_completed_at ?? job.actual_end_time) != null
          ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
          : null;
      const anchorVfp = step5ExtendAnchorMaxTapAndGps4(vworkStep5NoFence, null);
      const vworkSearchEndNoFence =
        anchorVfp != null && step5Ext > 0
          ? normalizeTimestampString(addMinutesToTimestampAsNZ(anchorVfp, step5Ext))
          : anchorVfp;
      const step5WinEndNoFence = step5ExitExclusiveUpper(options.positionBefore, vworkSearchEndNoFence);
      assignWineryStep5SearchWindowDebug(debug, {
        wineryFenceIds,
        step4Value: null,
        vworkStep5: vworkStep5NoFence,
        step5ExtendAnchor: anchorVfp,
        step5ExtMin: step5Ext,
        vworkStep5SearchEnd: vworkSearchEndNoFence,
        step5WindowEnd:
          step5WinEndNoFence != null ? normalizeTimestampString(step5WinEndNoFence) : null,
        positionBefore: options.positionBefore ?? null,
        step5ExitQueryRan: false,
        fetchSkippedReason: 'no_mapped_winery_fences',
      });
    }
  } else {
    const vworkStep5NoDel =
      (job.step_5_completed_at ?? job.actual_end_time) != null
        ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
        : null;
    const anchorVfpNd = step5ExtendAnchorMaxTapAndGps4(vworkStep5NoDel, null);
    const vworkSearchNd =
      anchorVfpNd != null && step5Ext > 0
        ? normalizeTimestampString(addMinutesToTimestampAsNZ(anchorVfpNd, step5Ext))
        : anchorVfpNd;
    const step5WinNd = step5ExitExclusiveUpper(options.positionBefore, vworkSearchNd);
    assignWineryStep5SearchWindowDebug(debug, {
      wineryFenceIds: [],
      step4Value: null,
      vworkStep5: vworkStep5NoDel,
      step5ExtendAnchor: anchorVfpNd,
      step5ExtMin: step5Ext,
      vworkStep5SearchEnd: vworkSearchNd,
      step5WindowEnd: step5WinNd != null ? normalizeTimestampString(step5WinNd) : null,
      positionBefore: options.positionBefore ?? null,
      step5ExitQueryRan: false,
      fetchSkippedReason: 'no_delivery_winery_on_job',
    });
  }
  const buf = options.jobEndCeilingBufferMinutes ?? JOB_END_CEILING_BUFFER_DEFAULT_MINUTES;
  const anchorVfpBracket = jobStep1Anchor(job);
  const twVfp =
    anchorVfpBracket != null &&
    options.positionAfter != null &&
    String(options.positionAfter).trim() !== ''
      ? minTimestampString(options.positionAfter, anchorVfpBracket) ?? options.positionAfter
      : options.positionAfter;
  const step1BracketFloorVfp =
    twVfp != null && String(twVfp).trim() !== ''
      ? normalizeTimestampString(twVfp) ?? String(twVfp).trim().slice(0, 19)
      : null;
  applyGpsGuardrails(candidates, job, buf, options.positionAfter, debug, {
    tentativeVineyardEnterForStep1Bracket: options.tentativeVineyardEnterForStep1Bracket ?? null,
    step1BracketTrackingFloor: step1BracketFloorVfp,
  });
  const decided = decideFinalSteps(candidates, job, step5Ext);
  attachStep5DecideDebug(debug, candidates.step5, job, step5Ext, decided.step5Gps, candidates.step4?.value);
  return decided;
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
  cleanupRulesReport: CleanupRulesReport;
} {
  const actual = resolveActualFromGpsAndVwork(gps, job);
  const beforeCleanup = actual.step1;
  const { step4RuleApplied, step3WindbackApplied, report: cleanupRulesReport } = applyCleanupRules(actual, job, gps);
  const step1CleanupApplied =
    normalizeTimestampString(beforeCleanup) !== normalizeTimestampString(actual.step1);
  const step1ActualOverride: Step1CleanupOverride = step1CleanupApplied ? actual.step1 : null;
  const { result, step1Via, step2Via, step3Via, step4Via, step5Via } = applyOrides(
    actual,
    job,
    gps,
    step1CleanupApplied,
    step4RuleApplied,
    step3WindbackApplied
  );
  return {
    ...result,
    step1Via,
    step2Via,
    step3Via,
    step4Via,
    step5Via,
    step1ActualOverride: step1ActualOverride ?? undefined,
    cleanupRulesReport,
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
  const emptyCleanup: CleanupRulesReport = {
    step1: { applied: false },
    step3Windback: null,
    step4Order: null,
    step4Mid35: null,
  };
  if (!options.device || !options.positionAfter) {
    return { ...emptyResult, debug, step1ActualOverride: null, cleanupRulesReport: emptyCleanup };
  }
  const candidates = await fetchGpsStepCandidates(job, options, debug);
  const step5Ext = options.step5ExtendWineryExitMinutes ?? STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;
  const gpsOnly = decideFinalSteps(candidates, job, step5Ext);
  attachStep5DecideDebug(debug, candidates.step5, job, step5Ext, gpsOnly.step5Gps, candidates.step4?.value);
  const finalized = finalizeDerivedSteps(gpsOnly, job);
  const { step1Via, step2Via, step3Via, step4Via, step5Via, step1ActualOverride, cleanupRulesReport, ...rest } =
    finalized;
  return {
    ...rest,
    debug,
    step1ActualOverride: step1ActualOverride ?? undefined,
    step1Via,
    step2Via,
    step3Via,
    step4Via,
    step5Via,
    cleanupRulesReport,
  };
}

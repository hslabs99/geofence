/**
 * Derive VWork **job steps 1–5** from tbl_tracking ENTER/EXIT and business rules.
 * **Steps+** (buffered vineyard polygon fallback for missing step 2/3) runs in `/api/tracking/derived-steps`, not in this module.
 */
import { query } from '@/lib/db';
import { addMinutesToTimestampAsNZ } from '@/lib/fetch-steps';
import { JOB_END_CEILING_BUFFER_DEFAULT_MINUTES } from '@/lib/job-end-ceiling-buffer-setting-names';
import { STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES } from '@/lib/step5-winery-exit-extend-setting-names';
import { dateToLiteral } from '@/lib/utils';

/** Vineyard special rule 1: Bankhouse South — if step 2/3 missing for South fences, use Bankhouse fences (tag VineSR1). */
const VINE_SR1_SOUTH_NAME = 'Bankhouse South';
const VINE_SR1_FALLBACK_VINEYARD_NAME = 'Bankhouse';

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
  // Do not return t.slice(0, 19) — English month strings truncate to invalid SQL (e.g. "Thu Mar 19 2026 02:").
  return null;
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
  /** Geofence id on the matched tbl_tracking row (which fence fired). */
  matchedGeofenceId: number | null;
  /** tbl_geofences.fence_name for matchedGeofenceId (Inspect: which mapped fence won). */
  matchedFenceName: string | null;
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
        matchedGeofenceId: null,
        matchedFenceName: null,
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
  };

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
  'GPS step 1 is the first mapped winery EXIT with lowerExclusive < t_exit < upperExclusive (strict). When a mapped vineyard ENTER exists, upperExclusive is capped to be strictly before that vineyard ENTER (so Step 1 cannot land after arriving at vineyard). Winery ENTER rows do not disqualify the EXIT.';

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
  const asGridRow = `${dev ?? '—'} | ${fn ?? '—'} | ${geofenceType} | ${tNz} | ${latLon} | tbl_tracking.id=${idStr}`;
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
    | 'no_step5_upper_bound'
    | 'vwork_step5_not_after_step4_enter';
  /** Lower bound (exclusive) passed to tbl_tracking for step 5 EXIT — same instant as chosen winery ENTER for step 4. */
  lowerExclusive: string | null;
  /** Upper bound (exclusive) — `min(positionBefore from options, jobEnd + Step5ExtendWineryExit)` when both exist. */
  upperExclusive: string | null;
  /** `step_5_completed_at ?? actual_end_time` (normalized), used for extend cap — not `gps_end_time`. */
  jobEndForStep5Rule: string | null;
  step5ExtendWineryExitMinutes: number;
  /** `jobEndForStep5Rule + extend` when extend &gt; 0; else same as job end; null if no job end. */
  jobEndPlusExtend: string | null;
  /** `options.positionBefore` passed into derivation (Inspect / tagging job window end). */
  positionBeforeFromOptions: string | null;
  /**
   * Which side set `upperExclusive` when both job-end+extend and positionBefore exist:
   * `position_before` = tighter cap from job window; `job_end_plus_extend` = tighter cap from step-5 rule.
   */
  upperExclusiveSource:
    | 'not_computed'
    | 'only_position_before'
    | 'only_job_end_plus_extend'
    | 'only_vwork_step5_no_extend_zero'
    | 'position_before_tighter'
    | 'job_end_plus_extend_tighter'
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

/** Why Part 2 kept or dropped GPS step 5 after guardrails (same rules as `decideFinalSteps`). */
export type Step5DecideDebug = {
  vworkStep5: string | null;
  step5ExtendWineryExitMinutes: number;
  /** Candidate entering `decideFinalSteps` (after `applyGpsGuardrails`). */
  fetchCandidateTime: string | null;
  fetchCandidateTrackingId: number | null;
  /** Exclusive upper for “after job end” acceptance: job end + extend (Part 2). */
  acceptAfterJobEndExclusiveUpper: string | null;
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
    /** Auditable morning winery EXIT (step 1) window + re-enter rule; Inspect Explanation surfaces this. */
    step1MorningExitSearch?: WineryStep1MorningExitSearchDebug;
    /** Auditable (X,Y) window for step 5 EXIT fetch; Inspect Explanation surfaces this. */
    step5SearchWindow?: WineryStep5SearchWindowDebug;
  };
  /** Part 2: why GPS step 5 was kept or dropped vs VWork job end + extend. */
  step5Decide?: Step5DecideDebug;
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
   * Minutes after VWork job end to search for winery EXIT and accept it as GPS step 5 (early “job complete” before physical leave).
   * Defaults to `STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES` when omitted (tbl_settings Step5ExtendWineryExit).
   */
  step5ExtendWineryExitMinutes?: number;
};

/**
 * STEP RULES (GPS-derived steps)
 * ------------------------------
 * Part 1 — Fetch: Get a valid (or probably valid) GPS entry (Winery/Vineyard, ENTER/EXIT). Part 2 — Decide: Steps 1–4 use GPS if exists else VWork; Step 5 use GPS if winery EXIT is before job end or within step-5 extend after job end.
 * Step 1 — Job start by GPS: First Winery EXIT strictly before arrive vineyard.
 *   - Upper bound: if polygon vineyard ENTER exists, min(data window end, polygon ENTER) only — VWork step 2 is not used (can be early vs GPS). If polygon step 2 is missing, min(data window end, VWork step 2) so we do not take a return-leg EXIT as “start job”.
 *   - May be absent if the job started after the driver had already left the winery fence.
 * Step 2 — Arrive vineyard: First Vineyard ENTER in window.
 * Step 3 — Leave vineyard: First Vineyard EXIT after step 2 (so we don't pick an earlier exit before the enter).
 *   GPS* (optional): If the driver briefly exits and re-enters the same vineyard fence set with no other fence
 *   ENTER/EXIT between, aggregate up to 3 such loops; step 3 becomes the last EXIT in the chain. Any alien fence
 *   event voids GPS* for that job (revert to first exit only). Marked step3Via = GPS* and calcnotes GPS*:.
 * Step 4 — Arrive winery (return leg): First mapped Winery ENTER strictly &lt; data window end (positionBefore).
 * Lower bound: **if both GPS step 2 and step 3 are present**, strictly &gt; max(GPS step1, step2, step3) (step 1 uses anchor∨GPS for the step‑1 leg). **If both GPS step 2 and step 3 are missing**, do not use vineyard times in the max — use max(GPS step1 leg, positionAfter) only so a partial or absent vineyard GPS leg does not block a valid winery ENTER/EXIT pair in the window.
 * Step 5 — Job end by GPS: VWork step 5 = step_5_completed_at (job completed in system). Use GPS when Winery EXIT is strictly &lt; VWork step 5 (forgot to end job), OR when EXIT is ≥ job end and strictly &lt; job end + `step5ExtendWineryExitMinutes` (tapped complete before leaving). Search first EXIT after step 4 with upper bound min(positionBefore, job end + extend). FIRST such EXIT wins.
 *
 * Derive GPS step timestamps for a job using these rules. Window is passed in (same as tbl_tracking UI)
 * — no server-side timezone or date logic. Uses tbl_gpsmappings + original vwork name → tbl_geofences
 * → fence_ids; then scans tbl_tracking in window.
 *
 * Guardrail (after fetch): If GPS step 1 exists, steps 2–3 and 5 must be strictly &gt; step 1 time (step 4 uses max(step1–3) floor — see below). Each tbl_tracking id may appear at most once. Step 5 cleared if step 4 cleared.
 * Guardrail — step 4: same floor as fetch (when GPS step 2 and 3 both present, max with step2/step3; when both missing, max(step1 leg, positionAfter) only).
 * Guardrail — VWork job end: GPS steps 1–2 must be strictly **before** VWork step 5. Step 3 may be before VWork step 5 plus `jobEndCeilingBufferMinutes` (Job End Ceiling Buffer). Step 5 fetch/accept may extend past job end by `step5ExtendWineryExitMinutes` (tbl_settings Step5ExtendWineryExit).
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

/**
 * If GPS step 1 exists, drop steps 2–3 and 5 with time &lt;= **job-start anchor** (step 4 excluded — uses max(step1–3) floor below).
 * Anchor = `step1oride` if set, else VWork step 1. **G1 vs anchor:** if G1 &gt; anchor, keep G1 only when G2 exists and G1 &lt; G2 (tap before physical exit, but left before vineyard); if G2 missing and G1 &gt; anchor, clear G1.
 * Step 4: drop if &lt;= floor (when GPS step 2+3 both present: max(anchor or GPS step1, step2, step3); when both missing: max(anchor or GPS step1, positionAfter) only).
 * VWork ceiling: steps 1–2 must be &lt; VWork job end. Step 3 must be &lt; job end + buffer (see `jobEndCeilingBufferMinutes`).
 */
function applyGpsGuardrails(
  candidates: FetchedGpsCandidates,
  job: JobForDerivedSteps,
  jobEndCeilingBufferMinutes: number = JOB_END_CEILING_BUFFER_DEFAULT_MINUTES,
  /** Data window start; used for step 4 floor when GPS step 2 or 3 is missing. */
  positionAfter?: string | null
): void {
  const vworkEnd =
    job.step_5_completed_at != null || job.actual_end_time != null
      ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
      : null;
  const step3Ceiling =
    vworkEnd != null && jobEndCeilingBufferMinutes > 0
      ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkEnd, jobEndCeilingBufferMinutes))
      : vworkEnd;
  if (vworkEnd != null && candidates.step1?.value != null) {
    const s1c = normalizeTimestampString(candidates.step1.value);
    if (s1c != null && s1c >= vworkEnd) {
      candidates.step1 = null;
    }
  }
  const anchor = jobStep1Anchor(job);
  const s1 = candidates.step1;
  /** GPS step 1 (morning winery EXIT) vs anchor: keep when anchor < GPS step 1 < GPS step 2 (arrive vineyard). If no step 2 GPS and step 1 > anchor, clear. Do not use VWork step 2/3 to qualify step 1. */
  if (anchor != null && s1?.value != null) {
    const g1 = normalizeTimestampString(s1.value);
    if (g1 != null && g1 > anchor) {
      const g2 = candidates.step2?.value != null ? normalizeTimestampString(candidates.step2.value) : null;
      const keepG1AfterV1 = g2 != null && g1 < g2;
      if (!keepG1AfterV1) {
        candidates.step1 = null;
      }
    }
  }
  const s1AfterDrop = candidates.step1;
  const s1Norm = s1AfterDrop?.value != null ? normalizeTimestampString(s1AfterDrop.value) : null;
  /** Ordering floor for steps 2/3/5: must be after leaving the winery (GPS step 1) and after business anchor. When both exist, use the **earlier** instant so a late VWork tap (e.g. 10:15) does not wipe a valid vineyard ENTER (e.g. 10:09) that is still after morning winery EXIT (09:47). */
  const floorFor235 =
    anchor != null && s1Norm != null ? minTimestampString(anchor, s1Norm) ?? s1Norm : anchor ?? s1Norm;
  if (floorFor235 != null) {
    for (const key of ['step2', 'step3', 'step5'] as const) {
      const c = candidates[key];
      if (c?.value == null) continue;
      const n = normalizeTimestampString(c.value);
      if (n != null && n <= floorFor235) candidates[key] = null;
    }
  }
  if (vworkEnd != null) {
    const s2 = candidates.step2?.value != null ? normalizeTimestampString(candidates.step2!.value) : null;
    if (s2 != null && s2 >= vworkEnd) {
      candidates.step2 = null;
      candidates.step3 = null;
    } else {
      const s3 = candidates.step3?.value != null ? normalizeTimestampString(candidates.step3!.value) : null;
      const ceiling = step3Ceiling ?? vworkEnd;
      if (s3 != null && s3 >= ceiling) {
        candidates.step3 = null;
      }
    }
  }
  const s1ForMax = candidates.step1?.value != null ? normalizeTimestampString(candidates.step1.value) : null;
  const step1ForStep4Max =
    anchor != null && s1ForMax != null ? minTimestampString(anchor, s1ForMax) ?? s1ForMax : anchor ?? s1ForMax;
  const rawPosAfter =
    positionAfter != null && String(positionAfter).trim() !== ''
      ? normalizeTimestampString(positionAfter) ?? String(positionAfter).trim().slice(0, 19)
      : null;
  const step4Floor =
    candidates.step2 == null && candidates.step3 == null
      ? maxTimestampString(step1ForStep4Max, rawPosAfter)
      : maxTimestampString(step1ForStep4Max, candidates.step2?.value, candidates.step3?.value);
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
 * Winery steps 4–5: first mapped Winery ENTER after a lower bound, then first Winery EXIT after step 4.
 * Lower bound: if **both** step2 and step3 values are non-null, max(step1, step2, step3); if **both** are null,
 * max(step1, positionAfter) only. Callers should pass step2/step3 **after** {@link pruneVineyardGpsForJobEnd} so times that
 * guardrails will drop (e.g. vineyard ENTER after VWork job end) do not raise the floor.
 * Step 5: upper bound min(positionBefore, VWork job end + step5ExtendWineryExitMinutes) when applicable.
 * Updates debug.winery.step4 / step5 and debug.winery.step5SearchWindow (Inspect-only audit).
 */
function assignWineryStep5SearchWindowDebug(
  debug: DerivedStepsDebug,
  args: {
    wineryFenceIds: number[];
    step4Value: string | null;
    vworkStep5: string | null;
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
    if (pbNorm === jPlus) upperExclusiveSource = 'position_before_equals_job_end_plus_extend';
    else if (pbNorm < jPlus) upperExclusiveSource = 'position_before_tighter';
    else upperExclusiveSource = 'job_end_plus_extend_tighter';
  } else if (winEnd != null && jPlus != null && pbNorm == null) {
    upperExclusiveSource = args.step5ExtMin > 0 ? 'only_job_end_plus_extend' : 'only_vwork_step5_no_extend_zero';
  } else if (winEnd != null && pbNorm != null) {
    upperExclusiveSource = 'only_position_before';
  }

  const lowerEx = args.step4Value != null ? normalizeTimestampString(args.step4Value) : null;
  const lines: string[] = [];
  lines.push(
    'Step 5 GPS fetch: first winery EXIT on mapped delivery_winery fences where (strict) step4_ENTER < t < upperExclusive.'
  );
  if (args.fetchSkippedReason != null) {
    lines.push(`Fetch skipped: ${args.fetchSkippedReason.replace(/_/g, ' ')}.`);
  } else {
    lines.push(`X (lowerExclusive) = GPS step 4 winery ENTER: ${lowerEx ?? '—'}.`);
    lines.push(`Y (upperExclusive) = min(job window end, job end + Step5Extend): ${winEnd ?? '—'}.`);
    lines.push(`Job end for step-5 rule (step_5_completed_at ?? actual_end_time): ${args.vworkStep5 ?? '—'}.`);
    lines.push(`positionBefore from request/options: ${pbNorm ?? '—'}.`);
    lines.push(`Step5ExtendWineryExit minutes: ${args.step5ExtMin}.`);
    lines.push(`jobEnd + extend (candidate for min): ${jPlus ?? '—'}.`);
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
  step3Value: string | null
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
  const step5WindowEndEarly =
    vworkStep5SearchEndEarly != null
      ? positionBefore != null && positionBefore < vworkStep5SearchEndEarly
        ? positionBefore
        : vworkStep5SearchEndEarly
      : positionBefore;

  if (wineryFenceIds.length === 0) {
    assignWineryStep5SearchWindowDebug(debug, {
      wineryFenceIds,
      step4Value: null,
      vworkStep5: vworkStep5Early,
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
  const step5ExtMin = options.step5ExtendWineryExitMinutes ?? STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;
  const vworkStep5SearchEnd =
    vworkStep5 != null && step5ExtMin > 0
      ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkStep5, step5ExtMin))
      : vworkStep5;
  const step5WindowEnd =
    vworkStep5SearchEnd != null
      ? positionBefore != null && positionBefore < vworkStep5SearchEnd
        ? positionBefore
        : vworkStep5SearchEnd
      : positionBefore;
  let step5: GpsStepCandidate | null = null;
  let fetchSkip: WineryStep5SearchWindowDebug['fetchSkippedReason'] = null;
  let exitRan = false;

  if (step4Value == null) {
    fetchSkip = 'no_gps_step4_enter';
  } else if (vworkStep5 == null) {
    fetchSkip = 'no_vwork_job_end_for_step5_rule';
  } else if (step5WindowEnd == null) {
    fetchSkip = 'no_step5_upper_bound';
  } else if (vworkStep5 <= step4Value) {
    fetchSkip = 'vwork_step5_not_after_step4_enter';
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
  const vineyardName = job.vineyard_name ? String(job.vineyard_name).trim() : '';
  const deliveryWinery = job.delivery_winery ? String(job.delivery_winery).trim() : '';
  let step2Value: string | null = null;
  let step3Value: string | null = null;
  /** Fence set used for step 2/3 (primary vineyard or VineSR1 fallback) — for refine-after-exit. */
  let vineyardFenceIdsForRefine: number[] | null = null;

  if (vineyardName) {
    const { fenceIds: vineyardFenceIds, debug: vineyardDebug } = await getFenceIdsForVworkNameWithDebug('Vineyard', vineyardName);
    Object.assign(debug.vineyard, vineyardDebug);
    if (vineyardFenceIds.length > 0) {
      vineyardFenceIdsForRefine = vineyardFenceIds;
      const step2Result = await getFirstTrackingInWindowWithDebug(truckId, trackingWindowAfter, vineyardBefore, vineyardFenceIds, 'ENTER');
      step2Value = step2Result.value;
      debug.vineyard.step2 = step2Result.debug;
      if (step2Result.value != null) candidates.step2 = { value: step2Result.value, trackingId: step2Result.trackingId };
      // Step 3 (Depart Vineyard): first Vineyard EXIT after step 2 (Enter Vineyard), so we don't pick an earlier exit
      const step3After = step2Value ?? trackingWindowAfter;
      const step3Result = await getFirstTrackingInWindowWithDebug(truckId, step3After, vineyardBefore, vineyardFenceIds, 'EXIT');
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
        const sr1Step2 = await getFirstTrackingInWindowWithDebug(
          truckId,
          trackingWindowAfter,
          vineyardBefore,
          sr1FenceIds,
          'ENTER'
        );
        step2Value = sr1Step2.value;
        debug.vineyard.step2 = sr1Step2.debug;
        if (sr1Step2.value != null) {
          candidates.step2 = { value: sr1Step2.value, trackingId: sr1Step2.trackingId };
        }
        const sr1Step3After = step2Value ?? trackingWindowAfter;
        const sr1Step3 = await getFirstTrackingInWindowWithDebug(
          truckId,
          sr1Step3After,
          vineyardBefore,
          sr1FenceIds,
          'EXIT'
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
    if (wineryFenceIds.length > 0) {
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
        wineryFenceIds
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
          const r2AfterExit = await getFirstTrackingInWindowWithDebug(
            truckId,
            lowerEnter,
            vineyardBefore,
            vineyardFenceIdsForRefine,
            'ENTER'
          );
          const entNorm =
            r2AfterExit.value != null ? normalizeTimestampString(r2AfterExit.value) : null;
          if (entNorm != null && entNorm > exitNorm && r2AfterExit.value != null) {
            candidates.step2 = { value: r2AfterExit.value, trackingId: r2AfterExit.trackingId };
            step2Value = r2AfterExit.value;
            debug.vineyard.step2 = r2AfterExit.debug;
            const step3AfterRefine = step2Value ?? lowerEnter;
            const r3After = await getFirstTrackingInWindowWithDebug(
              truckId,
              step3AfterRefine,
              vineyardBefore,
              vineyardFenceIdsForRefine,
              'EXIT'
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
              wineryFenceIds
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
        wineryFenceIds,
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
      const { step4, step5 } = await fetchWineryStep4And5ForValues(
        job,
        options,
        debug,
        truckId,
        wineryFenceIds,
        step1ValueForStep4,
        step23ForStep4Floor.step2,
        step23ForStep4Floor.step3
      );
      if (step4 != null) candidates.step4 = step4;
      if (step5 != null) candidates.step5 = step5;
    } else {
      const vworkStep5NoFence =
        (job.step_5_completed_at ?? job.actual_end_time) != null
          ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
          : null;
      const step5ExtNoFence = options.step5ExtendWineryExitMinutes ?? STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;
      const vworkSearchEndNoFence =
        vworkStep5NoFence != null && step5ExtNoFence > 0
          ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkStep5NoFence, step5ExtNoFence))
          : vworkStep5NoFence;
      const step5WinEndNoFence =
        vworkSearchEndNoFence != null
          ? positionBefore != null && positionBefore < vworkSearchEndNoFence
            ? positionBefore
            : vworkSearchEndNoFence
          : positionBefore;
      assignWineryStep5SearchWindowDebug(debug, {
        wineryFenceIds,
        step4Value: null,
        vworkStep5: vworkStep5NoFence,
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
    const vworkSearchNd =
      vworkStep5NoDel != null && step5ExtNd > 0
        ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkStep5NoDel, step5ExtNd))
        : vworkStep5NoDel;
    const step5WinNd =
      vworkSearchNd != null
        ? positionBefore != null && positionBefore < vworkSearchNd
          ? positionBefore
          : vworkSearchNd
        : positionBefore;
    assignWineryStep5SearchWindowDebug(debug, {
      wineryFenceIds: [],
      step4Value: null,
      vworkStep5: vworkStep5NoDel,
      step5ExtMin: step5ExtNd,
      vworkStep5SearchEnd: vworkSearchNd,
      step5WindowEnd: step5WinNd != null ? normalizeTimestampString(step5WinNd) : null,
      positionBefore,
      step5ExitQueryRan: false,
      fetchSkippedReason: 'no_delivery_winery_on_job',
    });
  }
  applyGpsGuardrails(candidates, job, jobEndCeilingBufferMinutes, positionAfter);
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
    if (gpsNorm != null) {
      if (gpsNorm < vworkStep5) {
        result.step5Gps = candidates.step5.value;
        result.step5TrackingId = candidates.step5.trackingId;
      } else if (step5ExtendWineryExitMinutes > 0) {
        const upper = normalizeTimestampString(addMinutesToTimestampAsNZ(vworkStep5, step5ExtendWineryExitMinutes));
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
  acceptedStep5Gps: string | null
): void {
  const vworkStep5 =
    (job.step_5_completed_at ?? job.actual_end_time) != null
      ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
      : null;
  const candNorm =
    step5Candidate?.value != null ? normalizeTimestampString(step5Candidate.value) : null;
  const upperExclusive =
    vworkStep5 != null && step5ExtendWineryExitMinutes > 0
      ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkStep5, step5ExtendWineryExitMinutes))
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
      summaryLine = `GPS step 5 kept: EXIT ${candNorm} is on/after job end ${vworkStep5} but strictly before exclusive upper ${upperExclusive ?? 'n/a'} (Step5Extend ${step5ExtendWineryExitMinutes} min).`;
    }
  } else if (candNorm < vworkStep5) {
    outcome = 'no_candidate_after_guardrails';
    summaryLine = 'Inconsistent: candidate before job end was not promoted — check logic.';
  } else if (step5ExtendWineryExitMinutes <= 0) {
    outcome = 'rejected_extend_disabled_and_exit_not_before_job_end';
    summaryLine = `EXIT ${candNorm} is at/after job end ${vworkStep5} and Step5ExtendWineryExit is ${step5ExtendWineryExitMinutes} — Part 2 rejects.`;
  } else if (upperExclusive != null && candNorm >= upperExclusive) {
    outcome = 'rejected_exit_at_or_after_job_end_outside_extend';
    summaryLine = `EXIT ${candNorm} is at/after exclusive upper ${upperExclusive} (job end + Step5Extend) — Part 2 rejects (outside “complete before leave” band).`;
  } else {
    outcome = 'rejected_exit_at_or_after_job_end_outside_extend';
    summaryLine = `EXIT ${candNorm} not accepted as GPS step 5 vs job end ${vworkStep5} / extend ${step5ExtendWineryExitMinutes} min.`;
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
        step23ForStep4Floor.step3
      );
      if (step4 != null) candidates.step4 = step4;
      if (step5 != null) candidates.step5 = step5;
    } else {
      const vworkStep5NoFence =
        (job.step_5_completed_at ?? job.actual_end_time) != null
          ? normalizeTimestampString((job.step_5_completed_at ?? job.actual_end_time) as string | Date)
          : null;
      const vworkSearchEndNoFence =
        vworkStep5NoFence != null && step5Ext > 0
          ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkStep5NoFence, step5Ext))
          : vworkStep5NoFence;
      const step5WinEndNoFence =
        vworkSearchEndNoFence != null
          ? options.positionBefore != null &&
            String(options.positionBefore).trim() !== '' &&
            options.positionBefore < vworkSearchEndNoFence
            ? options.positionBefore
            : vworkSearchEndNoFence
          : options.positionBefore;
      assignWineryStep5SearchWindowDebug(debug, {
        wineryFenceIds,
        step4Value: null,
        vworkStep5: vworkStep5NoFence,
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
    const vworkSearchNd =
      vworkStep5NoDel != null && step5Ext > 0
        ? normalizeTimestampString(addMinutesToTimestampAsNZ(vworkStep5NoDel, step5Ext))
        : vworkStep5NoDel;
    const step5WinNd =
      vworkSearchNd != null
        ? options.positionBefore != null &&
          String(options.positionBefore).trim() !== '' &&
          options.positionBefore < vworkSearchNd
          ? options.positionBefore
          : vworkSearchNd
        : options.positionBefore;
    assignWineryStep5SearchWindowDebug(debug, {
      wineryFenceIds: [],
      step4Value: null,
      vworkStep5: vworkStep5NoDel,
      step5ExtMin: step5Ext,
      vworkStep5SearchEnd: vworkSearchNd,
      step5WindowEnd: step5WinNd != null ? normalizeTimestampString(step5WinNd) : null,
      positionBefore: options.positionBefore ?? null,
      step5ExitQueryRan: false,
      fetchSkippedReason: 'no_delivery_winery_on_job',
    });
  }
  const buf = options.jobEndCeilingBufferMinutes ?? JOB_END_CEILING_BUFFER_DEFAULT_MINUTES;
  applyGpsGuardrails(candidates, job, buf, options.positionAfter);
  const decided = decideFinalSteps(candidates, job, step5Ext);
  attachStep5DecideDebug(debug, candidates.step5, job, step5Ext, decided.step5Gps);
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
  attachStep5DecideDebug(debug, candidates.step5, job, step5Ext, gpsOnly.step5Gps);
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

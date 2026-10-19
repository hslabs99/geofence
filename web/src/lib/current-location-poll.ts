import { execute, getClient, query } from '@/lib/db';
import { ensureCurrentLocationsSchema, fillCurrentLocationGpsTimeNz } from '@/lib/ensure-current-locations-schema';
import { getStepsPlusSettings } from '@/lib/gps-fence-settings';
import {
  clearTokenCache,
  getAccessToken,
  getCurrentLocations,
  getTracksolidTokenStatus,
  listDevices,
  tracksolidNodeKey,
  TracksolidApiError,
  type CurrentLocationFix,
  type TracksolidTokenStatus,
} from '@/lib/tracksolid';
import { tracksolidCallBudget, type TracksolidCallBudget } from '@/lib/tracksolid-call-budget';

const LOGTYPE = 'current_location_poll';
const SETTINGS_TYPE = 'CurrentLocationPoll';

export type CurrentLocationSettings = {
  pollIntervalSeconds: number;
  flushOlderThanHours: number;
  flushEveryMinutes: number;
  /** How often the driver phone reads the latest stored location. */
  driverAppPollSeconds: number;
  /** Ask before recording an arrival when the truck is farther than this from the fence. */
  confirmAwayMeters: number;
};

const DEFAULT_SETTINGS: CurrentLocationSettings = {
  pollIntervalSeconds: 30,
  flushOlderThanHours: 72,
  flushEveryMinutes: 60,
  driverAppPollSeconds: 30,
  confirmAwayMeters: 100,
};
/** TrackSolid jimi.device.location.get accepts up to 100 IMEIs per call. The account is ~53, so one call covers every group. */
const IMEIS_PER_CALL = 100;

/** YYYY-MM-DD HH:MM:SS from an API gpsTime, for an order comparison only. The stored value stays the original string. */
function gpsTimeKey(value: string | null | undefined): string | null {
  if (value == null) return null;
  const match = String(value).trim().match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})/);
  if (!match) return null;
  return `${match[1]}-${match[2]}-${match[3]} ${match[4]}:${match[5]}:${match[6]}`;
}

/** True when this GPS time is later than the newest one already stored for the truck. */
function gpsTimeIsNewer(incoming: string | null | undefined, stored: string | null | undefined): boolean {
  const next = gpsTimeKey(incoming);
  const previous = gpsTimeKey(stored);
  if (!previous) return next != null;
  if (!next) return false;
  return next > previous;
}

/** Server clock at the moment of the API call. Not derived from gpsTime. */
function pollClock(now = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

/** Same node as GPS import: HK unless TRACKSOLID_ENDPOINT is set. */
function tracksolidEndpoint(): string {
  const envKey = process.env.TRACKSOLID_ENDPOINT?.trim().toLowerCase();
  if (envKey === 'global' || envKey === 'hk' || envKey === 'eu' || envKey === 'us') return envKey;
  return 'hk';
}

type PollState = {
  /** Previous build stored the poll timer here. Cleared on load so two timers cannot run. */
  timer?: ReturnType<typeof setInterval> | null;
  pollTimer: ReturnType<typeof setInterval> | null;
  flushTimer: ReturnType<typeof setInterval> | null;
  running: boolean;
  inFlight: boolean;
  flushing: boolean;
  settings: CurrentLocationSettings;
  lastPolltime: string | null;
  lastSummary: string | null;
  lastError: string | null;
  lastFlushAt: string | null;
  lastFlushDeleted: number | null;
  nextPollAtMs: number | null;
  /** Bumped when the timer callback changes, so a running poll picks up the countdown. */
  generation: number;
  /** Set when the poll timer is armed or ticks. A stalled timer is restarted from this. */
  heartbeatMs: number;
};

const globalKey = '__geodataCurrentLocationPoll';

function state(): PollState {
  const g = globalThis as typeof globalThis & { [globalKey]?: PollState };
  if (!g[globalKey]) {
    g[globalKey] = {
      pollTimer: null,
      flushTimer: null,
      running: false,
      inFlight: false,
      flushing: false,
      settings: { ...DEFAULT_SETTINGS },
      lastPolltime: null,
      lastSummary: null,
      lastError: null,
      lastFlushAt: null,
      lastFlushDeleted: null,
      nextPollAtMs: null,
      generation: 0,
      heartbeatMs: 0,
    };
  }
  const s = g[globalKey];
  if (s.timer) {
    clearInterval(s.timer);
    s.timer = null;
    s.running = false;
  }
  if (!s.settings || s.settings.pollIntervalSeconds == null || s.settings.driverAppPollSeconds == null || s.settings.confirmAwayMeters == null) {
    s.settings = { ...DEFAULT_SETTINGS, ...s.settings };
  }
  if (s.pollTimer === undefined) s.pollTimer = null;
  if (s.flushTimer === undefined) s.flushTimer = null;
  if (s.flushing === undefined) s.flushing = false;
  if (s.nextPollAtMs === undefined) s.nextPollAtMs = null;
  if (s.heartbeatMs == null) s.heartbeatMs = 0;
  if (s.running && s.pollTimer) {
    const intervalMs = Math.max(5, s.settings.pollIntervalSeconds) * 1000;
    const stalled = s.heartbeatMs === 0 || Date.now() - s.heartbeatMs > intervalMs + 15_000;
    if (stalled) armTimers(s);
    else if (s.nextPollAtMs == null) markNextPoll(s);
  }
  return s;
}

function markNextPoll(s: PollState): void {
  s.nextPollAtMs = Date.now() + s.settings.pollIntervalSeconds * 1000;
}

function clampInt(value: unknown, min: number, max: number, fallback: number): number {
  const n = typeof value === 'number' ? value : parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(n)));
}

function normalizeSettings(input?: Partial<CurrentLocationSettings> | null): CurrentLocationSettings {
  return {
    pollIntervalSeconds: clampInt(input?.pollIntervalSeconds, 5, 3600, DEFAULT_SETTINGS.pollIntervalSeconds),
    flushOlderThanHours: clampInt(input?.flushOlderThanHours, 1, 8760, DEFAULT_SETTINGS.flushOlderThanHours),
    flushEveryMinutes: clampInt(input?.flushEveryMinutes, 1, 1440, DEFAULT_SETTINGS.flushEveryMinutes),
    driverAppPollSeconds: clampInt(input?.driverAppPollSeconds, 5, 3600, DEFAULT_SETTINGS.driverAppPollSeconds),
    confirmAwayMeters: clampInt(input?.confirmAwayMeters, 1, 100000, DEFAULT_SETTINGS.confirmAwayMeters),
  };
}

async function readSettings(): Promise<CurrentLocationSettings> {
  const rows = await query<{ settingname: string; settingvalue: string | null }>(
    `SELECT settingname, settingvalue
     FROM tbl_settings
     WHERE type = $1
       AND settingname IN ('pollIntervalSeconds', 'pollIntervalMinutes', 'flushOlderThanHours', 'flushEveryMinutes', 'driverAppPollSeconds', 'confirmAwayMeters')`,
    [SETTINGS_TYPE],
  );
  const found: Partial<CurrentLocationSettings> = {};
  for (const row of rows) {
    if (row.settingname === 'pollIntervalSeconds') found.pollIntervalSeconds = Number(row.settingvalue);
    if (row.settingname === 'flushOlderThanHours') found.flushOlderThanHours = Number(row.settingvalue);
    if (row.settingname === 'flushEveryMinutes') found.flushEveryMinutes = Number(row.settingvalue);
    if (row.settingname === 'driverAppPollSeconds') found.driverAppPollSeconds = Number(row.settingvalue);
    if (row.settingname === 'confirmAwayMeters') found.confirmAwayMeters = Number(row.settingvalue);
  }
  return normalizeSettings({ ...DEFAULT_SETTINGS, ...found });
}

async function writeSettings(settings: CurrentLocationSettings): Promise<void> {
  const client = await getClient();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM tbl_settings WHERE type = $1 AND settingname = $2', [
      SETTINGS_TYPE,
      'pollIntervalMinutes',
    ]);
    for (const [name, value] of Object.entries(settings)) {
      await client.query('DELETE FROM tbl_settings WHERE type = $1 AND settingname = $2', [SETTINGS_TYPE, name]);
      await client.query(
        'INSERT INTO tbl_settings (type, settingname, settingvalue) VALUES ($1, $2, $3)',
        [SETTINGS_TYPE, name, String(value)],
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

function clearTimers(s: PollState): void {
  if (s.timer) {
    clearInterval(s.timer);
    s.timer = null;
  }
  if (s.pollTimer) {
    clearInterval(s.pollTimer);
    s.pollTimer = null;
  }
  if (s.flushTimer) {
    clearInterval(s.flushTimer);
    s.flushTimer = null;
  }
}

function armTimers(s: PollState): void {
  clearTimers(s);
  s.heartbeatMs = Date.now();
  markNextPoll(s);
  s.pollTimer = setInterval(() => {
    s.heartbeatMs = Date.now();
    markNextPoll(s);
    void pollCurrentLocationsOnce();
  }, s.settings.pollIntervalSeconds * 1000);
  s.flushTimer = setInterval(() => {
    void flushOldCurrentLocations();
  }, s.settings.flushEveryMinutes * 60 * 1000);
}

export async function loadCurrentLocationSettings(): Promise<CurrentLocationSettings> {
  const settings = await readSettings();
  state().settings = settings;
  return settings;
}

/** Saves the poll interval and flush settings. Restarts the timers when the service is already running. */
export async function saveCurrentLocationSettings(
  input: Partial<CurrentLocationSettings>,
): Promise<CurrentLocationSettings> {
  const settings = normalizeSettings({ ...state().settings, ...input });
  await writeSettings(settings);
  const s = state();
  s.settings = settings;
  if (s.running) armTimers(s);
  return settings;
}

/** Removes every current-location row so the next poll starts from an empty table. */
export async function emptyCurrentLocations(): Promise<{ deleted: number }> {
  await ensureCurrentLocationsSchema();
  const deleted = await execute('DELETE FROM tbl_current_locations');
  await writeLog('empty-table', { at: pollClock(), deleted });
  return { deleted };
}

/** Deletes location rows older than the configured number of hours. */
export async function flushOldCurrentLocations(): Promise<{ deleted: number }> {
  const s = state();
  if (s.flushing) return { deleted: 0 };
  s.flushing = true;
  const at = pollClock();
  try {
    await ensureCurrentLocationsSchema();
    const cutoff = pollClock(new Date(Date.now() - s.settings.flushOlderThanHours * 60 * 60 * 1000));
    const deleted = await execute(
      `DELETE FROM tbl_current_locations WHERE polltime IS NULL OR polltime < $1`,
      [cutoff],
    );
    s.lastFlushAt = at;
    s.lastFlushDeleted = deleted;
    await writeLog('flush', {
      at,
      olderThanHours: s.settings.flushOlderThanHours,
      cutoff,
      deleted,
    });
    return { deleted };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await writeLog('flush-error', { at, error: message });
    return { deleted: 0 };
  } finally {
    s.flushing = false;
  }
}

export type CurrentLocationRow = {
  id: number;
  truck_id: string | null;
  location: string | null;
  gpstime: string | null;
  gpstime_nz: string | null;
  polltime: string | null;
  coordinates: string | null;
  fencename: string | null;
  softfencename: string | null;
  imei: string | null;
};

type FenceMatch = { fencename: string | null; softfencename: string | null };

/** Written when the geofence check ran and this column matched no fence. */
const OUTSIDE_FENCE = 'outside fence';

function fenceOrOutside(name: string | null | undefined): string {
  const trimmed = name?.trim();
  return trimmed ? trimmed : OUTSIDE_FENCE;
}

/**
 * Inside tbl_geofences → fencename. Inside the GPS+ buffer but outside the fence → softfencename.
 * Buffer metres come from tbl_settings GPSplusGrowth (same value Steps+ uses).
 */
async function fenceNamesForPoints(
  points: { key: string; lat: number; lng: number }[],
  bufferMeters: number,
): Promise<Map<string, FenceMatch>> {
  const found = new Map<string, FenceMatch>();
  if (points.length === 0) return found;
  const rows = await query<{ key: string; fencename: string | null; softfencename: string | null }>(
    `WITH pts AS (
       SELECT u.key, u.lat, u.lng,
         ST_Transform(ST_SetSRID(ST_MakePoint(u.lng, u.lat), 4326), 3857) AS p
       FROM unnest($1::text[], $2::float8[], $3::float8[]) AS u(key, lat, lng)
       WHERE u.lat IS NOT NULL AND u.lng IS NOT NULL
     )
     SELECT pts.key,
       (
         SELECT string_agg(sub.fence_name, ', ' ORDER BY sub.fence_name)
         FROM (
           SELECT DISTINCT g.fence_name
           FROM tbl_geofences g
           WHERE g.geom IS NOT NULL
             AND g.fence_name IS NOT NULL
             AND ST_Within(pts.p, ST_Transform(ST_Force2D(g.geom), 3857))
         ) sub
       ) AS fencename,
       (
         SELECT string_agg(sub.fence_name, ', ' ORDER BY sub.fence_name)
         FROM (
           SELECT DISTINCT g.fence_name
           FROM tbl_geofences g
           WHERE g.geom IS NOT NULL
             AND g.fence_name IS NOT NULL
             AND ST_Within(pts.p, ST_Buffer(ST_Transform(ST_Force2D(g.geom), 3857), $4::numeric))
             AND NOT ST_Within(pts.p, ST_Transform(ST_Force2D(g.geom), 3857))
         ) sub
       ) AS softfencename
     FROM pts`,
    [points.map((p) => p.key), points.map((p) => p.lat), points.map((p) => p.lng), bufferMeters],
  );
  for (const row of rows) {
    found.set(row.key, {
      fencename: row.fencename?.trim() || null,
      softfencename: row.softfencename?.trim() || null,
    });
  }
  return found;
}

async function writeLog(stage: string, details: Record<string, unknown>): Promise<void> {
  try {
    await execute(
      'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4)',
      [LOGTYPE, 'all', stage, JSON.stringify(details)],
    );
  } catch (err) {
    console.error('[current-location-poll] log failed', err);
  }
}

/**
 * The GPS time did not move, so no new row is written. The fetch clock on the
 * latest row is moved to this poll so the driver app can see we just checked.
 */
async function stampFetchedTime(truckIds: string[], polltime: string): Promise<void> {
  const keys = [...new Set(truckIds.map((id) => id.trim().toLowerCase()).filter(Boolean))];
  if (keys.length === 0) return;
  await execute(
    `UPDATE tbl_current_locations AS loc
     SET polltime = $1
     WHERE loc.id IN (
       SELECT DISTINCT ON (lower(btrim(truck_id))) id
       FROM tbl_current_locations
       WHERE lower(btrim(truck_id)) = ANY($2::text[])
       ORDER BY lower(btrim(truck_id)), id DESC
     )`,
    [polltime, keys],
  );
}

/** One poll: every truck on the account, one jimi.device.location.get call, then one inserted row per truck. */
export async function pollCurrentLocationsOnce(): Promise<{ inserted: number; summary: string }> {
  const s = state();
  if (s.inFlight) {
    return { inserted: 0, summary: 'Skipped: previous poll still running' };
  }
  s.inFlight = true;
  const polltime = pollClock();
  s.lastPolltime = polltime;
  try {
    await ensureCurrentLocationsSchema();
    const endpoint = tracksolidEndpoint();
    const token = { value: (await getAccessToken(endpoint)).token, refreshed: false };
    const listed = await withToken(endpoint, token, (accessToken) => listDevices(accessToken, endpoint));
    const devices = listed.devices.filter((d) => d.imei.trim() !== '');
    const groupsFound = [
      ...new Set(devices.map((d) => (d.deviceGroup ?? '').trim()).filter((g) => g !== '')),
    ];

    if (devices.length === 0) {
      const summary = 'No trucks on the TrackSolid account.';
      s.lastSummary = summary;
      s.lastError = null;
      await writeLog('empty', { polltime, endpoint, deviceCount: 0, groupsFound, summary });
      return { inserted: 0, summary };
    }

    const imeis = devices.map((d) => d.imei);
    const nameByImei = new Map(devices.map((d) => [d.imei, (d.deviceName ?? '').trim() || d.imei]));
    const groupByImei = new Map(devices.map((d) => [d.imei, (d.deviceGroup ?? '').trim() || null]));
    const fixes: CurrentLocationFix[] = [];
    let httpStatus = 0;
    let rawKeys: string[] = [];
    const calls = Math.ceil(imeis.length / IMEIS_PER_CALL);
    for (let i = 0; i < imeis.length; i += IMEIS_PER_CALL) {
      const located = await withToken(endpoint, token, (accessToken) =>
        getCurrentLocations(accessToken, imeis.slice(i, i + IMEIS_PER_CALL), endpoint),
      );
      httpStatus = located.debug.httpStatus;
      fixes.push(...located.fixes);
      if (rawKeys.length === 0) {
        const rawResult = (located.debug.fullResponse as { result?: unknown } | undefined)?.result;
        const rawRows = Array.isArray(rawResult) ? (rawResult as Record<string, unknown>[]) : [];
        rawKeys = rawRows[0] ? Object.keys(rawRows[0]) : [];
      }
    }

    const storedRows = await query<{ truck_id: string | null; gpstime: string | null }>(
      `SELECT DISTINCT ON (lower(truck_id)) truck_id, gpstime
       FROM tbl_current_locations
       ORDER BY lower(truck_id), id DESC`,
    );
    const storedGps = new Map<string, string | null>();
    for (const row of storedRows) {
      const key = (row.truck_id ?? '').trim().toLowerCase();
      if (key) storedGps.set(key, row.gpstime);
    }

    let inserted = 0;
    let skipped = 0;
    const unchangedTrucks: string[] = [];
    const pending: {
      truckId: string;
      imei: string | null;
      gpsTime: string | null;
      location: string | null;
      coordinates: string | null;
      lat: number | null;
      lng: number | null;
      group: string | null;
    }[] = [];
    for (const fix of fixes) {
      const truckId = (fix.deviceName || nameByImei.get(fix.imei) || fix.imei).trim() || fix.imei;
      const previous = storedGps.get(truckId.toLowerCase());
      if (storedGps.has(truckId.toLowerCase()) && !gpsTimeIsNewer(fix.gpsTime, previous)) {
        skipped += 1;
        unchangedTrucks.push(truckId);
        continue;
      }
      pending.push({
        truckId,
        imei: fix.imei.trim() || null,
        gpsTime: fix.gpsTime || null,
        location: fix.location,
        coordinates: fix.lat != null && fix.lng != null ? `${fix.lat},${fix.lng}` : null,
        lat: fix.lat,
        lng: fix.lng,
        group: groupByImei.get(fix.imei) ?? null,
      });
    }

    const { bufferMeters } = await getStepsPlusSettings();
    let fenceMatches = new Map<string, FenceMatch>();
    let fenceLookupOk = true;
    const fencePoints = pending.filter((row) => row.lat != null && row.lng != null) as {
      truckId: string;
      lat: number;
      lng: number;
    }[];
    if (fencePoints.length > 0) {
      try {
        fenceMatches = await fenceNamesForPoints(
          fencePoints.map((row) => ({ key: row.truckId, lat: row.lat, lng: row.lng })),
          bufferMeters,
        );
      } catch (err) {
        fenceLookupOk = false;
        const message = err instanceof Error ? err.message : String(err);
        await writeLog('fence-error', { polltime, bufferMeters, error: message });
      }
    }

    const trucks: Record<string, unknown>[] = [];
    for (const row of pending) {
      const match = fenceMatches.get(row.truckId);
      const fencename = fenceLookupOk ? fenceOrOutside(match?.fencename) : null;
      const softfencename = fenceLookupOk ? fenceOrOutside(match?.softfencename) : null;
      await execute(
        `INSERT INTO tbl_current_locations (truck_id, imei, location, gpstime, polltime, coordinates, fencename, softfencename)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [row.truckId, row.imei, row.location, row.gpsTime, polltime, row.coordinates, fencename, softfencename],
      );
      inserted += 1;
      storedGps.set(row.truckId.toLowerCase(), row.gpsTime);
      trucks.push({
        truck_id: row.truckId,
        group: row.group,
        gpstime: row.gpsTime,
        location: row.location,
        fencename,
        softfencename,
      });
    }

    if (inserted > 0) await fillCurrentLocationGpsTimeNz();
    if (unchangedTrucks.length > 0) await stampFetchedTime(unchangedTrucks, polltime);

    const summary = `Polled ${devices.length} truck(s) across ${groupsFound.length} group(s) in ${calls} location call(s), inserted ${inserted} newer row(s), refreshed ${unchangedTrucks.length} unchanged`;
    s.lastSummary = summary;
    s.lastError = null;
    await writeLog('ok', {
      polltime,
      endpoint,
      method: 'jimi.device.location.get',
      groups: groupsFound,
      deviceCount: devices.length,
      fetched: fixes.length,
      locationCalls: calls,
      inserted,
      written: inserted,
      skipped,
      bufferMeters,
      rawKeys,
      trucks,
      httpStatus,
    });
    return { inserted, summary };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const debug = err instanceof TracksolidApiError ? err.debug.method : undefined;
    s.lastError = message;
    s.lastSummary = null;
    await writeLog('error', { polltime, error: message, method: debug ?? null });
    return { inserted: 0, summary: message };
  } finally {
    s.inFlight = false;
  }
}

export async function startCurrentLocationPoll(
  input?: Partial<CurrentLocationSettings>,
): Promise<{ running: boolean; settings: CurrentLocationSettings }> {
  const s = state();
  if (input) await saveCurrentLocationSettings(input);
  else s.settings = await readSettings();
  if (s.running && s.pollTimer) return { running: true, settings: s.settings };
  s.running = true;
  armTimers(s);
  void pollCurrentLocationsOnce();
  void flushOldCurrentLocations();
  return { running: true, settings: s.settings };
}

export function stopCurrentLocationPoll(): { running: boolean; settings: CurrentLocationSettings } {
  const s = state();
  clearTimers(s);
  s.running = false;
  s.nextPollAtMs = null;
  return { running: false, settings: s.settings };
}

function isRejectedToken(err: unknown): boolean {
  return err instanceof TracksolidApiError && err.code === 1004;
}

/** After one forced refresh, further rejections wait before another token request. */
let lastTokenRefreshMs = 0;
const TOKEN_REFRESH_GAP_MS = 10 * 60 * 1000;

/** One TrackSolid call. A rejected token is refreshed once for the whole poll, then the call is tried again. */
async function withToken<T>(
  endpoint: string,
  token: { value: string; refreshed: boolean },
  run: (accessToken: string) => Promise<T>,
): Promise<T> {
  try {
    return await run(token.value);
  } catch (err) {
    if (!isRejectedToken(err) || token.refreshed) throw err;
    if (Date.now() - lastTokenRefreshMs < TOKEN_REFRESH_GAP_MS) throw err;
    token.refreshed = true;
    lastTokenRefreshMs = Date.now();
    await clearTokenCache(endpoint);
    const fresh = await getAccessToken(endpoint, { skipCache: true });
    token.value = fresh.token;
    await writeLog('token-refresh', {
      at: pollClock(),
      reason: 'TrackSolid rejected the token (1004). One new token was requested.',
    });
    return run(token.value);
  }
}

export function currentLocationPollStatus(): {
  running: boolean;
  intervalMs: number;
  settings: CurrentLocationSettings;
  lastPolltime: string | null;
  lastSummary: string | null;
  lastError: string | null;
  lastFlushAt: string | null;
  lastFlushDeleted: number | null;
  nextPollAtMs: number | null;
  serverNowMs: number;
} {
  const s = state();
  const running = s.running && s.pollTimer != null;
  return {
    running,
    intervalMs: s.settings.pollIntervalSeconds * 1000,
    settings: s.settings,
    lastPolltime: s.lastPolltime,
    lastSummary: s.lastSummary,
    lastError: s.lastError,
    lastFlushAt: s.lastFlushAt,
    lastFlushDeleted: s.lastFlushDeleted,
    nextPollAtMs: running ? s.nextPollAtMs : null,
    serverNowMs: Date.now(),
  };
}

export async function currentLocationTokenStatus(): Promise<TracksolidTokenStatus> {
  return getTracksolidTokenStatus(tracksolidEndpoint());
}

export async function currentLocationCallBudget(): Promise<TracksolidCallBudget> {
  const endpoint = tracksolidEndpoint();
  const token = await getTracksolidTokenStatus(endpoint);
  const s = state();
  return tracksolidCallBudget({
    endpointKey: tracksolidNodeKey(endpoint),
    pollIntervalSeconds: s.settings.pollIntervalSeconds,
    pollRunning: s.running && s.pollTimer != null,
    tokenExpiresAtMs: token.expiresAtMs,
  });
}

const LOCATION_ROW_SQL = `SELECT id, truck_id, imei, location, to_char(gpstime_nz, 'YYYY-MM-DD HH24:MI:SS') AS gpstime_nz, gpstime, polltime, coordinates, fencename, softfencename FROM tbl_current_locations`;

/** Newest row overall, and the newest row for one truck when a truck id is given. */
export async function latestCurrentLocation(truckId?: string | null): Promise<{
  truck: CurrentLocationRow | null;
  latest: CurrentLocationRow | null;
}> {
  await ensureCurrentLocationsSchema();
  await fillCurrentLocationGpsTimeNz();
  const latestRows = await query<CurrentLocationRow>(`${LOCATION_ROW_SQL} ORDER BY id DESC LIMIT 1`);
  const id = truckId?.trim() ?? '';
  if (!id) return { truck: null, latest: latestRows[0] ?? null };
  const truckRows = await query<CurrentLocationRow>(
    `${LOCATION_ROW_SQL} WHERE lower(truck_id) = lower($1) ORDER BY id DESC LIMIT 1`,
    [id],
  );
  return { truck: truckRows[0] ?? null, latest: latestRows[0] ?? null };
}

export async function listCurrentLocations(limit = 100): Promise<CurrentLocationRow[]> {
  await ensureCurrentLocationsSchema();
  await fillCurrentLocationGpsTimeNz();
  const cap = Math.min(Math.max(limit, 1), 500);
  return query<CurrentLocationRow>(
    `SELECT id, truck_id, imei, location, to_char(gpstime_nz, 'YYYY-MM-DD HH24:MI:SS') AS gpstime_nz, gpstime, polltime, coordinates, fencename, softfencename
     FROM tbl_current_locations
     ORDER BY id DESC
     LIMIT $1`,
    [cap],
  );
}

export async function listCurrentLocationLogs(limit = 30): Promise<{ logid: number; logdatetime: string | null; logcat2: string | null; logdetails: string | null }[]> {
  const cap = Math.min(Math.max(limit, 1), 100);
  const rows = await query<{ logid: number; logdatetime: string | null; logcat2: string | null; logdetails: string | null }>(
    `SELECT logid, logdatetime::text AS logdatetime, logcat2, logdetails
     FROM tbl_logs
     WHERE logtype = $1
     ORDER BY logid DESC
     LIMIT $2`,
    [LOGTYPE, cap],
  );
  return rows;
}

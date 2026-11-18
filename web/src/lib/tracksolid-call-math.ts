/** HK, EU, and US nodes. TrackSolid restores this 24 hours after the first call of the window. */
export const TRACKSOLID_DAILY_CALL_LIMIT = 8640;
const WINDOW_MS = 24 * 60 * 60 * 1000;
const TOKEN_MS = 7200 * 1000;
/** jimi.device.location.get accepts up to 100 IMEIs. One call covers this account. */
export const TRACKSOLID_IMEIS_PER_LOCATION_CALL = 100;

export type TracksolidDayCallCost = {
  locationPolls: number;
  deviceListPolls: number;
  locationCalls: number;
  deviceListCalls: number;
  calls: number;
  tokenCalls: number;
  locationCallsPerPoll: number;
  limit: number;
  overLimit: boolean;
};

export function pollsInWindow(ms: number, intervalSeconds: number): number {
  if (ms <= 0 || intervalSeconds <= 0) return 0;
  return Math.floor(ms / (intervalSeconds * 1000));
}

/** One location request covers 100 IMEIs. An unknown count is treated as one call. */
export function locationCallsPerPoll(deviceCount: number): number {
  const count = Number.isFinite(deviceCount) && deviceCount > 0 ? Math.floor(deviceCount) : 0;
  if (count === 0) return 1;
  return Math.ceil(count / TRACKSOLID_IMEIS_PER_LOCATION_CALL);
}

/** Token refreshes while a poll is running. A stored token lasts about 2 hours. */
export function tokenCallsInSpan(now: number, end: number, expiresAtMs: number | null, pollRunning: boolean): number {
  if (!pollRunning || end <= now) return 0;
  let next = expiresAtMs != null && expiresAtMs > now ? expiresAtMs : now;
  let count = 0;
  while (next < end && count < 100) {
    count += 1;
    next += TOKEN_MS;
  }
  return count;
}

/**
 * Full-day TrackSolid calls: location polls at one frequency, device-list polls at another, plus token refreshes.
 * The two schedules are added. They no longer share one interval.
 */
export function tracksolidDayCallCost(
  locationIntervalSeconds: number,
  deviceListIntervalSeconds: number,
  deviceCount = 0,
): TracksolidDayCallCost | null {
  if (!Number.isFinite(locationIntervalSeconds) || locationIntervalSeconds <= 0) return null;
  if (!Number.isFinite(deviceListIntervalSeconds) || deviceListIntervalSeconds <= 0) return null;
  const locationPolls = pollsInWindow(WINDOW_MS, locationIntervalSeconds);
  const deviceListPolls = pollsInWindow(WINDOW_MS, deviceListIntervalSeconds);
  const perLocation = locationCallsPerPoll(deviceCount);
  const locationCalls = locationPolls * perLocation;
  const deviceListCalls = deviceListPolls;
  const tokenCalls = tokenCallsInSpan(0, WINDOW_MS, 0, true);
  const calls = locationCalls + deviceListCalls + tokenCalls;
  return {
    locationPolls,
    deviceListPolls,
    locationCalls,
    deviceListCalls,
    calls,
    tokenCalls,
    locationCallsPerPoll: perLocation,
    limit: TRACKSOLID_DAILY_CALL_LIMIT,
    overLimit: calls > TRACKSOLID_DAILY_CALL_LIMIT,
  };
}

/** HK, EU, and US nodes. TrackSolid restores this 24 hours after the first call of the window. */
export const TRACKSOLID_DAILY_CALL_LIMIT = 8640;
const WINDOW_MS = 24 * 60 * 60 * 1000;
const TOKEN_MS = 7200 * 1000;
/** Device list plus one location call. The account is under the 100-IMEI limit, so one location call covers it. */
export const TRACKSOLID_CALLS_PER_POLL = 2;

export type TracksolidDayCallCost = {
  polls: number;
  calls: number;
  tokenCalls: number;
  callsPerPoll: number;
  limit: number;
  overLimit: boolean;
};

export function pollsInWindow(ms: number, intervalSeconds: number): number {
  if (ms <= 0 || intervalSeconds <= 0) return 0;
  return Math.floor(ms / (intervalSeconds * 1000));
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

/** Full-day TrackSolid calls for a poll interval: two calls per poll, plus the token refreshes. */
export function tracksolidDayCallCost(intervalSeconds: number): TracksolidDayCallCost | null {
  if (!Number.isFinite(intervalSeconds) || intervalSeconds <= 0) return null;
  const polls = pollsInWindow(WINDOW_MS, intervalSeconds);
  const tokenCalls = tokenCallsInSpan(0, WINDOW_MS, 0, true);
  const calls = polls * TRACKSOLID_CALLS_PER_POLL + tokenCalls;
  return {
    polls,
    calls,
    tokenCalls,
    callsPerPoll: TRACKSOLID_CALLS_PER_POLL,
    limit: TRACKSOLID_DAILY_CALL_LIMIT,
    overLimit: calls > TRACKSOLID_DAILY_CALL_LIMIT,
  };
}

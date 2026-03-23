/**
 * VERBATIM TIMES — THE ONLY WAY TO PREPARE position_time FROM API DATA.
 *
 * We NEVER adjust timezones. API → position_time must be 100% literal.
 * Use this module for every value that comes from the API and is written to position_time
 * (or used as a position_time bound in a query). It throws if given a Date or non-string,
 * so the codebase cannot accidentally pass a converted value.
 *
 * FORBIDDEN anywhere in the API→DB path for position_time:
 * - Passing a Date to the DB or to this function.
 * - Using toISOString(), getUTC*(), or formatDateForDebug() to produce position_time from API data.
 * - Appending Z or any timezone offset to an API timestamp before storage.
 * - Converting "to UTC" or "to local" for storage.
 *
 * See: web/docs/VERBATIM_TIMES.md and .cursor/rules/timestamps-raw-no-timezone.mdc
 */

const POSITION_TIME_FORBIDDEN =
  'VERBATIM_TIMES: position_time must be the raw API string. Never pass a Date or converted value. Use the string from the API only.';

/**
 * Prepare the value to store in position_time from API data (e.g. gpsTime).
 * Accepts string or number (API may return timestamp as number); coerces to string. Never alter.
 * Throws only if given a Date — so timezone conversion cannot slip through.
 */
export function positionTimeForStorage(apiValue: unknown): string {
  if (apiValue instanceof Date) {
    throw new Error(
      `${POSITION_TIME_FORBIDDEN} You passed a Date. Store the original API string (e.g. gpsTime) verbatim.`
    );
  }
  if (typeof apiValue === 'number') {
    return String(apiValue).trim();
  }
  if (typeof apiValue !== 'string') {
    throw new Error(
      `${POSITION_TIME_FORBIDDEN} Expected string or number from API, got ${typeof apiValue}.`
    );
  }
  return apiValue.trim();
}

/**
 * Prepare a position_time bound for a query (minTime/maxTime from the same API flow).
 * Accepts string or number (coerces to string). Returns trimmed. Throws only for Date or empty.
 */
export function positionTimeBoundForQuery(apiValue: unknown): string {
  if (apiValue instanceof Date) {
    throw new Error(
      `${POSITION_TIME_FORBIDDEN} Query bounds must be raw API strings, not Date.`
    );
  }
  const s = typeof apiValue === 'number' ? String(apiValue) : typeof apiValue === 'string' ? apiValue : '';
  const trimmed = s.trim();
  if (!trimmed) {
    throw new Error(`${POSITION_TIME_FORBIDDEN} Empty bound not allowed.`);
  }
  return trimmed;
}

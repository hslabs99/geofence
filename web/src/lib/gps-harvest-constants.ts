/**
 * GPS harvest limits — no db/pg. Safe to import from Client Components.
 * Keep in sync with `runGpsHarvestForDistanceId` in distances-gps-harvest.ts (LIMIT uses SQL_JOB_LIMIT).
 */

export const GPS_HARVEST_SQL_JOB_LIMIT = 24;
export const GPS_HARVEST_DEFAULT_MAX_JOBS = 10;
export const GPS_HARVEST_DEFAULT_MAX_SUCCESSES = 3;
export const DEFAULT_HARVEST_START_LESS_MINUTES = 10;
export const DEFAULT_HARVEST_END_PLUS_MINUTES = 60;
export const DEFAULT_HARVEST_WINDOW_MINUTES = 5;

/** Vineyard fence buffer (metres, Web Mercator plane — same family as Steps+). */
export const GPS_PLUS_VINEYARD_BUFFER_METERS = 100;

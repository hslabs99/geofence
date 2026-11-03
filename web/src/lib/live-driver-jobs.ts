import { query } from '@/lib/db';
import { ensureDriverDiversionsSchema } from '@/lib/driver-diversions';
import { ensureCurrentLocationsSchema, fillCurrentLocationGpsTimeNz } from '@/lib/ensure-current-locations-schema';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { loadCurrentLocationSettings } from '@/lib/current-location-poll';
import { DRIVER_JOB_COLUMNS, type DriverJob } from '@/lib/driver-jobs';

export type LivePlace = {
  vineyardMeters: number | null;
  vineyardInside: boolean;
  wineryMeters: number | null;
  wineryInside: boolean;
  /** Stored NZ GPS clock, verbatim. */
  gpstimeNz: string | null;
  /** Stored poll clock, verbatim. */
  fetchedAt: string | null;
  /** Latest fix, same order as the stored coordinates string. */
  lat: number | null;
  lng: number | null;
  /** Shortest metres between the winery fence and the vineyard fence. */
  journeyMeters: number | null;
};

export type LiveDiversion = {
  typeName: string;
  note: string | null;
  expectedDuration: string | null;
  startedAt: string;
};

export type LiveDriverJob = DriverJob & { place: LivePlace; diversion: LiveDiversion | null };

type LiveSqlRow = DriverJob & {
  gpstime_nz: string | null;
  fix_polltime: string | null;
  vineyard_meters: number | null;
  winery_meters: number | null;
  lat: number | null;
  lng: number | null;
  journey_meters: number | null;
};

function coordOf(value: unknown): number | null {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function metersOf(value: unknown): number | null {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? Math.round(n) : null;
}

function flagOf(value: unknown): boolean | null {
  if (value == null) return null;
  if (value === true || value === 1 || value === 't' || value === 'true') return true;
  if (value === false || value === 0 || value === 'f' || value === 'false') return false;
  return null;
}

/**
 * Jobs the driver has started and not ended, with metres from the latest truck fix
 * to the vineyard fence and the winery fence.
 */
export async function loadLiveDriverJobs(): Promise<{ rows: LiveDriverJob[]; driverAppPollSeconds: number }> {
  await ensureTblDriverJobsSchema();
  await ensureDriverDiversionsSchema();
  await ensureCurrentLocationsSchema();
  await fillCurrentLocationGpsTimeNz();
  const driverAppPollSeconds = (await loadCurrentLocationSettings()).driverAppPollSeconds;

  const sqlRows = await query<LiveSqlRow>(
    `WITH open_jobs AS (
       SELECT ${DRIVER_JOB_COLUMNS},
              btrim(coalesce(nullif(btrim(coalesce(vineyard_geo_fence, '')), ''), vineyard_name, '')) AS vineyard_fence,
              btrim(coalesce(nullif(btrim(coalesce(winery_geo_fence, '')), ''), winery_name, '')) AS winery_fence
       FROM tbl_driver_jobs
       WHERE step_1_click IS NOT NULL
         AND step_5_click IS NULL
     ),
     located AS (
       SELECT j.id,
              j.vineyard_fence,
              j.winery_fence,
              lat.gpstime_nz,
              lat.polltime,
              lat.lat,
              lat.lng
       FROM open_jobs j
       LEFT JOIN LATERAL (
         SELECT to_char(c.gpstime_nz, 'YYYY-MM-DD HH24:MI:SS') AS gpstime_nz,
                c.polltime,
                split_part(c.coordinates, ',', 1)::float8 AS lat,
                split_part(c.coordinates, ',', 2)::float8 AS lng
         FROM tbl_current_locations c
         WHERE c.coordinates ~ '^-?[0-9]+(\\.[0-9]+)?,-?[0-9]+(\\.[0-9]+)?$'
           AND (
             (btrim(coalesce(j.imei, '')) <> '' AND btrim(c.imei) = btrim(j.imei))
             OR (
               btrim(coalesce(j.truck_id, '')) <> ''
               AND lower(btrim(c.truck_id)) = lower(btrim(j.truck_id))
             )
           )
         ORDER BY CASE
                    WHEN btrim(coalesce(j.imei, '')) <> '' AND btrim(c.imei) = btrim(j.imei) THEN 0
                    ELSE 1
                  END,
                  c.id DESC
         LIMIT 1
       ) lat ON true
     )
     SELECT o.*,
            l.gpstime_nz,
            l.polltime AS fix_polltime,
            l.lat,
            l.lng,
            dist.vineyard_meters,
            dist.winery_meters,
            journey.journey_meters
     FROM open_jobs o
     LEFT JOIN located l ON l.id = o.id
     LEFT JOIN LATERAL (
       SELECT round(MIN(ST_Distance(
                ST_Transform(ST_Force2D(v.geom), 4326)::geography,
                ST_Transform(ST_Force2D(w.geom), 4326)::geography
              )))::int AS journey_meters
       FROM tbl_geofences v
       JOIN tbl_geofences w ON true
       WHERE v.geom IS NOT NULL
         AND w.geom IS NOT NULL
         AND o.vineyard_fence <> ''
         AND o.winery_fence <> ''
         AND lower(btrim(v.fence_name)) = lower(o.vineyard_fence)
         AND lower(btrim(w.fence_name)) = lower(o.winery_fence)
     ) journey ON true
     LEFT JOIN LATERAL (
       SELECT round(MIN(ST_Distance(
                ST_Transform(ST_Force2D(g.geom), 4326)::geography,
                ST_SetSRID(ST_MakePoint(l.lng, l.lat), 4326)::geography
              )) FILTER (WHERE lower(btrim(g.fence_name)) = lower(l.vineyard_fence)))::int AS vineyard_meters,
              round(MIN(ST_Distance(
                ST_Transform(ST_Force2D(g.geom), 4326)::geography,
                ST_SetSRID(ST_MakePoint(l.lng, l.lat), 4326)::geography
              )) FILTER (WHERE lower(btrim(g.fence_name)) = lower(l.winery_fence)))::int AS winery_meters
       FROM tbl_geofences g
       WHERE l.lat IS NOT NULL
         AND l.lng IS NOT NULL
         AND g.geom IS NOT NULL
         AND (
           (l.vineyard_fence <> '' AND lower(btrim(g.fence_name)) = lower(l.vineyard_fence))
           OR (l.winery_fence <> '' AND lower(btrim(g.fence_name)) = lower(l.winery_fence))
         )
     ) dist ON true`,
  );

  const rows = sqlRows.map((row) => {
    const vineyardMeters = metersOf(row.vineyard_meters);
    const wineryMeters = metersOf(row.winery_meters);
    const job: LiveDriverJob = {
      id: Number(row.id),
      job_id: row.job_id,
      truck_id: row.truck_id,
      imei: row.imei,
      driver: row.driver,
      planned_start_time: row.planned_start_time,
      winery_name: row.winery_name,
      vineyard_name: row.vineyard_name,
      winery_geo_fence: row.winery_geo_fence,
      vineyard_geo_fence: row.vineyard_geo_fence,
      step_1_click: row.step_1_click,
      step_2_click: row.step_2_click,
      step_3_click: row.step_3_click,
      step_4_click: row.step_4_click,
      step_5_click: row.step_5_click,
      step_2_manual: flagOf(row.step_2_manual),
      step_3_manual: flagOf(row.step_3_manual),
      step_4_manual: flagOf(row.step_4_manual),
      place: {
        vineyardMeters,
        vineyardInside: vineyardMeters === 0,
        wineryMeters,
        wineryInside: wineryMeters === 0,
        gpstimeNz: row.gpstime_nz?.trim() || null,
        fetchedAt: row.fix_polltime?.trim() || null,
        lat: coordOf(row.lat),
        lng: coordOf(row.lng),
        journeyMeters: metersOf(row.journey_meters),
      },
      diversion: null,
    };
    return job;
  });

  const ids = rows.map((row) => row.id);
  if (ids.length > 0) {
    const open = await query<{
      driver_job_id: number;
      type_name: string;
      note: string | null;
      expected_duration: string | null;
      started_at: string;
    }>(
      `SELECT driver_job_id,
              type_name,
              note,
              expected_duration,
              to_char(started_at, 'YYYY-MM-DD HH24:MI:SS') AS started_at
       FROM tbl_driver_job_diversions
       WHERE ended_at IS NULL
         AND driver_job_id = ANY($1::int[])`,
      [ids],
    );
    const byJob = new Map(open.map((row) => [Number(row.driver_job_id), row]));
    for (const job of rows) {
      const hit = byJob.get(job.id);
      if (!hit) continue;
      job.diversion = {
        typeName: hit.type_name,
        note: hit.note?.trim() || null,
        expectedDuration: hit.expected_duration,
        startedAt: hit.started_at,
      };
    }
  }

  return { rows, driverAppPollSeconds };
}

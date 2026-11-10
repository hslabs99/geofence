import { execute } from '@/lib/db';
import { loadCurrentLocationSettings } from '@/lib/current-location-poll';
import { fillCurrentLocationGpsTimeNz } from '@/lib/ensure-current-locations-schema';

let backfilled = false;

const COORD = `'^-?[0-9]+(\\.[0-9]+)?,-?[0-9]+(\\.[0-9]+)?$'`;

const DEVICE = `
  (
    (btrim(coalesce(j.imei, '')) <> '' AND btrim(c.imei) = btrim(j.imei))
    OR (
      btrim(coalesce(j.truck_id, '')) <> ''
      AND lower(btrim(c.truck_id)) = lower(btrim(j.truck_id))
    )
  )`;

/**
 * Fill step_2_fence, step_3_fence, and step_4_fence from the GPS point nearest each tap.
 * Rows with no point near the tap stay unset. gpstime is not rewritten; the compare uses the stored NZ clock.
 */
export async function backfillGateFences(): Promise<void> {
  if (backfilled) return;
  await fillCurrentLocationGpsTimeNz();
  const limit = (await loadCurrentLocationSettings()).confirmAwayMeters;
  await backfillArrival(2, limit);
  await backfillArrival(4, limit);
  await backfillDeparture(limit);
  backfilled = true;
}

function fenceNameSql(step: 2 | 4): string {
  const column = step === 2 ? 'j.vineyard_geo_fence' : 'j.winery_geo_fence';
  const fallback = step === 2 ? 'j.vineyard_name' : 'j.winery_name';
  return `btrim(coalesce(nullif(btrim(coalesce(${column}, '')), ''), ${fallback}, ''))`;
}

function pointSql(click: string): string {
  return `
    SELECT c.coordinates
    FROM tbl_current_locations c
    WHERE c.gpstime_nz IS NOT NULL
      AND c.coordinates ~ ${COORD}
      AND c.gpstime_nz BETWEEN j.${click} - interval '3 minutes'
                           AND j.${click} + interval '90 seconds'
      AND ${DEVICE}
    ORDER BY abs(extract(epoch FROM (c.gpstime_nz - j.${click}))) ASC, c.id DESC
    LIMIT 1`;
}

async function backfillArrival(step: 2 | 4, limit: number): Promise<void> {
  const click = step === 2 ? 'step_2_click' : 'step_4_click';
  const fenceCol = step === 2 ? 'step_2_fence' : 'step_4_fence';
  const manualCol = step === 2 ? 'step_2_manual' : 'step_4_manual';
  await execute(
    `UPDATE tbl_driver_jobs AS job
     SET ${fenceCol} = v.kind,
         ${manualCol} = (v.kind = 'manual')
     FROM (
       SELECT j.id,
              CASE
                WHEN names.fence = '' THEN 'manual'
                WHEN p.meters IS NULL THEN NULL
                WHEN p.meters = 0 THEN 'hard'
                WHEN p.meters <= $1 THEN 'soft'
                ELSE 'manual'
              END AS kind
       FROM tbl_driver_jobs j
       CROSS JOIN LATERAL (
         SELECT ${fenceNameSql(step)} AS fence
       ) names
       LEFT JOIN LATERAL (
         SELECT round(MIN(ST_Distance(
                  ST_Transform(ST_Force2D(g.geom), 4326)::geography,
                  ST_SetSRID(ST_MakePoint(
                    split_part(loc.coordinates, ',', 2)::float8,
                    split_part(loc.coordinates, ',', 1)::float8
                  ), 4326)::geography
                )))::int AS meters
         FROM (
           ${pointSql(click)}
         ) loc
         JOIN tbl_geofences g
           ON g.geom IS NOT NULL
          AND names.fence <> ''
          AND lower(btrim(g.fence_name)) = lower(names.fence)
       ) p ON true
       WHERE j.${click} IS NOT NULL
         AND j.${fenceCol} IS NULL
     ) v
     WHERE job.id = v.id
       AND v.kind IS NOT NULL`,
    [limit],
  );
}

async function backfillDeparture(limit: number): Promise<void> {
  await execute(
    `UPDATE tbl_driver_jobs AS job
     SET step_3_fence = v.kind,
         step_3_manual = (v.kind = 'manual')
     FROM (
       SELECT j.id,
              CASE
                WHEN names.fence = '' THEN 'manual'
                WHEN p.meters IS NULL AND NOT coalesce(been.hit, false) THEN NULL
                WHEN p.meters = 0 THEN 'hard'
                WHEN p.meters IS NOT NULL AND p.meters <= $1 THEN 'soft'
                WHEN coalesce(been.hit, false) THEN 'hard'
                WHEN p.meters IS NOT NULL THEN 'manual'
                ELSE NULL
              END AS kind
       FROM tbl_driver_jobs j
       CROSS JOIN LATERAL (
         SELECT btrim(coalesce(nullif(btrim(coalesce(j.vineyard_geo_fence, '')), ''), j.vineyard_name, '')) AS fence
       ) names
       LEFT JOIN LATERAL (
         SELECT round(MIN(ST_Distance(
                  ST_Transform(ST_Force2D(g.geom), 4326)::geography,
                  ST_SetSRID(ST_MakePoint(
                    split_part(loc.coordinates, ',', 2)::float8,
                    split_part(loc.coordinates, ',', 1)::float8
                  ), 4326)::geography
                )))::int AS meters
         FROM (
           ${pointSql('step_3_click')}
         ) loc
         JOIN tbl_geofences g
           ON g.geom IS NOT NULL
          AND names.fence <> ''
          AND lower(btrim(g.fence_name)) = lower(names.fence)
       ) p ON true
       LEFT JOIN LATERAL (
         SELECT EXISTS (
           SELECT 1
           FROM tbl_current_locations c
           JOIN tbl_geofences g
             ON g.geom IS NOT NULL
            AND names.fence <> ''
            AND lower(btrim(g.fence_name)) = lower(names.fence)
           WHERE c.gpstime_nz IS NOT NULL
             AND c.coordinates ~ ${COORD}
             AND c.gpstime_nz <= j.step_3_click + interval '90 seconds'
             AND (j.step_1_click IS NULL OR c.gpstime_nz >= j.step_1_click)
             AND ${DEVICE}
             AND round(ST_Distance(
                   ST_Transform(ST_Force2D(g.geom), 4326)::geography,
                   ST_SetSRID(ST_MakePoint(
                     split_part(c.coordinates, ',', 2)::float8,
                     split_part(c.coordinates, ',', 1)::float8
                   ), 4326)::geography
                 ))::int = 0
         ) AS hit
       ) been ON true
       WHERE j.step_3_click IS NOT NULL
         AND j.step_3_fence IS NULL
     ) v
     WHERE job.id = v.id
       AND v.kind IS NOT NULL`,
    [limit],
  );
}

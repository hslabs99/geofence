import { execute, query } from '@/lib/db';
import { loadCurrentLocationSettings } from '@/lib/current-location-poll';
import { fillCurrentLocationGpsTimeNz } from '@/lib/ensure-current-locations-schema';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { fenceGpsAtClick, type GateFenceKind, type MetersFix } from '@/lib/driver-jobs';

type Candidate = {
  id: number;
  imei: string;
  truck: string;
  step_1_click: string | null;
  step_2_click: string | null;
  step_3_click: string | null;
  step_4_click: string | null;
  step_5_click: string | null;
  step_2_fence: string | null;
  step_3_fence: string | null;
  step_4_fence: string | null;
  step_2_gps_done: boolean | null;
  step_3_gps_done: boolean | null;
  step_4_gps_done: boolean | null;
  vineyard_fence: string;
  winery_fence: string;
};

type PointRow = {
  id: number;
  gpstime_nz: string | null;
  vineyard_meters: number | null;
  winery_meters: number | null;
};

function textOf(value: string | null | undefined): string {
  return value?.trim() ?? '';
}

function fenceKind(value: string | null): GateFenceKind | null {
  if (value === 'hard' || value === 'soft' || value === 'manual') return value;
  return null;
}

function metersOf(value: unknown): number | null {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? Math.round(n) : null;
}

/**
 * Store the soft and hard GPS clocks for fence taps that do not have them yet.
 * Arrival uses the entry nearest the button. Departure uses the last inside sample nearest the button.
 * gpstime itself is not rewritten.
 */
export async function fillGateGpsTimes(onlyJobId?: number): Promise<void> {
  await ensureTblDriverJobsSchema();
  await fillCurrentLocationGpsTimeNz();
  const limit = (await loadCurrentLocationSettings()).confirmAwayMeters;
  const jobs = await query<Candidate>(
    `SELECT id,
            btrim(coalesce(imei, '')) AS imei,
            btrim(coalesce(truck_id, '')) AS truck,
            to_char(step_1_click, 'YYYY-MM-DD HH24:MI:SS') AS step_1_click,
            to_char(step_2_click, 'YYYY-MM-DD HH24:MI:SS') AS step_2_click,
            to_char(step_3_click, 'YYYY-MM-DD HH24:MI:SS') AS step_3_click,
            to_char(step_4_click, 'YYYY-MM-DD HH24:MI:SS') AS step_4_click,
            to_char(step_5_click, 'YYYY-MM-DD HH24:MI:SS') AS step_5_click,
            step_2_fence,
            step_3_fence,
            step_4_fence,
            step_2_gps_done,
            step_3_gps_done,
            step_4_gps_done,
            btrim(coalesce(nullif(btrim(coalesce(vineyard_geo_fence, '')), ''), vineyard_name, '')) AS vineyard_fence,
            btrim(coalesce(nullif(btrim(coalesce(winery_geo_fence, '')), ''), winery_name, '')) AS winery_fence
     FROM tbl_driver_jobs
     WHERE (
       (step_2_click IS NOT NULL AND step_2_fence IN ('hard', 'soft') AND step_2_gps_done IS NOT TRUE)
       OR (step_3_click IS NOT NULL AND step_3_fence IN ('hard', 'soft') AND step_3_gps_done IS NOT TRUE)
       OR (step_4_click IS NOT NULL AND step_4_fence IN ('hard', 'soft') AND step_4_gps_done IS NOT TRUE)
     )
       AND ($1::int IS NULL OR id = $1)`,
    [onlyJobId ?? null],
  );
  if (jobs.length === 0) return;

  const tracks = await loadTracks(jobs);
  for (const job of jobs) {
    const points = tracks.get(Number(job.id)) ?? [];
    const vineyard = points.map((point) => ({ at: point.at, meters: point.vineyardMeters }));
    const winery = points.map((point) => ({ at: point.at, meters: point.wineryMeters }));
    const step2 = pending(job, 2)
      ? fenceGpsAtClick(vineyard, limit, textOf(job.step_2_click), 'enter')
      : null;
    const step3 = pending(job, 3)
      ? fenceGpsAtClick(vineyard, limit, textOf(job.step_3_click), 'leave')
      : null;
    const step4 = pending(job, 4)
      ? fenceGpsAtClick(winery, limit, textOf(job.step_4_click), 'enter')
      : null;
    await execute(
      `UPDATE tbl_driver_jobs
       SET step_2_soft_gps = CASE WHEN $2::boolean THEN CAST($3 AS timestamp without time zone) ELSE step_2_soft_gps END,
           step_2_hard_gps = CASE WHEN $2::boolean THEN CAST($4 AS timestamp without time zone) ELSE step_2_hard_gps END,
           step_2_gps_done = CASE WHEN $2::boolean THEN $5 ELSE step_2_gps_done END,
           step_3_soft_gps = CASE WHEN $6::boolean THEN CAST($7 AS timestamp without time zone) ELSE step_3_soft_gps END,
           step_3_hard_gps = CASE WHEN $6::boolean THEN CAST($8 AS timestamp without time zone) ELSE step_3_hard_gps END,
           step_3_gps_done = CASE WHEN $6::boolean THEN $9 ELSE step_3_gps_done END,
           step_4_soft_gps = CASE WHEN $10::boolean THEN CAST($11 AS timestamp without time zone) ELSE step_4_soft_gps END,
           step_4_hard_gps = CASE WHEN $10::boolean THEN CAST($12 AS timestamp without time zone) ELSE step_4_hard_gps END,
           step_4_gps_done = CASE WHEN $10::boolean THEN $13 ELSE step_4_gps_done END
       WHERE id = $1`,
      [
        job.id,
        step2 != null,
        step2?.softAt ?? null,
        step2?.hardAt ?? null,
        step2 != null && arrivalDone(points.length, job.step_5_click),
        step3 != null,
        step3?.softAt ?? null,
        step3?.hardAt ?? null,
        step3 != null && departureDone(job, step3, points.length),
        step4 != null,
        step4?.softAt ?? null,
        step4?.hardAt ?? null,
        step4 != null && arrivalDone(points.length, job.step_5_click),
      ],
    );
  }
}

function pending(job: Candidate, step: 2 | 3 | 4): boolean {
  const click = step === 2 ? job.step_2_click : step === 3 ? job.step_3_click : job.step_4_click;
  const kind = fenceKind(step === 2 ? job.step_2_fence : step === 3 ? job.step_3_fence : job.step_4_fence);
  const done = step === 2 ? job.step_2_gps_done : step === 3 ? job.step_3_gps_done : job.step_4_gps_done;
  return Boolean(textOf(click)) && (kind === 'hard' || kind === 'soft') && done !== true;
}

function arrivalDone(pointCount: number, step5: string | null): boolean {
  return pointCount > 0 || Boolean(textOf(step5));
}

function departureDone(
  job: Candidate,
  gps: { softAt: string | null; hardAt: string | null },
  pointCount: number,
): boolean {
  const kind = fenceKind(job.step_3_fence);
  const found = kind === 'hard' ? gps.hardAt : gps.softAt;
  if (found) return true;
  if (textOf(job.step_4_click) || textOf(job.step_5_click)) return true;
  return pointCount === 0;
}

async function loadTracks(jobs: Candidate[]): Promise<Map<number, Array<MetersFix & { vineyardMeters: number | null; wineryMeters: number | null }>>> {
  const tracks = new Map<number, Array<MetersFix & { vineyardMeters: number | null; wineryMeters: number | null }>>();
  const rows = await query<PointRow>(
    `SELECT j.id,
            c.gpstime_nz,
            round(MIN(ST_Distance(
              ST_Transform(ST_Force2D(g.geom), 4326)::geography,
              ST_SetSRID(ST_MakePoint(
                split_part(c.coordinates, ',', 2)::float8,
                split_part(c.coordinates, ',', 1)::float8
              ), 4326)::geography
            )) FILTER (
              WHERE j.vineyard_fence <> ''
                AND lower(btrim(g.fence_name)) = lower(btrim(j.vineyard_fence))
            ))::int AS vineyard_meters,
            round(MIN(ST_Distance(
              ST_Transform(ST_Force2D(g.geom), 4326)::geography,
              ST_SetSRID(ST_MakePoint(
                split_part(c.coordinates, ',', 2)::float8,
                split_part(c.coordinates, ',', 1)::float8
              ), 4326)::geography
            )) FILTER (
              WHERE j.winery_fence <> ''
                AND lower(btrim(g.fence_name)) = lower(btrim(j.winery_fence))
            ))::int AS winery_meters
     FROM unnest($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])
       AS j(id, imei, truck, since_at, until_at, vineyard_fence, winery_fence)
     CROSS JOIN LATERAL (
       SELECT id,
              to_char(gpstime_nz, 'YYYY-MM-DD HH24:MI:SS') AS gpstime_nz,
              coordinates
       FROM tbl_current_locations c
       WHERE c.gpstime_nz IS NOT NULL
         AND c.coordinates ~ '^-?[0-9]+(\\.[0-9]+)?,-?[0-9]+(\\.[0-9]+)?$'
         AND (
           (j.imei <> '' AND btrim(c.imei) = j.imei)
           OR (j.truck <> '' AND lower(btrim(c.truck_id)) = lower(btrim(j.truck)))
         )
         AND (j.since_at = '' OR c.gpstime_nz >= j.since_at::timestamp)
         AND (j.until_at = '' OR c.gpstime_nz <= j.until_at::timestamp)
       ORDER BY c.gpstime_nz DESC, c.id DESC
       LIMIT 500
     ) c
     JOIN tbl_geofences g
       ON g.geom IS NOT NULL
      AND (
        (j.vineyard_fence <> '' AND lower(btrim(g.fence_name)) = lower(btrim(j.vineyard_fence)))
        OR (j.winery_fence <> '' AND lower(btrim(g.fence_name)) = lower(btrim(j.winery_fence)))
      )
     GROUP BY j.id, c.id, c.gpstime_nz
     ORDER BY j.id, c.gpstime_nz ASC`,
    [
      jobs.map((job) => job.id),
      jobs.map((job) => textOf(job.imei)),
      jobs.map((job) => textOf(job.truck)),
      jobs.map((job) => textOf(job.step_1_click) || textOf(job.step_2_click)),
      jobs.map((job) => textOf(job.step_5_click)),
      jobs.map((job) => textOf(job.vineyard_fence)),
      jobs.map((job) => textOf(job.winery_fence)),
    ],
  );
  for (const row of rows) {
    const id = Number(row.id);
    const list = tracks.get(id) ?? [];
    const at = row.gpstime_nz?.trim() || '';
    if (!at) continue;
    list.push({
      at,
      meters: null,
      vineyardMeters: metersOf(row.vineyard_meters),
      wineryMeters: metersOf(row.winery_meters),
    });
    tracks.set(id, list);
  }
  return tracks;
}

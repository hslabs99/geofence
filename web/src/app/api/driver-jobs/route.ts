import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { driverJobWriteFromBody } from '@/lib/driver-job-fields';
import { DRIVER_JOB_COLUMNS, type DriverJob } from '@/lib/driver-jobs';

export type DriverJobRow = DriverJob;

function rowsToSortedDistinct(rows: { val: string | null }[]): string[] {
  const seen = new Set<string>();
  for (const row of rows) {
    const s = row.val == null ? '' : String(row.val).trim();
    if (s) seen.add(s);
  }
  return [...seen].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
}

/** GET: Driver Visibility jobs, optionally filtered by driver, winery, or vineyard. */
export async function GET(request: Request) {
  try {
    await ensureTblDriverJobsSchema();
    const { searchParams } = new URL(request.url);
    const driver = (searchParams.get('driver') ?? '').trim();
    const winery = (searchParams.get('winery') ?? '').trim();
    const vineyard = (searchParams.get('vineyard') ?? '').trim();

    const conditions: string[] = [];
    const values: string[] = [];
    if (driver) {
      values.push(driver);
      conditions.push(`btrim(driver) = $${values.length}`);
    }
    if (winery) {
      values.push(winery);
      conditions.push(`btrim(winery_name) = $${values.length}`);
    }
    if (vineyard) {
      values.push(vineyard);
      conditions.push(`btrim(vineyard_name) = $${values.length}`);
    }
    const whereClause = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';

    const [rows, driverRows, wineryRows, vineyardRows, fenceRows] = await Promise.all([
      query<DriverJobRow>(
        `SELECT ${DRIVER_JOB_COLUMNS}
         FROM tbl_driver_jobs
         ${whereClause}
         ORDER BY planned_start_time DESC NULLS LAST, job_id`,
        values,
      ),
      query<{ val: string | null }>(
        `SELECT DISTINCT btrim(driver) AS val
         FROM tbl_driver_jobs
         WHERE driver IS NOT NULL AND btrim(driver) <> ''
         ORDER BY 1`,
      ),
      query<{ val: string | null }>(
        `SELECT DISTINCT btrim(winery_name) AS val
         FROM tbl_driver_jobs
         WHERE winery_name IS NOT NULL AND btrim(winery_name) <> ''
         ORDER BY 1`,
      ),
      query<{ val: string | null }>(
        `SELECT DISTINCT btrim(vineyard_name) AS val
         FROM tbl_driver_jobs
         WHERE vineyard_name IS NOT NULL AND btrim(vineyard_name) <> ''
         ORDER BY 1`,
      ),
      query<{ val: string | null }>(
        `SELECT DISTINCT ON (lower(btrim(fence_name))) btrim(fence_name) AS val
         FROM tbl_geofences
         WHERE fence_name IS NOT NULL AND btrim(fence_name) <> ''
         ORDER BY lower(btrim(fence_name)), btrim(fence_name)`,
      ),
    ]);

    return NextResponse.json({
      rows,
      total: rows.length,
      drivers: rowsToSortedDistinct(driverRows),
      wineries: rowsToSortedDistinct(wineryRows),
      vineyards: rowsToSortedDistinct(vineyardRows),
      fenceNames: rowsToSortedDistinct(fenceRows),
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** POST: add one manual driver job. Planned start is stored as typed, with no timezone shift. */
export async function POST(request: Request) {
  try {
    await ensureTblDriverJobsSchema();
    const body = (await request.json()) as Record<string, unknown>;
    const parsed = await driverJobWriteFromBody(body);
    if (!parsed.ok) return NextResponse.json({ error: parsed.error }, { status: 400 });
    const job = parsed.job;

    const rows = await query<DriverJobRow>(
      `INSERT INTO tbl_driver_jobs (
         job_id, truck_id, imei, driver, planned_start_time, winery_name, vineyard_name,
         winery_geo_fence, vineyard_geo_fence
       )
       VALUES (
         $1, $2, $3, $4,
         CAST(NULLIF($5, '') AS timestamp without time zone),
         $6, $7, $8, $9
       )
       RETURNING ${DRIVER_JOB_COLUMNS}`,
      [job.jobId, job.truckId, job.imei, job.driver, job.plannedRaw ?? '', job.winery, job.vineyard, job.wineryFence, job.vineyardFence],
    );
    const row = rows[0];
    if (!row) return NextResponse.json({ error: 'Insert failed' }, { status: 500 });
    return NextResponse.json({ row });
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code === '22007' || code === '22008') {
      return NextResponse.json(
        { error: 'Planned start must be a date and time, such as 2026-10-15 08:00' },
        { status: 400 },
      );
    }
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

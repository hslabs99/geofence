import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { driverJobWriteFromBody } from '@/lib/driver-job-fields';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { DRIVER_JOB_COLUMNS, type DriverJob } from '@/lib/driver-jobs';

/** GET one driver job for the phone job page. */
export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const jobId = Number(id);
    if (!Number.isInteger(jobId) || jobId <= 0) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }
    const rows = await query<DriverJob>(
      `SELECT ${DRIVER_JOB_COLUMNS} FROM tbl_driver_jobs WHERE id = $1`,
      [jobId],
    );
    const row = rows[0];
    if (!row) return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    return NextResponse.json({ row });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** PATCH: edit the job fields. Step clicks are left as they are. */
export async function PATCH(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const jobIdNum = Number(id);
    if (!Number.isInteger(jobIdNum) || jobIdNum <= 0) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }
    const body = (await request.json()) as Record<string, unknown>;
    const parsed = await driverJobWriteFromBody(body);
    if (!parsed.ok) return NextResponse.json({ error: parsed.error }, { status: 400 });
    const job = parsed.job;
    const rows = await query<DriverJob>(
      `UPDATE tbl_driver_jobs
       SET job_id = $2,
           truck_id = $3,
           imei = $4,
           driver = $5,
           planned_start_time = CAST(NULLIF($6, '') AS timestamp without time zone),
           winery_name = $7,
           vineyard_name = $8,
           winery_geo_fence = $9,
           vineyard_geo_fence = $10
       WHERE id = $1
       RETURNING ${DRIVER_JOB_COLUMNS}`,
      [jobIdNum, job.jobId, job.truckId, job.imei, job.driver, job.plannedRaw ?? '', job.winery, job.vineyard, job.wineryFence, job.vineyardFence],
    );
    const row = rows[0];
    if (!row) return NextResponse.json({ error: 'Job not found' }, { status: 404 });
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

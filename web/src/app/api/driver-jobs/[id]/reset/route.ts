import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { clearDriverJobLog, recordStaffControl } from '@/lib/driver-job-log';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { DRIVER_JOB_COLUMNS, phoneLocalStamp, type DriverJob } from '@/lib/driver-jobs';
import { staffActorFromBody } from '@/lib/job-actor';

/** POST: clear step click times so the job can be tested again. */
export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const body = await request.json().catch(() => ({}));
    const actor = (body as { control?: unknown }).control === true ? await staffActorFromBody(body) : null;
    if ((body as { control?: unknown }).control === true && !actor) {
      return NextResponse.json({ error: 'Only an admin can control this job.' }, { status: 403 });
    }
    const { id } = await params;
    const jobId = Number(id);
    if (!Number.isInteger(jobId) || jobId <= 0) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }

    const rows = await query<DriverJob>(
      `UPDATE tbl_driver_jobs
       SET step_0_click = NULL,
           confirmed_driver = NULL,
           step_1_click = NULL,
           step_2_click = NULL,
           step_3_click = NULL,
           step_4_click = NULL,
           step_5_click = NULL,
           step_2_manual = NULL,
           step_3_manual = NULL,
           step_4_manual = NULL,
           step_2_fence = NULL,
           step_3_fence = NULL,
           step_4_fence = NULL,
           step_2_soft_gps = NULL,
           step_2_hard_gps = NULL,
           step_3_soft_gps = NULL,
           step_3_hard_gps = NULL,
           step_4_soft_gps = NULL,
           step_4_hard_gps = NULL,
           step_2_gps_done = NULL,
           step_3_gps_done = NULL,
           step_4_gps_done = NULL
       WHERE id = $1
       RETURNING ${DRIVER_JOB_COLUMNS}`,
      [jobId],
    );
    const row = rows[0];
    if (!row) return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    const at = phoneLocalStamp();
    await clearDriverJobLog(jobId, at);
    if (actor) {
      await recordStaffControl({ driverJobId: jobId, at, detail: 'Reset the job', actor });
    }
    return NextResponse.json({ row });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

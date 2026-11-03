import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { clearDriverJobLog } from '@/lib/driver-job-log';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { DRIVER_JOB_COLUMNS, phoneLocalStamp, type DriverJob } from '@/lib/driver-jobs';

/** POST: clear step click times so the job can be tested again. */
export async function POST(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const jobId = Number(id);
    if (!Number.isInteger(jobId) || jobId <= 0) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }

    const rows = await query<DriverJob>(
      `UPDATE tbl_driver_jobs
       SET step_1_click = NULL,
           step_2_click = NULL,
           step_3_click = NULL,
           step_4_click = NULL,
           step_5_click = NULL,
           step_2_manual = NULL,
           step_3_manual = NULL,
           step_4_manual = NULL
       WHERE id = $1
       RETURNING ${DRIVER_JOB_COLUMNS}`,
      [jobId],
    );
    const row = rows[0];
    if (!row) return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    await clearDriverJobLog(jobId, phoneLocalStamp());
    return NextResponse.json({ row });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

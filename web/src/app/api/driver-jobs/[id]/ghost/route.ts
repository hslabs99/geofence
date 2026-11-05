import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { recordStaffControl } from '@/lib/driver-job-log';
import { isPhoneLocalStamp } from '@/lib/driver-jobs';
import { staffActorFromBody } from '@/lib/job-actor';

/** POST: an admin opened this job to act for the driver. */
export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const jobId = Number(id);
    if (!Number.isInteger(jobId) || jobId <= 0) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }
    const jobs = await query<{ id: number }>(`SELECT id FROM tbl_driver_jobs WHERE id = $1`, [jobId]);
    if (!jobs[0]) return NextResponse.json({ error: 'Job not found' }, { status: 404 });

    const body = (await request.json()) as { at?: unknown };
    const at = typeof body.at === 'string' ? body.at.trim() : '';
    if (!isPhoneLocalStamp(at)) {
      return NextResponse.json({ error: 'Time must be YYYY-MM-DD HH:MM:SS' }, { status: 400 });
    }
    const actor = await staffActorFromBody(body);
    if (!actor) return NextResponse.json({ error: 'Only an admin can control this job.' }, { status: 403 });

    await recordStaffControl({
      driverJobId: jobId,
      at,
      detail: 'Opened the job to act for the driver',
      actor,
    });
    return NextResponse.json({ ok: true, actor });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

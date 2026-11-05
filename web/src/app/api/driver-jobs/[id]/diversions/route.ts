import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { listDiversionTypes, listJobDiversions, startJobDiversion } from '@/lib/driver-diversions';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { recordStaffControl } from '@/lib/driver-job-log';
import { isPhoneLocalStamp, type DriverJob } from '@/lib/driver-jobs';
import { staffActorFromBody } from '@/lib/job-actor';

async function loadJob(id: string): Promise<DriverJob | null> {
  const jobId = Number(id);
  if (!Number.isInteger(jobId) || jobId <= 0) return null;
  const jobs = await query<Pick<DriverJob, 'id' | 'driver'>>(
    `SELECT id, driver FROM tbl_driver_jobs WHERE id = $1`,
    [jobId],
  );
  return (jobs[0] as DriverJob | undefined) ?? null;
}

export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const job = await loadJob(id);
    if (!job) return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    const [types, diversions] = await Promise.all([listDiversionTypes(), listJobDiversions(job.id)]);
    return NextResponse.json({ types, diversions });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** Start a diversion clock. It is not tied to a gate. */
export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const job = await loadJob(id);
    if (!job) return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    const body = (await request.json()) as { typeName?: unknown; note?: unknown; expected?: unknown; at?: unknown; control?: unknown };
    const typeName = typeof body.typeName === 'string' ? body.typeName : '';
    const note = typeof body.note === 'string' ? body.note : '';
    const expected = typeof body.expected === 'string' ? body.expected.trim() : '';
    const at = typeof body.at === 'string' ? body.at.trim() : '';
    if (!isPhoneLocalStamp(at)) {
      return NextResponse.json({ error: 'Time must be YYYY-MM-DD HH:MM:SS' }, { status: 400 });
    }
    const actor = body.control === true ? await staffActorFromBody(body) : null;
    if (body.control === true && !actor) {
      return NextResponse.json({ error: 'Only an admin can control this job.' }, { status: 403 });
    }
    const diversion = await startJobDiversion({
      driverJobId: job.id,
      driver: job.driver,
      typeName,
      note,
      expected,
      at,
    });
    if (actor) {
      await recordStaffControl({
        driverJobId: job.id,
        at,
        detail: `Started diversion ${diversion.type_name}`,
        actor,
      });
    }
    return NextResponse.json({ diversion });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const status = /choose|already running/i.test(message) ? 409 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

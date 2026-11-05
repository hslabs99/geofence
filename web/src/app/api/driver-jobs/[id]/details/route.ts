import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { recordDriverClick } from '@/lib/driver-job-log';
import { DRIVER_JOB_COLUMNS, isPhoneLocalStamp, jobDetailsConfirmed, type DriverJob } from '@/lib/driver-jobs';
import { staffActorFromBody } from '@/lib/job-actor';
import { cleanPlate, rememberTruckPlates } from '@/lib/truck-plates';

/** POST: step 0. The name is typed for this job. Plates are saved on the truck for the next job. */
export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const jobId = Number(id);
    if (!Number.isInteger(jobId) || jobId <= 0) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }

    const body = (await request.json()) as {
      at?: unknown;
      driverName?: unknown;
      truckRego?: unknown;
      trailerRego?: unknown;
      control?: unknown;
    };
    const actor = body.control === true ? await staffActorFromBody(body) : null;
    if (body.control === true && !actor) {
      return NextResponse.json({ error: 'Only an admin can control this job.' }, { status: 403 });
    }
    const at = typeof body.at === 'string' ? body.at.trim() : '';
    if (!isPhoneLocalStamp(at)) {
      return NextResponse.json({ error: 'Step time must be YYYY-MM-DD HH:MM:SS' }, { status: 400 });
    }
    const driverName = typeof body.driverName === 'string' ? body.driverName.trim() : '';
    const truckRego = cleanPlate(body.truckRego);
    const trailerRego = cleanPlate(body.trailerRego);
    if (!driverName) {
      return NextResponse.json({ error: 'Enter the driver name.' }, { status: 400 });
    }
    if (!truckRego) {
      return NextResponse.json({ error: 'Enter the truck registration.' }, { status: 400 });
    }
    if (driverName.length > 80 || truckRego.length > 20 || trailerRego.length > 20) {
      return NextResponse.json({ error: 'That name or registration is too long.' }, { status: 400 });
    }

    const existing = await query<DriverJob>(
      `SELECT ${DRIVER_JOB_COLUMNS} FROM tbl_driver_jobs WHERE id = $1`,
      [jobId],
    );
    const current = existing[0];
    if (!current) return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    if (current.step_1_click?.trim()) {
      return NextResponse.json({ error: 'Job details are locked after the job has started.' }, { status: 409 });
    }

    const rows = await query<DriverJob>(
      `UPDATE tbl_driver_jobs
       SET step_0_click = CAST($2 AS timestamp without time zone),
           confirmed_driver = $3,
           driver = $3,
           truck_rego = $4,
           trailer_rego = $5
       WHERE id = $1 AND step_1_click IS NULL
       RETURNING ${DRIVER_JOB_COLUMNS}`,
      [jobId, at, driverName, truckRego, trailerRego || null],
    );
    const row = rows[0];
    if (!row) return NextResponse.json({ error: 'Job details are locked after the job has started.' }, { status: 409 });

    await rememberTruckPlates(row.truck_id, { truckRego, trailerRego });
    const detail = [driverName, truckRego, trailerRego].filter(Boolean).join(' · ');
    try {
      await recordDriverClick({
        driverJobId: jobId,
        at,
        step: 0,
        outcome: 'auto_accept',
        detail,
        actor,
      });
    } catch {
      // The confirmation is already stored. The log can be rebuilt from it.
    }
    return NextResponse.json({ row, confirmed: jobDetailsConfirmed(row) });
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code === '22007' || code === '22008') {
      return NextResponse.json({ error: 'Step time must be YYYY-MM-DD HH:MM:SS' }, { status: 400 });
    }
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

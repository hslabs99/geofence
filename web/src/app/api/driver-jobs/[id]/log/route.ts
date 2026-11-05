import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { loadCurrentLocationSettings } from '@/lib/current-location-poll';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { listJobDiversions } from '@/lib/driver-diversions';
import { recordDriverClick, readDriverJobLog } from '@/lib/driver-job-log';
import { staffActorFromBody } from '@/lib/job-actor';
import { DRIVER_GATES, DRIVER_JOB_COLUMNS, completedGateCount, isPhoneLocalStamp, type DriverJob } from '@/lib/driver-jobs';

async function loadJob(id: string): Promise<DriverJob | null> {
  const jobId = Number(id);
  if (!Number.isInteger(jobId) || jobId <= 0) return null;
  const jobs = await query<DriverJob>(`SELECT ${DRIVER_JOB_COLUMNS} FROM tbl_driver_jobs WHERE id = $1`, [jobId]);
  return jobs[0] ?? null;
}

/** Fence crossings for this truck, plus gate clicks and whether each one was cautioned. */
export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const job = await loadJob(id);
    if (!job) return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    const driverAppPollSeconds = (await loadCurrentLocationSettings()).driverAppPollSeconds;
    const events = await readDriverJobLog(job);
    const clearedRows = await query<{ log_cleared_at: string | null }>(
      `SELECT to_char(log_cleared_at, 'YYYY-MM-DD HH24:MI:SS') AS log_cleared_at FROM tbl_driver_jobs WHERE id = $1`,
      [job.id],
    );
    const clearedAt = clearedRows[0]?.log_cleared_at?.trim() || '';
    const diversions = (await listJobDiversions(job.id)).filter((row) => !clearedAt || row.started_at >= clearedAt);
    return NextResponse.json({ events, diversions, driverAppPollSeconds });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** The driver was cautioned and chose Cancel. Accepts are written by the step route. */
export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const job = await loadJob(id);
    if (!job) return NextResponse.json({ error: 'Job not found' }, { status: 404 });

    const body = (await request.json()) as { step?: unknown; at?: unknown; detail?: unknown; control?: unknown };
    const actor = body.control === true ? await staffActorFromBody(body) : null;
    if (body.control === true && !actor) {
      return NextResponse.json({ error: 'Only an admin can control this job.' }, { status: 403 });
    }
    const step = Number(body.step);
    const at = typeof body.at === 'string' ? body.at.trim() : '';
    const detail = typeof body.detail === 'string' ? body.detail.trim().slice(0, 500) : '';
    if (!DRIVER_GATES.some((gate) => gate.step === step)) {
      return NextResponse.json({ error: 'Unknown step' }, { status: 400 });
    }
    if (!isPhoneLocalStamp(at)) {
      return NextResponse.json({ error: 'Time must be YYYY-MM-DD HH:MM:SS' }, { status: 400 });
    }
    const next = completedGateCount(job) + 1;
    if (step !== next) {
      return NextResponse.json({ error: 'That gate is not the next step' }, { status: 409 });
    }

    await recordDriverClick({
      driverJobId: job.id,
      at,
      step,
      outcome: 'caution_decline',
      detail: detail || null,
      actor,
    });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

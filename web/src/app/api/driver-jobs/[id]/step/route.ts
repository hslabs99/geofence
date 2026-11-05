import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { expectedDurationLabel, openJobDiversion, stopJobDiversion } from '@/lib/driver-diversions';
import { recordDriverClick } from '@/lib/driver-job-log';
import { readStepProximity } from '@/lib/gate-proximity';
import { staffActorFromBody } from '@/lib/job-actor';
import { pushDriverGateToVwork } from '@/lib/vwork-job-steps';
import {
  DRIVER_GATES,
  DRIVER_JOB_COLUMNS,
  completedGateCount,
  isPhoneLocalStamp,
  type DriverJob,
} from '@/lib/driver-jobs';

const STEP_COLUMN = {
  1: 'step_1_click',
  2: 'step_2_click',
  3: 'step_3_click',
  4: 'step_4_click',
  5: 'step_5_click',
} as const;

/** POST: record the next gate only, using the phone clock string as given. */
export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const jobId = Number(id);
    if (!Number.isInteger(jobId) || jobId <= 0) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }

    const body = (await request.json()) as {
      step?: unknown;
      at?: unknown;
      confirmed?: unknown;
      endDiversion?: unknown;
      control?: unknown;
    };
    const actor = body.control === true ? await staffActorFromBody(body) : null;
    if (body.control === true && !actor) {
      return NextResponse.json({ error: 'Only an admin can control this job.' }, { status: 403 });
    }
    const step = Number(body.step);
    const at = typeof body.at === 'string' ? body.at.trim() : '';
    if (!DRIVER_GATES.some((gate) => gate.step === step)) {
      return NextResponse.json({ error: 'Unknown step' }, { status: 400 });
    }
    if (!isPhoneLocalStamp(at)) {
      return NextResponse.json({ error: 'Step time must be YYYY-MM-DD HH:MM:SS' }, { status: 400 });
    }

    const existing = await query<DriverJob>(
      `SELECT ${DRIVER_JOB_COLUMNS} FROM tbl_driver_jobs WHERE id = $1`,
      [jobId],
    );
    const current = existing[0];
    if (!current) return NextResponse.json({ error: 'Job not found' }, { status: 404 });

    const next = completedGateCount(current) + 1;
    if (step !== next || next > DRIVER_GATES.length) {
      return NextResponse.json({ error: 'That gate is not the next step' }, { status: 409 });
    }
    if (step === 1 && !current.step_0_click?.trim()) {
      return NextResponse.json({ error: 'Confirm job details before starting the job.' }, { status: 409 });
    }

    if (step >= 2 && step <= 5) {
      const open = await openJobDiversion(jobId);
      if (open && body.endDiversion !== true) {
        const gateTitle = DRIVER_GATES.find((gate) => gate.step === step)?.title ?? 'the next step';
        const expected = expectedDurationLabel(open.expected_duration);
        const expectedBit = expected === '—' ? '' : ` Expected ${expected}.`;
        return NextResponse.json({
          needsConfirm: true,
          kind: 'diversion',
          message: `${open.type_name} is still running.${expectedBit} End it before ${gateTitle}?`,
        });
      }
      if (open && body.endDiversion === true) {
        await stopJobDiversion(jobId, at, step);
      }
    }

    const gateStep = step === 2 || step === 3 || step === 4 ? step : null;
    const warning = gateStep ? (await readStepProximity(current, gateStep)).warning : null;
    if (body.confirmed !== true && warning) {
      return NextResponse.json({ needsConfirm: true, message: warning });
    }

    const column = STEP_COLUMN[step as keyof typeof STEP_COLUMN];
    const manualColumn = step === 2 ? 'step_2_manual' : step === 3 ? 'step_3_manual' : step === 4 ? 'step_4_manual' : null;
    const rows = await query<DriverJob>(
      manualColumn
        ? `UPDATE tbl_driver_jobs
           SET ${column} = CAST($2 AS timestamp without time zone),
               ${manualColumn} = $3
           WHERE id = $1 AND ${column} IS NULL
           RETURNING ${DRIVER_JOB_COLUMNS}`
        : `UPDATE tbl_driver_jobs
           SET ${column} = CAST($2 AS timestamp without time zone)
           WHERE id = $1 AND ${column} IS NULL
           RETURNING ${DRIVER_JOB_COLUMNS}`,
      manualColumn ? [jobId, at, warning != null] : [jobId, at],
    );
    const row = rows[0];
    if (!row) return NextResponse.json({ error: 'That gate is not the next step' }, { status: 409 });
    try {
      await recordDriverClick({
        driverJobId: jobId,
        at,
        step,
        outcome: warning ? 'caution_accept' : 'auto_accept',
        detail: warning,
        actor,
      });
    } catch {
      // The gate click is already stored. The log can be rebuilt from it.
    }
    const vwork = await pushDriverGateToVwork(jobId, step, at);
    return NextResponse.json({ row, vwork });
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code === '22007' || code === '22008') {
      return NextResponse.json({ error: 'Step time must be YYYY-MM-DD HH:MM:SS' }, { status: 400 });
    }
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

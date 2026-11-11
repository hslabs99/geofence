import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { expectedDurationLabel, openJobDiversion, stopJobDiversion } from '@/lib/driver-diversions';
import { recordDriverClick } from '@/lib/driver-job-log';
import { readJobLegs, readJobStart, readStepProximity, START_AWAY_WARNING } from '@/lib/gate-proximity';
import { staffActorFromBody } from '@/lib/job-actor';
import { enqueueDriverGateUpdate } from '@/lib/vwork-gate-queue';
import {
  DRIVER_GATES,
  DRIVER_JOB_COLUMNS,
  completedGateCount,
  gateFenceKind,
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

/** POST: record the next gate only, using the phone clock string as given. VWork is queued, not called here. */
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
    const reading = gateStep ? await readStepProximity(current, gateStep) : null;
    const start = step === 1 ? await readJobStart(current) : null;
    const warning = start && !start.nearWinery ? START_AWAY_WARNING : (reading?.warning ?? null);
    if (body.confirmed !== true && warning) {
      return NextResponse.json({ needsConfirm: true, message: warning });
    }

    if (step === 1 && start) {
      const rows = await query<DriverJob>(
        `UPDATE tbl_driver_jobs
         SET step_1_click = CAST($2 AS timestamp without time zone),
             step_1_manual = $3,
             step_1_fence = $4,
             start_vineyard_meters = $5
         WHERE id = $1 AND step_1_click IS NULL
         RETURNING ${DRIVER_JOB_COLUMNS}`,
        [jobId, at, !start.nearWinery, start.fence, start.vineyardMeters],
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
      const gateTitle = DRIVER_GATES.find((gate) => gate.step === step)?.title ?? `Step ${step}`;
      let vwork: { queued: boolean; error?: string } = { queued: true };
      try {
        await enqueueDriverGateUpdate({
          driverJobId: jobId,
          jobId: current.job_id,
          step,
          stepTitle: gateTitle,
          stageAt: at,
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.error('[driver-step] gate update was not queued', message);
        vwork = { queued: false, error: message };
      }
      const legs = await readJobLegs(row);
      return NextResponse.json({ row, vwork, legs });
    }

    const column = STEP_COLUMN[step as keyof typeof STEP_COLUMN];
    const manualColumn = step === 2 ? 'step_2_manual' : step === 3 ? 'step_3_manual' : step === 4 ? 'step_4_manual' : null;
    const fenceColumn = step === 2 ? 'step_2_fence' : step === 3 ? 'step_3_fence' : step === 4 ? 'step_4_fence' : null;
    const fenceKind = reading
      ? gateFenceKind({
          step: reading.step,
          meters: reading.meters,
          beenInside: reading.beenInside,
          limitMeters: reading.limitMeters,
        })
      : null;
    const rows = await query<DriverJob>(
      manualColumn && fenceColumn
        ? `UPDATE tbl_driver_jobs
           SET ${column} = CAST($2 AS timestamp without time zone),
               ${manualColumn} = $3,
               ${fenceColumn} = $4
           WHERE id = $1 AND ${column} IS NULL
           RETURNING ${DRIVER_JOB_COLUMNS}`
        : `UPDATE tbl_driver_jobs
           SET ${column} = CAST($2 AS timestamp without time zone)
           WHERE id = $1 AND ${column} IS NULL
           RETURNING ${DRIVER_JOB_COLUMNS}`,
      manualColumn ? [jobId, at, fenceKind === 'manual', fenceKind] : [jobId, at],
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
    const gateTitle = DRIVER_GATES.find((gate) => gate.step === step)?.title ?? `Step ${step}`;
    let vwork: { queued: boolean; error?: string } = { queued: true };
    try {
      await enqueueDriverGateUpdate({
        driverJobId: jobId,
        jobId: current.job_id,
        step,
        stepTitle: gateTitle,
        stageAt: at,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error('[driver-step] gate update was not queued', message);
      vwork = { queued: false, error: message };
    }
    const legs = await readJobLegs(row);
    return NextResponse.json({ row, vwork, legs });
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code === '22007' || code === '22008') {
      return NextResponse.json({ error: 'Step time must be YYYY-MM-DD HH:MM:SS' }, { status: 400 });
    }
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';
import { deriveGpsStepsForJob } from '@/lib/derived-steps';

/** Recursively convert BigInt to number (or string if too large) so JSON.stringify works. */
function sanitizeForJson<T>(value: T): T {
  if (typeof value === 'bigint') {
    return (Number(value) <= Number.MAX_SAFE_INTEGER && Number(value) >= Number.MIN_SAFE_INTEGER
      ? Number(value)
      : String(value)) as T;
  }
  if (Array.isArray(value)) return value.map(sanitizeForJson) as T;
  if (value != null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) out[k] = sanitizeForJson(v);
    return out as T;
  }
  return value;
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const jobIdParam = searchParams.get('jobId');
    const device = searchParams.get('device');
    const positionAfter = searchParams.get('positionAfter');
    const positionBefore = searchParams.get('positionBefore');
    const windowMinutesParam = searchParams.get('windowMinutes');
    const writeBack = searchParams.get('writeBack') === '1' || searchParams.get('writeBack') === 'true';

    if (!jobIdParam) {
      return NextResponse.json({ error: 'jobId is required' }, { status: 400 });
    }
    if (!device || !positionAfter) {
      return NextResponse.json({ error: 'device and positionAfter are required (same as tbl_tracking window)' }, { status: 400 });
    }

    const windowMinutes = Math.min(
      120,
      Math.max(0, parseInt(windowMinutesParam ?? '5', 10) || 5)
    );

    const jobId = jobIdParam.trim();
    const debugBase = { jobId, windowMinutes, step: 'job_lookup' as const };

    let jobRows: {
      job_id: unknown;
      vineyard_name: string | null;
      delivery_winery: string | null;
      truck_id: string | null;
      worker: string | null;
      actual_start_time: string | null;
      actual_end_time: string | null;
      step_5_completed_at: string | null;
    }[] = [];

    try {
      jobRows = await prisma.$queryRawUnsafe<
        typeof jobRows
      >(
        `SELECT job_id, vineyard_name, delivery_winery, truck_id, worker, actual_start_time, actual_end_time, step_5_completed_at FROM tbl_vworkjobs WHERE job_id::text = $1 LIMIT 1`,
        jobId
      );
    } catch (queryErr) {
      const qMsg = queryErr instanceof Error ? queryErr.message : String(queryErr);
      return NextResponse.json(sanitizeForJson({
        error: qMsg,
        step1: null,
        step2: null,
        step3: null,
        step4: null,
        step5: null,
        step1TrackingId: null,
        step2TrackingId: null,
        step3TrackingId: null,
        step4TrackingId: null,
        step5TrackingId: null,
        debug: { ...debugBase, error: qMsg, failedStep: 'SELECT from tbl_vworkjobs', hint: 'Check job_id type and table name' },
      }), { status: 500 });
    }

    const job = jobRows[0];
    if (!job) {
      return NextResponse.json(sanitizeForJson({
        error: 'Job not found',
        step1: null,
        step2: null,
        step3: null,
        step4: null,
        step5: null,
        step1TrackingId: null,
        step2TrackingId: null,
        step3TrackingId: null,
        step4TrackingId: null,
        step5TrackingId: null,
        debug: { ...debugBase, error: 'Job not found', triedJobId: jobId, rowCount: 0, hint: 'job_id may not exist in tbl_vworkjobs' },
      }), { status: 404 });
    }

    /** Use worker (tbl_vwork.worker = tbl_tracking.device_name) for GPS scan; fallback to request device. */
    const deviceForTracking = (job.worker != null && String(job.worker).trim() !== ''
      ? String(job.worker).trim()
      : device.trim());

    const result = await deriveGpsStepsForJob(prisma, job, {
      windowMinutes,
      device: deviceForTracking,
      positionAfter: positionAfter.trim(),
      positionBefore: positionBefore?.trim() || null,
    });

    if (writeBack) {
      try {
        await prisma.$executeRawUnsafe(
          `UPDATE tbl_vworkjobs SET
            Step_1_GPS_completed_at = $1::timestamp,
            Step1_gps_id = $2,
            Step_2_GPS_completed_at = $3::timestamp,
            Step2_gps_id = $4,
            Step_3_GPS_completed_at = $5::timestamp,
            Step3_gps_id = $6,
            Step_4_GPS_completed_at = $7::timestamp,
            Step4_gps_id = $8,
            Step_5_GPS_completed_at = $9::timestamp,
            Step5_gps_id = $10
          WHERE job_id::text = $11`,
          result.step1 ?? null,
          result.step1TrackingId ?? null,
          result.step2 ?? null,
          result.step2TrackingId ?? null,
          result.step3 ?? null,
          result.step3TrackingId ?? null,
          result.step4 ?? null,
          result.step4TrackingId ?? null,
          result.step5 ?? null,
          result.step5TrackingId ?? null,
          jobId
        );
        await prisma.$executeRawUnsafe(
          `UPDATE tbl_vworkjobs SET
            step_1_actual_time = COALESCE(step_1_gps_completed_at, step_1_completed_at),
            step_1_via = CASE WHEN step_1_gps_completed_at IS NOT NULL THEN 'GPS' ELSE 'VW' END,
            step_2_actual_time = COALESCE(step_2_gps_completed_at, step_2_completed_at),
            step_2_via = CASE WHEN step_2_gps_completed_at IS NOT NULL THEN 'GPS' ELSE 'VW' END,
            step_3_actual_time = COALESCE(step_3_gps_completed_at, step_3_completed_at),
            step_3_via = CASE WHEN step_3_gps_completed_at IS NOT NULL THEN 'GPS' ELSE 'VW' END,
            step_4_actual_time = COALESCE(step_4_gps_completed_at, step_4_completed_at),
            step_4_via = CASE WHEN step_4_gps_completed_at IS NOT NULL THEN 'GPS' ELSE 'VW' END,
            step_5_actual_time = COALESCE(step_5_gps_completed_at, step_5_completed_at),
            step_5_via = CASE WHEN step_5_gps_completed_at IS NOT NULL THEN 'GPS' ELSE 'VW' END
          WHERE job_id::text = $1`,
          jobId
        );
        try {
          await prisma.$executeRawUnsafe(
            `UPDATE tbl_vworkjobs SET steps_fetched = true, steps_fetched_when = now() WHERE job_id::text = $1`,
            jobId
          );
        } catch {
          /* steps_fetched / steps_fetched_when columns may not exist yet; run prisma/migrations/add_steps_fetched_tbl_vworkjobs.sql */
        }
      } catch (writeErr) {
        const wMsg = writeErr instanceof Error ? writeErr.message : String(writeErr);
        return NextResponse.json(sanitizeForJson({
          ...result,
          error: `Derived steps OK but write-back failed: ${wMsg}`,
          debug: { ...result.debug, writeBackError: wMsg },
        }), { status: 500 });
      }
    }

    return NextResponse.json(sanitizeForJson(result));
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const stack = err instanceof Error ? err.stack : undefined;
    return NextResponse.json(
      sanitizeForJson({
        error: message,
        step1: null,
        step2: null,
        step3: null,
        step4: null,
        step5: null,
        step1TrackingId: null,
        step2TrackingId: null,
        step3TrackingId: null,
        step4TrackingId: null,
        step5TrackingId: null,
        debug: { error: message, stack, failedStep: 'deriveGpsStepsForJob or job lookup' },
      }),
      { status: 500 }
    );
  }
}

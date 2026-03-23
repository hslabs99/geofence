import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';
import { addMinutesToTimestampAsNZ } from '@/lib/fetch-steps';
import { deriveGpsStepsForJob, type JobForDerivedSteps } from '@/lib/derived-steps';
import { runStepsPlusQuery } from '@/lib/steps-plus-query';

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

    let jobRows: Record<string, unknown>[] = [];

    try {
      jobRows = await query<Record<string, unknown>>(
        'SELECT * FROM tbl_vworkjobs WHERE job_id::text = $1 LIMIT 1',
        [jobId]
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

    const rawJob = jobRows[0];
    if (!rawJob) {
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

    /** Normalize job so we have worker/vineyard_name/delivery_winery regardless of DB column case (Worker vs worker, etc.). */
    const row = rawJob as Record<string, unknown>;
    const pick = (...keys: string[]): unknown => {
      for (const k of keys) {
        const v = row[k];
        if (v !== undefined) return v;
      }
      return null;
    };
    const job: JobForDerivedSteps = {
      job_id: pick('job_id', 'Job_ID'),
      vineyard_name: (pick('vineyard_name', 'Vineyard_Name') as string | null) ?? undefined,
      delivery_winery: (pick('delivery_winery', 'Delivery_Winery') as string | null) ?? undefined,
      truck_id: (pick('truck_id', 'Truck_ID') as string | null) ?? undefined,
      worker: (pick('worker', 'Worker') as string | null) ?? undefined,
      actual_start_time: (pick('actual_start_time', 'Actual_Start_Time') as string | null) ?? undefined,
      actual_end_time: (pick('actual_end_time', 'Actual_End_Time') as string | null) ?? undefined,
      step_5_completed_at: (pick('step_5_completed_at', 'Step_5_Completed_At') as string | null) ?? undefined,
      step_1_completed_at: (pick('step_1_completed_at', 'Step_1_Completed_At') as string | null) ?? undefined,
      step1oride: (pick('step1oride', 'Step1oride') as string | null) ?? undefined,
      step2oride: (pick('step2oride', 'Step2oride') as string | null) ?? undefined,
      step3oride: (pick('step3oride', 'Step3oride') as string | null) ?? undefined,
      step4oride: (pick('step4oride', 'Step4oride') as string | null) ?? undefined,
      step5oride: (pick('step5oride', 'Step5oride') as string | null) ?? undefined,
    };

    /** Use worker (tbl_vwork.worker = tbl_tracking.device_name) for GPS scan; fallback to request device. */
    const deviceForTracking = (job.worker != null && String(job.worker).trim() !== ''
      ? String(job.worker).trim()
      : device.trim());

    /**
     * Window end for derivation + Steps+ (buffered vineyard). Client may omit positionBefore when the job
     * row has no actual_end (bulk tagging); without a real upper bound, runFetchSteps used to pass nothing
     * and Steps+ used positionAfter as both bounds → empty SQL window. Recompute from DB row + endPlusMinutes.
     */
    const endPlusMinutes = Math.min(1440, Math.max(0, parseInt(searchParams.get('endPlusMinutes') ?? '60', 10) || 60));
    const pickTrim = (...keys: string[]): string => {
      for (const k of keys) {
        const v = row[k];
        if (v != null && String(v).trim() !== '') return String(v).trim();
      }
      return '';
    };
    let effectivePositionBefore: string | null = positionBefore?.trim() || null;
    if (!effectivePositionBefore) {
      const end = pickTrim('actual_end_time', 'Actual_End_Time', 'gps_end_time', 'Gps_End_Time');
      if (end) {
        effectivePositionBefore = addMinutesToTimestampAsNZ(end, endPlusMinutes);
      } else {
        const start = pickTrim('actual_start_time', 'Actual_Start_Time', 'planned_start_time', 'Planned_Start_Time');
        if (start) {
          effectivePositionBefore = addMinutesToTimestampAsNZ(start, 24 * 60 + endPlusMinutes);
        }
      }
    }

    // When writing back, clear GPS step fields 1–5 and final (actual/via) first so we start from VWork only.
    // Otherwise a previous run (e.g. VineFence+ / Steps+) leaves step2/step3 populated and Steps+ never runs again.
    if (writeBack) {
      await execute(
        `UPDATE tbl_vworkjobs SET
          Step_1_GPS_completed_at = NULL, Step1_gps_id = NULL,
          Step_2_GPS_completed_at = NULL, Step2_gps_id = NULL,
          Step_3_GPS_completed_at = NULL, Step3_gps_id = NULL,
          Step_4_GPS_completed_at = NULL, Step4_gps_id = NULL,
          Step_5_GPS_completed_at = NULL, Step5_gps_id = NULL,
          step_1_actual_time = NULL, step_1_via = NULL,
          step_2_actual_time = NULL, step_2_via = NULL,
          step_3_actual_time = NULL, step_3_via = NULL,
          step_4_actual_time = NULL, step_4_via = NULL,
          step_5_actual_time = NULL, step_5_via = NULL
        WHERE job_id::text = $1`,
        [jobId]
      );
    }

    let result = await deriveGpsStepsForJob(job, {
      windowMinutes,
      device: deviceForTracking,
      positionAfter: positionAfter.trim(),
      positionBefore: effectivePositionBefore,
    });

    // Steps+ (buffered vineyard fence): if standard derivation still misses VWork step 2 or 3, try expanded polygons.
    const vineyardName = job.vineyard_name ? String(job.vineyard_name).trim() : '';
    if (writeBack && vineyardName && (result.step2 == null || result.step3 == null)) {
      const mappings = await query<{ vwname: string | null; gpsname: string | null }>(
        "SELECT vwname, gpsname FROM tbl_gpsmappings WHERE type = 'Vineyard' AND (TRIM(COALESCE(vwname,'')) = $1 OR TRIM(COALESCE(gpsname,'')) = $1)",
        [vineyardName]
      );
      const fenceNames: string[] = [vineyardName];
      for (const m of mappings) {
        const gps = (m.gpsname ?? '').trim();
        if (gps && !fenceNames.includes(gps)) fenceNames.push(gps);
      }
      if (fenceNames.length > 0) {
        const stepsPlusEnd =
          effectivePositionBefore ?? addMinutesToTimestampAsNZ(positionAfter.trim(), 24 * 60);
        const stepsPlusRows = await runStepsPlusQuery(
          deviceForTracking,
          positionAfter.trim(),
          stepsPlusEnd,
          fenceNames,
          10
        );
        const stays = stepsPlusRows.filter((r) => Number(r.duration_seconds) >= 300);
        if (stays.length === 1) {
          result = {
            ...result,
            step2: stays[0].enter_time,
            step3: stays[0].exit_time,
            step2Via: 'VineFence+',
            step3Via: 'VineFence+',
          };
        }
      }
    }

    if (writeBack) {
      try {
        await execute(
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
          [
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
            jobId,
          ]
        );
        const step1Actual = result.step1 ?? (job.step_1_completed_at as string | null) ?? null;
        const step1Via = result.step1Via ?? (result.step1ActualOverride != null ? 'RULE' : (result.step1 != null ? 'GPS' : 'VW'));
        await execute(
          `UPDATE tbl_vworkjobs SET
            step_1_actual_time = $1::timestamp,
            step_1_via = $2,
            step_2_actual_time = $3::timestamp,
            step_2_via = $4,
            step_3_actual_time = $5::timestamp,
            step_3_via = $6,
            step_4_actual_time = $7::timestamp,
            step_4_via = $8,
            step_5_actual_time = $9::timestamp,
            step_5_via = $10
          WHERE job_id::text = $11`,
          [
            step1Actual,
            step1Via,
            result.step2 ?? null,
            result.step2Via ?? 'VW',
            result.step3 ?? null,
            result.step3Via ?? 'VW',
            result.step4 ?? null,
            result.step4Via ?? 'VW',
            result.step5 ?? null,
            result.step5Via ?? 'VW',
            jobId,
          ]
        );
        try {
          await execute(
            'UPDATE tbl_vworkjobs SET steps_fetched = true, steps_fetched_when = now() WHERE job_id::text = $1',
            [jobId]
          );
        } catch {
          /* steps_fetched / steps_fetched_when columns may not exist yet */
        }
        if (result.step2Via === 'VineFence+' || result.step3Via === 'VineFence+') {
          await execute(
            `UPDATE tbl_vworkjobs SET calcnotes = COALESCE(TRIM(calcnotes) || ' ', '') || 'VineFence+:'
             WHERE job_id::text = $1`,
            [jobId]
          );
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

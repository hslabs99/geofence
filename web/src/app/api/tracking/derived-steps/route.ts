import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';
import { addMinutesToTimestampAsNZ } from '@/lib/fetch-steps';
import {
  deriveGpsLayerAfterVineFencePlus,
  deriveGpsStepsForJob,
  finalizeDerivedSteps,
  type JobForDerivedSteps,
  normalizeTimestampString,
  vineyardBufferWidensPolygonEnter,
  vineyardEnterMinutesEarlierThanPolygon,
  type StepVia,
} from '@/lib/derived-steps';
import { getJobEndCeilingBufferMinutes } from '@/lib/job-end-ceiling-buffer-setting';
import { getStep5ExtendWineryExitMinutes } from '@/lib/step5-winery-exit-extend-setting';
import { getStep1FromPreviousJobLimitMinutes } from '@/lib/step1-from-previous-job-limit-setting';
import { tryStepsPlusSnapshotForVineyardJob, type StepsPlusSnapshotAttempt } from '@/lib/steps-plus-merge-snapshot';
import {
  applyStep1LastJobEndIfEligible,
  type Step1LastJobEndResult,
} from '@/lib/step1-last-job-end';

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
      step_2_completed_at: (pick('step_2_completed_at', 'Step_2_Completed_At') as string | null) ?? undefined,
      step_3_completed_at: (pick('step_3_completed_at', 'Step_3_Completed_At') as string | null) ?? undefined,
      step_4_completed_at: (pick('step_4_completed_at', 'Step_4_Completed_At') as string | null) ?? undefined,
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
     * Guardrail: lastjobendstep1limiter
     * Clamp the entire derived-steps tracking window start so it cannot extend into the previous job for this worker.
     *
     * Why: if the lookback window reaches into the previous job, the "first winery EXIT" (GPS step 1) can be
     * picked from the prior job, which then cascades to step ordering (step 2/3) and wrong job story.
     *
     * Rule: previous job = same worker, same day, with actual_start_time < this actual_start_time, ordered by
     * actual_start_time DESC, LIMIT 1. Use previous job's final step_5_actual_time as truth. Then:
     *   positionAfter := max(original positionAfter, previous.step_5_actual_time - 2 minutes)
     *
     * Easy removal: delete this block and use raw request positionAfter everywhere.
     */
    const lastJobEndStep1Limiter: Record<string, unknown> = {
      guardrail: 'lastjobendstep1limiter',
      applied: false,
      worker: deviceForTracking,
      originalPositionAfter: positionAfter.trim(),
      clampedPositionAfter: positionAfter.trim(),
      previousJobId: null,
      previousJobStep5ActualTime: null,
      capAfter: null,
      reason: 'not_computed',
    };
    const thisActualStart =
      job.actual_start_time != null && String(job.actual_start_time).trim() !== ''
        ? normalizeTimestampString(String(job.actual_start_time).trim())
        : null;
    let effectivePositionAfter: string = positionAfter.trim();
    if (thisActualStart) {
      type PrevRow = { job_id: unknown; step_5_actual_time: unknown; actual_start_time: unknown };
      const prevRows = await query<PrevRow>(
        `SELECT trim(job_id::text) AS job_id,
                to_char(step_5_actual_time, 'YYYY-MM-DD HH24:MI:SS') AS step_5_actual_time,
                to_char(actual_start_time, 'YYYY-MM-DD HH24:MI:SS') AS actual_start_time
         FROM tbl_vworkjobs
         WHERE LOWER(TRIM(COALESCE(worker::text, ''))) = LOWER(TRIM($1::text))
           AND actual_start_time IS NOT NULL
           AND (actual_start_time::date) = ($2::timestamp)::date
           AND actual_start_time < $2::timestamp
           AND step_5_actual_time IS NOT NULL
         ORDER BY actual_start_time DESC, trim(job_id::text) DESC
         LIMIT 1`,
        [deviceForTracking, thisActualStart]
      );
      const prev = prevRows[0];
      if (!prev) {
        lastJobEndStep1Limiter.reason = 'no_previous_job';
      } else {
        const prevEndRaw = prev.step_5_actual_time != null ? String(prev.step_5_actual_time).trim() : '';
        const prevEnd = prevEndRaw ? (normalizeTimestampString(prevEndRaw) ?? prevEndRaw.slice(0, 19)) : null;
        lastJobEndStep1Limiter.previousJobId = prev.job_id != null ? String(prev.job_id).trim() : null;
        lastJobEndStep1Limiter.previousJobStep5ActualTime = prevEnd;
        if (!prevEnd) {
          lastJobEndStep1Limiter.reason = 'previous_missing_step5_actual_time';
        } else {
          const capAfter = addMinutesToTimestampAsNZ(prevEnd, -2);
          const capNorm = normalizeTimestampString(capAfter) ?? capAfter.slice(0, 19);
          const origNorm =
            normalizeTimestampString(effectivePositionAfter) ?? String(effectivePositionAfter).trim().slice(0, 19);
          lastJobEndStep1Limiter.capAfter = capNorm;
          if (capNorm && origNorm && capNorm > origNorm) {
            effectivePositionAfter = capNorm;
            lastJobEndStep1Limiter.applied = true;
            lastJobEndStep1Limiter.clampedPositionAfter = effectivePositionAfter;
            lastJobEndStep1Limiter.reason = 'clamped_to_previous_job_end_minus_2m';
          } else {
            lastJobEndStep1Limiter.reason = 'no_clamp_needed';
          }
        }
      }
    } else {
      lastJobEndStep1Limiter.reason = 'no_actual_start_time';
    }

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
    // Clear calcnotes so this run does not append duplicate VineFence+:/GPS*:/… tokens on top of prior runs.
    const [jobEndCeilingBufferMinutes, step5ExtendWineryExitMinutes, step1FromPreviousJobLimitMinutes] =
      await Promise.all([
        getJobEndCeilingBufferMinutes(),
        getStep5ExtendWineryExitMinutes(),
        getStep1FromPreviousJobLimitMinutes(),
      ]);

    if (writeBack) {
      await execute(
        `UPDATE tbl_vworkjobs SET
          step_1_gps_completed_at = NULL, step1_gps_id = NULL,
          step_2_gps_completed_at = NULL, step2_gps_id = NULL,
          step_3_gps_completed_at = NULL, step3_gps_id = NULL,
          step_4_gps_completed_at = NULL, step4_gps_id = NULL,
          step_5_gps_completed_at = NULL, step5_gps_id = NULL,
          step_1_actual_time = NULL, step_1_via = NULL,
          step_2_actual_time = NULL, step_2_via = NULL,
          step_3_actual_time = NULL, step_3_via = NULL,
          step_4_actual_time = NULL, step_4_via = NULL,
          step_5_actual_time = NULL, step_5_via = NULL,
          calcnotes = NULL
        WHERE job_id::text = $1`,
        [jobId]
      );
    }

    /** Step1(lastJobEnd): first — may set step_1_completed_at from previous job Step 5; step_1_safe write-once. */
    let step1LastJobEndReport: Step1LastJobEndResult = {
      applied: false,
      reason: 'write_back_disabled',
      limitMinutes: step1FromPreviousJobLimitMinutes,
      debug: { limitMinutes: step1FromPreviousJobLimitMinutes },
    };
    if (writeBack) {
      step1LastJobEndReport = await applyStep1LastJobEndIfEligible(
        jobId,
        job,
        row,
        deviceForTracking,
        {
          windowMinutes,
          device: deviceForTracking,
          positionAfter: effectivePositionAfter,
          positionBefore: effectivePositionBefore,
          jobEndCeilingBufferMinutes,
          step5ExtendWineryExitMinutes,
        },
        true,
        step1FromPreviousJobLimitMinutes
      );
      if (step1LastJobEndReport.applied) {
        const refreshed = await query<Record<string, unknown>>(
          'SELECT * FROM tbl_vworkjobs WHERE job_id::text = $1 LIMIT 1',
          [jobId]
        );
        if (refreshed[0]) {
          Object.assign(row, refreshed[0]);
          job.step_1_completed_at =
            (pick('step_1_completed_at', 'Step_1_Completed_At') as string | null) ?? undefined;
        }
      }
    }

    const vineyardSnapName = job.vineyard_name ? String(job.vineyard_name).trim() : '';
    const vworkEndRawSnap =
      pick('step_5_completed_at', 'Step_5_Completed_At') ?? pick('actual_end_time', 'Actual_End_Time');
    const vworkEndSnap =
      vworkEndRawSnap != null && String(vworkEndRawSnap).trim() !== ''
        ? normalizeTimestampString(vworkEndRawSnap as string | Date)
        : null;
    let stepsPlusSnapshotAttempt: StepsPlusSnapshotAttempt | null = null;
    if (writeBack && vineyardSnapName) {
      stepsPlusSnapshotAttempt = await tryStepsPlusSnapshotForVineyardJob({
        device: deviceForTracking,
        positionAfter: effectivePositionAfter,
        positionBefore: effectivePositionBefore,
        vineyardName: vineyardSnapName,
        jobEndCeilingBufferMinutes,
        vworkEnd: vworkEndSnap,
      });
    }
    const tentativeVineyardEnterForStep1Bracket =
      stepsPlusSnapshotAttempt?.ok === true ? stepsPlusSnapshotAttempt.snapshot.mergedEnter : null;

    let result = await deriveGpsStepsForJob(job, {
      windowMinutes,
      device: deviceForTracking,
      positionAfter: effectivePositionAfter,
      positionBefore: effectivePositionBefore,
      jobEndCeilingBufferMinutes,
      step5ExtendWineryExitMinutes,
      tentativeVineyardEnterForStep1Bracket,
    });

    /** Snapshot before Steps+ (VineFence+/VineFenceV+) so Inspect can show polygon vs buffer in plain English. */
    const vDebug = result.debug?.vineyard;
    const step2Dbg = vDebug?.step2;
    const candidateFenceLabels =
      vDebug?.resolvedFenceNames?.length ?
        vDebug.resolvedFenceNames
          .map((r) => (r.fence_name != null && String(r.fence_name).trim() !== '' ? String(r.fence_name).trim() : null))
          .filter((x): x is string => x != null)
      : [];

    const initialPass = {
      step1Gps: result.step1Gps ?? null,
      step2Gps: result.step2Gps ?? null,
      step3Gps: result.step3Gps ?? null,
      step4Gps: result.step4Gps ?? null,
      step5Gps: result.step5Gps ?? null,
      step1TrackingId: result.step1TrackingId ?? null,
      step2TrackingId: result.step2TrackingId ?? null,
      step3TrackingId: result.step3TrackingId ?? null,
      step4TrackingId: result.step4TrackingId ?? null,
      step5TrackingId: result.step5TrackingId ?? null,
      step1Via: result.step1Via ?? null,
      step2Via: result.step2Via ?? null,
      step3Via: result.step3Via ?? null,
      step4Via: result.step4Via ?? null,
      step5Via: result.step5Via ?? null,
      /** tbl_geofences.fence_name on the winning ENTER row (which mapped fence matched). */
      step2PolygonFenceName: step2Dbg?.matchedFenceName ?? null,
      step2MatchedGeofenceId: step2Dbg?.matchedGeofenceId ?? null,
      /** All vineyard fence names in the search set (VWork name + tbl_gpsmappings). */
      step2PolygonSearchFenceNames: candidateFenceLabels.length > 0 ? candidateFenceLabels : null,
    };

    /** Steps+ merged multiple buffered segments (GPS* rule); append calcnotes GPS*: on write-back. */
    let stepsPlusGpsStarMerge = false;
    /** Set when VineFenceV+ applied successfully; includes minutes delta for calcnotes (write-back only). */
    let vineFenceVPlusCalcnote: string | null = null;

    /** Inspect “Explanation” tab: human-readable Steps+ trail (buffered vineyard). */
    let stepsPlusReport: Record<string, unknown> = {
      eligible: false,
      reason: writeBack ? 'no_vineyard_name_on_job' : 'write_back_disabled',
      writeBack,
    };

    // Steps+ (buffered vineyard fence): fill missing step 2/3 (VineFence+), or widen only polygon ENTER (VineFenceV+); polygon EXIT stays verbatim.
    const vineyardName = job.vineyard_name ? String(job.vineyard_name).trim() : '';
    if (writeBack && vineyardName) {
      const mappings = await query<{ vwname: string | null; gpsname: string | null }>(
        `SELECT vwname, gpsname FROM tbl_gpsmappings WHERE type = 'Vineyard'
         AND (
           LOWER(TRIM(COALESCE(vwname,''))) = LOWER(TRIM($1::text))
           OR LOWER(TRIM(COALESCE(gpsname,''))) = LOWER(TRIM($1::text))
         )`,
        [vineyardName]
      );
      const fenceNames: string[] = [vineyardName];
      for (const m of mappings) {
        const gps = (m.gpsname ?? '').trim();
        if (gps && !fenceNames.includes(gps)) fenceNames.push(gps);
      }
      stepsPlusReport = {
        eligible: true,
        vineyardName,
        fenceNames: [...fenceNames],
      };
      if (fenceNames.length > 0) {
        const att = stepsPlusSnapshotAttempt;
        if (!att) {
          Object.assign(stepsPlusReport, {
            outcome: 'steps_plus_snapshot_unavailable',
            detail: 'Internal: fence names resolved but Steps+ snapshot was not precomputed.',
          });
        } else {
          const meta = att.ok ? att.snapshot : att;
          const stepsPlusRows = meta.stepsPlusRows;
          const stays = meta.stays;
          const staysInJob = meta.staysInJob;
          const stepsPlusEnd = meta.stepsPlusEnd;
          const stepsPlusBufferM = meta.bufferMeters;
          const stepsPlusMinSec = meta.minDurationSeconds;
          const rawDurNums = meta.rawSegmentDurationsSeconds;
          const maxRawSegmentDurationSeconds = meta.maxRawSegmentDurationSeconds;
          const exitCeil = meta.exitCeilForStayFilter;
          const vworkEnd = meta.vworkEnd;

          Object.assign(stepsPlusReport, {
            bufferMeters: stepsPlusBufferM,
            minDurationSeconds: stepsPlusMinSec,
            stepsPlusEnd,
            rawSegmentCount: stepsPlusRows.length,
            afterMinDurationCount: stays.length,
            staysInJobCount: staysInJob.length,
            rawSegmentDurationsSeconds: rawDurNums,
            maxRawSegmentDurationSeconds,
            vworkEnd: vworkEnd ?? null,
            exitCeilForStayFilter: exitCeil ?? null,
            polygonHadBothStepsGps: result.step2Gps != null && result.step3Gps != null,
          });

          if (!att.ok) {
            const outcome =
              att.fail === 'no_rows'
                ? 'no_buffered_stays_found'
                : att.fail === 'no_stays_in_job'
                  ? 'no_stay_in_job_window'
                  : 'all_segments_below_min_duration';
            Object.assign(stepsPlusReport, {
              outcome,
              detail:
                att.fail === 'no_rows'
                  ? 'No lat/lon points formed a contiguous inside-buffer segment in the window, or fence geometries did not match tbl_geofences names.'
                  : att.fail === 'no_stays_in_job'
                    ? 'Segments existed but none met enter-before-job-end and exit-before-ceiling (job end + job end ceiling buffer).'
                    : 'Points were inside the buffer but merge produced no valid enter/exit window.',
            });
          }

          if (att.ok && att.snapshot.staysInJob.length >= 1) {
            const mergedEnter = att.snapshot.mergedEnter;
            const mergedExit = att.snapshot.mergedExit;
            const usedGpsStarMerge = att.snapshot.usedGpsStarMerge;
            const hadBothPolygonGps = result.step2Gps != null && result.step3Gps != null;
            let bufferVia: StepVia = 'VineFence+';
            let applyBuffer = !hadBothPolygonGps;
            if (hadBothPolygonGps) {
              applyBuffer = vineyardBufferWidensPolygonEnter(
                mergedEnter,
                result.step2Gps!,
                result.step3Gps!
              );
              bufferVia = 'VineFenceV+';
            }
            Object.assign(stepsPlusReport, {
              mergedEnter,
              mergedExit,
              usedGpsStarMerge,
              hadBothPolygonGps,
              applyBuffer,
              bufferVia,
            });
            const step2ForBuffer: { value: string; trackingId: number | null } = {
              value: mergedEnter,
              trackingId: null,
            };
            const step3ForBuffer: { value: string; trackingId: number | null } =
              hadBothPolygonGps && applyBuffer
                ? { value: result.step3Gps!, trackingId: result.step3TrackingId }
                : { value: mergedExit, trackingId: null };
            if (!applyBuffer && hadBothPolygonGps) {
              Object.assign(stepsPlusReport, {
                outcome: 'buffer_skipped_polygon_complete_vinefencev_not_wider',
                detail:
                  'Both polygon vineyard steps existed; buffered enter was not enough earlier than polygon enter for VineFenceV+ (see VINE_FENCE_V_PLUS_MIN_ENTER_DELTA_MINUTES and queue rules).',
              });
            }
            if (applyBuffer) {
              const preBufferResult = result;
              const derivedAfterPlus = await deriveGpsLayerAfterVineFencePlus(
                job,
                {
                  windowMinutes,
                  device: deviceForTracking,
                  positionAfter: effectivePositionAfter,
                  positionBefore: effectivePositionBefore,
                  jobEndCeilingBufferMinutes,
                  step5ExtendWineryExitMinutes,
                  tentativeVineyardEnterForStep1Bracket,
                },
                {
                  step1:
                    result.step1Gps != null
                      ? { value: result.step1Gps, trackingId: result.step1TrackingId }
                      : null,
                  step2: step2ForBuffer,
                  step3: step3ForBuffer,
                },
                result.debug
              );
              const fin = finalizeDerivedSteps(
                {
                  ...derivedAfterPlus,
                  step2Via: bufferVia,
                  step3Via: hadBothPolygonGps ? 'GPS' : bufferVia,
                },
                job
              );
              const bufferGpsOk = fin.step2Gps != null && fin.step3Gps != null;
              if (!bufferGpsOk) {
                result = { ...preBufferResult, debug: result.debug };
                Object.assign(stepsPlusReport, {
                  outcome: 'buffer_pipeline_guardrails_failed',
                  detail:
                    'Buffered enter/exit were merged but re-derived GPS layer failed guardrails (step 2/3 GPS cleared). Previous polygon pass kept.',
                });
              } else {
                stepsPlusGpsStarMerge = usedGpsStarMerge;
                result = { ...fin, debug: result.debug };
                Object.assign(stepsPlusReport, {
                  outcome:
                    hadBothPolygonGps && bufferVia === 'VineFenceV+' ? 'applied_vinefence_v_plus' : 'applied_vinefence_plus',
                });
                if (hadBothPolygonGps && bufferVia === 'VineFenceV+') {
                  const deltaMin = vineyardEnterMinutesEarlierThanPolygon(mergedEnter, preBufferResult.step2Gps!);
                  vineFenceVPlusCalcnote =
                    deltaMin > 0 ? `VineFenceV+(${deltaMin} min enter earlier):` : 'VineFenceV+:';
                  Object.assign(stepsPlusReport, { vineFenceVPlusDeltaMinutes: deltaMin });
                }
              }
            }
          }
        }
      }
    }

    if (writeBack) {
      try {
        // Step_N_GPS_completed_at = GPS layer times: raw polygon ENTER/EXIT, VineFence+/VineFenceV+ buffered stay, GPS*, VineSR1, etc.
        // Via distinguishes source; GPS columns always hold the derived timestamp when present (not VWork-only).
        await execute(
          `UPDATE tbl_vworkjobs SET
            step_1_gps_completed_at = $1::timestamp,
            step1_gps_id = $2,
            step_2_gps_completed_at = $3::timestamp,
            step2_gps_id = $4,
            step_3_gps_completed_at = $5::timestamp,
            step3_gps_id = $6,
            step_4_gps_completed_at = $7::timestamp,
            step4_gps_id = $8,
            step_5_gps_completed_at = $9::timestamp,
            step5_gps_id = $10
          WHERE job_id::text = $11`,
          [
            result.step1Gps ?? null,
            result.step1TrackingId ?? null,
            result.step2Gps ?? null,
            result.step2TrackingId ?? null,
            result.step3Gps ?? null,
            result.step3TrackingId ?? null,
            result.step4Gps ?? null,
            result.step4TrackingId ?? null,
            result.step5Gps ?? null,
            result.step5TrackingId ?? null,
            jobId,
          ]
        );
        const step1Actual =
          result.step1 ??
          result.step1ActualOverride ??
          (pickTrim('step_1_completed_at', 'Step_1_Completed_At') ||
            pickTrim('actual_start_time', 'Actual_Start_Time') ||
            null);
        const step1Via = result.step1Via ?? (result.step1ActualOverride != null ? 'RULE' : (result.step1Gps != null ? 'GPS' : 'VW'));
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
        if (vineFenceVPlusCalcnote) {
          await execute(
            `UPDATE tbl_vworkjobs SET calcnotes = COALESCE(TRIM(calcnotes) || ' ', '') || $2
             WHERE job_id::text = $1`,
            [jobId, vineFenceVPlusCalcnote]
          );
        }
        if (result.step3Via === 'GPS*' || stepsPlusGpsStarMerge) {
          await execute(
            `UPDATE tbl_vworkjobs SET calcnotes = COALESCE(TRIM(calcnotes) || ' ', '') || 'GPS*:'
             WHERE job_id::text = $1`,
            [jobId]
          );
        }
        if (result.step2Via === 'VineSR1' || result.step3Via === 'VineSR1') {
          await execute(
            `UPDATE tbl_vworkjobs SET calcnotes = COALESCE(TRIM(calcnotes) || ' ', '') || 'VineSR1:'
             WHERE job_id::text = $1`,
            [jobId]
          );
        }
        if (result.step3Via === 'Step3windback') {
          await execute(
            `UPDATE tbl_vworkjobs SET calcnotes = COALESCE(TRIM(calcnotes) || ' ', '') || 'Step3windback:'
             WHERE job_id::text = $1`,
            [jobId]
          );
        }
      } catch (writeErr) {
        const wMsg = writeErr instanceof Error ? writeErr.message : String(writeErr);
        return NextResponse.json(
          sanitizeForJson({
            ...result,
            initialPass,
            stepsPlusReport,
            error: `Derived steps OK but write-back failed: ${wMsg}`,
            debug: {
              ...result.debug,
              lastJobEndStep1Limiter,
              writeBackError: wMsg,
              cleanupRulesReport: result.cleanupRulesReport,
            },
          }),
          { status: 500 }
        );
      }
    }

    return NextResponse.json(
      sanitizeForJson({
        ...result,
        initialPass,
        stepsPlusReport,
        step1LastJobEnd: step1LastJobEndReport,
        debug: {
          ...result.debug,
          lastJobEndStep1Limiter,
          cleanupRulesReport: result.cleanupRulesReport,
        },
      })
    );
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

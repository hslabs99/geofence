/**
 * One-off: load job + run deriveGpsStepsForJob; print why a tbl_tracking id was or was not used.
 * Usage (from web/): npx tsx scripts/diagnose-job-gps.ts <jobId> [trackingId]
 * Requires DATABASE_URL or PG* env (same as @/lib/db).
 */
import { query } from '../src/lib/db';
import { addMinutesToTimestampAsNZ } from '../src/lib/fetch-steps';
import {
  deriveGpsStepsForJob,
  type JobForDerivedSteps,
} from '../src/lib/derived-steps';

function pick(row: Record<string, unknown>, ...keys: string[]): unknown {
  for (const k of keys) {
    if (row[k] !== undefined) return row[k];
  }
  return null;
}

async function main() {
  const jobId = process.argv[2]?.trim() ?? '';
  const wantTid = process.argv[3] != null ? parseInt(String(process.argv[3]).trim(), 10) : NaN;
  if (!jobId) {
    console.error('Usage: npx tsx scripts/diagnose-job-gps.ts <jobId> [tbl_tracking_id]');
    process.exit(1);
  }

  const jobRows = await query<Record<string, unknown>>(
    'SELECT * FROM tbl_vworkjobs WHERE job_id::text = $1 LIMIT 1',
    [jobId]
  );
  const rawJob = jobRows[0];
  if (!rawJob) {
    console.log(JSON.stringify({ error: 'job not found', jobId }, null, 2));
    process.exit(1);
  }

  const job: JobForDerivedSteps = {
    job_id: pick(rawJob, 'job_id', 'Job_ID'),
    vineyard_name: (pick(rawJob, 'vineyard_name', 'Vineyard_Name') as string | null) ?? undefined,
    delivery_winery: (pick(rawJob, 'delivery_winery', 'Delivery_Winery') as string | null) ?? undefined,
    truck_id: (pick(rawJob, 'truck_id', 'Truck_ID') as string | null) ?? undefined,
    worker: (pick(rawJob, 'worker', 'Worker') as string | null) ?? undefined,
    actual_start_time: (pick(rawJob, 'actual_start_time', 'Actual_Start_Time') as string | null) ?? undefined,
    actual_end_time: (pick(rawJob, 'actual_end_time', 'Actual_End_Time') as string | null) ?? undefined,
    step_5_completed_at: (pick(rawJob, 'step_5_completed_at', 'Step_5_Completed_At') as string | null) ?? undefined,
    step_1_completed_at: (pick(rawJob, 'step_1_completed_at', 'Step_1_Completed_At') as string | null) ?? undefined,
    step_2_completed_at: (pick(rawJob, 'step_2_completed_at', 'Step_2_Completed_At') as string | null) ?? undefined,
    step_3_completed_at: (pick(rawJob, 'step_3_completed_at', 'Step_3_Completed_At') as string | null) ?? undefined,
    step_4_completed_at: (pick(rawJob, 'step_4_completed_at', 'Step_4_Completed_At') as string | null) ?? undefined,
    step1oride: (pick(rawJob, 'step1oride', 'Step1oride') as string | null) ?? undefined,
    step2oride: (pick(rawJob, 'step2oride', 'Step2oride') as string | null) ?? undefined,
    step3oride: (pick(rawJob, 'step3oride', 'Step3oride') as string | null) ?? undefined,
    step4oride: (pick(rawJob, 'step4oride', 'Step4oride') as string | null) ?? undefined,
    step5oride: (pick(rawJob, 'step5oride', 'Step5oride') as string | null) ?? undefined,
  };

  const pickTrim = (...keys: string[]): string => {
    for (const k of keys) {
      const v = rawJob[k];
      if (v != null && String(v).trim() !== '') return String(v).trim();
    }
    return '';
  };

  const device =
    job.worker != null && String(job.worker).trim() !== '' ? String(job.worker).trim() : String(job.truck_id ?? '').trim();
  if (!device) {
    console.log(JSON.stringify({ error: 'no worker/truck for device', job }, null, 2));
    process.exit(1);
  }

  const start = pickTrim('actual_start_time', 'Actual_Start_Time', 'planned_start_time', 'Planned_Start_Time');
  if (!start) {
    console.log(JSON.stringify({ error: 'no job start time', jobId }, null, 2));
    process.exit(1);
  }
  const positionAfter = addMinutesToTimestampAsNZ(start, -120);
  const end = pickTrim('actual_end_time', 'Actual_End_Time', 'gps_end_time', 'Gps_End_Time');
  const positionBefore = end ? addMinutesToTimestampAsNZ(end, 60) : addMinutesToTimestampAsNZ(start, 24 * 60 + 60);

  const result = await deriveGpsStepsForJob(job, {
    windowMinutes: 5,
    device,
    positionAfter,
    positionBefore,
    jobEndCeilingBufferMinutes: 35,
    step5ExtendWineryExitMinutes: 0,
  });

  const d = result.debug;
  const part1 = d?.vineyard?.part1FetchGuardrail;
  const s2 = d?.vineyard?.step2;
  const out: Record<string, unknown> = {
    jobId,
    device,
    positionAfter,
    positionBefore,
    step2Gps: result.step2Gps,
    step2TrackingId: result.step2TrackingId,
    part1FetchGuardrail: part1 ?? null,
    vineyardStep2Debug: s2
      ? {
          found: s2.found,
          position_time_nz: s2.position_time_nz,
          trackingId: s2.trackingId,
          matchedGeofenceId: s2.matchedGeofenceId,
          matchedFenceName: s2.matchedFenceName,
          device: s2.device,
          fenceIds: s2.fenceIds,
          positionAfter: s2.positionAfter,
          positionBefore: s2.positionBefore,
        }
      : null,
  };

  if (Number.isFinite(wantTid)) {
    const rows = await query<Record<string, unknown>>(
      `SELECT t.id, t.device_name, t.geofence_id, g.fence_name, t.geofence_type,
              to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz
       FROM tbl_tracking t
       LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
       WHERE t.id = $1
       LIMIT 1`,
      [wantTid]
    );
    out.trackingRowById = rows[0] ?? null;
    out.trackingIdMatchesPart1Winner = result.step2TrackingId === wantTid;
    out.trackingIdMatchesPreclear =
      part1?.preclearStep2?.trackingId != null && part1.preclearStep2.trackingId === wantTid;
  }

  console.log(JSON.stringify(out, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

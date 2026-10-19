import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { ensureCurrentLocationsSchema, fillCurrentLocationGpsTimeNz } from '@/lib/ensure-current-locations-schema';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { readNextGateProximity } from '@/lib/gate-proximity';
import { loadCurrentLocationSettings } from '@/lib/current-location-poll';
import { DRIVER_JOB_COLUMNS, type DriverJob } from '@/lib/driver-jobs';

/**
 * Distance and fence-transition marker for the next gate.
 * Step 2 and step 4: green while closing on the fence, red once inside.
 * Step 3: green just outside the vineyard, red once farther than the metre limit.
 */
export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    await ensureCurrentLocationsSchema();
    await fillCurrentLocationGpsTimeNz();
    const driverAppPollSeconds = (await loadCurrentLocationSettings()).driverAppPollSeconds;
    const { id } = await params;
    const jobId = Number(id);
    if (!Number.isInteger(jobId) || jobId <= 0) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }
    const jobs = await query<DriverJob>(`SELECT ${DRIVER_JOB_COLUMNS} FROM tbl_driver_jobs WHERE id = $1`, [jobId]);
    const job = jobs[0];
    if (!job) return NextResponse.json({ error: 'Job not found' }, { status: 404 });

    const reading = await readNextGateProximity(job);
    if (!reading) return NextResponse.json({ active: false, driverAppPollSeconds });

    return NextResponse.json({
      active: true,
      step: reading.step,
      target: reading.target,
      fence: reading.fence,
      truck: job.truck_id,
      meters: reading.meters,
      inside: reading.inside,
      beenInside: reading.beenInside,
      proximity: reading.proximity,
      crossedAt: reading.crossedAt,
      limitMeters: reading.limitMeters,
      gpstimeNz: reading.gpstimeNz,
      fetchedAt: reading.fetchedAt,
      driverAppPollSeconds,
      error: reading.meters == null ? reading.warning : undefined,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

import { query } from '@/lib/db';
import { loadCurrentLocationSettings } from '@/lib/current-location-poll';
import { completedGateCount, type DriverJob } from '@/lib/driver-jobs';

export type GateProximity = {
  step: 2 | 3 | 4;
  target: 'vineyard' | 'winery';
  fence: string | null;
  meters: number | null;
  inside: boolean;
  beenInside: boolean;
  proximity: 'green' | 'red' | null;
  /** New Zealand GPS time of the fence crossing, when the marker is red. */
  crossedAt: string | null;
  limitMeters: number;
  gpstimeNz: string | null;
  fetchedAt: string | null;
  /** Set when a click now would be a false fence transition. */
  warning: string | null;
};

type FixRow = {
  gpstime_nz: string | null;
  polltime: string | null;
  meters: number | null;
};

function fenceFor(job: DriverJob, step: 2 | 3 | 4): { target: 'vineyard' | 'winery'; fence: string } {
  const target = step === 4 ? 'winery' : 'vineyard';
  const fence = (target === 'vineyard' ? job.vineyard_geo_fence || job.vineyard_name : job.winery_geo_fence || job.winery_name)?.trim() || '';
  return { target, fence };
}

function sinceFor(job: DriverJob, step: 2 | 3 | 4): string {
  const stamp = step === 4 ? job.step_3_click : job.step_1_click;
  return stamp?.trim() ?? '';
}

function awayText(meters: number): string {
  const shown = meters.toLocaleString('en-NZ');
  return `${shown} ${meters === 1 ? 'meter' : 'meters'}`;
}

/** Fixes for this device since the previous gate, oldest first, with metres to the fence edge. */
async function fixesSince(imei: string, truck: string, fence: string, since: string): Promise<FixRow[]> {
  return query<FixRow>(
    `SELECT to_char(c.gpstime_nz, 'YYYY-MM-DD HH24:MI:SS') AS gpstime_nz,
            c.polltime,
            round(MIN(
              ST_Distance(
                ST_Transform(ST_Force2D(g.geom), 4326)::geography,
                ST_SetSRID(ST_MakePoint(
                  split_part(c.coordinates, ',', 2)::float8,
                  split_part(c.coordinates, ',', 1)::float8
                ), 4326)::geography
              )
            ))::int AS meters
     FROM (
       SELECT id, gpstime_nz, polltime, coordinates
       FROM tbl_current_locations
       WHERE coordinates ~ '^-?[0-9]+(\\.[0-9]+)?,-?[0-9]+(\\.[0-9]+)?$'
         AND (
           ($1 <> '' AND btrim(imei) = $1)
           OR ($2 <> '' AND lower(btrim(truck_id)) = lower(btrim($2)))
         )
         AND (
           $4 = ''
           OR gpstime_nz >= $4::timestamp
           OR polltime >= $4
           OR id = (
             SELECT id
             FROM tbl_current_locations
             WHERE (
               ($1 <> '' AND btrim(imei) = $1)
               OR ($2 <> '' AND lower(btrim(truck_id)) = lower(btrim($2)))
             )
             ORDER BY id DESC
             LIMIT 1
           )
         )
       ORDER BY id DESC
       LIMIT 400
     ) c
     JOIN tbl_geofences g
       ON g.geom IS NOT NULL
      AND lower(btrim(g.fence_name)) = lower(btrim($3))
     GROUP BY c.id, c.gpstime_nz, c.polltime
     ORDER BY c.id ASC`,
    [imei, truck, fence, since],
  );
}

function readingFromFixes(step: 2 | 3 | 4, target: 'vineyard' | 'winery', fence: string, limitMeters: number, fixes: FixRow[]): GateProximity {
  const latest = fixes.length > 0 ? fixes[fixes.length - 1] : null;
  const meters = latest?.meters == null ? null : Number(latest.meters);
  const inside = meters === 0;
  const beenInside = fixes.some((fix) => fix.meters === 0);
  let proximity: 'green' | 'red' | null = null;
  let crossedAt: string | null = null;

  if (meters != null && (step === 2 || step === 4)) {
    if (inside) {
      proximity = 'red';
      const entry = [...fixes].reverse().find((fix) => fix.meters !== 0);
      const entryIndex = entry ? fixes.indexOf(entry) + 1 : 0;
      crossedAt = fixes[entryIndex]?.gpstime_nz ?? latest?.gpstime_nz ?? null;
    } else if (meters <= limitMeters) {
      proximity = 'green';
    }
  }

  if (meters != null && step === 3 && beenInside && !inside) {
    const lastInside = fixes.reduce((found, fix, index) => (fix.meters === 0 ? index : found), -1);
    const left = fixes[lastInside + 1];
    if (meters <= limitMeters) proximity = 'green';
    else {
      proximity = 'red';
      crossedAt = left?.gpstime_nz ?? latest?.gpstime_nz ?? null;
    }
  }

  return {
    step,
    target,
    fence,
    meters,
    inside,
    beenInside,
    proximity,
    crossedAt,
    limitMeters,
    gpstimeNz: latest?.gpstime_nz ?? null,
    fetchedAt: latest?.polltime ?? null,
    warning: warningFor(step, target, fence, meters, inside, beenInside, limitMeters),
  };
}

function warningFor(
  step: 2 | 3 | 4,
  target: 'vineyard' | 'winery',
  fence: string,
  meters: number | null,
  inside: boolean,
  beenInside: boolean,
  limitMeters: number,
): string | null {
  if (step === 3) {
    if (!fence) return 'This job has no vineyard GPS fence. Please confirm you are leaving the vineyard now.';
    if (meters == null) return 'We have no GPS reading for this truck. Please confirm you are leaving the vineyard now.';
    if (inside) return 'You are still inside the vineyard. Please confirm you are leaving the vineyard now.';
    if (!beenInside) return 'You have not been in the vineyard. Please confirm you are leaving the vineyard now.';
    return null;
  }
  if (!fence) return `This job has no ${target} GPS fence. Please confirm you are arriving at the ${target}.`;
  if (meters == null) return `We have no GPS reading for this truck. Please confirm you are arriving at the ${target}.`;
  if (meters <= limitMeters) return null;
  return `Are you sure you are arriving at the ${target}? Your last GPS reading has you ${awayText(meters)} away from it.`;
}

/** Distance and fence-transition marker for one gate. Step 4 always uses the winery fence. */
export async function readStepProximity(job: DriverJob, step: 2 | 3 | 4): Promise<GateProximity> {
  const settings = await loadCurrentLocationSettings();
  const { target, fence } = fenceFor(job, step);
  const limitMeters = settings.confirmAwayMeters;
  if (!fence) {
    return {
      step,
      target,
      fence: null,
      meters: null,
      inside: false,
      beenInside: false,
      proximity: null,
      crossedAt: null,
      limitMeters,
      gpstimeNz: null,
      fetchedAt: null,
      warning: warningFor(step, target, '', null, false, false, limitMeters),
    };
  }
  const fixes = await fixesSince(job.imei?.trim() || '', job.truck_id?.trim() || '', fence, sinceFor(job, step));
  return readingFromFixes(step, target, fence, limitMeters, fixes);
}

/** Proximity for whichever of gates 2, 3, or 4 is the next click. */
export async function readNextGateProximity(job: DriverJob): Promise<GateProximity | null> {
  const done = completedGateCount(job);
  const step = done === 1 ? 2 : done === 2 ? 3 : done === 3 ? 4 : null;
  if (!step) return null;
  return readStepProximity(job, step);
}

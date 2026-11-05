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
  proximity: 'yellow' | 'red' | null;
  /**
   * New Zealand GPS time of the fence exit, when the red mark is a real crossing.
   * Empty for the heading-to-winery reminder, because that tap is the departure record.
   */
  crossedAt: string | null;
  /** True when the red mark is a reminder to tap gate 3, not a fence exit. */
  remind: boolean;
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

/**
 * Marker the driver button shows for an arrival checkpoint.
 * Yellow is the soft approach (within the confirm distance, still outside).
 * Red is inside the arrival fence.
 * Gate 3 uses vineyardDeparture instead of this.
 */
export function fenceProximity(
  step: 2 | 3 | 4,
  meters: number | null,
  beenInside: boolean,
  limitMeters: number,
): 'yellow' | 'red' | null {
  if (meters == null) return null;
  if (step === 2 || step === 4) {
    if (meters === 0) return 'red';
    if (meters <= limitMeters) return 'yellow';
    return null;
  }
  if (beenInside && meters !== 0) return 'red';
  return null;
}

function readingFromFixes(step: 2 | 3 | 4, target: 'vineyard' | 'winery', fence: string, limitMeters: number, fixes: FixRow[]): GateProximity {
  const latest = fixes.length > 0 ? fixes[fixes.length - 1] : null;
  const meters = latest?.meters == null ? null : Number(latest.meters);
  const inside = meters === 0;
  const beenInside = fixes.some((fix) => fix.meters === 0);
  const proximity = fenceProximity(step, meters, beenInside, limitMeters);
  let crossedAt: string | null = null;

  if (proximity === 'red' && (step === 2 || step === 4)) {
    const entry = [...fixes].reverse().find((fix) => fix.meters !== 0);
    const entryIndex = entry ? fixes.indexOf(entry) + 1 : 0;
    crossedAt = fixes[entryIndex]?.gpstime_nz ?? latest?.gpstime_nz ?? null;
  }

  // Leaving the vineyard has no approach warning. The first fix outside is already the tap.
  if (proximity === 'red' && step === 3) {
    const lastInside = fixes.reduce((found, fix, index) => (fix.meters === 0 ? index : found), -1);
    const left = fixes[lastInside + 1];
    crossedAt = left?.gpstime_nz ?? latest?.gpstime_nz ?? null;
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
    remind: false,
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

export type DepartureFix = {
  gpstimeNz: string | null;
  polltime: string | null;
  vineyardMeters: number | null;
  wineryMeters: number | null;
};

export type VineyardDeparture = {
  proximity: 'red' | null;
  crossedAt: string | null;
  /** Reminder only. The driver's gate 3 tap is the departure time. */
  remind: boolean;
};

const NO_DEPARTURE: VineyardDeparture = { proximity: null, crossedAt: null, remind: false };

function metersOf(value: unknown): number | null {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? Math.round(n) : null;
}

function atOrBefore(fix: DepartureFix, step2At: string): boolean {
  if (!step2At) return false;
  const gps = fix.gpstimeNz?.trim() ?? '';
  const polled = fix.polltime?.trim() ?? '';
  return (gps !== '' && gps <= step2At) || (polled !== '' && polled <= step2At);
}

function usableFixes(fixes: DepartureFix[]): DepartureFix[] {
  return fixes.filter((fix) => fix.vineyardMeters != null);
}

/** Last fix at the gate 2 tap, then every later fix. The drive to the vineyard stays out of the later list. */
function splitAtGate2(fixes: DepartureFix[], step2At: string): { arrival: DepartureFix | null; after: DepartureFix[]; all: DepartureFix[] } {
  const all = usableFixes(fixes);
  let arrivalIndex = -1;
  for (let i = 0; i < all.length; i++) {
    if (atOrBefore(all[i], step2At)) arrivalIndex = i;
  }
  if (arrivalIndex === -1 && all.length > 0) arrivalIndex = 0;
  if (arrivalIndex === -1) return { arrival: null, after: [], all };
  return { arrival: all[arrivalIndex], after: all.slice(arrivalIndex + 1), all };
}

/** Red while the latest fix is outside the polygon. The time is the fix after the last inside fix. */
function polygonExit(fixes: DepartureFix[]): VineyardDeparture {
  const lastInside = fixes.reduce((found, fix, index) => (fix.vineyardMeters === 0 ? index : found), -1);
  const latest = fixes.length > 0 ? fixes[fixes.length - 1] : null;
  if (lastInside < 0 || latest == null || latest.vineyardMeters === 0) return NO_DEPARTURE;
  const left = fixes[lastInside + 1] ?? latest;
  return { proximity: 'red', crossedAt: left.gpstimeNz ?? latest.gpstimeNz ?? null, remind: false };
}

/**
 * Gate 3 marker after gate 2.
 * A tap inside the polygon still goes red on the first later fix outside that polygon.
 * A tap inside the soft band goes red on the first later fix outside the band, and stays red.
 * A manual tap outside the band goes red once two fixes after that tap are each farther from the vineyard
 * and closer to the winery, unless a later fix enters the polygon. That red is only a reminder.
 */
export function vineyardDeparture(input: {
  step2Manual: boolean;
  step2At: string;
  limitMeters: number;
  fixes: DepartureFix[];
}): VineyardDeparture {
  const { arrival, after, all } = splitAtGate2(input.fixes, input.step2At);
  if (arrival?.vineyardMeters === 0) return polygonExit(all);

  if (!input.step2Manual) {
    const exit = after.find((fix) => (fix.vineyardMeters ?? 0) > input.limitMeters);
    if (!exit) return NO_DEPARTURE;
    return { proximity: 'red', crossedAt: exit.gpstimeNz ?? null, remind: false };
  }

  const enteredAt = after.findIndex((fix) => fix.vineyardMeters === 0);
  if (enteredAt >= 0) return polygonExit(after.slice(enteredAt));

  // Both readings are after the tap, so the loading fix itself is not the start of the trip.
  for (let i = 1; i < after.length; i++) {
    const prev = after[i - 1];
    const cur = after[i];
    if (prev.vineyardMeters == null || cur.vineyardMeters == null) continue;
    if (prev.wineryMeters == null || cur.wineryMeters == null) continue;
    if (cur.vineyardMeters > prev.vineyardMeters && cur.wineryMeters < prev.wineryMeters) {
      return { proximity: 'red', crossedAt: null, remind: true };
    }
  }
  return NO_DEPARTURE;
}

type DepartureJob = {
  id: number;
  imei: string;
  truck: string;
  since: string;
  vineyardFence: string;
  wineryFence: string;
};

type DepartureSqlRow = {
  id: number;
  gpstime_nz: string | null;
  polltime: string | null;
  vineyard_meters: number | null;
  winery_meters: number | null;
};

/** Fixes since gate 1 for each job, oldest first, with metres to the vineyard and the winery. */
export async function loadDepartureFixes(jobs: DepartureJob[]): Promise<Map<number, DepartureFix[]>> {
  const wanted = jobs.filter((job) => job.vineyardFence.trim() !== '');
  const tracks = new Map<number, DepartureFix[]>();
  if (wanted.length === 0) return tracks;
  const rows = await query<DepartureSqlRow>(
    `SELECT j.id,
            c.gpstime_nz,
            c.polltime,
            round(MIN(ST_Distance(
              ST_Transform(ST_Force2D(g.geom), 4326)::geography,
              ST_SetSRID(ST_MakePoint(
                split_part(c.coordinates, ',', 2)::float8,
                split_part(c.coordinates, ',', 1)::float8
              ), 4326)::geography
            )) FILTER (WHERE lower(btrim(g.fence_name)) = lower(btrim(j.vineyard_fence))))::int AS vineyard_meters,
            round(MIN(ST_Distance(
              ST_Transform(ST_Force2D(g.geom), 4326)::geography,
              ST_SetSRID(ST_MakePoint(
                split_part(c.coordinates, ',', 2)::float8,
                split_part(c.coordinates, ',', 1)::float8
              ), 4326)::geography
            )) FILTER (
              WHERE j.winery_fence <> ''
                AND lower(btrim(g.fence_name)) = lower(btrim(j.winery_fence))
            ))::int AS winery_meters
     FROM unnest($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])
       AS j(id, imei, truck, since_at, vineyard_fence, winery_fence)
     CROSS JOIN LATERAL (
       SELECT id,
              to_char(gpstime_nz, 'YYYY-MM-DD HH24:MI:SS') AS gpstime_nz,
              polltime,
              coordinates
       FROM tbl_current_locations c
       WHERE c.coordinates ~ '^-?[0-9]+(\\.[0-9]+)?,-?[0-9]+(\\.[0-9]+)?$'
         AND (
           (j.imei <> '' AND btrim(c.imei) = j.imei)
           OR (j.truck <> '' AND lower(btrim(c.truck_id)) = lower(btrim(j.truck)))
         )
         AND (
           j.since_at = ''
           OR c.gpstime_nz >= NULLIF(j.since_at, '')::timestamp
           OR c.polltime >= NULLIF(j.since_at, '')
           OR c.id = (
             SELECT c2.id
             FROM tbl_current_locations c2
             WHERE (
               (j.imei <> '' AND btrim(c2.imei) = j.imei)
               OR (j.truck <> '' AND lower(btrim(c2.truck_id)) = lower(btrim(j.truck)))
             )
             ORDER BY c2.id DESC
             LIMIT 1
           )
         )
       ORDER BY c.id DESC
       LIMIT 400
     ) c
     JOIN tbl_geofences g
       ON g.geom IS NOT NULL
      AND (
        (j.vineyard_fence <> '' AND lower(btrim(g.fence_name)) = lower(btrim(j.vineyard_fence)))
        OR (j.winery_fence <> '' AND lower(btrim(g.fence_name)) = lower(btrim(j.winery_fence)))
      )
     GROUP BY j.id, c.id, c.gpstime_nz, c.polltime
     ORDER BY j.id, c.id ASC`,
    [
      wanted.map((job) => job.id),
      wanted.map((job) => job.imei),
      wanted.map((job) => job.truck),
      wanted.map((job) => job.since),
      wanted.map((job) => job.vineyardFence),
      wanted.map((job) => job.wineryFence),
    ],
  );
  for (const row of rows) {
    const id = Number(row.id);
    const list = tracks.get(id) ?? [];
    list.push({
      gpstimeNz: row.gpstime_nz?.trim() || null,
      polltime: row.polltime?.trim() || null,
      vineyardMeters: metersOf(row.vineyard_meters),
      wineryMeters: metersOf(row.winery_meters),
    });
    tracks.set(id, list);
  }
  return tracks;
}

function departureWarning(
  fence: string,
  meters: number | null,
  inside: boolean,
  beenInside: boolean,
  limitMeters: number,
  depart: VineyardDeparture,
): string | null {
  if (depart.proximity === 'red' && depart.remind) {
    return 'You have not been in the vineyard. Please confirm you are leaving the vineyard now.';
  }
  if (depart.proximity === 'red' && !inside) return null;
  return warningFor(3, 'vineyard', fence, meters, inside, beenInside, limitMeters);
}

async function readDepartureProximity(job: DriverJob, fence: string, limitMeters: number): Promise<GateProximity> {
  const tracks = await loadDepartureFixes([
    {
      id: job.id,
      imei: job.imei?.trim() || '',
      truck: job.truck_id?.trim() || '',
      since: job.step_1_click?.trim() ?? '',
      vineyardFence: fence,
      wineryFence: (job.winery_geo_fence || job.winery_name)?.trim() || '',
    },
  ]);
  const fixes = tracks.get(job.id) ?? [];
  const latest = fixes.length > 0 ? fixes[fixes.length - 1] : null;
  const meters = latest?.vineyardMeters ?? null;
  const inside = meters === 0;
  const beenInside = fixes.some((fix) => fix.vineyardMeters === 0);
  const depart = vineyardDeparture({
    step2Manual: job.step_2_manual === true,
    step2At: job.step_2_click?.trim() ?? '',
    limitMeters,
    fixes,
  });
  return {
    step: 3,
    target: 'vineyard',
    fence,
    meters,
    inside,
    beenInside,
    proximity: depart.proximity,
    crossedAt: depart.crossedAt,
    remind: depart.remind,
    limitMeters,
    gpstimeNz: latest?.gpstimeNz ?? null,
    fetchedAt: latest?.polltime ?? null,
    warning: departureWarning(fence, meters, inside, beenInside, limitMeters, depart),
  };
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
      remind: false,
      limitMeters,
      gpstimeNz: null,
      fetchedAt: null,
      warning: warningFor(step, target, '', null, false, false, limitMeters),
    };
  }
  if (step === 3) return readDepartureProximity(job, fence, limitMeters);
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

/** Driver phone app gates. Staff screens do not import this. */

export const DRIVER_GATES = [
  { step: 1, key: 'step_1_click', title: 'Start job' },
  { step: 2, key: 'step_2_click', title: 'Arrive vineyard' },
  { step: 3, key: 'step_3_click', title: 'Depart vineyard' },
  { step: 4, key: 'step_4_click', title: 'Arrive winery' },
  { step: 5, key: 'step_5_click', title: 'End job' },
] as const;

export type DriverGateKey = (typeof DRIVER_GATES)[number]['key'];

export type DriverJob = {
  id: number;
  job_id: string;
  truck_id: string | null;
  imei: string | null;
  driver: string | null;
  planned_start_time: string | null;
  winery_name: string | null;
  vineyard_name: string | null;
  winery_geo_fence: string | null;
  vineyard_geo_fence: string | null;
  step_1_click: string | null;
  step_2_click: string | null;
  step_3_click: string | null;
  step_4_click: string | null;
  step_5_click: string | null;
  /** True when the driver confirmed the click while GPS did not. */
  step_2_manual: boolean | null;
  step_3_manual: boolean | null;
  step_4_manual: boolean | null;
};

export const DRIVER_JOB_COLUMNS = `
  id,
  job_id,
  truck_id,
  imei,
  driver,
  to_char(planned_start_time, 'YYYY-MM-DD HH24:MI:SS') AS planned_start_time,
  winery_name,
  vineyard_name,
  winery_geo_fence,
  vineyard_geo_fence,
  to_char(step_1_click, 'YYYY-MM-DD HH24:MI:SS') AS step_1_click,
  to_char(step_2_click, 'YYYY-MM-DD HH24:MI:SS') AS step_2_click,
  to_char(step_3_click, 'YYYY-MM-DD HH24:MI:SS') AS step_3_click,
  to_char(step_4_click, 'YYYY-MM-DD HH24:MI:SS') AS step_4_click,
  to_char(step_5_click, 'YYYY-MM-DD HH24:MI:SS') AS step_5_click,
  step_2_manual,
  step_3_manual,
  step_4_manual
`;

/** The driver confirmed this gate while the GPS reading did not. */
export function gateManual(job: Pick<DriverJob, 'step_2_manual' | 'step_3_manual' | 'step_4_manual'>, step: number): boolean {
  if (step === 2) return job.step_2_manual === true;
  if (step === 3) return job.step_3_manual === true;
  if (step === 4) return job.step_4_manual === true;
  return false;
}

export type TruckLoad = 'empty' | 'half' | 'full';

/** Truck on the gate strip after a step is passed, until the job is finished. */
export function truckStage(done: number): { label: string; load: TruckLoad } | null {
  if (done === 1) return { label: 'To\nVineyard', load: 'empty' };
  if (done === 2) return { label: 'Loading', load: 'half' };
  if (done === 3) return { label: 'To\nWinery', load: 'full' };
  if (done === 4) return { label: 'Unloading', load: 'empty' };
  return null;
}

/**
 * Whole minutes from an earlier stamp to a later one.
 * Both values are the stored clock strings; the difference is not timezone-shifted.
 * Returns null when either stamp is missing or the later stamp is earlier.
 */
export function minutesBetween(earlier: string | null | undefined, later: string | null | undefined): number | null {
  const start = stampUtcMs(earlier);
  const end = stampUtcMs(later);
  if (start == null || end == null) return null;
  const delta = end - start;
  if (delta < 0) return null;
  return Math.round(delta / 60000);
}

/** Parse YYYY-MM-DD HH:MM:SS as calendar numbers. Difference math uses UTC fields so the clock is not shifted. */
function stampUtcMs(stamp: string | null | undefined): number | null {
  if (stamp == null) return null;
  const match = String(stamp).trim().match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  return Date.UTC(year, month - 1, day, hour, minute, second);
}

/**
 * After leaving the vineyard (step 3), winery ETA is that time plus the vineyard trip (step 2 − step 1).
 * Returns HH:MM, or dd/mm HH:MM when the estimate falls on another day.
 */
export function wineryArrivalEta(job: Pick<DriverJob, 'step_1_click' | 'step_2_click' | 'step_3_click'>): string | null {
  const started = stampUtcMs(job.step_1_click);
  const atVineyard = stampUtcMs(job.step_2_click);
  const departed = stampUtcMs(job.step_3_click);
  if (started == null || atVineyard == null || departed == null) return null;
  const travelMs = atVineyard - started;
  if (travelMs < 0) return null;
  const eta = new Date(departed + travelMs);
  const pad = (n: number) => String(n).padStart(2, '0');
  const clock = `${pad(eta.getUTCHours())}:${pad(eta.getUTCMinutes())}`;
  const departDay = String(job.step_3_click).trim().slice(0, 10);
  const etaDay = `${eta.getUTCFullYear()}-${pad(eta.getUTCMonth() + 1)}-${pad(eta.getUTCDate())}`;
  if (etaDay !== departDay) return `${pad(eta.getUTCDate())}/${pad(eta.getUTCMonth() + 1)} ${clock}`;
  return clock;
}

/** How many gates are done in order, stopping at the first gap. */
export function completedGateCount(job: Pick<DriverJob, DriverGateKey>): number {
  let n = 0;
  for (const gate of DRIVER_GATES) {
    const value = job[gate.key];
    if (value == null || String(value).trim() === '') break;
    n += 1;
  }
  return n;
}

/** HH:MM from a stored timestamp string. Pending gates show --:--. */
export function gateClock(stamp: string | null | undefined): string {
  if (stamp == null) return '--:--';
  const match = String(stamp).match(/(\d{2}):(\d{2})/);
  return match ? `${match[1]}:${match[2]}` : '--:--';
}

/**
 * Clock time on the phone, as a literal string. Uses local calendar fields, not UTC conversion.
 */
export function phoneLocalStamp(now = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

const STAMP = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;

export function isPhoneLocalStamp(value: string): boolean {
  return STAMP.test(value);
}

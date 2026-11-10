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
  /** VWork customer. Shown above the job id. */
  company_name: string | null;
  /** VWork group name. Shown above the job id. */
  group_name: string | null;
  truck_id: string | null;
  imei: string | null;
  driver: string | null;
  planned_start_time: string | null;
  winery_name: string | null;
  vineyard_name: string | null;
  winery_geo_fence: string | null;
  vineyard_geo_fence: string | null;
  step_0_click: string | null;
  step_1_click: string | null;
  truck_rego: string | null;
  trailer_rego: string | null;
  /** Name typed at step 0. Blank on the next job; plates are remembered on the truck. */
  confirmed_driver: string | null;
  step_2_click: string | null;
  step_3_click: string | null;
  step_4_click: string | null;
  step_5_click: string | null;
  /** True when the driver confirmed the click from outside the soft fence. */
  step_2_manual: boolean | null;
  step_3_manual: boolean | null;
  step_4_manual: boolean | null;
  /** Fence the truck was in when the driver tapped: hard, soft, or manual. */
  step_2_fence: GateFenceKind | null;
  step_3_fence: GateFenceKind | null;
  step_4_fence: GateFenceKind | null;
  /** GPS clock for the soft-band crossing nearest this tap. Verbatim, same digits as gpstime_nz. */
  step_2_soft_gps: string | null;
  step_2_hard_gps: string | null;
  step_3_soft_gps: string | null;
  step_3_hard_gps: string | null;
  step_4_soft_gps: string | null;
  step_4_hard_gps: string | null;
  /** VWork account this job was polled from: delegat or others. */
  vwork_account: string | null;
};

export const DRIVER_JOB_COLUMNS = `
  id,
  job_id,
  company_name,
  group_name,
  truck_id,
  imei,
  driver,
  to_char(planned_start_time, 'YYYY-MM-DD HH24:MI:SS') AS planned_start_time,
  winery_name,
  vineyard_name,
  winery_geo_fence,
  vineyard_geo_fence,
  to_char(step_0_click, 'YYYY-MM-DD HH24:MI:SS') AS step_0_click,
  to_char(step_1_click, 'YYYY-MM-DD HH24:MI:SS') AS step_1_click,
  truck_rego,
  trailer_rego,
  confirmed_driver,
  to_char(step_2_click, 'YYYY-MM-DD HH24:MI:SS') AS step_2_click,
  to_char(step_3_click, 'YYYY-MM-DD HH24:MI:SS') AS step_3_click,
  to_char(step_4_click, 'YYYY-MM-DD HH24:MI:SS') AS step_4_click,
  to_char(step_5_click, 'YYYY-MM-DD HH24:MI:SS') AS step_5_click,
  step_2_manual,
  step_3_manual,
  step_4_manual,
  step_2_fence,
  step_3_fence,
  step_4_fence,
  to_char(step_2_soft_gps, 'YYYY-MM-DD HH24:MI:SS') AS step_2_soft_gps,
  to_char(step_2_hard_gps, 'YYYY-MM-DD HH24:MI:SS') AS step_2_hard_gps,
  to_char(step_3_soft_gps, 'YYYY-MM-DD HH24:MI:SS') AS step_3_soft_gps,
  to_char(step_3_hard_gps, 'YYYY-MM-DD HH24:MI:SS') AS step_3_hard_gps,
  to_char(step_4_soft_gps, 'YYYY-MM-DD HH24:MI:SS') AS step_4_soft_gps,
  to_char(step_4_hard_gps, 'YYYY-MM-DD HH24:MI:SS') AS step_4_hard_gps,
  vwork_account
`;

/** Where the truck was when the driver tapped a fence gate. */
export type GateFenceKind = 'hard' | 'soft' | 'manual';

/**
 * Hard is inside the polygon. Soft is outside it but inside the confirm band.
 * Manual is outside that band, or there is no GPS fix. A vineyard departure that
 * already crossed the polygon stays hard even when the tap itself is past the band.
 */
export function gateFenceKind(input: {
  step: 2 | 3 | 4;
  meters: number | null;
  beenInside: boolean;
  limitMeters: number;
}): GateFenceKind {
  if (input.meters === 0) return 'hard';
  if (input.meters != null && input.meters <= input.limitMeters) return 'soft';
  if (input.step === 3 && input.beenInside) return 'hard';
  return 'manual';
}

function storedFence(value: string | null | undefined): GateFenceKind | null {
  if (value === 'hard' || value === 'soft' || value === 'manual') return value;
  return null;
}

/** Stored fence for a completed gate. Null on steps that are not a fence tap. */
export function gateFence(
  job: Pick<DriverJob, 'step_2_fence' | 'step_3_fence' | 'step_4_fence'>,
  step: number,
): GateFenceKind | null {
  if (step === 2) return storedFence(job.step_2_fence);
  if (step === 3) return storedFence(job.step_3_fence);
  if (step === 4) return storedFence(job.step_4_fence);
  return null;
}

/**
 * Hard, soft, or manual for a fence tap.
 * Older rows with no stored fence still count a manual confirm as manual.
 */
export function gateActivity(
  job: Pick<DriverJob, 'step_2_manual' | 'step_3_manual' | 'step_4_manual' | 'step_2_fence' | 'step_3_fence' | 'step_4_fence'>,
  step: number,
): GateFenceKind | null {
  const stored = gateFence(job, step);
  if (stored) return stored;
  if (step === 2 && job.step_2_manual === true) return 'manual';
  if (step === 3 && job.step_3_manual === true) return 'manual';
  if (step === 4 && job.step_4_manual === true) return 'manual';
  return null;
}

/** Step 0 is saved. Step 1 stays locked until this is true. */
export function jobDetailsConfirmed(job: { step_0_click?: string | null }): boolean {
  return Boolean(job.step_0_click?.trim());
}

export type MetersFix = { at: string; meters: number | null };

/**
 * GPS times for the soft band and the hard polygon nearest this tap.
 * Enter keeps the first sample inside. Leave keeps the last sample still inside.
 */
export function fenceGpsAtClick(
  fixes: MetersFix[],
  limitMeters: number,
  clickAt: string,
  edge: 'enter' | 'leave',
): { softAt: string | null; hardAt: string | null } {
  const soft: string[] = [];
  const hard: string[] = [];
  let prevSoft = false;
  let prevHard = false;
  let prevAt: string | null = null;
  for (const fix of fixes) {
    const at = fix.at?.trim() ?? '';
    if (!at || fix.meters == null) continue;
    const inSoft = fix.meters <= limitMeters;
    const inHard = fix.meters === 0;
    if (edge === 'enter') {
      if (inSoft && !prevSoft) soft.push(at);
      if (inHard && !prevHard) hard.push(at);
    } else if (prevAt) {
      if (!inSoft && prevSoft) soft.push(prevAt);
      if (!inHard && prevHard) hard.push(prevAt);
    }
    prevSoft = inSoft;
    prevHard = inHard;
    prevAt = at;
  }
  return { softAt: closestStamp(soft, clickAt), hardAt: closestStamp(hard, clickAt) };
}

function closestStamp(stamps: string[], clickAt: string): string | null {
  const click = stampUtcMs(clickAt);
  let best: string | null = null;
  let bestDist = Number.POSITIVE_INFINITY;
  for (const stamp of stamps) {
    const ms = stampUtcMs(stamp);
    if (ms == null || click == null) continue;
    const dist = Math.abs(ms - click);
    if (dist < bestDist) {
      best = stamp;
      bestDist = dist;
    }
  }
  return best ?? stamps[0] ?? null;
}

type FenceGpsJob = Pick<
  DriverJob,
  | 'step_2_click'
  | 'step_3_click'
  | 'step_4_click'
  | 'step_2_manual'
  | 'step_3_manual'
  | 'step_4_manual'
  | 'step_2_fence'
  | 'step_3_fence'
  | 'step_4_fence'
  | 'step_2_soft_gps'
  | 'step_2_hard_gps'
  | 'step_3_soft_gps'
  | 'step_3_hard_gps'
  | 'step_4_soft_gps'
  | 'step_4_hard_gps'
>;

function gpsStamp(job: FenceGpsJob, step: number, edge: 'soft' | 'hard'): string | null {
  const value =
    step === 2
      ? edge === 'soft'
        ? job.step_2_soft_gps
        : job.step_2_hard_gps
      : step === 3
        ? edge === 'soft'
          ? job.step_3_soft_gps
          : job.step_3_hard_gps
        : step === 4
          ? edge === 'soft'
            ? job.step_4_soft_gps
            : job.step_4_hard_gps
          : null;
  const stamp = value?.trim() ?? '';
  return stamp || null;
}

/** GPS clock shown under a hard or soft gate. The circle itself stays the button time. */
export function gateShownGps(job: FenceGpsJob, step: number): string | null {
  const kind = gateActivity(job, step);
  if (kind !== 'hard' && kind !== 'soft') return null;
  return gpsStamp(job, step, kind);
}

function gapSentence(earlierName: string, earlier: string, laterName: string, later: string): string | null {
  const forward = minutesBetween(earlier, later);
  if (forward === 0) return `${earlierName} and ${laterName} are the same minute`;
  if (forward != null) {
    const mins = forward === 1 ? '1 min' : `${forward} min`;
    return `${laterName} is ${mins} after ${earlierName}`;
  }
  const back = minutesBetween(later, earlier);
  if (back === 0) return `${earlierName} and ${laterName} are the same minute`;
  if (back != null) {
    const mins = back === 1 ? '1 min' : `${back} min`;
    return `${earlierName} is ${mins} after ${laterName}`;
  }
  return null;
}

/** Hover text: button time, soft GPS, hard GPS, and the gap between them. */
export function gateFenceHover(job: FenceGpsJob, step: number): string | null {
  const kind = gateActivity(job, step);
  if (kind == null || (step !== 2 && step !== 3 && step !== 4)) return null;
  const clickKey = step === 2 ? 'step_2_click' : step === 3 ? 'step_3_click' : 'step_4_click';
  const click = job[clickKey]?.trim() || '';
  const button = click || '--:--';
  if (kind === 'manual') return `Button ${button}. Outside the soft fence, so there is no GPS fence time.`;
  const soft = gpsStamp(job, step, 'soft');
  const hard = gpsStamp(job, step, 'hard');
  const verb = step === 3 ? 'Left' : 'Entered';
  const parts = [
    `Button ${button}`,
    soft ? `${verb} the soft fence ${soft}` : 'No soft fence time',
    hard ? `${verb} the hard fence ${hard}` : 'No hard fence time',
  ];
  if (soft && hard) {
    const between = gapSentence('the soft fence', soft, 'the hard fence', hard);
    if (between) parts.push(between.charAt(0).toUpperCase() + between.slice(1));
  }
  const shown = kind === 'hard' ? hard : soft;
  if (shown && click) {
    const between = gapSentence('the button', click, 'the GPS', shown);
    if (between) parts.push(between.charAt(0).toUpperCase() + between.slice(1));
  }
  return `${parts.join('. ')}.`;
}

/** The driver tapped this gate from outside the soft fence. */
export function gateManual(
  job: Pick<DriverJob, 'step_2_manual' | 'step_3_manual' | 'step_4_manual' | 'step_2_fence' | 'step_3_fence' | 'step_4_fence'>,
  step: number,
): boolean {
  return gateActivity(job, step) === 'manual';
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

/** Whole seconds from an earlier stamp to a later one. Same verbatim clock math as minutesBetween. */
export function secondsBetween(earlier: string | null | undefined, later: string | null | undefined): number | null {
  const start = stampUtcMs(earlier);
  const end = stampUtcMs(later);
  if (start == null || end == null) return null;
  const delta = end - start;
  if (delta < 0) return null;
  return Math.round(delta / 1000);
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

const GATE_WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'] as const;
const GATE_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'] as const;

/**
 * Weekday and calendar date from a stored timestamp, for the step 1 marker.
 * The date is the digits in the string. Nothing is shifted into another timezone.
 */
export function gateStartDate(stamp: string | null | undefined): string | null {
  if (stamp == null) return null;
  const match = String(stamp).trim().match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!match) return null;
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const weekday = GATE_WEEKDAYS[new Date(Date.UTC(Number(match[1]), month - 1, day)).getUTCDay()];
  return `${weekday} ${day} ${GATE_MONTHS[month - 1]}`;
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

'use client';

import { useLocationAge } from '@/components/driver/useLocationAge';
import type { GpsGhost } from '@/lib/gate-proximity';
import { DRIVER_GATES, completedGateCount, gateActivity, gateClock, gateFenceHover, gateShownGps, gateStartDate, minutesBetween, secondsBetween, truckStage, wineryArrivalEta, type DriverJob, type GateFenceKind, type TruckLoad } from '@/lib/driver-jobs';

function TruckIcon({ load }: { load: TruckLoad }) {
  return (
    <svg width="44" height="26" viewBox="0 0 48 28" aria-hidden="true" className="text-[#1f9d55]">
      <rect x="1.5" y="3.5" width="27" height="14" rx="1.4" fill="#f7f8fa" stroke="currentColor" strokeWidth="1.6" />
      {load === 'half' && <rect x="3.2" y="10.6" width="23.6" height="5.5" fill="currentColor" />}
      {load === 'full' && <rect x="3.2" y="5.2" width="23.6" height="10.9" fill="currentColor" />}
      <path d="M28.6 8.8h5.4l6.2 4.4v4.3H28.6V8.8z" fill="currentColor" />
      <circle cx="12" cy="21.6" r="2.7" fill="currentColor" />
      <circle cx="36.5" cy="21.6" r="2.7" fill="currentColor" />
    </svg>
  );
}

/** Minutes for a finished leg, sitting on the route where the truck was. */
function PhaseMinutes({ minutes, left }: { minutes: number; left: string }) {
  return (
    <div
      className="absolute top-[1.15rem] z-[1] -translate-x-1/2 -translate-y-1/2 text-center"
      style={{ left }}
      aria-label={`${minutes} minutes`}
    >
      <span className="block rounded bg-[#f3f5f7] px-0.5 text-[12px] font-bold leading-none tabular-nums text-[#1f9d55]">
        {minutes}
      </span>
      <span className="mt-1.5 block text-[8px] font-bold uppercase leading-none tracking-wide text-[#1f9d55]">min</span>
    </div>
  );
}

/** Metres to the next fence, shown under the truck on the way to the vineyard or the winery. */
export type TruckMeters = { meters: number | null; inside: boolean };

/**
 * The two road lengths.
 * Out is the GPS fix at job start to the vineyard fence.
 * Back is the vineyard fence to the winery fence.
 */
export type JourneyMeters = {
  outMeters: number | null;
  backMeters: number | null;
  /** Which connector the truck is on, when it is travelling. */
  active: 'out' | 'back' | null;
  remaining: number | null;
  /** Rough clock, from the share of the journey already covered. */
  eta: string | null;
};

const OUT_TITLE =
  'Metres from the truck location at job start to the vineyard fence. The percentage compares metres still to go with that trip.';
const BACK_TITLE =
  'Metres between the vineyard fence and the winery fence. The percentage compares metres still to go with that trip.';

function journeyPercent(total: number, remaining: number | null): number | null {
  if (remaining == null || total <= 0) return null;
  return Math.min(100, Math.max(0, Math.round((1 - remaining / total) * 100)));
}

function JourneyOnBar({
  meters,
  percent,
  remaining,
  eta,
  left,
  title,
}: {
  meters: number;
  percent: number | null;
  remaining: number | null;
  eta: string | null;
  left: string;
  title: string;
}) {
  return (
    <div
      className="absolute top-[2.05rem] z-[1] -translate-x-1/2 text-center"
      style={{ left }}
      title={title}
    >
      <span className="block whitespace-nowrap rounded bg-[#f3f5f7] px-0.5 text-[11px] font-bold leading-none tabular-nums text-[#344054]">
        {meters.toLocaleString('en-NZ')} m
      </span>
      {percent != null ? (
        <span className="mt-0.5 block whitespace-nowrap text-[9px] font-bold leading-none text-[#175cd3]">
          {percent}%{remaining != null ? ` · ${remaining.toLocaleString('en-NZ')} m left` : ''}
        </span>
      ) : null}
      {eta ? <span className="mt-0.5 block whitespace-nowrap text-[9px] font-semibold leading-none text-[#1f9d55]">ETA {eta}</span> : null}
    </div>
  );
}

const FENCE_WORD: Record<GateFenceKind, string> = { hard: 'Hard', soft: 'Soft', manual: 'Manual' };

function fenceCircleClass(kind: GateFenceKind): string {
  if (kind === 'soft') return 'bg-[#e0a100] text-[11px] font-semibold text-[#1c2430] shadow-sm';
  if (kind === 'manual') return 'bg-[#b54708] text-[11px] font-semibold text-white shadow-sm';
  return 'bg-[#1f9d55] text-[11px] font-semibold text-white shadow-sm';
}

function FenceWord({ kind, gps }: { kind: GateFenceKind; gps: string | null }) {
  const tone = kind === 'hard' ? 'text-[#1f9d55]' : 'text-[#b54708]';
  const clock = gps ? gateClock(gps) : null;
  return (
    <span className="mt-1 text-center">
      <span className={`block text-[8px] font-bold uppercase leading-none ${tone}`}>{FENCE_WORD[kind]}</span>
      {clock && clock !== '--:--' ? (
        <span className={`mt-0.5 block text-[9px] font-semibold tabular-nums leading-none ${tone}`}>{clock}</span>
      ) : null}
    </span>
  );
}

/** Portrait gate strip: done gates show the tap time, the truck sits on the way to the next gate. */
function PhoneGateTrack({
  job,
  travelMeters,
  placeNames,
  journey,
}: {
  job: DriverJob;
  travelMeters?: TruckMeters | null;
  /** Staff listing only. Vineyard under gate 2, winery under gate 4. */
  placeNames?: { vineyard?: string | null; winery?: string | null } | null;
  /** Staff listing only. Outbound metres from the start fix, and the return between the fences. */
  journey?: JourneyMeters | null;
}) {
  const done = completedGateCount(job);
  const stage = truckStage(done);
  const eta = done === 3 ? wineryArrivalEta(job) : null;
  const metersLabel = !travelMeters
    ? null
    : travelMeters.inside
      ? 'At fence'
      : travelMeters.meters != null
        ? `${travelMeters.meters.toLocaleString('en-NZ')} m`
        : null;
  const outMeters = journey?.outMeters ?? null;
  const backMeters = journey?.backMeters ?? null;
  const showJourney = outMeters != null || backMeters != null;
  const finishedLegs = DRIVER_GATES.slice(0, -1).flatMap((gate, index) => {
    if (done <= index + 1) return [];
    const minutes = minutesBetween(job[gate.key], job[DRIVER_GATES[index + 1].key]);
    if (minutes == null) return [];
    return [{ step: gate.step, minutes, left: `${(index + 1) * 20}%` }];
  });

  return (
    <div className="bg-[#f3f5f7] px-1 pb-1 pt-2">
      <div className="grid grid-cols-5">
        {DRIVER_GATES.map((gate) => (
          <div key={gate.step} className="px-0.5 text-center">
            <div className="text-[9px] font-semibold uppercase tracking-wide text-[#9aa3ad]">Gate {gate.step}</div>
            <div className="mt-0.5 min-h-[2.35em] text-[10px] leading-tight text-[#6d7580]">{gate.title}</div>
            {placeNames && (gate.step === 2 || gate.step === 4) ? (
              <div className="mt-0.5 line-clamp-2 text-[11px] font-semibold leading-tight text-[#1c2430]" title={gate.step === 2 ? placeNames.vineyard ?? '' : placeNames.winery ?? ''}>
                {(gate.step === 2 ? placeNames.vineyard : placeNames.winery)?.trim() || '—'}
              </div>
            ) : null}
          </div>
        ))}
      </div>
      <div className={`relative mt-1 ${showJourney ? 'min-h-[5.4rem]' : 'min-h-[4.6rem]'}`}>
        <div className="absolute left-[10%] right-[10%] top-[1.05rem] h-[2px] bg-[#d5dbe3]" />
        <div className="relative grid grid-cols-5">
          {DRIVER_GATES.map((gate, index) => {
            const stamp = job[gate.key];
            const complete = index < done;
            const activity = complete ? gateActivity(job, gate.step) : null;
            const gps = activity ? gateShownGps(job, gate.step) : null;
            const hover = activity ? gateFenceHover(job, gate.step) : null;
            const estimate = !complete && gate.step === 4 && eta ? (eta.match(/(\d{2}:\d{2})$/)?.[1] ?? eta) : null;
            return (
              <div key={gate.step} className="flex flex-col items-center" title={hover ?? undefined}>
                <div
                  className={
                    activity
                      ? `flex h-9 w-9 items-center justify-center rounded-full ${fenceCircleClass(activity)}`
                      : complete
                        ? 'flex h-9 w-9 items-center justify-center rounded-full bg-[#1f9d55] text-[11px] font-semibold text-white shadow-sm'
                        : estimate
                          ? 'flex h-9 w-9 items-center justify-center rounded-full border-2 border-dashed border-[#1f9d55] bg-[#f7f8fa] text-[10px] font-semibold text-[#1f9d55]'
                          : 'flex h-9 w-9 items-center justify-center rounded-full border-2 border-dashed border-[#c5ccd6] bg-[#f7f8fa] text-[11px] font-medium text-[#b0b7c3]'
                  }
                >
                  {complete ? gateClock(stamp) : estimate ?? '--:--'}
                </div>
                {activity ? <FenceWord kind={activity} gps={gps} /> : null}
              </div>
            );
          })}
        </div>
        {finishedLegs.map((leg) => (
          <PhaseMinutes key={leg.step} minutes={leg.minutes} left={leg.left} />
        ))}
        {outMeters != null && (
          <JourneyOnBar
            meters={outMeters}
            percent={journey?.active === 'out' ? journeyPercent(outMeters, journey.remaining) : null}
            remaining={journey?.active === 'out' ? journey.remaining : null}
            eta={journey?.active === 'out' ? journey.eta : null}
            left="20%"
            title={OUT_TITLE}
          />
        )}
        {backMeters != null && (
          <JourneyOnBar
            meters={backMeters}
            percent={journey?.active === 'back' ? journeyPercent(backMeters, journey.remaining) : null}
            remaining={journey?.active === 'back' ? journey.remaining : null}
            eta={journey?.active === 'back' ? journey.eta : null}
            left="60%"
            title={BACK_TITLE}
          />
        )}
        {stage && (
          <div
            className="absolute top-0 z-[2] flex w-[4.6rem] -translate-x-1/2 flex-col items-center"
            style={{ left: `${done * 20}%` }}
          >
            <TruckIcon load={stage.load} />
            {journey?.active ? null : (
              <span className="mt-0.5 whitespace-pre-line text-center text-[9px] font-bold leading-tight text-[#1f9d55]">
                {stage.label}
              </span>
            )}
            {metersLabel && !journey ? (
              <span className="mt-0.5 text-center text-[10px] font-bold tabular-nums leading-none text-[#1c2430]">{metersLabel}</span>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}

/** Same yellow/red marker the driver button shows. */
export type CheckpointMark = {
  step: 2 | 3 | 4;
  alert: 'yellow' | 'red' | null;
} | null;

const LIVE_COLS = '3.25rem minmax(0,1fr) auto minmax(0,1fr) auto';
const POD_COLS = 'minmax(0,3.25rem) 5.2rem minmax(0,3.25rem)';
/** Border plus padding that insets the place circles, so the road meets their rims. */
const POD_INSET = 6;
const CIRCLE_CENTER = POD_INSET + 18;
/** Gate 1 is 3.25rem wide and its circle is 2.25rem, so 8px reaches the rim and stops short of the time. */
const RIM_REACH = 8;

function snappedShare(percent: number | null): number {
  if (percent == null) return 0;
  return Math.min(100, Math.max(0, Math.round(percent / 20) * 20));
}

function formatDuration(seconds: number): string {
  const s = Math.max(0, seconds);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  const pad = (n: number) => String(n).padStart(2, '0');
  if (h > 0) return `${h}:${pad(m)}:${pad(sec)}`;
  return `${pad(m)}:${pad(sec)}`;
}

function LiveHeading({ step }: { step: number }) {
  const gate = DRIVER_GATES[step - 1];
  return (
    <div className="min-w-0 overflow-hidden px-0.5 text-center">
      <div className="text-[9px] font-semibold uppercase tracking-wide text-[#9aa3ad]">Gate {gate.step}</div>
      <div className="mt-0.5 min-h-[2.35em] text-[10px] leading-tight text-[#6d7580]">{gate.title}</div>
    </div>
  );
}

function LiveGateDot({
  job,
  step,
  alert,
  eta,
}: {
  job: DriverJob;
  step: number;
  alert: 'yellow' | 'red' | null;
  eta: string | null;
}) {
  const gate = DRIVER_GATES[step - 1];
  const done = completedGateCount(job);
  const complete = step <= done;
  const activity = complete ? gateActivity(job, step) : null;
  const gps = activity ? gateShownGps(job, step) : null;
  const hover = activity ? gateFenceHover(job, step) : null;
  const estimate = !complete && step === 4 && eta ? (eta.match(/(\d{2}:\d{2})$/)?.[1] ?? eta) : null;
  const stamp = job[gate.key];
  const startedOn = step === 1 && complete ? gateStartDate(stamp) : null;
  const face = activity
    ? fenceCircleClass(activity)
    : complete
      ? 'bg-[#1f9d55] text-[11px] font-semibold text-white shadow-sm'
      : alert === 'red'
        ? 'border-2 border-[#f04438] bg-[#fef3f2] text-[11px] font-semibold text-[#b42318]'
        : alert === 'yellow'
          ? 'border-2 border-[#e0a100] bg-[#fffaeb] text-[11px] font-semibold text-[#b54708]'
          : estimate
            ? 'border-2 border-dashed border-[#1f9d55] bg-[#f7f8fa] text-[10px] font-semibold text-[#1f9d55]'
            : 'border-2 border-dashed border-[#c5ccd6] bg-[#f7f8fa] text-[11px] font-medium text-[#b0b7c3]';
  return (
    <div className="relative z-[2] flex flex-col items-center" title={hover ?? undefined}>
      <div className={`flex h-9 w-9 items-center justify-center rounded-full ${face}`}>
        {complete ? gateClock(stamp) : estimate ?? '--:--'}
      </div>
      {alert === 'yellow' || alert === 'red' ? (
        <span className={`mt-1 text-[8px] font-bold uppercase leading-none ${alert === 'red' ? 'text-[#b42318]' : 'text-[#b54708]'}`}>
          {alert === 'red' ? 'Hard' : 'Soft'}
        </span>
      ) : activity ? (
        <FenceWord kind={activity} gps={gps} />
      ) : startedOn ? (
        <span className="mt-1 whitespace-nowrap text-[9px] font-semibold leading-none text-[#344054]" title={stamp ?? undefined}>
          {startedOn}
        </span>
      ) : (
        <span className="mt-1 block h-[8px]" />
      )}
    </div>
  );
}

/** Running while step 3 is open. Frozen at step 3 minus step 2 once the truck has left. */
function VineyardStay({ job }: { job: DriverJob }) {
  const open = !job.step_3_click?.trim();
  const live = useLocationAge(open ? job.step_2_click : null);
  if (!job.step_2_click?.trim()) return null;
  const seconds = open ? live : secondsBetween(job.step_2_click, job.step_3_click);
  if (seconds == null) return null;
  return (
    <div className="mt-1 text-center" aria-label={open ? 'Time in the vineyard' : 'Time spent in the vineyard'}>
      <span className="block text-[12px] font-bold tabular-nums leading-none text-[#1f9d55]">{formatDuration(seconds)}</span>
      <span className="mt-0.5 block text-[8px] font-bold uppercase leading-none tracking-wide text-[#1f9d55]">
        {open ? 'in vineyard' : 'vineyard'}
      </span>
    </div>
  );
}

function RoadLeg({
  connect,
  truck,
  load,
  percent,
  meters,
  remaining,
  eta,
  minutes,
  title,
}: {
  /** Outbound line starts at the gate 1 rim. The return road runs between the two places. */
  connect: 'from-previous' | 'between';
  truck: boolean;
  load: TruckLoad;
  percent: number | null;
  meters: number | null;
  remaining: number | null;
  eta: string | null;
  minutes: number | null;
  title: string;
}) {
  const snapped = snappedShare(truck ? percent : null);
  const reach = connect === 'from-previous' ? RIM_REACH : 0;
  const truckLeft =
    connect === 'from-previous'
      ? `calc(${snapped}% - ${(1 - snapped / 100) * reach}px)`
      : `${snapped}%`;
  return (
    <div className="relative min-h-[5.8rem]">
      <div
        className="absolute h-[2px] bg-[#d5dbe3]"
        style={
          connect === 'from-previous'
            ? { top: CIRCLE_CENTER - 1, left: -reach, right: 0 }
            : { top: CIRCLE_CENTER - 1, left: 0, right: 0 }
        }
      />
      {minutes != null && (
        <div className="absolute left-1/2 z-[1] -translate-x-1/2 -translate-y-1/2 text-center" style={{ top: CIRCLE_CENTER }}>
          <span className="block rounded bg-[#f3f5f7] px-0.5 text-[12px] font-bold leading-none tabular-nums text-[#1f9d55]">{minutes}</span>
          <span className="mt-1.5 block text-[8px] font-bold uppercase leading-none tracking-wide text-[#1f9d55]">min</span>
        </div>
      )}
      {meters != null && (
        <div
          className="absolute inset-x-0 z-[1] text-center"
          style={{ top: 42 }}
          title={title}
        >
          <span className="block whitespace-nowrap text-[11px] font-bold leading-none tabular-nums text-[#344054]">
            {meters.toLocaleString('en-NZ')} m
          </span>
          {percent != null ? (
            <span className="mt-0.5 block whitespace-nowrap text-[9px] font-bold leading-none text-[#175cd3]">
              {percent}%{remaining != null ? ` · ${remaining.toLocaleString('en-NZ')} m left` : ''}
            </span>
          ) : null}
          {eta ? <span className="mt-0.5 block whitespace-nowrap text-[9px] font-semibold leading-none text-[#1f9d55]">ETA {eta}</span> : null}
        </div>
      )}
      {truck && (
        <div className="absolute z-[2] flex w-[4.6rem] -translate-x-1/2 flex-col items-center" style={{ top: 2, left: truckLeft }}>
          <TruckIcon load={load} />
        </div>
      )}
    </div>
  );
}

function GhostTruck() {
  return (
    <span className="relative z-[2] block" title="GPS position. Driver taps do not move this truck.">
      <svg width="28" height="16" viewBox="0 0 48 28" aria-hidden="true" className="text-[#667085]">
        <rect x="1.5" y="3.5" width="27" height="14" rx="1.4" fill="#f3f5f7" stroke="currentColor" strokeWidth="2.2" />
        <path d="M28.6 8.8h5.4l6.2 4.4v4.3H28.6V8.8z" fill="currentColor" />
        <circle cx="12" cy="21.6" r="2.7" fill="currentColor" />
        <circle cx="36.5" cy="21.6" r="2.7" fill="currentColor" />
      </svg>
    </span>
  );
}

function GhostStamp({ at, title }: { at: string | null; title: string }) {
  const clock = at ? gateClock(at) : '';
  const shown = clock && clock !== '--:--' ? clock : '';
  return (
    <span
      className="relative z-[2] block text-center text-[9px] font-semibold tabular-nums leading-none text-[#667085]"
      title={shown ? `${title} · ${at}` : undefined}
    >
      {shown ? <span className="rounded bg-[#f3f5f7] px-0.5">{shown}</span> : '\u00a0'}
    </span>
  );
}

function GhostLine() {
  return <div className="pointer-events-none absolute inset-x-0 top-1/2 h-px bg-[#98a2b3]" />;
}

function GhostTruckAt({ percent }: { percent: number | null }) {
  const snapped = snappedShare(percent);
  return (
    <div className="absolute top-1/2 z-[2] -translate-x-1/2 -translate-y-1/2" style={{ left: `${snapped}%` }}>
      <GhostTruck />
    </div>
  );
}

/** Grey GPS cells. They share the driver-track columns, so the times sit under the same gates. */
function GpsGhostCells({ ghost }: { ghost: GpsGhost }) {
  return (
    <>
      <div className="relative flex h-7 items-center justify-center" aria-hidden="true">
        <GhostLine />
        <span className="relative z-[2] rounded bg-[#f3f5f7] px-0.5 text-[8px] font-bold uppercase tracking-wide text-[#667085]">GPS</span>
      </div>
      <div className="relative h-7">
        <GhostLine />
        {ghost.place === 'out' ? <GhostTruckAt percent={ghost.percent} /> : null}
      </div>
      <div
        className="relative grid h-7 items-center border-2 border-transparent px-1.5"
        style={{ gridTemplateColumns: POD_COLS }}
        aria-label="GPS position. Driver taps do not move this truck."
      >
        <GhostLine />
        <GhostStamp at={ghost.vineyardEnteredAt} title="GPS entered the vineyard fence" />
        <div className="relative z-[2] flex justify-center">{ghost.place === 'vineyard' ? <GhostTruck /> : null}</div>
        <GhostStamp at={ghost.vineyardLeftAt} title="GPS left the vineyard fence" />
      </div>
      <div className="relative h-7">
        <GhostLine />
        {ghost.place === 'back' ? <GhostTruckAt percent={ghost.percent} /> : null}
      </div>
      <div className="relative grid h-7 items-center border-2 border-transparent px-1.5" style={{ gridTemplateColumns: POD_COLS }}>
        <GhostLine />
        <GhostStamp at={ghost.wineryEnteredAt} title="GPS entered the winery fence" />
        <div className="relative z-[2] flex justify-center">{ghost.place === 'winery' ? <GhostTruck /> : null}</div>
        <span />
      </div>
    </>
  );
}

/**
 * Staff live track. The two long gaps are the roads.
 * Steps 2 and 3 sit inside the vineyard. Steps 4 and 5 sit inside the winery.
 * The green truck follows the driver taps. The grey truck under it follows GPS.
 */
function LiveGateTrack({
  job,
  placeNames,
  journey,
  checkpoint,
  ghost,
}: {
  job: DriverJob;
  placeNames?: { vineyard?: string | null; winery?: string | null } | null;
  journey?: JourneyMeters | null;
  checkpoint?: CheckpointMark;
  ghost?: GpsGhost | null;
}) {
  const done = completedGateCount(job);
  const stage = truckStage(done);
  const eta = done === 3 ? wineryArrivalEta(job) : null;
  const alertFor = (step: 2 | 3 | 4) => (checkpoint?.step === step ? checkpoint.alert : null);
  const outMeters = journey?.outMeters ?? null;
  const backMeters = journey?.backMeters ?? null;
  const outPercent = journey?.active === 'out' && outMeters != null ? journeyPercent(outMeters, journey.remaining) : null;
  const backPercent = journey?.active === 'back' && backMeters != null ? journeyPercent(backMeters, journey.remaining) : null;
  const outMinutes = done > 1 ? minutesBetween(job.step_1_click, job.step_2_click) : null;
  const backMinutes = done > 3 ? minutesBetween(job.step_3_click, job.step_4_click) : null;
  const unloadMinutes = done > 4 ? minutesBetween(job.step_4_click, job.step_5_click) : null;
  const load = stage?.load ?? 'empty';

  return (
    <div className="bg-[#f3f5f7] px-1 pb-2 pt-2">
      <div className="grid items-end" style={{ gridTemplateColumns: LIVE_COLS }}>
        <LiveHeading step={1} />
        <div />
        <div className="rounded-full border-2 border-transparent px-1.5">
          <div className="grid" style={{ gridTemplateColumns: POD_COLS }}>
            <LiveHeading step={2} />
            <div />
            <LiveHeading step={3} />
          </div>
          {placeNames ? (
            <div className="truncate text-center text-[11px] font-semibold leading-tight text-[#1c2430]" title={placeNames.vineyard ?? ''}>
              {placeNames.vineyard?.trim() || '—'}
            </div>
          ) : null}
        </div>
        <div />
        <div className="rounded-full border-2 border-transparent px-1.5">
          <div className="grid" style={{ gridTemplateColumns: POD_COLS }}>
            <LiveHeading step={4} />
            <div />
            <LiveHeading step={5} />
          </div>
          {placeNames ? (
            <div className="truncate text-center text-[11px] font-semibold leading-tight text-[#1c2430]" title={placeNames.winery ?? ''}>
              {placeNames.winery?.trim() || '—'}
            </div>
          ) : null}
        </div>
      </div>
      <div className="relative mt-1 grid items-start" style={{ gridTemplateColumns: LIVE_COLS }}>
        <div style={{ paddingTop: POD_INSET }}>
          <LiveGateDot job={job} step={1} alert={null} eta={eta} />
        </div>
        <RoadLeg
          connect="from-previous"
          truck={done === 1}
          load={load}
          percent={outPercent}
          meters={outMeters}
          remaining={journey?.active === 'out' ? journey.remaining : null}
          eta={journey?.active === 'out' ? journey.eta : null}
          minutes={outMinutes}
          title={OUT_TITLE}
        />
        <div
          className="relative z-[1] rounded-full border-2 border-[#3f6212] bg-[#eef6e8] px-1.5 py-1"
          title="Inside the vineyard. The truck waits here between arriving and leaving."
        >
          <div className="grid items-start" style={{ gridTemplateColumns: POD_COLS }}>
            <LiveGateDot job={job} step={2} alert={alertFor(2)} eta={eta} />
            <div className="flex flex-col items-center">
              {done === 2 && (
                <div style={{ marginTop: -4 }}>
                  <TruckIcon load={load} />
                </div>
              )}
              <VineyardStay job={job} />
            </div>
            <LiveGateDot job={job} step={3} alert={alertFor(3)} eta={eta} />
          </div>
        </div>
        <RoadLeg
          connect="between"
          truck={done === 3}
          load={load}
          percent={backPercent}
          meters={backMeters}
          remaining={journey?.active === 'back' ? journey.remaining : null}
          eta={journey?.active === 'back' ? journey.eta : null}
          minutes={backMinutes}
          title={BACK_TITLE}
        />
        <div
          className="relative z-[1] rounded-full border-2 border-[#3f6212] bg-[#eef6e8] px-1.5 py-1"
          title="Inside the winery. The truck waits here between arriving and the end of the job."
        >
          <div className="grid items-start" style={{ gridTemplateColumns: POD_COLS }}>
            <LiveGateDot job={job} step={4} alert={alertFor(4)} eta={eta} />
            <div className="flex flex-col items-center">
              {done === 4 && (
                <div style={{ marginTop: -4 }}>
                  <TruckIcon load={load} />
                  <span className="mt-0.5 block whitespace-pre-line text-center text-[9px] font-bold leading-tight text-[#1f9d55]">
                    Unloading
                  </span>
                </div>
              )}
              {unloadMinutes != null && (
                <div className="mt-1 text-center">
                  <span className="block text-[12px] font-bold leading-none tabular-nums text-[#1f9d55]">{unloadMinutes}</span>
                  <span className="mt-1.5 block text-[8px] font-bold uppercase leading-none tracking-wide text-[#1f9d55]">min</span>
                </div>
              )}
            </div>
            <LiveGateDot job={job} step={5} alert={null} eta={eta} />
          </div>
        </div>
        {ghost ? <GpsGhostCells ghost={ghost} /> : null}
      </div>
    </div>
  );
}

/** Driver phone keeps the even gate strip. The live board stretches the two roads and pods the vineyard. */
export default function GateTrack({
  live,
  ...props
}: {
  job: DriverJob;
  travelMeters?: TruckMeters | null;
  placeNames?: { vineyard?: string | null; winery?: string | null } | null;
  journey?: JourneyMeters | null;
  /** Staff listing. Draws the road / vineyard wheel and moves the truck along the active road. */
  live?: boolean;
  checkpoint?: CheckpointMark;
  /** Staff listing. GPS truck and fence times, drawn under the driver track. */
  ghost?: GpsGhost | null;
}) {
  if (live) {
    return (
      <LiveGateTrack
        job={props.job}
        placeNames={props.placeNames}
        journey={props.journey}
        checkpoint={props.checkpoint}
        ghost={props.ghost}
      />
    );
  }
  return <PhoneGateTrack job={props.job} travelMeters={props.travelMeters} placeNames={props.placeNames} journey={props.journey} />;
}

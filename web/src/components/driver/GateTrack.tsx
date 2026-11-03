import { DRIVER_GATES, completedGateCount, gateClock, gateManual, minutesBetween, truckStage, wineryArrivalEta, type DriverJob, type TruckLoad } from '@/lib/driver-jobs';

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

/** Fence-to-fence journey. The same winery is the start of the trip out and the end of the trip back. */
export type JourneyMeters = {
  meters: number | null;
  /** Which connector the truck is on, when it is travelling. */
  active: 'out' | 'back' | null;
  remaining: number | null;
  /** Rough clock, from the share of the journey already covered. */
  eta: string | null;
};

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
}: {
  meters: number;
  percent: number | null;
  remaining: number | null;
  eta: string | null;
  left: string;
}) {
  return (
    <div
      className="absolute top-[2.05rem] z-[1] -translate-x-1/2 text-center"
      style={{ left }}
      title="Estimated metres between the fences. The percentage compares metres still to go with that journey. The ETA is a rough rate from the share already covered."
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

/** Portrait gate strip: done gates show the tap time, the truck sits on the way to the next gate. */
export default function GateTrack({
  job,
  travelMeters,
  placeNames,
  journey,
}: {
  job: DriverJob;
  travelMeters?: TruckMeters | null;
  /** Staff listing only. Vineyard under gate 2, winery under gate 4. */
  placeNames?: { vineyard?: string | null; winery?: string | null } | null;
  /** Staff listing only. Estimated fence-to-fence metres on the connectors. */
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
      <div className={`relative mt-1 ${journey?.meters != null ? 'h-[5.4rem]' : 'h-[4.6rem]'}`}>
        <div className="absolute left-[10%] right-[10%] top-[1.05rem] h-[2px] bg-[#d5dbe3]" />
        <div className="relative grid grid-cols-5">
          {DRIVER_GATES.map((gate, index) => {
            const stamp = job[gate.key];
            const complete = index < done;
            const manual = complete && gateManual(job, gate.step);
            const estimate = !complete && gate.step === 4 && eta ? (eta.match(/(\d{2}:\d{2})$/)?.[1] ?? eta) : null;
            return (
              <div key={gate.step} className="flex flex-col items-center">
                <div
                  className={
                    manual
                      ? 'flex h-9 w-9 items-center justify-center rounded-full bg-[#b54708] text-[11px] font-semibold text-white shadow-sm'
                      : complete
                        ? 'flex h-9 w-9 items-center justify-center rounded-full bg-[#1f9d55] text-[11px] font-semibold text-white shadow-sm'
                        : estimate
                          ? 'flex h-9 w-9 items-center justify-center rounded-full border-2 border-dashed border-[#1f9d55] bg-[#f7f8fa] text-[10px] font-semibold text-[#1f9d55]'
                          : 'flex h-9 w-9 items-center justify-center rounded-full border-2 border-dashed border-[#c5ccd6] bg-[#f7f8fa] text-[11px] font-medium text-[#b0b7c3]'
                  }
                >
                  {complete ? gateClock(stamp) : estimate ?? '--:--'}
                </div>
                {manual && (
                  <span className="mt-1 text-center text-[8px] font-bold uppercase leading-tight text-[#b54708]">
                    Manual
                    <br />
                    no GPS
                  </span>
                )}
              </div>
            );
          })}
        </div>
        {finishedLegs.map((leg) => (
          <PhaseMinutes key={leg.step} minutes={leg.minutes} left={leg.left} />
        ))}
        {journey?.meters != null && (
          <>
            <JourneyOnBar
              meters={journey.meters}
              percent={journey.active === 'out' ? journeyPercent(journey.meters, journey.remaining) : null}
              remaining={journey.active === 'out' ? journey.remaining : null}
              eta={journey.active === 'out' ? journey.eta : null}
              left="20%"
            />
            <JourneyOnBar
              meters={journey.meters}
              percent={journey.active === 'back' ? journeyPercent(journey.meters, journey.remaining) : null}
              remaining={journey.active === 'back' ? journey.remaining : null}
              eta={journey.active === 'back' ? journey.eta : null}
              left="60%"
            />
          </>
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

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

/** Portrait gate strip: done gates show the tap time, the truck sits on the way to the next gate. */
export default function GateTrack({ job }: { job: DriverJob }) {
  const done = completedGateCount(job);
  const stage = truckStage(done);
  const eta = done === 3 ? wineryArrivalEta(job) : null;
  const finishedLegs = DRIVER_GATES.slice(0, -1).flatMap((gate, index) => {
    if (done <= index + 1) return [];
    const minutes = minutesBetween(job[gate.key], job[DRIVER_GATES[index + 1].key]);
    if (minutes == null) return [];
    return [{ step: gate.step, minutes, left: `${(index + 1) * 20}%` }];
  });

  return (
    <div className="bg-[#f3f5f7] px-1 pb-2 pt-3">
      <div className="grid grid-cols-5">
        {DRIVER_GATES.map((gate) => (
          <div key={gate.step} className="px-0.5 text-center">
            <div className="text-[9px] font-semibold uppercase tracking-wide text-[#9aa3ad]">Gate {gate.step}</div>
            <div className="mt-0.5 min-h-[2.35em] text-[10px] leading-tight text-[#6d7580]">{gate.title}</div>
          </div>
        ))}
      </div>
      <div className="relative mt-1 h-[6.25rem]">
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
        {stage && (
          <div
            className="absolute top-0 z-[2] flex w-[4.6rem] -translate-x-1/2 flex-col items-center"
            style={{ left: `${done * 20}%` }}
          >
            <TruckIcon load={stage.load} />
            <span className="mt-0.5 whitespace-pre-line text-center text-[9px] font-bold leading-tight text-[#1f9d55]">
              {stage.label}
            </span>
          </div>
        )}
      </div>
    </div>
  );
}

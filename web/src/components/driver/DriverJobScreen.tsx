'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import LocationServiceStatus from '@/components/LocationServiceStatus';
import ApproachMeters, { useJobApproach } from '@/components/driver/ApproachMeters';
import { DriverLink } from '@/components/driver/driver-nav';
import DriverDiversionControl from '@/components/driver/DriverDiversionControl';
import DriverJobLog from '@/components/driver/DriverJobLog';
import GateTrack from '@/components/driver/GateTrack';
import { DRIVER_GATES, completedGateCount, gateClock, gateManual, phoneLocalStamp, wineryArrivalEta, type DriverJob } from '@/lib/driver-jobs';

function ProximityMark({ kind }: { kind: 'yellow' | 'red' }) {
  const color = kind === 'yellow' ? '#e0a100' : '#f04438';
  const title = kind === 'yellow' ? 'Close to the fence. Tap as you cross.' : 'Tap now. The fence crossing has already happened.';
  const pin =
    'M12 0C7.6 0 4 3.4 4 7.6 4 12.8 12 22 12 22s8-9.2 8-14.4C20 3.4 16.4 0 12 0zm0 10.6a3 3 0 1 1 0-6 3 3 0 0 1 0 6z';
  return (
    <span className="inline-flex h-9 w-11 shrink-0 items-center justify-center rounded-lg bg-white" title={title}>
      <svg width="40" height="24" viewBox="0 0 64 38" aria-hidden="true">
        <g transform="translate(1 0) scale(0.78)">
          <path fill={color} fillRule="evenodd" d={pin} />
          <circle cx="12" cy="7.6" r="2.3" fill="white" />
        </g>
        <g transform="translate(33 0) scale(0.78)">
          <path fill={color} fillRule="evenodd" d={pin} />
          <circle cx="12" cy="7.6" r="2.3" fill="white" />
        </g>
        {[8, 18, 28, 38, 48, 58].map((cx) => (
          <circle key={cx} cx={cx} cy="34.5" r="1.7" fill={color} />
        ))}
      </svg>
    </span>
  );
}

function FenceLine({ label, name, jobId }: { label: string; name: string | null; jobId: number }) {
  const fence = name?.trim() ?? '';
  const mapHref = fence
    ? `/map?job=${jobId}&fence=${encodeURIComponent(fence)}`
    : null;
  return (
    <div className="flex items-center justify-between gap-3 border-t border-[#eaecf0] py-3 first:border-t-0">
      <div className="min-w-0">
        <p className="text-xs font-medium uppercase tracking-wide text-[#667085]">{label}</p>
        <p className="mt-0.5 truncate text-base text-[#1c2430]">{fence || '—'}</p>
      </div>
      {mapHref ? (
        <DriverLink to={mapHref} className="shrink-0 text-base font-semibold text-[#175cd3]">
          View on Map
        </DriverLink>
      ) : null}
    </div>
  );
}

function JobDetails({ job }: { job: DriverJob }) {
  return (
    <details className="group mx-4 mb-3 rounded-2xl bg-white px-4 shadow-[0_1px_2px_rgba(16,24,40,0.06)]">
      <summary className="flex h-12 cursor-pointer list-none items-center justify-between text-base font-semibold text-[#1c2430] [&::-webkit-details-marker]:hidden">
        Job details
        <span className="text-sm font-medium text-[#667085] group-open:hidden">Show</span>
        <span className="hidden text-sm font-medium text-[#667085] group-open:inline">Hide</span>
      </summary>
      <FenceLine label="Vineyard geofence" name={job.vineyard_geo_fence} jobId={job.id} />
      <FenceLine label="Winery geofence" name={job.winery_geo_fence} jobId={job.id} />
    </details>
  );
}

export default function DriverJobScreen({ id: idProp }: { id?: string }) {
  const params = useParams<{ id?: string }>();
  const id = idProp ?? params.id ?? '';
  const [job, setJob] = useState<DriverJob | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [savingStep, setSavingStep] = useState<number | null>(null);
  const [menuOpen, setMenuOpen] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [confirm, setConfirm] = useState<{ kind: 'diversion' | 'gate'; step: number; message: string } | null>(null);
  const [diversionRefresh, setDiversionRefresh] = useState(0);
  const [view, setView] = useState<'job' | 'log'>('job');

  useEffect(() => {
    let cancelled = false;
    fetch(`/api/driver-jobs/${encodeURIComponent(id)}`, { cache: 'no-store' })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        return data;
      })
      .then((data) => {
        if (!cancelled) setJob(data.row as DriverJob);
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [id]);

  async function passGate(step: number, options?: { confirmed?: boolean; endDiversion?: boolean }) {
    if (!job) return;
    setError(null);
    setSavingStep(step);
    try {
      const res = await fetch(`/api/driver-jobs/${encodeURIComponent(id)}/step`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          step,
          at: phoneLocalStamp(),
          confirmed: options?.confirmed === true,
          endDiversion: options?.endDiversion === true,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      if (options?.endDiversion) setDiversionRefresh((n) => n + 1);
      if (data?.needsConfirm && typeof data.message === 'string') {
        setConfirm({
          kind: data.kind === 'diversion' ? 'diversion' : 'gate',
          step,
          message: data.message,
        });
        return;
      }
      setConfirm(null);
      setJob(data.row as DriverJob);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSavingStep(null);
    }
  }

  async function declineGate(pending: { step: number; message: string }) {
    setConfirm(null);
    try {
      const res = await fetch(`/api/driver-jobs/${encodeURIComponent(id)}/log`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ step: pending.step, at: phoneLocalStamp(), detail: pending.message }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function resetJob() {
    setMenuOpen(false);
    setError(null);
    setResetting(true);
    try {
      const res = await fetch(`/api/driver-jobs/${encodeURIComponent(id)}/reset`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      setJob(data.row as DriverJob);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setResetting(false);
    }
  }

  const done = job ? completedGateCount(job) : 0;
  const nextStep = done < DRIVER_GATES.length ? done + 1 : null;
  const wineryEta = job && done === 3 ? wineryArrivalEta(job) : null;
  const approach = useJobApproach(job?.id ?? 0, done);
  const travelMeters =
    job && (done === 1 || done === 3) && approach.reading
      ? { meters: approach.reading.meters ?? null, inside: approach.reading.inside === true }
      : null;

  return (
    <div className="relative flex min-h-full flex-1 flex-col">
      <div className="px-4 pb-2 pt-[max(0.75rem,env(safe-area-inset-top))]">
        <div className="flex items-start gap-2">
          <div className="flex min-w-0 flex-1 items-start gap-1 pt-0.5">
            <DriverLink
              to="/home"
              aria-label="Job list"
              className="inline-flex h-8 w-8 shrink-0 items-center justify-center text-[1.75rem] leading-none text-[#175cd3]"
            >
              ‹
            </DriverLink>
            <div className="min-w-0">
            <h1 className="truncate text-xl font-semibold tracking-tight text-[#1c2430]">{job?.job_id || (loading ? '…' : 'Job')}</h1>
            {job && (
              <p className="mt-0.5 truncate text-sm text-[#475467]">
                {[job.vineyard_name, job.winery_name].filter(Boolean).join(' → ') || 'No vineyard or winery'}
              </p>
            )}
            </div>
          </div>
          <div className="relative shrink-0">
            <button
              type="button"
              aria-label="Job menu"
              aria-expanded={menuOpen}
              onClick={() => setMenuOpen((open) => !open)}
              className="flex h-11 w-11 items-center justify-center rounded-full text-2xl leading-none text-[#344054]"
            >
              ⋯
            </button>
            {menuOpen && (
              <>
                <button
                  type="button"
                  aria-label="Close menu"
                  className="fixed inset-0 z-10 cursor-default"
                  onClick={() => setMenuOpen(false)}
                />
                <div className="absolute right-0 top-11 z-20 min-w-[10.5rem] overflow-hidden rounded-xl bg-white shadow-lg ring-1 ring-black/10">
                  <button
                    type="button"
                    onClick={() => {
                      setMenuOpen(false);
                      setView('log');
                    }}
                    className="block w-full px-4 py-3 text-left text-base text-[#1c2430]"
                  >
                    Log
                  </button>
                  <button
                    type="button"
                    disabled={resetting}
                    onClick={() => void resetJob()}
                    className="block w-full border-t border-[#eaecf0] px-4 py-3 text-left text-base text-[#1c2430] disabled:opacity-60"
                  >
                    {resetting ? 'Resetting…' : 'Reset job'}
                  </button>
                  <DriverLink
                    to="/home"
                    className="block w-full border-t border-[#eaecf0] px-4 py-3 text-left text-base text-[#1c2430]"
                  >
                    All jobs
                  </DriverLink>
                </div>
              </>
            )}
          </div>
          <LocationServiceStatus variant="phone" inline />
        </div>
      </div>

      {error && <p className="mx-4 mb-3 rounded-xl bg-[#fee4e2] px-3 py-2 text-base text-[#b42318]">{error}</p>}

      {loading ? (
        <p className="px-4 text-base text-[#667085]">Loading job…</p>
      ) : !job ? (
        <p className="px-4 text-base text-[#667085]">Job not found.</p>
      ) : (
        view === 'log' ? (
          <>
            <div className="px-4 pb-2">
              <button type="button" onClick={() => setView('job')} className="h-10 text-base font-semibold text-[#175cd3]">
                Return
              </button>
            </div>
            <DriverJobLog jobId={job.id} />
          </>
        ) : (
        <>
          <JobDetails job={job} />
          <GateTrack job={job} travelMeters={travelMeters} />
          {done === 1 || done === 2 || done === 3 ? (
            <ApproachMeters
              reading={approach.reading}
              loading={approach.loading}
              corner={<DriverDiversionControl jobId={job.id} refreshKey={diversionRefresh} embedded />}
            />
          ) : (
            <DriverDiversionControl jobId={job.id} refreshKey={diversionRefresh} />
          )}
          <div className="flex flex-col gap-3 px-4 py-4 pb-[max(1.25rem,env(safe-area-inset-bottom))]">
            {DRIVER_GATES.map((gate) => {
              const active = gate.step === nextStep;
              const complete = gate.step <= done;
              const manual = complete && gateManual(job, gate.step);
              const label = complete
                ? `Step ${gate.step} · ${gate.title} · ${gateClock(job[gate.key])}`
                : gate.step === 4 && wineryEta
                  ? `Step ${gate.step} · ${gate.title} · ${wineryEta}`
                  : `Step ${gate.step} · ${gate.title}`;
              const marker = active ? approach.reading?.proximity : null;
              const crossed = active && marker === 'red' ? approach.reading?.crossedAt : null;
              return (
                <div key={gate.step} className="flex items-center gap-2">
                  <button
                    type="button"
                    disabled={!active || savingStep != null || confirm != null}
                    onClick={() => void passGate(gate.step)}
                    className={
                      active
                        ? 'flex h-[3.15rem] flex-1 items-center gap-2 rounded-2xl bg-[#1f9d55] px-4 text-left text-lg font-semibold text-white disabled:opacity-60'
                        : 'flex h-[3.15rem] flex-1 items-center rounded-2xl bg-[#e4e7ec] px-4 text-left text-base font-medium text-[#98a2b3]'
                    }
                  >
                    <span className="min-w-0 flex-1 truncate">{savingStep === gate.step ? 'Saving…' : label}</span>
                    {manual ? <span className="ml-2 shrink-0 text-xs font-medium text-[#667085]">Manual</span> : null}
                    {marker === 'yellow' || marker === 'red' ? <ProximityMark kind={marker} /> : null}
                  </button>
                  {crossed ? (
                    <span className="w-12 shrink-0 text-right text-sm font-semibold tabular-nums text-[#b42318]" title={crossed}>
                      {gateClock(crossed)}
                    </span>
                  ) : null}
                </div>
              );
            })}
            {nextStep == null && (
              <p className="text-center text-base font-medium text-[#1f9d55]">Job finished</p>
            )}
          </div>
          {confirm && (
            <div className="fixed inset-0 z-[80] flex items-center justify-center bg-black/45 p-4" role="dialog" aria-modal="true">
              <div className="w-full max-w-[22rem] rounded-2xl bg-white p-5 shadow-xl">
                <p className="text-lg leading-snug text-[#1c2430]">{confirm.message}</p>
                <div className="mt-5 flex gap-3">
                  <button
                    type="button"
                    onClick={() => {
                      if (confirm.kind === 'gate') void declineGate(confirm);
                      else setConfirm(null);
                    }}
                    className="h-12 flex-1 rounded-2xl bg-[#e4e7ec] text-base font-semibold text-[#1c2430]"
                  >
                    Cancel
                  </button>
                  <button
                    type="button"
                    disabled={savingStep != null}
                    onClick={() =>
                      void passGate(
                        confirm.step,
                        confirm.kind === 'diversion' ? { endDiversion: true } : { confirmed: true },
                      )
                    }
                    className="h-12 flex-1 rounded-2xl bg-[#1f9d55] text-base font-semibold text-white disabled:opacity-60"
                  >
                    {savingStep === confirm.step ? 'Saving…' : confirm.kind === 'diversion' ? 'End diversion' : 'Yes'}
                  </button>
                </div>
              </div>
            </div>
          )}
        </>
        )
      )}
    </div>
  );
}

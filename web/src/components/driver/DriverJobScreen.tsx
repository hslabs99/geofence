'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import ApproachMeters, { useJobApproach } from '@/components/driver/ApproachMeters';
import { DriverLink } from '@/components/driver/driver-nav';
import GateTrack from '@/components/driver/GateTrack';
import { DRIVER_GATES, completedGateCount, gateClock, gateManual, phoneLocalStamp, wineryArrivalEta, type DriverJob } from '@/lib/driver-jobs';

function ProximityMark({ kind }: { kind: 'green' | 'red' }) {
  const color = kind === 'green' ? '#12b76a' : '#f04438';
  return (
    <span
      className="inline-flex h-8 w-8 shrink-0 items-center justify-center"
      title={kind === 'green' ? 'Cross the fence and tap' : 'The fence crossing has already happened'}
    >
      <span className="h-3.5 w-3.5 rounded-full" style={{ backgroundColor: color, boxShadow: `0 0 0 5px ${color}33` }} />
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
  const [confirm, setConfirm] = useState<{ step: number; message: string } | null>(null);

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

  async function passGate(step: number, confirmed = false) {
    if (!job) return;
    setError(null);
    setSavingStep(step);
    try {
      const res = await fetch(`/api/driver-jobs/${encodeURIComponent(id)}/step`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ step, at: phoneLocalStamp(), confirmed }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      if (data?.needsConfirm && typeof data.message === 'string') {
        setConfirm({ step, message: data.message });
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

  return (
    <div className="relative flex min-h-full flex-1 flex-col">
      <div className="px-4 pt-[max(0.75rem,env(safe-area-inset-top))]">
        <div className="flex items-center justify-between">
          <DriverLink to="/home" className="inline-flex h-11 items-center text-base font-medium text-[#175cd3]">
            Jobs
          </DriverLink>
          <div className="relative">
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
                    disabled={resetting}
                    onClick={() => void resetJob()}
                    className="block w-full px-4 py-3 text-left text-base text-[#1c2430] disabled:opacity-60"
                  >
                    {resetting ? 'Resetting…' : 'Reset job'}
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
        {job && (
          <div className="pb-3">
            <h1 className="text-2xl font-semibold text-[#1c2430]">{job.job_id}</h1>
            <p className="mt-1 text-base text-[#475467]">
              {[job.vineyard_name, job.winery_name].filter(Boolean).join(' → ') || 'No vineyard or winery'}
            </p>
          </div>
        )}
      </div>

      {error && <p className="mx-4 mb-3 rounded-xl bg-[#fee4e2] px-3 py-2 text-base text-[#b42318]">{error}</p>}

      {loading ? (
        <p className="px-4 text-base text-[#667085]">Loading job…</p>
      ) : !job ? (
        <p className="px-4 text-base text-[#667085]">Job not found.</p>
      ) : (
        <>
          <JobDetails job={job} />
          <GateTrack job={job} />
          <ApproachMeters reading={approach.reading} loading={approach.loading} />
          <div className="flex flex-col gap-3 px-4 py-4 pb-[max(1.25rem,env(safe-area-inset-bottom))]">
            {DRIVER_GATES.map((gate) => {
              const active = gate.step === nextStep;
              const complete = gate.step <= done;
              const manual = complete && gateManual(job, gate.step);
              const label = complete
                ? `Step ${gate.step} · ${gate.title} · ${gateClock(job[gate.key])}${manual ? ' · Manual, no GPS' : ''}`
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
                      manual
                        ? 'flex min-h-14 flex-1 items-center gap-2 rounded-2xl bg-[#b54708] px-4 py-2 text-left text-base font-semibold leading-snug text-white'
                        : active
                          ? 'flex h-14 flex-1 items-center gap-2 rounded-2xl bg-[#1f9d55] px-4 text-left text-lg font-semibold text-white disabled:opacity-60'
                          : 'flex h-14 flex-1 items-center rounded-2xl bg-[#e4e7ec] px-4 text-left text-base font-medium text-[#98a2b3]'
                    }
                  >
                    <span className="flex-1">{savingStep === gate.step ? 'Saving…' : label}</span>
                    {marker === 'green' || marker === 'red' ? <ProximityMark kind={marker} /> : null}
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
            <div className="absolute inset-0 z-30 flex items-end bg-black/45 p-4" role="dialog" aria-modal="true">
              <div className="w-full rounded-2xl bg-white p-5 shadow-xl">
                <p className="text-lg leading-snug text-[#1c2430]">{confirm.message}</p>
                <div className="mt-5 flex gap-3">
                  <button
                    type="button"
                    onClick={() => setConfirm(null)}
                    className="h-12 flex-1 rounded-2xl bg-[#e4e7ec] text-base font-semibold text-[#1c2430]"
                  >
                    Cancel
                  </button>
                  <button
                    type="button"
                    disabled={savingStep != null}
                    onClick={() => void passGate(confirm.step, true)}
                    className="h-12 flex-1 rounded-2xl bg-[#1f9d55] text-base font-semibold text-white disabled:opacity-60"
                  >
                    {savingStep === confirm.step ? 'Saving…' : 'Yes'}
                  </button>
                </div>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

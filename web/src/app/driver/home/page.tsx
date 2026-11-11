'use client';

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { DriverLink, useDriverNav } from '@/components/driver/driver-nav';
import { DRIVER_GATES, completedGateCount, jobDetailsConfirmed, truckStage, type DriverJob } from '@/lib/driver-jobs';

const OPEN_ONLY_KEY = 'geodata-driver-open-jobs-only';

function nextLabel(job: DriverJob): string {
  const done = completedGateCount(job);
  if (done >= DRIVER_GATES.length) return 'Finished';
  if (done === 0 && !jobDetailsConfirmed(job)) return 'Confirm job details';
  return truckStage(done)?.label ?? DRIVER_GATES[0].title;
}

/** Step 5 records the end of the job. Until then the job is still open. */
function isOpenJob(job: DriverJob): boolean {
  return !job.step_5_click?.trim();
}

/** Oldest planned start first. Jobs with no planned start stay at the end. */
function byPlannedStart(a: DriverJob, b: DriverJob): number {
  const aTime = a.planned_start_time?.trim() || '';
  const bTime = b.planned_start_time?.trim() || '';
  if (aTime !== bTime) {
    if (!aTime) return 1;
    if (!bTime) return -1;
    return aTime < bTime ? -1 : 1;
  }
  return a.job_id.localeCompare(b.job_id, undefined, { sensitivity: 'base' });
}

function DriverMoreMenu() {
  const nav = useDriverNav();
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function onPointer(event: MouseEvent) {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false);
    }
    function onKey(event: KeyboardEvent) {
      if (event.key === 'Escape') setOpen(false);
    }
    document.addEventListener('mousedown', onPointer);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onPointer);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  return (
    <div ref={rootRef} className="relative z-20 flex items-center">
      <button
        type="button"
        aria-label="More"
        aria-expanded={open}
        aria-haspopup="menu"
        onClick={() => setOpen((value) => !value)}
        className="inline-flex h-11 w-11 items-center justify-center rounded-full text-2xl leading-none tracking-widest text-[#344054]"
      >
        ···
      </button>
      <button type="button" onClick={() => nav.signOut()} className="h-11 rounded-full px-3 text-base font-medium text-[#344054]">
        Sign out
      </button>
      {open ? (
        <div
          role="menu"
          className="absolute right-0 top-full z-20 mt-1 min-w-[9.5rem] overflow-hidden rounded-xl bg-white py-1 shadow-[0_8px_24px_rgba(16,24,40,0.16)]"
        >
          <button
            type="button"
            role="menuitem"
            className="block w-full px-4 py-3 text-left text-base font-medium text-[#1c2430]"
            onClick={() => nav.push('/locations')}
          >
            GPS Log
          </button>
        </div>
      ) : null}
    </div>
  );
}

export default function DriverHomePage() {
  const nav = useDriverNav();
  const [jobs, setJobs] = useState<DriverJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [openOnly, setOpenOnly] = useState(false);

  useLayoutEffect(() => {
    try {
      setOpenOnly(sessionStorage.getItem(OPEN_ONLY_KEY) === '1');
    } catch {
      setOpenOnly(false);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    fetch('/api/driver-jobs', { cache: 'no-store' })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data?.error ?? r.statusText);
        return data;
      })
      .then((data) => {
        if (!cancelled) setJobs(Array.isArray(data?.rows) ? (data.rows as DriverJob[]) : []);
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
  }, []);

  function toggleOpenOnly() {
    setOpenOnly((current) => {
      const next = !current;
      try {
        sessionStorage.setItem(OPEN_ONLY_KEY, next ? '1' : '0');
      } catch {
        /* The list still switches for this visit. */
      }
      return next;
    });
  }

  const visibleJobs = useMemo(() => {
    const sorted = [...jobs].sort(byPlannedStart);
    return openOnly ? sorted.filter(isOpenJob) : sorted;
  }, [jobs, openOnly]);

  return (
    <div className="flex flex-1 flex-col">
      <header className="flex items-center justify-between px-5 pb-3 pt-[max(1rem,env(safe-area-inset-top))]">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.16em] text-[#1f9d55]">Driver</p>
          <h1 className="text-2xl font-semibold text-[#1c2430]">Jobs</h1>
          {nav.driverName && <p className="text-sm text-[#475467]">{nav.driverName}</p>}
        </div>
        <DriverMoreMenu />
      </header>

      <div className="flex items-center justify-between gap-3 px-5 pb-3">
        <p className="text-sm font-medium text-[#475467]">{openOnly ? 'Show open jobs only' : 'All jobs'}</p>
        <button
          type="button"
          aria-pressed={openOnly}
          onClick={toggleOpenOnly}
          className={`h-10 shrink-0 rounded-full px-3 text-sm font-medium ${
            openOnly ? 'bg-[#1f9d55] text-white' : 'bg-white text-[#175cd3] shadow-[0_1px_2px_rgba(16,24,40,0.06)]'
          }`}
        >
          {openOnly ? 'Show all jobs' : 'Show open jobs only'}
        </button>
      </div>

      {error && <p className="mx-5 mb-3 rounded-xl bg-[#fee4e2] px-3 py-2 text-base text-[#b42318]">{error}</p>}

      {loading ? (
        <p className="px-5 text-base text-[#667085]">Loading jobs…</p>
      ) : jobs.length === 0 ? (
        <p className="px-5 text-base leading-relaxed text-[#667085]">No jobs yet.</p>
      ) : visibleJobs.length === 0 ? (
        <p className="px-5 text-base leading-relaxed text-[#667085]">No open jobs.</p>
      ) : (
        <ul className="flex flex-col gap-3 px-4 pb-[max(1.5rem,env(safe-area-inset-bottom))]">
          {visibleJobs.map((job) => (
            <li key={job.id}>
              <DriverLink
                to={`/job/${job.id}`}
                className="block rounded-2xl bg-white px-4 py-4 shadow-[0_1px_2px_rgba(16,24,40,0.06)]"
              >
                <div className="min-w-0">
                  {job.company_name?.trim() ? (
                    <div className="truncate text-sm font-medium text-[#344054]">{job.company_name}</div>
                  ) : null}
                  {job.group_name?.trim() ? <div className="truncate text-sm text-[#667085]">{job.group_name}</div> : null}
                  <div className="text-xl font-semibold text-[#175cd3] underline decoration-[#175cd3]/40 underline-offset-2">
                    {job.job_id}
                  </div>
                </div>
                <p className="mt-1 text-base text-[#1c2430]">
                  {[job.vineyard_name, job.winery_name].filter(Boolean).join(' → ') || 'No vineyard or winery'}
                </p>
                <p className="mt-1 text-sm text-[#667085]">
                  {[job.planned_start_time, job.truck_id ? `Truck ${job.truck_id}` : null].filter(Boolean).join(' · ') ||
                    'No planned start'}
                </p>
                <p className="mt-2 whitespace-pre-line text-sm font-medium text-[#1f9d55]">{nextLabel(job)}</p>
              </DriverLink>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

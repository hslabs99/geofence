'use client';

import { useEffect, useState } from 'react';
import { DriverLink, useDriverNav } from '@/components/driver/driver-nav';
import { DRIVER_GATES, completedGateCount, truckStage, type DriverJob } from '@/lib/driver-jobs';

function nextLabel(job: DriverJob): string {
  const done = completedGateCount(job);
  if (done >= DRIVER_GATES.length) return 'Finished';
  return truckStage(done)?.label ?? DRIVER_GATES[0].title;
}

export default function DriverHomePage() {
  const nav = useDriverNav();
  const [jobs, setJobs] = useState<DriverJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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

  function signOut() {
    nav.signOut();
  }

  return (
    <div className="flex flex-1 flex-col">
      <header className="flex items-center justify-between px-5 pb-3 pt-[max(1rem,env(safe-area-inset-top))]">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.16em] text-[#1f9d55]">Driver</p>
          <h1 className="text-2xl font-semibold text-[#1c2430]">Jobs</h1>
          {nav.driverName && <p className="text-sm text-[#475467]">{nav.driverName}</p>}
        </div>
        <div className="flex items-center gap-1">
          <DriverLink to="/locations" className="inline-flex h-11 items-center px-2 text-base font-medium text-[#175cd3]">
            Locations
          </DriverLink>
          <button type="button" onClick={signOut} className="h-11 rounded-full px-3 text-base font-medium text-[#344054]">
            Sign out
          </button>
        </div>
      </header>

      {error && <p className="mx-5 mb-3 rounded-xl bg-[#fee4e2] px-3 py-2 text-base text-[#b42318]">{error}</p>}

      {loading ? (
        <p className="px-5 text-base text-[#667085]">Loading jobs…</p>
      ) : jobs.length === 0 ? (
        <p className="px-5 text-base leading-relaxed text-[#667085]">No jobs yet.</p>
      ) : (
        <ul className="flex flex-col gap-3 px-4 pb-[max(1.5rem,env(safe-area-inset-bottom))]">
          {jobs.map((job) => (
            <li key={job.id}>
              <DriverLink
                to={`/job/${job.id}`}
                className="block rounded-2xl bg-white px-4 py-4 shadow-[0_1px_2px_rgba(16,24,40,0.06)]"
              >
                <div className="text-xl font-semibold text-[#175cd3] underline decoration-[#175cd3]/40 underline-offset-2">
                  {job.job_id}
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

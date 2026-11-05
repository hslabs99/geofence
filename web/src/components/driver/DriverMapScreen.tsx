'use client';

import { Suspense, useEffect, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import { DriverLink } from '@/components/driver/driver-nav';

function mapsUrl(lat: number, lon: number): string {
  return `https://www.google.com/maps?q=${lat},${lon}`;
}

function DriverMapLookup({ job: jobProp, fence: fenceProp }: { job?: string; fence?: string }) {
  const searchParams = useSearchParams();
  const fence = (fenceProp ?? searchParams.get('fence') ?? '').trim();
  const job = (jobProp ?? searchParams.get('job') ?? '').trim();
  const backHref = /^\d+$/.test(job) ? `/job/${job}` : '/home';
  const [error, setError] = useState<string | null>(null);
  const [href, setHref] = useState<string | null>(null);

  useEffect(() => {
    if (!fence) {
      setError('This job has no geofence name.');
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`/api/driver-jobs/fence-center?fence=${encodeURIComponent(fence)}`, { cache: 'no-store' });
        const data = await res.json().catch(() => ({}));
        if (cancelled) return;
        if (!res.ok) {
          setError(typeof data?.error === 'string' ? data.error : `Failed (${res.status})`);
          return;
        }
        const lat = typeof data.center_lat === 'number' ? data.center_lat : NaN;
        const lon = typeof data.center_lon === 'number' ? data.center_lon : NaN;
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
          setError('No map coordinates for this geofence');
          return;
        }
        const url = mapsUrl(lat, lon);
        setHref(url);
        // This page is opened in its own tab from the job. That tab becomes Google Maps, and the job stays put.
        // A staff preview that is still inside the admin page opens Google Maps beside it.
        if (window.location.pathname.startsWith('/driver')) {
          window.location.assign(url);
          return;
        }
        window.open(url, '_blank', 'noopener,noreferrer');
      } catch {
        if (!cancelled) setError('Could not load geofence');
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [fence]);

  const label = fence || 'This geofence';

  return (
    <div className="flex flex-1 flex-col px-5 pb-[max(1.5rem,env(safe-area-inset-bottom))] pt-[max(0.75rem,env(safe-area-inset-top))]">
      <DriverLink to={backHref} className="inline-flex h-11 items-center text-base font-medium text-[#175cd3]">
        Back
      </DriverLink>
      <h1 className="mt-2 text-2xl font-semibold text-[#1c2430]">Map</h1>
      {error ? (
        <p className="mt-3 text-base leading-relaxed text-[#b42318]">{error}</p>
      ) : (
        <p className="mt-3 text-base leading-relaxed text-[#475467]">
          {href ? `Opening ${label} in Google Maps…` : `Finding ${label}…`}
        </p>
      )}
      {href ? (
        <a href={href} target="_blank" rel="noopener noreferrer" className="mt-4 text-base font-semibold text-[#175cd3]">
          Open in Google Maps
        </a>
      ) : null}
    </div>
  );
}

export default function DriverMapScreen({ job, fence }: { job?: string; fence?: string }) {
  return (
    <Suspense fallback={<p className="px-5 pt-6 text-base text-[#667085]">Loading…</p>}>
      <DriverMapLookup job={job} fence={fence} />
    </Suspense>
  );
}

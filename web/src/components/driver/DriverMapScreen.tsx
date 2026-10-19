'use client';

import { Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import { DriverLink } from '@/components/driver/driver-nav';

function DriverMapPlaceholder({ job: jobProp, fence: fenceProp }: { job?: string; fence?: string }) {
  const searchParams = useSearchParams();
  const fence = (fenceProp ?? searchParams.get('fence') ?? '').trim();
  const job = (jobProp ?? searchParams.get('job') ?? '').trim();
  const backHref = /^\d+$/.test(job) ? `/job/${job}` : '/home';

  return (
    <div className="flex flex-1 flex-col px-5 pb-[max(1.5rem,env(safe-area-inset-bottom))] pt-[max(0.75rem,env(safe-area-inset-top))]">
      <DriverLink to={backHref} className="inline-flex h-11 items-center text-base font-medium text-[#175cd3]">
        Back
      </DriverLink>
      <h1 className="mt-2 text-2xl font-semibold text-[#1c2430]">Map</h1>
      <p className="mt-3 text-base leading-relaxed text-[#475467]">
        {fence ? fence : 'This geofence'} is not on the map yet. The name is stored on the job and will open here once
        the geofence lookup is connected.
      </p>
    </div>
  );
}

export default function DriverMapScreen({ job, fence }: { job?: string; fence?: string }) {
  return (
    <Suspense fallback={<p className="px-5 pt-6 text-base text-[#667085]">Loading…</p>}>
      <DriverMapPlaceholder job={job} fence={fence} />
    </Suspense>
  );
}

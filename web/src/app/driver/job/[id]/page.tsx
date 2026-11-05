'use client';

import { Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import DriverJobScreen from '@/components/driver/DriverJobScreen';

function DriverJobPageInner() {
  const params = useSearchParams();
  const raw = params.get('mode');
  const mode = raw === 'control' || raw === 'watch' ? raw : null;
  return <DriverJobScreen mode={mode} />;
}

export default function DriverJobPage() {
  return (
    <Suspense fallback={<p className="px-4 pt-6 text-base text-[#667085]">Loading job…</p>}>
      <DriverJobPageInner />
    </Suspense>
  );
}

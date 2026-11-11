'use client';

import { useEffect, useState, type ReactNode } from 'react';

const POLL_MS = 30_000;

export type ApproachReading = {
  active?: boolean;
  step?: 2 | 3 | 4;
  target?: 'vineyard' | 'winery';
  fence?: string | null;
  truck?: string;
  meters?: number | null;
  inside?: boolean;
  beenInside?: boolean;
  proximity?: 'yellow' | 'red' | null;
  crossedAt?: string | null;
  remind?: boolean;
  limitMeters?: number;
  gpstimeNz?: string | null;
  fetchedAt?: string | null;
  driverAppPollSeconds?: number;
  error?: string;
};

/** Polls the next gate's fence distance. Active for the vineyard arrival, vineyard departure, and winery arrival. */
export function useJobApproach(jobId: number, done: number) {
  const watching = done === 1 || done === 2 || done === 3;
  const [reading, setReading] = useState<ApproachReading | null>(null);
  const [loading, setLoading] = useState(watching);
  const [pollMs, setPollMs] = useState(POLL_MS);

  useEffect(() => {
    if (!watching || !jobId) return;
    let cancelled = false;

    async function load() {
      try {
        const res = await fetch(`/api/driver-jobs/${jobId}/approach`, { cache: 'no-store' });
        const data = (await res.json()) as ApproachReading;
        if (!res.ok) throw new Error(data?.error ?? res.statusText);
        if (!cancelled) {
          setReading(data);
          const seconds = data?.driverAppPollSeconds;
          if (typeof seconds === 'number' && seconds >= 5) setPollMs(seconds * 1000);
        }
      } catch (err) {
        if (!cancelled) setReading({ active: true, error: err instanceof Error ? err.message : String(err) });
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    void load();
    const timer = setInterval(() => void load(), pollMs);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [jobId, watching, done, pollMs]);

  return { reading: watching ? reading : null, loading: watching && loading };
}

/** Metres from the latest stored truck fix to the vineyard or winery fence edge. */
export default function ApproachMeters({
  reading,
  loading,
  corner,
  tripMeters,
}: {
  reading: ApproachReading | null;
  loading: boolean;
  corner?: ReactNode;
  /** Full length of this road. Outbound is from the job-start fix. The return is fence to fence. */
  tripMeters?: number | null;
}) {
  const watching = reading?.active !== false && (reading != null || loading);
  if (!watching) {
    return corner ? <div className="mx-4 mb-2 flex justify-end">{corner}</div> : null;
  }

  const leaving = reading?.step === 3;
  const place = reading?.target === 'winery' ? 'winery' : 'vineyard';
  const meters = typeof reading?.meters === 'number' ? reading.meters.toLocaleString('en-NZ') : null;
  const trip =
    typeof tripMeters === 'number'
      ? place === 'winery'
        ? `This trip is ${tripMeters.toLocaleString('en-NZ')} m from the vineyard fence to the winery fence.`
        : `This trip is ${tripMeters.toLocaleString('en-NZ')} m from where the job started.`
      : null;

  return (
    <div className="mx-4 mb-2 rounded-2xl bg-white px-4 py-2.5 shadow-[0_1px_2px_rgba(16,24,40,0.06)]">
      <div className="flex items-center justify-between gap-3">
        <p className="min-w-0 text-xs font-medium uppercase tracking-wide text-[#667085]">
          {leaving ? 'From the vineyard fence' : `To the ${place} fence`}
        </p>
        {corner}
      </div>
      {loading ? (
        <p className="mt-1 text-base text-[#667085]">Checking the latest location…</p>
      ) : reading?.error ? (
        <p className="mt-1 text-base text-[#b42318]">{reading.error}</p>
      ) : reading?.inside ? (
        <p className={`mt-1 text-2xl font-semibold ${leaving ? 'text-[#b54708]' : 'text-[#1f9d55]'}`}>
          {leaving ? 'Still inside' : 'At the fence'}
        </p>
      ) : (
        <p className="mt-1 text-2xl font-semibold tabular-nums text-[#1c2430]">
          {meters} <span className="text-lg font-medium text-[#475467]">m</span>
        </p>
      )}
      {!loading && !reading?.error && (
        <p className="mt-1 text-sm text-[#475467]">
          {reading?.inside
            ? leaving
              ? 'Leave the vineyard before tapping depart.'
              : 'The latest fix is on or inside the fence.'
            : leaving
              ? `Outside the edge of ${reading?.fence || place}.`
              : `Remaining to the edge of ${reading?.fence || place}.${trip ? ` ${trip}` : ''}`}
        </p>
      )}
    </div>
  );
}

'use client';

import { useEffect, useState } from 'react';
import { DriverLink } from '@/components/driver/driver-nav';
import { useLocationAge } from '@/components/driver/useLocationAge';

type Row = {
  id: number;
  truck_id: string | null;
  location: string | null;
  gpstime_nz: string | null;
  polltime: string | null;
  coordinates: string | null;
  fencename: string | null;
  softfencename: string | null;
};

function line(label: string, value: string | null) {
  return (
    <p className="mt-1 text-sm text-[#475467]">
      <span className="font-medium text-[#667085]">{label} </span>
      {value?.trim() || '—'}
    </p>
  );
}

function LocationCard({ row }: { row: Row }) {
  const gpsAge = useLocationAge(row.gpstime_nz);
  return (
    <li className="rounded-2xl bg-white px-4 py-3 shadow-[0_1px_2px_rgba(16,24,40,0.06)]">
      <p className="text-lg font-semibold text-[#1c2430]">{row.truck_id || '—'}</p>
      {line('Fetched', row.polltime ? `${row.polltime} NZ` : null)}
      {line('GPS', row.gpstime_nz ? `${row.gpstime_nz} NZ${gpsAge != null ? ` (${gpsAge}s)` : ''}` : null)}
      {line('Where', row.location)}
      {line('Coordinates', row.coordinates)}
      {line('Fence', row.fencename)}
      {line('Soft fence', row.softfencename)}
    </li>
  );
}

export default function DriverLocationsPage() {
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pollMs, setPollMs] = useState(30_000);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const res = await fetch('/api/current-locations?limit=100', { cache: 'no-store' });
        const data = await res.json();
        if (!res.ok) throw new Error(data?.error ?? res.statusText);
        if (cancelled) return;
        setRows(Array.isArray(data?.rows) ? (data.rows as Row[]) : []);
        const seconds = data?.driverAppPollSeconds;
        if (typeof seconds === 'number' && seconds >= 5) setPollMs(seconds * 1000);
        setError(null);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
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
  }, [pollMs]);

  return (
    <div className="flex flex-1 flex-col">
      <header className="px-5 pb-3 pt-[max(0.75rem,env(safe-area-inset-top))]">
        <DriverLink to="/home" className="inline-flex h-11 items-center text-base font-medium text-[#175cd3]">
          Jobs
        </DriverLink>
        <h1 className="text-2xl font-semibold text-[#1c2430]">GPS Log</h1>
        <p className="mt-1 text-sm text-[#667085]">Each poll adds a row. Newest first.</p>
      </header>
      {error && <p className="mx-5 mb-3 rounded-xl bg-[#fee4e2] px-3 py-2 text-base text-[#b42318]">{error}</p>}
      {loading ? (
        <p className="px-5 text-base text-[#667085]">Loading…</p>
      ) : rows.length === 0 ? (
        <p className="px-5 text-base text-[#667085]">No GPS log yet.</p>
      ) : (
        <ul className="flex flex-col gap-3 px-4 pb-[max(1.5rem,env(safe-area-inset-bottom))]">
          {rows.map((row) => (
            <LocationCard key={row.id} row={row} />
          ))}
        </ul>
      )}
    </div>
  );
}

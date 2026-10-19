'use client';

import { useEffect, useState } from 'react';
import { usePathname } from 'next/navigation';
import { useDriverNavOptional } from '@/components/driver/driver-nav';
import { useLocationAge } from '@/components/driver/useLocationAge';
import { GEODATA_USER_STORAGE_KEY } from '@/contexts/ViewModeContext';

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

type Status = {
  running: boolean;
  intervalSeconds: number;
  driverAppPollSeconds?: number;
  lastPolltime: string | null;
  truckId: string | null;
  truck: Row | null;
  latest: Row | null;
  error?: string;
};

function field(label: string, value: string | null | undefined) {
  return (
    <p className="mt-1 text-sm">
      <span className="font-medium opacity-70">{label} </span>
      {value?.trim() || '—'}
    </p>
  );
}

function RowCard({ title, row, empty }: { title: string; row: Row | null; empty: string }) {
  const gpsAge = useLocationAge(row?.gpstime_nz);
  return (
    <div className="rounded-xl bg-white/80 px-3 py-2 text-[#1c2430]">
      <p className="text-xs font-semibold uppercase tracking-wide text-[#667085]">{title}</p>
      {row ? (
        <>
          <p className="mt-1 text-base font-semibold">{row.truck_id || '—'}</p>
          {field('Fetched', row.polltime ? `${row.polltime} NZ` : null)}
          {field('GPS', row.gpstime_nz ? `${row.gpstime_nz} NZ${gpsAge != null ? ` (${gpsAge}s)` : ''}` : null)}
          {field('Where', row.location)}
          {field('Coordinates', row.coordinates)}
          {field('Fence', row.fencename)}
          {field('Soft fence', row.softfencename)}
        </>
      ) : (
        <p className="mt-1 text-sm text-[#667085]">{empty}</p>
      )}
    </div>
  );
}

function reading(status: Status | null, truckId: string | null): string {
  if (!status) return 'Checking the location service.';
  if (!status.running) return 'The location service is stopped.';
  if (!truckId) return 'The location service is running.';
  if (!status.truck) return 'The service is running. There is no row yet for this truck.';
  if (status.latest && status.truck.polltime && status.truck.polltime === status.latest.polltime) {
    return 'This truck was in the latest poll.';
  }
  return 'A newer poll has a row for another truck. This truck was not in that poll.';
}

export default function LocationServiceStatus({ variant }: { variant: 'phone' | 'staff' }) {
  const routePath = usePathname();
  const driverNav = useDriverNavOptional();
  const pathname = driverNav?.pathname ?? routePath;
  const [status, setStatus] = useState<Status | null>(null);
  const [open, setOpen] = useState(false);
  const [truckId, setTruckId] = useState<string | null>(null);
  const [pollMs, setPollMs] = useState(variant === 'phone' ? 30_000 : 10_000);

  const hidden = variant === 'phone' && pathname === '/driver';

  useEffect(() => {
    if (hidden) return;
    let cancelled = false;
    async function resolveTruck() {
      if (variant !== 'phone') {
        if (!cancelled) setTruckId(null);
        return;
      }
      const jobMatch = pathname.match(/^\/driver\/job\/([^/]+)/);
      if (jobMatch) {
        try {
          const res = await fetch(`/api/driver-jobs/${jobMatch[1]}`, { cache: 'no-store' });
          const data = await res.json();
          const id = typeof data?.row?.truck_id === 'string' ? data.row.truck_id.trim() : '';
          if (!cancelled && id) {
            setTruckId(id);
            return;
          }
        } catch {
          /* fall through to the signed-in truck */
        }
      }
      const fromNav = driverNav?.truckRego?.trim() || '';
      if (fromNav) {
        if (!cancelled) setTruckId(fromNav);
        return;
      }
      let rego = '';
      try {
        const raw = localStorage.getItem(GEODATA_USER_STORAGE_KEY);
        const user = raw ? (JSON.parse(raw) as { truckRego?: string }) : null;
        rego = typeof user?.truckRego === 'string' ? user.truckRego.trim() : '';
      } catch {
        rego = '';
      }
      if (!cancelled) setTruckId(rego || null);
    }
    void resolveTruck();
    return () => {
      cancelled = true;
    };
  }, [driverNav?.truckRego, hidden, pathname, variant]);

  useEffect(() => {
    if (hidden) return;
    let cancelled = false;
    async function load() {
      try {
        const query = truckId ? `?truck=${encodeURIComponent(truckId)}` : '';
        const res = await fetch(`/api/current-locations/status${query}`, { cache: 'no-store' });
        const data = (await res.json()) as Status;
        if (!res.ok) throw new Error(data?.error ?? res.statusText);
        if (!cancelled) {
          setStatus(data);
          if (variant === 'phone' && typeof data?.driverAppPollSeconds === 'number' && data.driverAppPollSeconds >= 5) {
            setPollMs(data.driverAppPollSeconds * 1000);
          }
        }
      } catch {
        if (!cancelled) setStatus(null);
      }
    }
    void load();
    const id = setInterval(load, variant === 'phone' ? pollMs : 10_000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [hidden, pollMs, truckId, variant]);

  if (hidden) return null;

  const running = status?.running === true;
  const label = running ? 'Service running' : 'Service stopped';
  const dot = running ? 'bg-[#12b76a]' : 'bg-[#f04438]';

  const button =
    variant === 'phone' ? (
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="inline-flex h-9 items-center gap-2 rounded-full bg-white px-3 text-sm font-semibold text-[#1c2430] shadow-[0_1px_2px_rgba(16,24,40,0.08)]"
      >
        <span className={`h-2.5 w-2.5 rounded-full ${dot}`} />
        {label}
      </button>
    ) : (
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        className="flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-xs font-medium text-zinc-700 hover:bg-zinc-100 dark:text-zinc-200 dark:hover:bg-zinc-800"
      >
        <span className={`h-2.5 w-2.5 shrink-0 rounded-full ${dot}`} />
        {label}
      </button>
    );

  const panel = open ? (
    <div
      className={
        variant === 'phone'
          ? 'absolute inset-0 z-40 flex flex-col bg-[#f4f7f8]'
          : 'absolute bottom-12 left-2 right-2 z-40 rounded-lg border border-zinc-200 bg-white p-3 shadow-lg dark:border-zinc-700 dark:bg-zinc-900'
      }
    >
      <div className={variant === 'phone' ? 'flex items-center justify-between px-4 pt-[max(0.75rem,env(safe-area-inset-top))]' : 'flex items-center justify-between'}>
        <p className={variant === 'phone' ? 'text-lg font-semibold text-[#1c2430]' : 'text-sm font-semibold text-zinc-900 dark:text-zinc-100'}>
          {label}
        </p>
        <button
          type="button"
          onClick={() => setOpen(false)}
          className={variant === 'phone' ? 'h-11 px-2 text-base font-medium text-[#175cd3]' : 'text-xs font-medium text-zinc-500'}
        >
          Close
        </button>
      </div>
      <div className={variant === 'phone' ? 'flex flex-col gap-3 px-4 py-3' : 'mt-2 flex flex-col gap-2 text-sm text-zinc-700 dark:text-zinc-200'}>
        <p className={variant === 'phone' ? 'text-sm text-[#475467]' : ''}>{reading(status, truckId)}</p>
        <p className={variant === 'phone' ? 'text-sm text-[#475467]' : 'text-xs text-zinc-500'}>
          {status?.lastPolltime ? `Last poll ${status.lastPolltime}` : 'No poll yet'}
          {status ? ` · every ${status.intervalSeconds} sec` : ''}
        </p>
        {variant === 'phone' && (
          <RowCard
            title={truckId ? `This truck · ${truckId}` : 'This truck'}
            row={status?.truck ?? null}
            empty={truckId ? 'No location row for this truck.' : 'No truck is set on this login.'}
          />
        )}
        <RowCard
          title="Any truck"
          row={status?.latest ?? null}
          empty="No location rows yet."
        />
      </div>
    </div>
  ) : null;

  if (variant === 'phone') {
    return (
      <>
        <div className="flex justify-end px-3 pt-[max(0.4rem,env(safe-area-inset-top))]">{button}</div>
        {panel}
      </>
    );
  }

  return (
    <div className="relative mb-2">
      {button}
      {panel}
    </div>
  );
}

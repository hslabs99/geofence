'use client';

import { useEffect, useState } from 'react';
import LocationServiceStatus from '@/components/LocationServiceStatus';
import { DriverEmbedNav, type DriverIdentity } from '@/components/driver/driver-nav';
import DriverHomePage from '@/app/driver/home/page';
import DriverJobScreen from '@/components/driver/DriverJobScreen';
import DriverLocationsPage from '@/app/driver/locations/page';
import DriverMapScreen from '@/components/driver/DriverMapScreen';

type DriverChoice = DriverIdentity & { key: string };

function screenFor(path: string) {
  const splitAt = path.indexOf('?');
  const pathname = splitAt === -1 ? path : path.slice(0, splitAt);
  const params = new URLSearchParams(splitAt === -1 ? '' : path.slice(splitAt + 1));
  if (pathname === '/locations') return <DriverLocationsPage />;
  if (pathname === '/map') {
    return (
      <DriverMapScreen
        job={params.get('job') ?? ''}
        fence={params.get('fence') ?? ''}
        route={params.get('route') === '1'}
      />
    );
  }
  const job = pathname.match(/^\/job\/([^/]+)/);
  if (job) return <DriverJobScreen id={decodeURIComponent(job[1])} />;
  return <DriverHomePage />;
}

/** Staff view of the phone driver app. The screens are the /driver pages. */
export default function DriverAppPreview() {
  const [drivers, setDrivers] = useState<DriverChoice[]>([]);
  const [driverKey, setDriverKey] = useState('');
  const [orientation, setOrientation] = useState<'portrait' | 'landscape'>('portrait');
  const [path, setPath] = useState('/home');
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch('/api/users', { cache: 'no-store' })
      .then(async (response) => {
        const data = await response.json();
        if (!response.ok) throw new Error(data?.error ?? response.statusText);
        return data;
      })
      .then((data) => {
        if (cancelled) return;
        const users = Array.isArray(data?.users) ? data.users : [];
        const choices: DriverChoice[] = users
          .filter((user: { userType?: string }) => user.userType === 'Driver')
          .map((user: { email?: string; firstname?: string | null; truck_rego?: string | null }) => {
            const email = String(user.email ?? '').trim();
            const driverName = String(user.firstname ?? '').trim() || email || 'Driver';
            return {
              key: email || driverName,
              driverName,
              email: email || null,
              truckRego: String(user.truck_rego ?? '').trim() || null,
            };
          })
          .sort((a: DriverChoice, b: DriverChoice) => a.driverName.localeCompare(b.driverName, undefined, { sensitivity: 'base' }));
        setDrivers(choices);
      })
      .catch((err) => {
        if (!cancelled) setLoadError(err instanceof Error ? err.message : String(err));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const driver = drivers.find((choice) => choice.key === driverKey) ?? null;
  const frame =
    orientation === 'portrait'
      ? 'h-[min(844px,78vh)] w-[min(390px,100%)]'
      : 'h-[min(420px,72vh)] w-[min(844px,100%)]';

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-end gap-3">
        <div>
          <label className="mb-0.5 block text-[11px] font-medium text-zinc-500 dark:text-zinc-400">Driver</label>
          <select
            value={driverKey}
            onChange={(event) => {
              setDriverKey(event.target.value);
              setPath('/home');
            }}
            className="w-full min-w-[16rem] rounded border border-zinc-300 bg-white px-2 py-1 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          >
            <option value="">Select a driver</option>
            {drivers.map((choice) => (
              <option key={choice.key} value={choice.key}>
                {choice.driverName}
                {choice.truckRego ? ` · ${choice.truckRego}` : ''}
              </option>
            ))}
          </select>
        </div>
        <div className="flex gap-1">
          {(['portrait', 'landscape'] as const).map((mode) => (
            <button
              key={mode}
              type="button"
              onClick={() => setOrientation(mode)}
              className={`rounded px-3 py-1 text-xs font-medium ${
                orientation === mode
                  ? 'bg-zinc-900 text-white dark:bg-zinc-100 dark:text-zinc-900'
                  : 'border border-zinc-300 text-zinc-600 dark:border-zinc-600 dark:text-zinc-300'
              }`}
            >
              {mode === 'portrait' ? 'Portrait' : 'Landscape'}
            </button>
          ))}
        </div>
      </div>
      {loadError && <p className="mb-3 text-sm text-red-700 dark:text-red-300">{loadError}</p>}
      {!driver ? (
        <p className="text-sm text-zinc-600 dark:text-zinc-400">Choose a driver to open the driver app as that driver.</p>
      ) : (
        <DriverEmbedNav identity={driver} path={path} onPath={setPath} onSignOut={() => setDriverKey('')}>
          <div className="flex justify-center rounded-xl bg-[#d5dee6] p-4">
            <div className={`relative flex flex-col overflow-hidden bg-[#f4f7f8] text-[#1c2430] shadow-[0_0_0_1px_rgba(15,23,42,0.06)] ${frame}`}>
              <LocationServiceStatus variant="phone" />
              <div className="flex min-h-0 flex-1 flex-col overflow-auto">{screenFor(path)}</div>
            </div>
          </div>
        </DriverEmbedNav>
      )}
    </div>
  );
}

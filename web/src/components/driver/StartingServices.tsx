'use client';

import { useEffect, useLayoutEffect, useState } from 'react';
import { usePathname } from 'next/navigation';
import { GEODATA_USER_STORAGE_KEY } from '@/contexts/ViewModeContext';

type ServiceRow = { name: string; running: boolean };

type Phase = 'pending' | 'checking' | 'starting' | 'ready' | 'error';

function isDriverSession(): boolean {
  try {
    const raw = localStorage.getItem(GEODATA_USER_STORAGE_KEY);
    const user = raw ? (JSON.parse(raw) as { userType?: string }) : null;
    return user?.userType === 'Driver';
  } catch {
    return false;
  }
}

/** Holds a signed-in driver until Track Solid, VWork, gate updates, and location alerts are running. */
export default function StartingServices({ children }: { children: React.ReactNode }) {
  const pathname = usePathname() || '/driver';
  const onLogin = pathname === '/driver';
  const [signedIn, setSignedIn] = useState(false);
  const [phase, setPhase] = useState<Phase>('pending');
  const [services, setServices] = useState<ServiceRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);

  useLayoutEffect(() => {
    if (onLogin) {
      setSignedIn(false);
      return;
    }
    setSignedIn(isDriverSession());
  }, [onLogin]);

  useEffect(() => {
    if (onLogin || !signedIn) return;
    let cancelled = false;
    async function run() {
      setPhase('checking');
      setError(null);
      try {
        const first = await fetch('/api/driver-services/ensure', { cache: 'no-store' });
        const status = (await first.json()) as { error?: string; services?: ServiceRow[] };
        if (!first.ok) throw new Error(status.error ?? first.statusText);
        const rows = status.services ?? [];
        if (cancelled) return;
        if (rows.length > 0 && rows.every((row) => row.running)) {
          setPhase('ready');
          return;
        }
        setServices(rows);
        setPhase('starting');
        const res = await fetch('/api/driver-services/ensure', { method: 'POST', cache: 'no-store' });
        const started = (await res.json()) as { error?: string; services?: ServiceRow[]; errors?: string[] };
        if (!res.ok) throw new Error(started.error ?? res.statusText);
        if (cancelled) return;
        const next = started.services ?? [];
        setServices(next);
        const problems = (started.errors ?? []).filter(Boolean);
        if (problems.length > 0 || next.some((row) => !row.running)) {
          setError(problems.join(' ') || 'A service did not start.');
          setPhase('error');
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, 900));
        if (!cancelled) setPhase('ready');
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
        setPhase('error');
      }
    }
    void run();
    return () => {
      cancelled = true;
    };
  }, [attempt, onLogin, signedIn]);

  if (onLogin || !signedIn || phase === 'ready' || phase === 'pending') return <>{children}</>;

  const title = phase === 'checking' ? 'Checking services' : phase === 'error' ? 'Services did not start' : 'Starting services';
  const detail =
    phase === 'checking'
      ? 'Pause here while the services are checked.'
      : phase === 'error'
        ? 'These services are still stopped.'
        : 'These services were stopped. Pause here until each one is running.';

  return (
    <div className="flex flex-1 flex-col px-5 pb-[max(1.5rem,env(safe-area-inset-bottom))] pt-[max(2.5rem,env(safe-area-inset-top))]">
      <p className="text-sm font-semibold uppercase tracking-[0.18em] text-[#1f9d55]">Driver</p>
      <h1 className="mt-2 text-3xl font-semibold tracking-tight text-[#1c2430]">{title}</h1>
      <p className="mt-2 text-base leading-snug text-[#667085]">{detail}</p>
      {phase !== 'checking' && (
        <ul className="mt-8 flex flex-col gap-3">
          {services.map((service) => (
            <li key={service.name} className="flex items-center justify-between gap-3 rounded-2xl bg-white px-4 py-3">
              <span className="text-base font-medium text-[#1c2430]">{service.name}</span>
              <span className={service.running ? 'text-sm font-semibold text-[#1f9d55]' : 'text-sm font-semibold text-[#b54708]'}>
                {service.running ? 'Running' : phase === 'error' ? 'Stopped' : 'Starting'}
              </span>
            </li>
          ))}
        </ul>
      )}
      {error && <p className="mt-4 text-base text-[#b42318]">{error}</p>}
      {phase === 'error' && (
        <button
          type="button"
          onClick={() => setAttempt((n) => n + 1)}
          className="mt-6 h-14 w-full rounded-2xl bg-[#1f9d55] text-lg font-semibold text-white"
        >
          Try again
        </button>
      )}
    </div>
  );
}

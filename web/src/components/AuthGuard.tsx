'use client';

import { usePathname } from 'next/navigation';
import { useEffect, useState } from 'react';
import { readStoredUserType } from '@/contexts/ViewModeContext';

const DRIVER_HOME = '/driver/home';

function isDriverAppPath(pathname: string): boolean {
  return pathname === '/driver' || pathname.startsWith('/driver/');
}

/**
 * Staff routes use /login. The driver phone app uses /driver and stays on /driver/*.
 * Blocks the first paint until the session is checked.
 */
export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const [ready, setReady] = useState(false);

  useEffect(() => {
    if (typeof localStorage === 'undefined') return;

    const session = readStoredUserType();
    const onLogin = pathname === '/login';
    const onDriverLogin = pathname === '/driver';
    const onDriverApp = isDriverAppPath(pathname);
    const onDbSwitch = pathname === '/db-switch';

    if (onLogin && session) {
      window.location.replace(session === 'Driver' ? DRIVER_HOME : '/');
      return;
    }
    if (onDriverLogin && session) {
      window.location.replace(session === 'Driver' ? DRIVER_HOME : '/');
      return;
    }
    if (session === 'Driver' && !onDriverApp && !onDbSwitch) {
      window.location.replace(DRIVER_HOME);
      return;
    }
    if (!session && onDriverApp && !onDriverLogin) {
      window.location.replace('/driver');
      return;
    }
    if (!onLogin && !onDriverApp && !onDbSwitch && !session) {
      window.location.replace('/login');
      return;
    }

    setReady(true);
  }, [pathname]);

  if (!ready) {
    const driverChrome = isDriverAppPath(pathname);
    return (
      <div
        className={
          driverChrome
            ? 'flex min-h-dvh items-center justify-center bg-[#e8eef2]'
            : 'flex min-h-screen items-center justify-center bg-zinc-50 dark:bg-zinc-950'
        }
      >
        <p className={driverChrome ? 'text-base text-[#667085]' : 'text-sm text-zinc-500 dark:text-zinc-400'}>
          Loading…
        </p>
      </div>
    );
  }

  return <>{children}</>;
}

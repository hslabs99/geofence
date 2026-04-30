'use client';

import { usePathname, useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { readStoredUserType } from '@/contexts/ViewModeContext';

/**
 * Require a valid `geodata_user` session (recognized userType in localStorage) for every route except `/login`.
 * Blocks the first paint until the session is checked so the app does not briefly render behind the login wall.
 * Unauthenticated hits use a full navigation to `/login` so the gate cannot get stuck if client routing is slow.
 */
export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const [ready, setReady] = useState(false);

  useEffect(() => {
    if (typeof localStorage === 'undefined') return;

    const session = readStoredUserType();
    const onLogin = pathname === '/login';

    if (onLogin && session) {
      router.replace('/');
      return;
    }
    if (!onLogin && !session) {
      window.location.replace('/login');
      return;
    }

    setReady(true);
  }, [pathname, router]);

  if (!ready) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-zinc-50 dark:bg-zinc-950">
        <p className="text-sm text-zinc-500 dark:text-zinc-400">Loading…</p>
      </div>
    );
  }

  return <>{children}</>;
}

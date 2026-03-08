'use client';

import { usePathname, useRouter } from 'next/navigation';
import { useEffect } from 'react';
import { GEODATA_USER_STORAGE_KEY } from '@/contexts/ViewModeContext';

/**
 * Redirect to /login when not logged in (no geodata_user in localStorage).
 * Redirect to / when on /login but already logged in.
 */
export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();

  useEffect(() => {
    if (typeof localStorage === 'undefined') return;
    const raw = localStorage.getItem(GEODATA_USER_STORAGE_KEY);
    const hasUser = !!raw;
    if (pathname === '/login') {
      if (hasUser) router.replace('/');
      return;
    }
    if (!hasUser) router.replace('/login');
  }, [pathname, router]);

  return <>{children}</>;
}

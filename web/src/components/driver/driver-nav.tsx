'use client';

import { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { usePathname, useRouter } from 'next/navigation';
import { GEODATA_USER_STORAGE_KEY } from '@/contexts/ViewModeContext';

export type DriverIdentity = {
  driverName: string;
  email: string | null;
  truckRego: string | null;
};

type DriverNavValue = {
  /** App path, always under /driver, without a query string. */
  pathname: string;
  driverName: string | null;
  truckRego: string | null;
  push: (to: string) => void;
  signOut: () => void;
};

const DriverNavContext = createContext<DriverNavValue | null>(null);

export function useDriverNav(): DriverNavValue {
  const value = useContext(DriverNavContext);
  if (!value) throw new Error('Driver navigation is not available');
  return value;
}

export function useDriverNavOptional(): DriverNavValue | null {
  return useContext(DriverNavContext);
}

function appPath(to: string): string {
  const raw = to.startsWith('/driver') ? to.slice('/driver'.length) : to;
  const path = raw.startsWith('/') ? raw : `/${raw}`;
  return path || '/home';
}

export function DriverRouteNav({ children }: { children: React.ReactNode }) {
  const pathname = usePathname() || '/driver';
  const router = useRouter();
  const [identity, setIdentity] = useState<DriverIdentity | null>(null);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(GEODATA_USER_STORAGE_KEY);
      const user = raw
        ? (JSON.parse(raw) as { userType?: string; driverName?: string; email?: string; truckRego?: string })
        : null;
      if (!user || user.userType !== 'Driver') {
        setIdentity(null);
        return;
      }
      const driverName = (user.driverName || user.email || 'Driver').trim();
      setIdentity({
        driverName,
        email: user.email ?? null,
        truckRego: user.truckRego?.trim() || null,
      });
    } catch {
      setIdentity(null);
    }
  }, [pathname]);

  const value = useMemo<DriverNavValue>(
    () => ({
      pathname,
      driverName: identity?.driverName ?? null,
      truckRego: identity?.truckRego ?? null,
      push: (to) => router.push(`/driver${appPath(to)}`),
      signOut: () => {
        localStorage.removeItem(GEODATA_USER_STORAGE_KEY);
        window.location.href = '/driver';
      },
    }),
    [pathname, identity, router],
  );

  return <DriverNavContext.Provider value={value}>{children}</DriverNavContext.Provider>;
}

export function DriverEmbedNav({
  identity,
  path,
  onPath,
  onSignOut,
  children,
}: {
  identity: DriverIdentity;
  path: string;
  onPath: (path: string) => void;
  onSignOut: () => void;
  children: React.ReactNode;
}) {
  const value = useMemo<DriverNavValue>(
    () => ({
      pathname: `/driver${appPath(path).split('?')[0]}`,
      driverName: identity.driverName,
      truckRego: identity.truckRego,
      push: (to) => onPath(appPath(to)),
      signOut: onSignOut,
    }),
    [identity, path, onPath, onSignOut],
  );

  return <DriverNavContext.Provider value={value}>{children}</DriverNavContext.Provider>;
}

/** Same control on the phone and in the staff preview. Click stays inside the current host. */
export function DriverLink({
  to,
  className,
  children,
}: {
  to: string;
  className?: string;
  children: React.ReactNode;
}) {
  const nav = useDriverNav();
  return (
    <a
      href={`/driver${appPath(to)}`}
      className={className}
      onClick={(event) => {
        event.preventDefault();
        nav.push(to);
      }}
    >
      {children}
    </a>
  );
}

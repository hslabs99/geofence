'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useViewMode } from '@/contexts/ViewModeContext';

/** Where Client users are sent when they hit staff-only pages. */
export const CLIENT_USER_HOME = '/query/summary';

/**
 * Redirect tbl_users Client accounts away from staff-only UI (admin, inspect, etc.).
 * UI-only guard — APIs are not role-checked here.
 */
export function useRequireStaffUser(): { blocked: boolean } {
  const { userType } = useViewMode();
  const router = useRouter();

  useEffect(() => {
    if (userType === 'Client') {
      router.replace(CLIENT_USER_HOME);
    }
  }, [userType, router]);

  const blocked = userType === null || userType === 'Client';
  return { blocked };
}

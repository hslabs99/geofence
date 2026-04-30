'use client';

import React, { createContext, useCallback, useContext, useEffect, useState } from 'react';
import { usePathname } from 'next/navigation';

/** Super and Admin both use admin view in Summary; Client uses client view. */
export type ViewMode = 'super' | 'admin' | 'client';

/** User type from tbl_users; determines which view modes are allowed. */
export type UserTypeLabel = 'Super Admin' | 'Admin' | 'Client';

const STORAGE_KEY = 'geodata_user';

const VALID_USER_TYPES: UserTypeLabel[] = ['Super Admin', 'Admin', 'Client'];

/** Parse `geodata_user` from localStorage; null if missing or invalid. */
export function readStoredUserType(): UserTypeLabel | null {
  if (typeof localStorage === 'undefined') return null;
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as StoredUser;
    const t = parsed?.userType;
    return t && VALID_USER_TYPES.includes(t as UserTypeLabel) ? (t as UserTypeLabel) : null;
  } catch {
    return null;
  }
}

function userTypeToViewMode(t: UserTypeLabel): ViewMode {
  if (t === 'Super Admin') return 'super';
  if (t === 'Admin') return 'admin';
  return 'client';
}

/** Allowed view modes for each user type (rights go down: super can pick all, client only client). */
function allowedViewModesForUserType(t: UserTypeLabel | null): ViewMode[] {
  if (!t) return [];
  if (t === 'Super Admin') return ['super', 'admin', 'client'];
  if (t === 'Admin') return ['admin', 'client'];
  return ['client'];
}

export interface StoredUser {
  email?: string;
  userType?: UserTypeLabel;
  /** For Client users: their assigned customer from tbl_users.customer (locked). */
  customer?: string | null;
}

const ViewModeContext = createContext<{
  viewMode: ViewMode;
  setViewMode: (mode: ViewMode) => void;
  /** When viewMode === 'client', Summary (and similar) lock customer filter to this value. */
  clientCustomer: string;
  setClientCustomer: (value: string) => void;
  /** True when user is Client: clientCustomer is from session and must not be changed. */
  clientCustomerLocked: boolean;
  /** Logged-in user type from localStorage; null if not logged in. */
  userType: UserTypeLabel | null;
  /** View modes this user is allowed to select (empty when not logged in). */
  allowedViewModes: ViewMode[];
  /** Re-read user from localStorage (e.g. after login on another tab or after redirect). */
  refreshUser: () => void;
} | null>(null);

export function ViewModeProvider({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const [userType, setUserType] = useState<UserTypeLabel | null>(null);
  const [viewMode, setViewModeState] = useState<ViewMode>('client');
  const [clientCustomer, setClientCustomer] = useState('');

  const refreshUser = useCallback(() => {
    if (typeof localStorage === 'undefined') return;
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) {
        setUserType(null);
        setViewModeState('client');
        return;
      }
      const parsed = JSON.parse(raw) as StoredUser;
      const t = parsed?.userType;
      const ut: UserTypeLabel | null =
        t && VALID_USER_TYPES.includes(t as UserTypeLabel) ? (t as UserTypeLabel) : null;
      setUserType(ut);
      // After login or refresh: default view mode to the user's type (Super Admin → super, etc.)
      setViewModeState(ut ? userTypeToViewMode(ut) : 'client');
      // For Client users, lock clientCustomer to their assigned customer from tbl_users
      if (ut === 'Client') {
        const lockedCustomer = (parsed?.customer ?? '').trim() || '';
        setClientCustomer(lockedCustomer);
      }
    } catch {
      setUserType(null);
      setViewModeState('client');
    }
  }, []);

  // Run on mount and whenever route changes (e.g. after login redirect to /)
  useEffect(() => {
    refreshUser();
  }, [refreshUser, pathname]);

  const allowedViewModes = allowedViewModesForUserType(userType);

  const setViewMode = useCallback(
    (mode: ViewMode) => {
      if (!allowedViewModes.includes(mode)) return;
      setViewModeState(mode);
    },
    [allowedViewModes]
  );

  const clientCustomerLocked = userType === 'Client';

  const setClientCustomerOrNoOp = useCallback(
    (value: string) => {
      if (!clientCustomerLocked) setClientCustomer(value);
    },
    [clientCustomerLocked]
  );

  return (
    <ViewModeContext.Provider
      value={{
        viewMode,
        setViewMode,
        clientCustomer,
        setClientCustomer: setClientCustomerOrNoOp,
        clientCustomerLocked,
        userType,
        allowedViewModes,
        refreshUser,
      }}
    >
      {children}
    </ViewModeContext.Provider>
  );
}

/** True when Summary (and similar) should show client view (reduced columns). */
export function isClientView(mode: ViewMode): boolean {
  return mode === 'client';
}

export function useViewMode() {
  const ctx = useContext(ViewModeContext);
  if (!ctx) throw new Error('useViewMode must be used within ViewModeProvider');
  return ctx;
}

export { STORAGE_KEY as GEODATA_USER_STORAGE_KEY };

'use client';

import { GEODATA_USER_STORAGE_KEY, readStoredUserType } from '@/contexts/ViewModeContext';

/** Signed-in Super Admin or Admin, for requests that must be stored as staff. */
export function staffActorPayload(): { email: string; role: 'Super Admin' | 'Admin' } | null {
  if (typeof localStorage === 'undefined') return null;
  const role = readStoredUserType();
  if (role !== 'Super Admin' && role !== 'Admin') return null;
  try {
    const raw = localStorage.getItem(GEODATA_USER_STORAGE_KEY);
    const email = raw ? String((JSON.parse(raw) as { email?: unknown }).email ?? '').trim() : '';
    if (!email) return null;
    return { email, role };
  } catch {
    return null;
  }
}

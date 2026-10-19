export const USER_TYPE_LABELS = ['Super Admin', 'Admin', 'Client', 'Driver'] as const;
export type UserTypeLabel = (typeof USER_TYPE_LABELS)[number];

/** Map a stored user_type onto the four app roles. Unknown values read as Admin. */
export function normalizeUserType(raw: string | null | undefined): UserTypeLabel {
  const s = (raw ?? '').trim();
  if (/super\s*admin/i.test(s)) return 'Super Admin';
  if (/^admin$/i.test(s)) return 'Admin';
  if (/^client$/i.test(s)) return 'Client';
  if (/^driver$/i.test(s)) return 'Driver';
  return 'Admin';
}

/** Value written to tbl_users.user_type. Empty becomes Admin; unknown text is kept. */
export function userTypeForStorage(raw: string | null | undefined): string {
  const s = (raw ?? '').trim();
  if (!s) return 'Admin';
  if (/super\s*admin/i.test(s)) return 'Super Admin';
  if (/^admin$/i.test(s)) return 'Admin';
  if (/^client$/i.test(s)) return 'Client';
  if (/^driver$/i.test(s)) return 'Driver';
  return s;
}

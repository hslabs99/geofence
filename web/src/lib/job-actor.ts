import { query } from '@/lib/db';
import { normalizeUserType } from '@/lib/user-types';

/** Staff member acting on a driver job. Looked up from tbl_users, not taken from the browser label. */
export type JobActor = {
  name: string;
  email: string;
  role: 'Super Admin' | 'Admin';
};

export function actorPhrase(actor: JobActor): string {
  return `${actor.role} ${actor.name}`;
}

/** Email the signed-in staff browser claims. The database decides whether that person is admin. */
export async function staffActorFromBody(body: unknown): Promise<JobActor | null> {
  if (!body || typeof body !== 'object') return null;
  const raw = (body as { actor?: unknown }).actor;
  if (!raw || typeof raw !== 'object') return null;
  const email = String((raw as { email?: unknown }).email ?? '').trim().toLowerCase();
  if (!email || email.length > 200 || !email.includes('@')) return null;
  const claimed = normalizeUserType(String((raw as { role?: unknown }).role ?? ''));
  const rows = await query<{ firstname: string | null; surname: string | null; user_type: string | null; email: string | null }>(
    `SELECT firstname, surname, user_type, email
     FROM tbl_users
     WHERE lower(btrim(email)) = $1
     LIMIT 1`,
    [email],
  );
  const row = rows[0];
  if (row) {
    const role = normalizeUserType(row.user_type);
    if (role !== 'Super Admin' && role !== 'Admin') return null;
    const name = [row.firstname, row.surname].map((part) => part?.trim() || '').filter(Boolean).join(' ') || row.email?.trim() || email;
    return { name, email: row.email?.trim() || email, role };
  }
  // The development super-admin sign-in is not a tbl_users row. A known driver or client email is already rejected above.
  if (claimed !== 'Super Admin' && claimed !== 'Admin') return null;
  return { name: email, email, role: claimed };
}

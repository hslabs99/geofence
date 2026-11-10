import bcrypt from 'bcryptjs';
import { execute, query } from '@/lib/db';
import { ensureDriverUserSchema } from '@/lib/ensure-driver-user';
import { listVworkUsers, readyVworkAccounts } from '@/lib/vwork-api';

/** VWork driver IDs: digits, a hyphen, then letters. 01-HBD and 2026-Lee both pass. */
const DRIVER_CODE = /^\d+-[A-Za-z]+$/;
const DEFAULT_PASSWORD = '123456';

export type VworkDriverSyncResult = {
  created: string[];
  already: number;
  scanned: number;
  accounts: { id: string; label: string; users: number; error: string | null }[];
};

/** Creates a Driver login for each new VWork driver code. Existing logins are left as they are. */
export async function syncVworkDrivers(): Promise<VworkDriverSyncResult> {
  await ensureDriverUserSchema();
  const accounts = readyVworkAccounts();
  if (accounts.length === 0) throw new Error('No VWork API key is ready');

  const existing = await query<{ key: string }>(
    `SELECT LOWER(TRIM(email)) AS key FROM tbl_users
     UNION
     SELECT LOWER(TRIM(firstname)) AS key FROM tbl_users WHERE firstname IS NOT NULL AND TRIM(firstname) <> ''`,
  );
  const known = new Set(existing.map((row) => row.key));
  const hashed = await bcrypt.hash(DEFAULT_PASSWORD, 10);
  const created: string[] = [];
  let already = 0;
  let scanned = 0;
  const accountResults: VworkDriverSyncResult['accounts'] = [];

  for (const account of accounts) {
    try {
      const listed = await listVworkUsers(account.apiKey);
      const codes = [...new Set(listed.users.map((user) => user.name.trim()).filter((name) => DRIVER_CODE.test(name)))];
      scanned += codes.length;
      let accountCreated = 0;
      for (const code of codes) {
        const key = code.toLowerCase();
        if (known.has(key)) {
          already += 1;
          continue;
        }
        await execute(
          `INSERT INTO tbl_users (email, password, firstname, user_type)
           VALUES ($1, $2, $3, 'Driver')`,
          [code, hashed, code],
        );
        known.add(key);
        created.push(code);
        accountCreated += 1;
      }
      accountResults.push({ id: account.id, label: account.label, users: accountCreated, error: listed.truncated ? 'The user list was cut off at the page limit.' : null });
    } catch (err) {
      accountResults.push({
        id: account.id,
        label: account.label,
        users: 0,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  const failed = accountResults.filter((account) => account.error && account.users === 0 && !account.error.includes('page limit'));
  if (accountResults.length > 0 && failed.length === accountResults.length) {
    throw new Error(failed.map((account) => `${account.label}: ${account.error}`).join('; '));
  }
  return { created, already, scanned, accounts: accountResults };
}

import bcrypt from 'bcryptjs';
import { execute, query } from '@/lib/db';

let driverUserEnsured = false;

/**
 * Adds tbl_users.truck_rego and seeds driver Mike / 123456 once if that login does not exist.
 * Safe to call repeatedly.
 */
export async function ensureDriverUserSchema(): Promise<void> {
  if (driverUserEnsured) return;

  await execute(`ALTER TABLE tbl_users ADD COLUMN IF NOT EXISTS truck_rego text`);

  const existing = await query<{ userid: string }>(
    `SELECT userid
     FROM tbl_users
     WHERE LOWER(TRIM(email)) = 'mike'
        OR (
          LOWER(TRIM(COALESCE(user_type, ''))) = 'driver'
          AND LOWER(TRIM(COALESCE(firstname, ''))) = 'mike'
        )
     LIMIT 1`,
  );

  if (existing.length === 0) {
    const hashed = await bcrypt.hash('123456', 10);
    await execute(
      `INSERT INTO tbl_users (email, password, firstname, user_type, truck_rego)
       VALUES ($1, $2, $3, 'Driver', NULL)`,
      ['mike', hashed, 'Mike'],
    );
  }

  driverUserEnsured = true;
}

import { NextResponse } from 'next/server';
import bcrypt from 'bcryptjs';
import { query } from '@/lib/db';
import { ensureDriverUserSchema } from '@/lib/ensure-driver-user';
import { normalizeUserType } from '@/lib/user-types';

type DriverRow = {
  userid: string;
  email: string;
  password: string;
  firstname: string | null;
  surname: string | null;
  user_type: string | null;
  truck_rego: string | null;
};

/** POST: driver portal login. Driver ID matches email or first name. Driver users only. */
export async function POST(request: Request) {
  try {
    await ensureDriverUserSchema();
    const body = await request.json();
    const { driverId, password } = body as { driverId?: string; password?: string };
    const id = (driverId ?? '').trim().toLowerCase();

    if (!id) {
      return NextResponse.json({ error: 'Driver ID is required' }, { status: 400 });
    }
    if (!password || typeof password !== 'string') {
      return NextResponse.json({ error: 'Password is required' }, { status: 400 });
    }

    const rows = await query<DriverRow>(
      `SELECT userid, email, password, firstname, surname, user_type, truck_rego
       FROM tbl_users
       WHERE LOWER(TRIM(COALESCE(user_type, ''))) = 'driver'
         AND (
           LOWER(TRIM(email)) = $1
           OR LOWER(TRIM(COALESCE(firstname, ''))) = $1
         )`,
      [id],
    );
    const user = rows[0];
    if (!user || normalizeUserType(user.user_type) !== 'Driver') {
      return NextResponse.json({ error: 'Invalid driver ID or password' }, { status: 401 });
    }

    const match = await bcrypt.compare(password, user.password);
    if (!match) {
      return NextResponse.json({ error: 'Invalid driver ID or password' }, { status: 401 });
    }

    const truckRego = (user.truck_rego ?? '').trim() || null;
    return NextResponse.json({
      ok: true,
      user: {
        email: user.email,
        userType: 'Driver' as const,
        firstname: user.firstname,
        surname: user.surname,
        driverName: (user.firstname ?? '').trim() || user.email,
        ...(truckRego != null && { truckRego }),
      },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import bcrypt from 'bcryptjs';

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { email, password } = body as { email?: string; password?: string };

    if (!email || typeof email !== 'string' || !email.trim()) {
      return NextResponse.json({ error: 'Email is required' }, { status: 400 });
    }
    if (!password || typeof password !== 'string') {
      return NextResponse.json({ error: 'Password is required' }, { status: 400 });
    }

    const rows = await query<{ userid: string; email: string; password: string; firstname: string | null; surname: string | null; user_type: string | null }>(
      'SELECT userid, email, password, firstname, surname, user_type FROM tbl_users WHERE LOWER(TRIM(email)) = $1',
      [email.trim().toLowerCase()]
    );
    const user = rows[0];

    if (!user) {
      return NextResponse.json({ error: 'Invalid email or password' }, { status: 401 });
    }

    const match = await bcrypt.compare(password, user.password);
    if (!match) {
      return NextResponse.json({ error: 'Invalid email or password' }, { status: 401 });
    }

    return NextResponse.json({
      ok: true,
      user: {
        email: user.email,
        userType: 'Super Admin' as const,
        firstname: user.firstname,
        surname: user.surname,
      },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

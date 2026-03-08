import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';
import { dateToLiteralUTC } from '@/lib/utils';
import bcrypt from 'bcryptjs';

function jsonSafe<T>(obj: T): T {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'bigint') return String(obj) as T;
  if (typeof obj === 'object' && obj instanceof Date) return dateToLiteralUTC(obj) as T;
  if (Array.isArray(obj)) return obj.map(jsonSafe) as T;
  if (typeof obj === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj)) {
      out[k] = jsonSafe(v);
    }
    return out as T;
  }
  return obj;
}

const CANONICAL_USER_TYPES = ['Super Admin', 'Admin', 'Client'] as const;
type CanonicalUserType = (typeof CANONICAL_USER_TYPES)[number];

function normalizeUserType(raw: string | null | undefined): CanonicalUserType {
  const s = (raw ?? '').trim();
  if (/super\s*admin/i.test(s)) return 'Super Admin';
  if (/^admin$/i.test(s)) return 'Admin';
  if (/^client$/i.test(s)) return 'Client';
  return 'Admin';
}

/** Read user_type from row; driver may return different key casing (user_type, User_Type, userType, etc.). */
function getRawUserType(row: Record<string, unknown>): string | null | undefined {
  const keys = ['user_type', 'User_Type', 'userType', 'UserType', 'usertype'];
  for (const k of keys) {
    const v = row[k];
    if (v != null && typeof v === 'string') return v;
  }
  return undefined;
}

function mapUserRow(row: Record<string, unknown>): Record<string, unknown> & { userType: string } {
  const safe = jsonSafe(row) as Record<string, unknown>;
  const raw = getRawUserType(safe);
  const userType = normalizeUserType(raw);
  return { ...safe, userType };
}

export async function GET() {
  try {
    const users = await query(
      'SELECT * FROM tbl_users ORDER BY userid ASC'
    );
    const mapped = (users as Record<string, unknown>[]).map(mapUserRow);
    return NextResponse.json({ users: mapped });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { email, password, firstname, surname, phone, userType, customer } = body as {
      email?: string;
      password?: string;
      firstname?: string | null;
      surname?: string | null;
      phone?: string | null;
      userType?: string | null;
      customer?: string | null;
    };
    if (!email || typeof email !== 'string' || !email.trim()) {
      return NextResponse.json({ error: 'email required' }, { status: 400 });
    }
    if (!password || typeof password !== 'string' || password.length < 6) {
      return NextResponse.json({ error: 'password required, min 6 chars' }, { status: 400 });
    }
    const uType = userType?.trim() || 'Admin';
    const cust = uType === 'Client' ? (customer?.trim() || null) : null;
    const hashed = await bcrypt.hash(password, 10);
    const rows = await query(
      `INSERT INTO tbl_users (email, password, firstname, surname, phone, user_type, customer)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING *`,
      [email.trim().toLowerCase(), hashed, firstname?.trim() || null, surname?.trim() || null, phone?.trim() || null, uType, cust]
    );
    const user = rows[0];
    if (!user) return NextResponse.json({ error: 'Insert failed' }, { status: 500 });
    return NextResponse.json({ user: jsonSafe(user) });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const code = (err as { code?: string }).code;
    if (code === '23505') {
      return NextResponse.json({ error: 'Email already exists' }, { status: 400 });
    }
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

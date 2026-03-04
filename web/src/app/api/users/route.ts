import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';
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

export async function GET() {
  try {
    const users = await prisma.user.findMany({
      orderBy: { userid: 'asc' },
    });
    return NextResponse.json({ users: jsonSafe(users) });
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
    const user = await prisma.user.create({
      data: {
        email: email.trim().toLowerCase(),
        password: hashed,
        firstname: firstname?.trim() || null,
        surname: surname?.trim() || null,
        phone: phone?.trim() || null,
        userType: uType,
        customer: cust,
      },
    });
    return NextResponse.json({ user: jsonSafe(user) });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const isDuplicateEmail =
      (err as { code?: string; meta?: { target?: string[] } }).code === 'P2002' &&
      ((err as { meta?: { target?: string[] } }).meta?.target?.includes('email') ?? true);
    if (isDuplicateEmail) {
      return NextResponse.json({ error: 'Email already exists' }, { status: 400 });
    }
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

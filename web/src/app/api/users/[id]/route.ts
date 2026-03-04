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

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const userid = BigInt(id);
    const user = await prisma.user.findUnique({ where: { userid } });
    if (!user) return NextResponse.json({ error: 'User not found' }, { status: 404 });
    return NextResponse.json({ user: jsonSafe(user) });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function PUT(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const userid = BigInt(id);
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
    const data: {
      email?: string; password?: string; firstname?: string | null; surname?: string | null; phone?: string | null;
      userType?: string; customer?: string | null;
    } = {};
    if (email !== undefined) data.email = String(email).trim().toLowerCase();
    if (firstname !== undefined) data.firstname = firstname?.trim() || null;
    if (surname !== undefined) data.surname = surname?.trim() || null;
    if (phone !== undefined) data.phone = phone?.trim() || null;
    if (userType !== undefined) {
      data.userType = userType?.trim() || 'Admin';
      data.customer = data.userType === 'Client' ? (customer?.trim() ?? null) : null;
    }
    if (customer !== undefined && data.userType === undefined) {
      const existing = await prisma.user.findUnique({ where: { userid }, select: { userType: true } });
      if (existing?.userType === 'Client') data.customer = customer?.trim() || null;
    }
    if (password !== undefined && password !== '' && typeof password === 'string' && password.length >= 6) {
      data.password = await bcrypt.hash(password, 10);
    }
    if (Object.keys(data).length === 0) {
      return NextResponse.json({ error: 'Nothing to update' }, { status: 400 });
    }
    const user = await prisma.user.update({
      where: { userid },
      data,
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

export async function DELETE(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const userid = BigInt(id);
    await prisma.user.delete({ where: { userid } });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { createDiversionType, listDiversionTypes } from '@/lib/driver-diversions';

export async function GET() {
  try {
    const types = await listDiversionTypes();
    return NextResponse.json({ types });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const status = /required|too long|already/i.test(message) ? 400 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as { name?: unknown };
    const name = typeof body.name === 'string' ? body.name : '';
    const type = await createDiversionType(name);
    return NextResponse.json({ type });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const code = (err as { code?: string }).code;
    if (code === '23505') return NextResponse.json({ error: 'That diversion already exists' }, { status: 409 });
    const status = /required|too long/i.test(message) ? 400 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

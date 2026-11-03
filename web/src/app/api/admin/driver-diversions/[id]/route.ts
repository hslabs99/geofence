import { NextResponse } from 'next/server';
import { deleteDiversionType, renameDiversionType } from '@/lib/driver-diversions';

export async function PATCH(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    const typeId = Number(id);
    if (!Number.isInteger(typeId) || typeId <= 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    const body = (await request.json()) as { name?: unknown };
    const name = typeof body.name === 'string' ? body.name : '';
    const type = await renameDiversionType(typeId, name);
    if (!type) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ type });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const code = (err as { code?: string }).code;
    if (code === '23505') return NextResponse.json({ error: 'That diversion already exists' }, { status: 409 });
    const status = /required|too long/i.test(message) ? 400 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

export async function DELETE(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    const typeId = Number(id);
    if (!Number.isInteger(typeId) || typeId <= 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    const removed = await deleteDiversionType(typeId);
    if (!removed) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

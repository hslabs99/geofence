import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

export async function PUT(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const rowId = parseInt(id, 10);
    if (isNaN(rowId)) {
      return NextResponse.json({ error: 'Invalid id' }, { status: 400 });
    }
    const body = await request.json();
    const { type, vwname, gpsname } = body as {
      type?: string;
      vwname?: string;
      gpsname?: string;
    };
    const data: { type?: string; vwname?: string; gpsname?: string } = {};
    if (type !== undefined) data.type = String(type).trim();
    if (vwname !== undefined) data.vwname = String(vwname).trim();
    if (gpsname !== undefined) data.gpsname = String(gpsname).trim();
    if (Object.keys(data).length === 0) {
      return NextResponse.json({ error: 'Nothing to update' }, { status: 400 });
    }
    const row = await prisma.tblGpsmapping.update({
      where: { id: rowId },
      data,
    });
    return NextResponse.json({ row });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function DELETE(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const rowId = parseInt(id, 10);
    if (isNaN(rowId)) {
      return NextResponse.json({ error: 'Invalid id' }, { status: 400 });
    }
    await prisma.tblGpsmapping.delete({ where: { id: rowId } });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

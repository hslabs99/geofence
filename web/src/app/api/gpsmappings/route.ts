import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

export async function GET() {
  try {
    const rows = await prisma.tblGpsmapping.findMany({
      orderBy: { id: 'asc' },
    });
    return NextResponse.json({ rows });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { type, vwname, gpsname } = body as {
      type?: string;
      vwname?: string;
      gpsname?: string;
    };
    if (!type || typeof type !== 'string' || !type.trim()) {
      return NextResponse.json({ error: 'type required' }, { status: 400 });
    }
    if (!vwname || typeof vwname !== 'string' || !vwname.trim()) {
      return NextResponse.json({ error: 'vwname required' }, { status: 400 });
    }
    if (!gpsname || typeof gpsname !== 'string' || !gpsname.trim()) {
      return NextResponse.json({ error: 'gpsname required' }, { status: 400 });
    }
    const row = await prisma.tblGpsmapping.create({
      data: {
        type: type.trim(),
        vwname: vwname.trim(),
        gpsname: gpsname.trim(),
      },
    });
    return NextResponse.json({ row });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

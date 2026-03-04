import { NextResponse } from 'next/server';
import { Prisma } from '@prisma/client';
import { prisma } from '@/lib/prisma';

const SOURCE = 'tracksolid';
const BATCH_SIZE = 500;

function parsePositionTime(gpsTime: string): Date | null {
  const s = String(gpsTime ?? '').trim();
  if (!s) return null;
  const normalized = s.includes('T') ? s : `${s.replace(' ', 'T')}Z`;
  const d = new Date(normalized);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as {
      deviceName?: string;
      imei?: string;
      points?: Array<{ lat: number; lng: number; gpsTime: string }>;
    };
    const deviceName = body?.deviceName?.trim();
    const imei = typeof body?.imei === 'string' ? body.imei.trim() : null;
    const points = Array.isArray(body?.points) ? body.points : [];
    if (!deviceName) {
      return NextResponse.json({ ok: false, error: 'deviceName required' }, { status: 400 });
    }
    if (points.length === 0) {
      return NextResponse.json({ ok: false, error: 'points array required and non-empty' }, { status: 400 });
    }

    const rows: { positionTime: Date; location: string; payloadJson: string }[] = [];
    const imeiVal = imei ?? null;
    let skipped = 0;
    for (const p of points) {
      const positionTime = parsePositionTime(p.gpsTime);
      if (!positionTime) {
        skipped += 1;
        continue;
      }
      rows.push({
        positionTime,
        location: `${Number(p.lat) ?? 0},${Number(p.lng) ?? 0}`,
        payloadJson: JSON.stringify({ lat: p.lat, lng: p.lng, gpsTime: p.gpsTime }),
      });
    }

    if (rows.length === 0) {
      return NextResponse.json({ ok: true, inserted: 0, skipped, total: points.length, deleted: 0 });
    }

    const minTime = new Date(Math.min(...rows.map((r) => r.positionTime.getTime())));
    const maxTime = new Date(Math.max(...rows.map((r) => r.positionTime.getTime())));

    const deleted = await prisma.$executeRaw`
      DELETE FROM tbl_apifeed
      WHERE device_name = ${deviceName}
        AND position_time >= ${minTime}
        AND position_time <= ${maxTime}
    `;

    let inserted = 0;
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const values = batch.map(
        (r) =>
          Prisma.sql`(${deviceName}, ${imeiVal}, ${r.location}, ${r.positionTime}, ${SOURCE}, ${r.payloadJson}::jsonb)`
      );
      await prisma.$executeRaw`
        INSERT INTO tbl_apifeed (device_name, imei, location, position_time, source, payload)
        VALUES ${Prisma.join(values, ',')}
      `;
      inserted += batch.length;
    }

    return NextResponse.json({
      ok: true,
      inserted,
      skipped,
      total: points.length,
      deleted: Number(deleted),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.error('[api/apifeed/import POST] Error:', message, err instanceof Error ? err.stack : '');
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { execute } from '@/lib/db';

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

    const deleted = await execute(
      'DELETE FROM tbl_apifeed WHERE device_name = $1 AND position_time >= $2 AND position_time <= $3',
      [deviceName, minTime, maxTime]
    );

    let inserted = 0;
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const values: unknown[] = [];
      const placeholders: string[] = [];
      let paramIdx = 1;
      for (const r of batch) {
        placeholders.push(`($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2}, $${paramIdx + 3}, $${paramIdx + 4}, $${paramIdx + 5}::jsonb)`);
        values.push(deviceName, imeiVal, r.location, r.positionTime, SOURCE, r.payloadJson);
        paramIdx += 6;
      }
      await execute(
        `INSERT INTO tbl_apifeed (device_name, imei, location, position_time, source, payload) VALUES ${placeholders.join(', ')}`,
        values
      );
      inserted += batch.length;
    }

    return NextResponse.json({
      ok: true,
      inserted,
      skipped,
      total: points.length,
      deleted,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.error('[api/apifeed/import POST] Error:', message, err instanceof Error ? err.stack : '');
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

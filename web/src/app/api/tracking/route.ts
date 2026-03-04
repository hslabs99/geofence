import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';
import { dateToLiteralUTC } from '@/lib/utils';

/** Parse to YYYY-MM-DD HH:mm:ss — no timezone logic, use literal digits only. */
function toTimestampLiteral(s: string | null): string | null {
  if (!s || typeof s !== 'string') return null;
  const t = s.trim();
  const dmy = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2}):(\d{2})/);
  if (dmy) {
    const yy = dmy[3].length === 2 ? `20${dmy[3]}` : dmy[3];
    return `${yy}-${dmy[2].padStart(2, '0')}-${dmy[1].padStart(2, '0')} ${dmy[4].padStart(2, '0')}:${dmy[5]}:${dmy[6]}`;
  }
  const iso = t.match(/^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2})?:?(\d{2})?:?(\d{2})?/);
  if (iso) {
    const h = iso[4]?.padStart(2, '0') ?? '00';
    const m = iso[5]?.padStart(2, '0') ?? '00';
    const sec = iso[6]?.padStart(2, '0') ?? '00';
    return `${iso[1]}-${iso[2]}-${iso[3]} ${h}:${m}:${sec}`;
  }
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(t)) return t;
  return t;
}

/** JSON serialization — no Date/toISOString; timestamps shown as stored. */
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

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const device = searchParams.get('device');
    const positionAfter = searchParams.get('positionAfter');

    if (!device || !positionAfter) {
      return NextResponse.json(
        { error: 'device and positionAfter are required' },
        { status: 400 }
      );
    }

    const positionBefore = searchParams.get('positionBefore');
    const tsAfter = toTimestampLiteral(positionAfter) ?? positionAfter;
    const escapedAfter = String(tsAfter).replace(/'/g, "''");
    const escapedDevice = String(device).replace(/'/g, "''");
    const tsBefore = positionBefore ? (toTimestampLiteral(positionBefore) ?? positionBefore) : null;
    const escapedBefore = tsBefore ? String(tsBefore).replace(/'/g, "''") : null;

    const limit = Math.min(Math.max(1, parseInt(searchParams.get('limit') ?? '50', 10) || 50), 500);
    const offset = Math.max(0, parseInt(searchParams.get('offset') ?? '0', 10) || 0);

    const geofenceFilter = searchParams.get('geofenceFilter') ?? 'all';
    const geofenceTypeCondition = geofenceFilter === 'entry_or_exit'
      ? ` AND (t.geofence_type = 'ENTER' OR t.geofence_type = 'EXIT')`
      : '';

    let timeCondition = `t.position_time_nz > '${escapedAfter}'`;
    if (escapedBefore) {
      timeCondition += ` AND t.position_time_nz < '${escapedBefore}'`;
    }
    const whereClause = `t.device_name='${escapedDevice}' AND ${timeCondition}${geofenceTypeCondition}`;

    const countResult = await prisma.$queryRawUnsafe<{ count: bigint }[]>(
      `SELECT COUNT(*) AS count FROM tbl_tracking t LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id WHERE ${whereClause}`
    );
    const total = Number(countResult[0]?.count ?? 0);

    const sql = `SELECT t.device_name, g.fence_name, t.geofence_type, t.position_time_nz, t.position_time FROM tbl_tracking t LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id WHERE ${whereClause} ORDER BY t.position_time_nz ASC LIMIT ${limit} OFFSET ${offset}`;

    const rows = await prisma.$queryRawUnsafe<unknown[]>(sql);

    return NextResponse.json({ rows: jsonSafe(rows), sql, total });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

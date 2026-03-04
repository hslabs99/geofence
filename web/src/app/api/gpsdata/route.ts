import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';
import { dateToLiteralUTC } from '@/lib/utils';

/** JSON serialization — no Date/toISOString; timestamps shown as stored (raw). */
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

/**
 * GET: Return tbl_tracking rows for the GPS Tracking tab.
 * Raw dates only — no timezone conversion. to_char only for display format.
 * Pagination: limit (default 500), offset (default 0).
 * Order: orderBy (position_time_nz | position_time), orderDir (asc | desc). Default: position_time_nz asc (earliest first).
 * Optional ?device= to filter by device_name.
 * Optional ?geofenceType=: '' (all), 'ENTER', 'EXIT', 'entry_or_exit' (both ENTER and EXIT) — server-side so you get 500 matching rows per page.
 */
const TRACKING_SELECT = `
  t.device_name,
  t.imei,
  to_char(t.position_time, 'YYYY-MM-DD HH24:MI:SS') as position_time,
  to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') as position_time_nz,
  t.geofence_id,
  g.fence_name,
  t.geofence_type,
  t.apirow
`;

const ALLOWED_ORDER_COLUMNS = ['position_time_nz', 'position_time'];

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const device = searchParams.get('device');
    const limit = Math.min(500, Math.max(1, parseInt(searchParams.get('limit') ?? '500', 10) || 500));
    const offset = Math.max(0, parseInt(searchParams.get('offset') ?? '0', 10) || 0);
    const orderBy = ALLOWED_ORDER_COLUMNS.includes(searchParams.get('orderBy') ?? '')
      ? searchParams.get('orderBy')!
      : 'position_time_nz';
    const orderDir = (searchParams.get('orderDir') ?? 'asc').toLowerCase() === 'desc' ? 'DESC' : 'ASC';
    const geofenceType = searchParams.get('geofenceType') ?? '';

    const conditions: string[] = [];
    if (device && String(device).trim() !== '') {
      conditions.push(`t.device_name = '${String(device).trim().replace(/'/g, "''")}'`);
    }
    if (geofenceType === 'ENTER' || geofenceType === 'EXIT') {
      conditions.push(`t.geofence_type = '${geofenceType.replace(/'/g, "''")}'`);
    } else if (geofenceType === 'entry_or_exit') {
      conditions.push(`(t.geofence_type = 'ENTER' OR t.geofence_type = 'EXIT')`);
    }
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    // Order by raw table column — no timezone or expression, so earliest/latest are correct
    const orderClause = `ORDER BY t.${orderBy} ${orderDir} NULLS LAST`;

    const countSql = `SELECT COUNT(*) AS total FROM tbl_tracking t LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id ${whereClause}`;
    const countResult = await prisma.$queryRawUnsafe<{ total: bigint }[]>(countSql);
    const total = Number(countResult[0]?.total ?? 0);

    const sql = `SELECT ${TRACKING_SELECT}
FROM tbl_tracking t
LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
${whereClause}
${orderClause}
LIMIT ${limit} OFFSET ${offset}`;

    const rows = await prisma.$queryRawUnsafe<unknown[]>(sql);
    return NextResponse.json({
      rows: jsonSafe(rows),
      total,
      limit,
      offset,
      orderBy,
      orderDir: orderDir.toLowerCase(),
      sql,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

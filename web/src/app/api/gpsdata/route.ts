import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { dateToLiteral } from '@/lib/utils';

/** JSON serialization — no UTC; dates as raw so NZ-stored values display correctly. */
function jsonSafe<T>(obj: T): T {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'bigint') return String(obj) as T;
  if (typeof obj === 'object' && obj instanceof Date) return dateToLiteral(obj) as T;
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
 * Pagination and filters via query params; uses parameterized queries.
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
    const geofenceIdParam = searchParams.get('geofenceId');
    const geofenceId = geofenceIdParam != null && geofenceIdParam !== '' ? parseInt(geofenceIdParam, 10) : null;

    const conditions: string[] = [];
    const params: unknown[] = [];
    let idx = 1;
    if (device && String(device).trim() !== '') {
      conditions.push(`t.device_name = $${idx++}`);
      params.push(String(device).trim());
    }
    if (geofenceId != null && !Number.isNaN(geofenceId)) {
      conditions.push(`t.geofence_id = $${idx++}`);
      params.push(geofenceId);
    }
    if (geofenceType === 'ENTER' || geofenceType === 'EXIT') {
      conditions.push(`t.geofence_type = $${idx++}`);
      params.push(geofenceType);
    } else if (geofenceType === 'entry_or_exit') {
      conditions.push(`(t.geofence_type = 'ENTER' OR t.geofence_type = 'EXIT')`);
    }
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const orderClause = `ORDER BY t.${orderBy} ${orderDir} NULLS LAST`;

    const countRows = await query<{ total: string }>(
      `SELECT COUNT(*) AS total FROM tbl_tracking t LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id ${whereClause}`,
      params
    );
    const total = Number(countRows[0]?.total ?? 0);

    const limitNum = idx;
    const offsetNum = idx + 1;
    params.push(limit, offset);
    const rows = await query(
      `SELECT ${TRACKING_SELECT}
       FROM tbl_tracking t
       LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
       ${whereClause}
       ${orderClause}
       LIMIT $${limitNum} OFFSET $${offsetNum}`,
      params
    );

    const sql = `SELECT ... FROM tbl_tracking t ... ${whereClause} ${orderClause} LIMIT ${limit} OFFSET ${offset}`;
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

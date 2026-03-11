import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { dateToLiteral } from '@/lib/utils';

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

/** JSON serialization — no UTC; dates as raw (local) so NZ-stored values display correctly. */
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
    const tsBefore = positionBefore ? (toTimestampLiteral(positionBefore) ?? positionBefore) : null;

    const limit = Math.min(Math.max(1, parseInt(searchParams.get('limit') ?? '50', 10) || 50), 500);
    const offset = Math.max(0, parseInt(searchParams.get('offset') ?? '0', 10) || 0);

    const geofenceFilter = searchParams.get('geofenceFilter') ?? 'all';
    const geofenceTypeCondition = geofenceFilter === 'entry_or_exit'
      ? ` AND (t.geofence_type = 'ENTER' OR t.geofence_type = 'EXIT')`
      : '';

    const fenceNames = searchParams.getAll('fenceNames').map((s) => s.trim()).filter(Boolean);

    const params: unknown[] = [device.trim(), tsAfter];
    let timeCondition = 't.position_time_nz > $2';
    if (tsBefore) {
      params.push(tsBefore);
      timeCondition += ' AND t.position_time_nz < $3';
    }
    let fenceCondition = '';
    if (fenceNames.length > 0) {
      const startIdx = params.length + 1;
      fenceNames.forEach((_, i) => params.push(fenceNames[i]));
      const placeholders = fenceNames.map((_, i) => `$${startIdx + i}`).join(', ');
      fenceCondition = ` AND g.fence_name IN (${placeholders})`;
    }
    const whereClause = `t.device_name = $1 AND ${timeCondition}${fenceCondition}${geofenceTypeCondition}`;
    const countParams = [...params];
    params.push(limit, offset);

    const countRows = await query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM tbl_tracking t LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id WHERE ${whereClause}`,
      countParams
    );
    const total = Number(countRows[0]?.count ?? 0);

    const limitPlaceholder = params.length - 1;
    const offsetPlaceholder = params.length;
    const sql = `SELECT t.device_name, g.fence_name, t.geofence_type, to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz, to_char(t.position_time, 'YYYY-MM-DD HH24:MI:SS') AS position_time, t.lat, t.lon FROM tbl_tracking t LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id WHERE ${whereClause} ORDER BY t.position_time_nz ASC LIMIT $${limitPlaceholder} OFFSET $${offsetPlaceholder}`;

    const rows = await query(sql, params);

    const esc = (v: string) => `'${String(v).replace(/'/g, "''")}'`;
    const timePart = tsBefore
      ? `t.position_time_nz > ${esc(tsAfter)} AND t.position_time_nz < ${esc(tsBefore)}`
      : `t.position_time_nz > ${esc(tsAfter)}`;
    const fencePart = fenceNames.length > 0
      ? ` AND g.fence_name IN (${fenceNames.map((n) => esc(n)).join(', ')})`
      : '';
    const sqlCopyPaste = `SELECT t.device_name, g.fence_name, t.geofence_type, to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz, to_char(t.position_time, 'YYYY-MM-DD HH24:MI:SS') AS position_time, t.lat, t.lon FROM tbl_tracking t LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id WHERE t.device_name = ${esc(device.trim())} AND ${timePart}${fencePart}${geofenceTypeCondition} ORDER BY t.position_time_nz ASC LIMIT ${limit} OFFSET ${offset}`;

    return NextResponse.json({ rows: jsonSafe(rows), sql, sqlCopyPaste, total });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

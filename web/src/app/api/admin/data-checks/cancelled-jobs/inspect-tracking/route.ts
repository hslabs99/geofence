import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { buildInspectGpsWindowForJob } from '@/lib/inspect-gps-window';
import {
  DEFAULT_HARVEST_END_PLUS_MINUTES,
  DEFAULT_HARVEST_START_LESS_MINUTES,
} from '@/lib/gps-harvest-constants';
import { dateToLiteral } from '@/lib/utils';

/** Same literal normalisation as `/api/tracking` GET. */
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
 * GET: tbl_tracking for one job using the same Inspect GPS window and `/api/tracking` query shape as Query → Inspect
 * (entry/exit filter matches Inspect default “Entry/Exit” view).
 *
 * Params: jobId (required), startLessMinutes, endPlusMinutes, limit (default 200, max 500), offset (default 0).
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const jobId = searchParams.get('jobId')?.trim() ?? '';
    if (!jobId) {
      return NextResponse.json({ error: 'jobId is required' }, { status: 400 });
    }

    const startLess = Math.min(
      1440,
      Math.max(
        0,
        parseInt(searchParams.get('startLessMinutes') ?? String(DEFAULT_HARVEST_START_LESS_MINUTES), 10) ||
          DEFAULT_HARVEST_START_LESS_MINUTES,
      ),
    );
    const endPlus = Math.min(
      1440,
      Math.max(
        0,
        parseInt(searchParams.get('endPlusMinutes') ?? String(DEFAULT_HARVEST_END_PLUS_MINUTES), 10) || DEFAULT_HARVEST_END_PLUS_MINUTES,
      ),
    );

    let limit = parseInt(searchParams.get('limit') ?? '200', 10);
    if (!Number.isFinite(limit) || limit < 1) limit = 200;
    limit = Math.min(500, limit);
    let offset = parseInt(searchParams.get('offset') ?? '0', 10);
    if (!Number.isFinite(offset) || offset < 0) offset = 0;

    const jobRows = await query<Record<string, unknown>>(
      `SELECT * FROM tbl_vworkjobs WHERE trim(job_id::text) = trim($1::text) LIMIT 1`,
      [jobId],
    );
    const rawJob = jobRows[0];
    if (!rawJob) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }

    const win = buildInspectGpsWindowForJob(rawJob, {
      startLessMinutes: startLess,
      endPlusMinutes: endPlus,
      displayExpandBefore: 0,
      displayExpandAfter: 0,
    });
    if (win.error || !win.device || !win.positionAfter) {
      return NextResponse.json({
        ok: false,
        error: win.error ?? 'missing worker or actual_start_time',
        device: win.device,
        positionAfter: win.positionAfter,
        positionBefore: win.positionBefore,
        rows: [],
        total: 0,
        sql: '',
        sqlCopyPaste: '',
      });
    }

    const device = win.device;
    const positionAfter = win.positionAfter;
    const positionBefore = win.positionBefore;
    const tsAfter = toTimestampLiteral(positionAfter) ?? positionAfter;
    const tsBefore = positionBefore ? (toTimestampLiteral(positionBefore) ?? positionBefore) : null;

    const geofenceTypeCondition = ` AND (t.geofence_type = 'ENTER' OR t.geofence_type = 'EXIT')`;

    const params: unknown[] = [device.trim(), tsAfter];
    let timeCondition = 't.position_time_nz > $2';
    if (tsBefore) {
      params.push(tsBefore);
      timeCondition += ' AND t.position_time_nz < $3';
    }
    const whereClause = `t.device_name = $1 AND ${timeCondition}${geofenceTypeCondition}`;
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
    const sqlCopyPaste = `SELECT t.device_name, g.fence_name, t.geofence_type, to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz, to_char(t.position_time, 'YYYY-MM-DD HH24:MI:SS') AS position_time, t.lat, t.lon FROM tbl_tracking t LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id WHERE t.device_name = ${esc(device.trim())} AND ${timePart}${geofenceTypeCondition} ORDER BY t.position_time_nz ASC LIMIT ${limit} OFFSET ${offset}`;

    return NextResponse.json({
      ok: true,
      device,
      positionAfter: tsAfter,
      positionBefore: tsBefore,
      limit,
      offset,
      total,
      rows: jsonSafe(rows),
      sql,
      sqlCopyPaste,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { getClient } from '@/lib/db';

/**
 * Read-only: returns tbl_tracking rows. position_time is stored verbatim from API — we never alter it.
 * GET: Return tbl_tracking rows for a device in a position_time window:
 * from - 5 minutes to to + 90 minutes (so you can scroll a larger data set after the gap).
 * Params: device (required), from (position_time YYYY-MM-DD HH:mm:ss), to (position_time YYYY-MM-DD HH:mm:ss).
 * Session uses UTC so timestamp literals are interpreted as given.
 * fence_name comes from tbl_geofences via geofence_id: LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id.
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const device = searchParams.get('device')?.trim();
    const fromParam = searchParams.get('from')?.trim();
    const toParam = searchParams.get('to')?.trim();
    if (!device || !fromParam || !toParam) {
      return NextResponse.json(
        { error: 'device, from, and to are required (from/to as position_time YYYY-MM-DD HH:mm:ss)' },
        { status: 400 }
      );
    }

    const client = await getClient();
    try {
      await client.query("SET LOCAL timezone = 'UTC'");
      const rows = await client.query(
        `SELECT t.device_name,
          to_char(t.position_time, 'YYYY-MM-DD HH24:MI:SS') AS position_time,
          to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz,
          g.fence_name,
          t.geofence_type,
          t.lat,
          t.lon
         FROM tbl_tracking t
         LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
         WHERE t.device_name = $1
           AND t.position_time >= $2::timestamp - interval '5 minutes'
           AND t.position_time <= $3::timestamp + interval '90 minutes'
         ORDER BY t.position_time ASC, t.ctid ASC`,
        [device, fromParam, toParam]
      );
      return NextResponse.json({ rows: rows.rows ?? [] });
    } finally {
      client.release();
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

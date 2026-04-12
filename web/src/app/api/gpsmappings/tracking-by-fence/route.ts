import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/**
 * GET: Return tbl_tracking rows for a given fence_id (for in-page table on GPS Mappings).
 * Query params: fenceId (required).
 * Returns rows with position_time, position_time_nz, fence_name, geofence_type, lat, lon, device_name.
 * Sorted by worker (device_name), then position_time_nz. Limit 500.
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const fenceIdParam = searchParams.get('fenceId');
    const fenceId = fenceIdParam != null && fenceIdParam !== '' ? parseInt(fenceIdParam, 10) : null;
    if (fenceId == null || Number.isNaN(fenceId)) {
      return NextResponse.json({ error: 'fenceId is required and must be a number' }, { status: 400 });
    }

    const rows = await query<{
      device_name: string;
      position_time: string;
      position_time_nz: string;
      fence_name: string | null;
      geofence_type: string | null;
      lat: number | null;
      lon: number | null;
    }>(
      `SELECT t.device_name,
        to_char(t.position_time, 'YYYY-MM-DD HH24:MI:SS') AS position_time,
        to_char(t.position_time_nz, 'YYYY-MM-DD HH24:MI:SS') AS position_time_nz,
        g.fence_name,
        t.geofence_type,
        t.lat,
        t.lon
       FROM tbl_tracking t
       LEFT JOIN tbl_geofences g ON g.fence_id = t.geofence_id
       WHERE t.geofence_id = $1
       ORDER BY t.device_name ASC NULLS LAST, t.position_time_nz ASC NULLS LAST
       LIMIT 500`,
      [fenceId]
    );

    return NextResponse.json({ rows: rows ?? [] });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

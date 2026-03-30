import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/**
 * GET: Latest tracking timestamps in tbl_tracking.
 * - All rows: overall max position_time / position_time_nz.
 * - Fence attempted: max where geofence_attempted is true (store_fences has run on that row).
 */
export async function GET() {
  try {
    const rows = await query<{
      max_pt: string | null;
      max_pt_attempted: string | null;
      max_pt_nz: string | null;
      max_pt_nz_attempted: string | null;
    }>(
      `SELECT
         to_char(max(t.position_time), 'YYYY-MM-DD HH24:MI:SS') AS max_pt,
         to_char(max(t.position_time) FILTER (WHERE t.geofence_attempted IS TRUE), 'YYYY-MM-DD HH24:MI:SS') AS max_pt_attempted,
         to_char(max(t.position_time_nz), 'YYYY-MM-DD HH24:MI:SS') AS max_pt_nz,
         to_char(max(t.position_time_nz) FILTER (WHERE t.geofence_attempted IS TRUE), 'YYYY-MM-DD HH24:MI:SS') AS max_pt_nz_attempted
       FROM tbl_tracking t`
    );
    const row = rows[0];
    return NextResponse.json({
      ok: true,
      maxPositionTime: row?.max_pt ?? null,
      maxPositionTimeFenceAttempted: row?.max_pt_attempted ?? null,
      maxPositionTimeNz: row?.max_pt_nz ?? null,
      maxPositionTimeNzFenceAttempted: row?.max_pt_nz_attempted ?? null,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/**
 * GET: Return max date/timestamp from tbl_vworkjobs.actual_start_time and tbl_tracking.position_time_nz
 * to help decide which date to import and process next.
 */
export async function GET() {
  try {
    const [vworkResult, trackingResult] = await Promise.all([
      query<{ max_actual_start: string | null }>(
        `SELECT to_char(MAX(actual_start_time), 'YYYY-MM-DD HH24:MI:SS') AS max_actual_start FROM tbl_vworkjobs WHERE actual_start_time IS NOT NULL`
      ),
      query<{ max_position_time_nz: string | null }>(
        `SELECT to_char(MAX(position_time_nz), 'YYYY-MM-DD HH24:MI:SS') AS max_position_time_nz FROM tbl_tracking WHERE position_time_nz IS NOT NULL`
      ),
    ]);

    const maxVwork = vworkResult[0]?.max_actual_start ?? null;
    const maxTracking = trackingResult[0]?.max_position_time_nz ?? null;

    return NextResponse.json({
      ok: true,
      maxVworkjobsActualStart: maxVwork,
      maxTrackingPositionTimeNz: maxTracking,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/import-max-dates]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

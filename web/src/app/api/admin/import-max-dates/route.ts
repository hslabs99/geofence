import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/**
 * GET: Return max timestamps from tbl_vworkjobs.actual_start_time and tbl_tracking
 * (position_time and position_time_nz) to help decide which date to import next.
 */
export async function GET() {
  try {
    const [vworkResult, trackingResult] = await Promise.all([
      query<{ max_actual_start: string | null }>(
        `SELECT to_char(MAX(actual_start_time), 'YYYY-MM-DD HH24:MI:SS') AS max_actual_start FROM tbl_vworkjobs WHERE actual_start_time IS NOT NULL`
      ),
      query<{ max_position_time: string | null; max_position_time_nz: string | null }>(
        `SELECT
           to_char(MAX(t.position_time), 'YYYY-MM-DD HH24:MI:SS') AS max_position_time,
           to_char(MAX(t.position_time_nz), 'YYYY-MM-DD HH24:MI:SS') AS max_position_time_nz
         FROM tbl_tracking t
         WHERE t.position_time IS NOT NULL OR t.position_time_nz IS NOT NULL`
      ),
    ]);

    const maxVwork = vworkResult[0]?.max_actual_start ?? null;
    const tr = trackingResult[0];
    const maxTrackingPt = tr?.max_position_time ?? null;
    const maxTrackingNz = tr?.max_position_time_nz ?? null;

    return NextResponse.json({
      ok: true,
      maxVworkjobsActualStart: maxVwork,
      maxTrackingPositionTime: maxTrackingPt,
      maxTrackingPositionTimeNz: maxTrackingNz,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/import-max-dates]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { execute, query } from '@/lib/db';

/**
 * VERBATIM TIMES — THE ONE EXCEPTION: position_time_nz.
 * We NEVER alter API data. position_time is stored exactly as received from the API.
 * position_time_nz is derived only in SQL (same pattern as other admin tracking routes):
 * UTC-naive position_time → Pacific/Auckland wall time for display/ordering. API → position_time must remain 100% literal.
 * If we get timezone info from geofence API or our own API, ignore it; treat all dates/times as verbatim.
 * Only rows with position_time_nz IS NULL are updated (existing nz values are left unchanged).
 *
 * Run once after all devices have been merged to tbl_tracking:
 * 1. UPDATE position_time_nz from position_time (AT TIME ZONE UTC → Pacific/Auckland) where nz is null
 * 2. SELECT "store_fences"()
 * Must be run after merge and before the per-device tagging step.
 */
export async function POST() {
  const positionTimeNzStart = Date.now();
  try {
    const updateCount = await execute(`
      UPDATE public.tbl_tracking
      SET position_time_nz = (position_time AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific/Auckland')
      WHERE position_time IS NOT NULL
        AND position_time_nz IS NULL
    `);
    const positionTimeNzMs = Date.now() - positionTimeNzStart;

    const storeFencesStart = Date.now();
    await query(`SELECT "store_fences"()`);
    const storeFencesMs = Date.now() - storeFencesStart;

    return NextResponse.json({
      ok: true,
      updated: Number(updateCount),
      positionTimeNzMs,
      storeFencesMs,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/tracking/apply-position-time-nz-and-fences]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

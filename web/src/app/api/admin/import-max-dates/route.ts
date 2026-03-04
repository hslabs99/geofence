import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

/**
 * GET: Return max date/timestamp from tbl_vworkjobs.actual_start_time and tbl_tracking.position_time_nz
 * to help decide which date to import and process next.
 */
export async function GET() {
  try {
    const [vworkResult, trackingResult] = await Promise.all([
      prisma.$queryRawUnsafe<{ max_actual_start: Date | string | null }[]>(
        `SELECT MAX(actual_start_time) AS max_actual_start FROM tbl_vworkjobs WHERE actual_start_time IS NOT NULL`
      ),
      prisma.$queryRawUnsafe<{ max_position_time_nz: Date | string | null }[]>(
        `SELECT MAX(position_time_nz) AS max_position_time_nz FROM tbl_tracking WHERE position_time_nz IS NOT NULL`
      ),
    ]);

    const rawVwork = vworkResult[0]?.max_actual_start;
    const rawTracking = trackingResult[0]?.max_position_time_nz;

    const toIso = (v: Date | string | null): string | null => {
      if (v == null) return null;
      if (typeof v === 'string') return v;
      if (v instanceof Date) return v.toISOString();
      return String(v);
    };

    return NextResponse.json({
      ok: true,
      maxVworkjobsActualStart: toIso(rawVwork),
      maxTrackingPositionTimeNz: toIso(rawTracking),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/import-max-dates]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/**
 * GET: Return distinct device_name from tbl_tracking for the given date range (position_time_nz::date between dateFrom and dateTo).
 * Query: ?dateFrom=YYYY-MM-DD&dateTo=YYYY-MM-DD
 * Returns { ok: true, devices: string[] }.
 */
export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const dateFrom = searchParams.get('dateFrom')?.trim() ?? '';
    const dateTo = searchParams.get('dateTo')?.trim() ?? '';
    const re = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateFrom || !re.test(dateFrom) || !dateTo || !re.test(dateTo)) {
      return NextResponse.json(
        { ok: false, error: 'Query params dateFrom and dateTo (YYYY-MM-DD) required' },
        { status: 400 }
      );
    }
    if (new Date(dateFrom + 'T00:00:00Z').getTime() > new Date(dateTo + 'T00:00:00Z').getTime()) {
      return NextResponse.json(
        { ok: false, error: 'dateFrom must be ≤ dateTo' },
        { status: 400 }
      );
    }
    const rows = await query<{ device_name: string }>(
      `SELECT DISTINCT device_name
       FROM tbl_tracking
       WHERE position_time_nz::date >= $1::date
         AND position_time_nz::date <= $2::date
       ORDER BY device_name`,
      [dateFrom, dateTo]
    );
    const devices = rows.map((r) => r.device_name).filter(Boolean);
    return NextResponse.json({ ok: true, devices });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/tracking/devices-for-date-range]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

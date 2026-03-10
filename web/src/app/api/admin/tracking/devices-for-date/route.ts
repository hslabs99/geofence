import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/**
 * GET: Return distinct device_name from tbl_tracking for the given date (position_time_nz::date = date).
 * Query: ?date=YYYY-MM-DD
 * Returns { ok: true, devices: string[] }.
 */
export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const date = searchParams.get('date')?.trim() ?? '';
    const re = /^\d{4}-\d{2}-\d{2}$/;
    if (!date || !re.test(date)) {
      return NextResponse.json(
        { ok: false, error: 'Query param date (YYYY-MM-DD) required' },
        { status: 400 }
      );
    }
    const rows = await query<{ device_name: string }>(
      `SELECT DISTINCT device_name
       FROM tbl_tracking
       WHERE position_time_nz::date = $1::date
       ORDER BY device_name`,
      [date]
    );
    const devices = rows.map((r) => r.device_name).filter(Boolean);
    return NextResponse.json({ ok: true, devices });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/tracking/devices-for-date]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

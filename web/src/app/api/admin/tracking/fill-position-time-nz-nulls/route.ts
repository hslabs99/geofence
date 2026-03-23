import { NextResponse } from 'next/server';
import { fillPositionTimeNzNulls } from '@/lib/fill-position-time-nz';

/** POST: backfill position_time_nz for rows that have position_time but null nz (after GPS merge). */
export async function POST() {
  try {
    const updated = await fillPositionTimeNzNulls();
    return NextResponse.json({ ok: true, updated });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/tracking/fill-position-time-nz-nulls]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { fillPositionTimeNzNulls } from '@/lib/fill-position-time-nz';

/**
 * POST: Same NZ backfill as after auto GPS import — set position_time_nz from position_time
 * via (AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific/Auckland') where nz is null (DST-aware).
 * Does not modify position_time. See fill-position-time-nz.ts and VERBATIM_TIMES rules.
 */
export async function POST() {
  try {
    const updated = await fillPositionTimeNzNulls();
    return NextResponse.json({ ok: true, updated });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/data-checks/fill-position-time-nz]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { execute } from '@/lib/db';

/**
 * VERBATIM TIMES — THE ONE EXCEPTION: position_time_nz.
 * Same UPDATE as the first step of apply-position-time-nz-and-fences, without store_fences().
 * Use when fence prep runs elsewhere (e.g. SQL pipeline) and only NZ backfill is needed before tagging.
 * Only fills rows where position_time_nz is still null (does not overwrite existing nz values).
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

    return NextResponse.json({
      ok: true,
      updated: Number(updateCount),
      positionTimeNzMs,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/tracking/apply-position-time-nz]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

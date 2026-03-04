import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

/**
 * Run once after all devices have been merged to tbl_tracking:
 * 1. UPDATE position_time_nz = position_time + interval '13 hours' for all rows with position_time
 * 2. SELECT "store_fences"()
 * Must be run after merge and before the per-device tagging step.
 */
export async function POST() {
  const positionTimeNzStart = Date.now();
  try {
    const updateCount = await prisma.$executeRawUnsafe(`
      UPDATE public.tbl_tracking
      SET position_time_nz = position_time + interval '13 hours'
      WHERE position_time IS NOT NULL
    `);
    const positionTimeNzMs = Date.now() - positionTimeNzStart;

    const storeFencesStart = Date.now();
    await prisma.$queryRawUnsafe(`SELECT "store_fences"()`);
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

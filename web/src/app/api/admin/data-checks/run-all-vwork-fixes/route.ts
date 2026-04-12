import { NextResponse } from 'next/server';
import { runAllVworkFixesBatch } from '@/lib/vwork-fixes-batch';

/**
 * POST: Run all tbl_vworkjobs data-check fixes in order (see VWORK_FIX_BATCH_STEPS).
 * Writes one tbl_logs row per step (logtype=AutoRun, logcat1=vwork-fixes-batch, logcat2=step id).
 */
export async function POST() {
  try {
    const { ok, steps } = await runAllVworkFixesBatch();
    return NextResponse.json({ ok, steps });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: msg, steps: [] }, { status: 500 });
  }
}

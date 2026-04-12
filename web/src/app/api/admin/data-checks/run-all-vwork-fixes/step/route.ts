import { NextResponse } from 'next/server';
import { runVworkFixBatchStep, VWORK_FIX_BATCH_STEP_COUNT } from '@/lib/vwork-fixes-batch';

/**
 * POST JSON { index: number } — run a single batch step (0 .. stepCount-1) with the same logging as the full batch.
 */
export async function POST(request: Request) {
  try {
    let body: unknown;
    try {
      body = await request.json();
    } catch {
      return NextResponse.json({ ok: false, error: 'Invalid JSON body' }, { status: 400 });
    }
    const o = body != null && typeof body === 'object' ? (body as Record<string, unknown>) : {};
    const index = Number(o.index);
    if (!Number.isInteger(index) || index < 0 || index >= VWORK_FIX_BATCH_STEP_COUNT) {
      return NextResponse.json(
        { ok: false, error: `index must be an integer 0..${VWORK_FIX_BATCH_STEP_COUNT - 1}` },
        { status: 400 }
      );
    }
    const step = await runVworkFixBatchStep(index);
    return NextResponse.json(step);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

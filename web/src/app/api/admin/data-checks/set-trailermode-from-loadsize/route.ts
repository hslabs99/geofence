import { NextResponse } from 'next/server';
import { runSetTrailermodeFromLoadsize } from '@/lib/run-vwork-data-fixes';

/**
 * POST: For rows with loadsize > 0, set tbl_vworkjobs.trailermode from loadsize vs Admin → Settings → TT Load Size (tbl_settings). Other rows unchanged.
 */
export async function POST() {
  try {
    const data = await runSetTrailermodeFromLoadsize();
    return NextResponse.json(data);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

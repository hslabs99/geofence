import { NextResponse } from 'next/server';
import { runSetTrailerType } from '@/lib/run-vwork-data-fixes';

/**
 * POST: Set tbl_vworkjobs.trailermode from trailer_rego.
 * - TT when trailer_rego is non-empty and not an exception (valid trailer).
 * - T when null/empty or exception: "No Trailer Required", or n/a/na with no numerics (case-insensitive).
 *   e.g. "n/A" → T, "na768" → TT (has digits).
 */
export async function POST() {
  try {
    const data = await runSetTrailerType();
    return NextResponse.json(data);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

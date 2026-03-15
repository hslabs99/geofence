import { NextResponse } from 'next/server';
import { execute } from '@/lib/db';

/**
 * POST: Set tbl_vworkjobs.trailermode from trailer_rego.
 * - TT when trailer_rego is non-empty and not an exception (valid trailer).
 * - T when null/empty or exception: "No Trailer Required", or n/a/na with no numerics (case-insensitive).
 *   e.g. "n/A" → T, "na768" → TT (has digits).
 */
export async function POST() {
  try {
    const updatedTT = await execute(
      `UPDATE tbl_vworkjobs SET trailermode = 'TT'
       WHERE trailer_rego IS NOT NULL AND TRIM(trailer_rego) <> ''
         AND LOWER(TRIM(trailer_rego)) <> 'no trailer required'
         AND NOT (
           TRIM(trailer_rego) !~ '[0-9]'
           AND (LOWER(TRIM(trailer_rego)) LIKE '%n/a%' OR LOWER(TRIM(trailer_rego)) LIKE '%na%')
         )`
    );
    const updatedT = await execute(
      `UPDATE tbl_vworkjobs SET trailermode = 'T'
       WHERE trailer_rego IS NULL OR TRIM(trailer_rego) = ''
         OR LOWER(TRIM(trailer_rego)) = 'no trailer required'
         OR (
           TRIM(trailer_rego) !~ '[0-9]'
           AND (LOWER(TRIM(trailer_rego)) LIKE '%n/a%' OR LOWER(TRIM(trailer_rego)) LIKE '%na%')
         )`
    );
    return NextResponse.json({
      ok: true,
      updatedTT,
      updatedT,
      totalUpdated: updatedTT + updatedT,
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

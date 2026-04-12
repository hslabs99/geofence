import { NextResponse } from 'next/server';
import { execute } from '@/lib/db';

/**
 * POST: For each tbl_vworkjobs row with non-empty delivery_winery and vineyard_name,
 * set distance and minutes from the matching tbl_distances row (lower(trim) pair match).
 * Jobs with no matching tbl_distances row are left unchanged.
 */
export async function POST() {
  try {
    const updated = await execute(
      `UPDATE tbl_vworkjobs AS j
       SET
         distance = d.distance_m::numeric,
         minutes = d.duration_min
       FROM tbl_distances d
       WHERE lower(trim(COALESCE(j.delivery_winery, ''))) = lower(trim(COALESCE(d.delivery_winery, '')))
         AND lower(trim(COALESCE(j.vineyard_name, ''))) = lower(trim(COALESCE(d.vineyard_name, '')))
         AND trim(COALESCE(j.delivery_winery, '')) <> ''
         AND trim(COALESCE(j.vineyard_name, '')) <> ''`
    );
    return NextResponse.json({ ok: true, updated });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/column (j\.)?(distance|minutes) .* does not exist|column "distance" .* does not exist|column "minutes" .* does not exist/i.test(msg)) {
      return NextResponse.json(
        {
          error:
            'Columns distance and/or minutes missing on tbl_vworkjobs. Run web/sql/add_distance_minutes_tbl_vworkjobs.sql.',
          detail: msg,
        },
        { status: 500 }
      );
    }
    if (/relation "tbl_distances" does not exist/i.test(msg)) {
      return NextResponse.json(
        {
          error: 'Table tbl_distances does not exist. Run web/sql/create_tbl_distances.sql.',
          detail: msg,
        },
        { status: 500 }
      );
    }
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

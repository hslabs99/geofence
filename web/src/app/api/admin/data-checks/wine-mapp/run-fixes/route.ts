import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

type WineMappRow = {
  id: number;
  oldvworkname: string;
  newvworkname: string;
};

/**
 * POST: Apply all tbl_wine_mapp fixes to tbl_vworkjobs.
 * For each mapping: SET delivery_winery_old = current delivery_winery, delivery_winery = new name
 * WHERE trim(delivery_winery) matches old name.
 * Requires delivery_winery_old column (run add_delivery_winery_old_tbl_vworkjobs.sql first).
 */
export async function POST() {
  try {
    const mappings = await query<WineMappRow>(
      `SELECT id, oldvworkname, newvworkname FROM tbl_wine_mapp ORDER BY id`
    );
    if (mappings.length === 0) {
      return NextResponse.json({ ok: true, totalUpdated: 0, perMapping: [] });
    }

    const perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[] = [];
    let totalUpdated = 0;

    for (const m of mappings) {
      const n = await execute(
        `UPDATE tbl_vworkjobs
         SET delivery_winery_old = delivery_winery, delivery_winery = $1
         WHERE trim(delivery_winery) = trim($2)`,
        [m.newvworkname, m.oldvworkname]
      );
      perMapping.push({
        id: m.id,
        oldvworkname: m.oldvworkname,
        newvworkname: m.newvworkname,
        updated: n,
      });
      totalUpdated += n;
    }

    return NextResponse.json({ ok: true, totalUpdated, perMapping });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

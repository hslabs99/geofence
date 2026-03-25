import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

type VineMappRow = {
  id: number;
  oldvworkname: string;
  newvworkname: string;
};

/**
 * POST: Apply all tbl_vine_mapp fixes to tbl_vworkjobs.
 * For each mapping: SET vineyard_name_old = current vineyard_name, vineyard_name = new name
 * WHERE trim(vineyard_name) matches old name.
 * Requires vineyard_name_old column (run add_vineyard_name_old_tbl_vworkjobs.sql first).
 */
export async function POST() {
  try {
    const mappings = await query<VineMappRow>(
      `SELECT id, oldvworkname, newvworkname FROM tbl_vine_mapp ORDER BY id`
    );
    if (mappings.length === 0) {
      return NextResponse.json({ ok: true, totalUpdated: 0, perMapping: [] });
    }

    const perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[] = [];
    let totalUpdated = 0;

    for (const m of mappings) {
      const n = await execute(
        `UPDATE tbl_vworkjobs
         SET vineyard_name_old = vineyard_name, vineyard_name = $1
         WHERE trim(vineyard_name) = trim($2)`,
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

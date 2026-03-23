import { NextResponse } from 'next/server';
import { execute, query } from '@/lib/db';

/**
 * POST: Set tbl_vworkjobs.vineyard_group to 'NA' for all rows, then set matching rows
 * from tbl_vineyardgroups (match on delivery_winery = winery_name and vineyard_name = vineyard_name;
 * when winery_name is null in tbl_vineyardgroups, match on vineyard_name only).
 * Requires vineyard_group column (run add_vineyard_group_tbl_vworkjobs.sql first).
 */
export async function POST() {
  try {
    const setToNa = await execute(
      `UPDATE tbl_vworkjobs SET vineyard_group = 'NA'`
    );

    const matched = await execute(
      `UPDATE tbl_vworkjobs v
       SET vineyard_group = (
         SELECT vg.vineyard_group
         FROM tbl_vineyardgroups vg
         WHERE trim(v.vineyard_name) = trim(vg.vineyard_name)
           AND (vg.winery_name IS NULL OR trim(v.delivery_winery) = trim(vg.winery_name))
         ORDER BY vg.winery_name NULLS LAST
         LIMIT 1
       )
       WHERE EXISTS (
         SELECT 1 FROM tbl_vineyardgroups vg
         WHERE trim(v.vineyard_name) = trim(vg.vineyard_name)
           AND (vg.winery_name IS NULL OR trim(v.delivery_winery) = trim(vg.winery_name))
       )`
    );

    const totalRows = await query<{ n: string }>(
      `SELECT count(*)::text AS n FROM tbl_vworkjobs`
    );
    const total = parseInt(totalRows[0]?.n ?? '0', 10) || 0;

    return NextResponse.json({
      ok: true,
      setToNa: setToNa,
      matched,
      totalRows: total,
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

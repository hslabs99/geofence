import { NextResponse } from 'next/server';
import { runUpdateVineyardGroup } from '@/lib/run-vwork-data-fixes';

/**
 * POST: Set tbl_vworkjobs.vineyard_group to 'NA' for all rows, then set matching rows
 * from tbl_vineyardgroups (match on delivery_winery = winery_name and vineyard_name = vineyard_name;
 * when winery_name is null in tbl_vineyardgroups, match on vineyard_name only).
 * Requires vineyard_group column (run add_vineyard_group_tbl_vworkjobs.sql first).
 */
export async function POST() {
  try {
    const data = await runUpdateVineyardGroup();
    return NextResponse.json(data);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

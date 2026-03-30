import { NextResponse } from 'next/server';
import { runWineMappFixes } from '@/lib/run-vwork-data-fixes';

/**
 * POST: Apply all tbl_wine_mapp fixes to tbl_vworkjobs.
 * For each mapping: SET delivery_winery_old = current delivery_winery, delivery_winery = new name
 * WHERE trim(delivery_winery) matches old name.
 * Requires delivery_winery_old column (run add_delivery_winery_old_tbl_vworkjobs.sql first).
 */
export async function POST() {
  try {
    const data = await runWineMappFixes();
    return NextResponse.json(data);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

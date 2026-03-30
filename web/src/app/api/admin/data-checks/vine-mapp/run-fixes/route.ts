import { NextResponse } from 'next/server';
import { runVineMappFixes } from '@/lib/run-vwork-data-fixes';

/**
 * POST: Apply all tbl_vine_mapp fixes to tbl_vworkjobs.
 * For each mapping: SET vineyard_name_old = current vineyard_name, vineyard_name = new name
 * WHERE trim(vineyard_name) matches old name.
 * Requires vineyard_name_old column (run add_vineyard_name_old_tbl_vworkjobs.sql first).
 */
export async function POST() {
  try {
    const data = await runVineMappFixes();
    return NextResponse.json(data);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

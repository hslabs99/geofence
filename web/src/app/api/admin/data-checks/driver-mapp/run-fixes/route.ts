import { NextResponse } from 'next/server';
import { runDriverMappFixes } from '@/lib/run-vwork-data-fixes';

/**
 * POST: Apply all tbl_driver_mapp fixes to tbl_vworkjobs.
 * For each mapping: SET worker_old = current worker, worker = new name
 * WHERE trim(worker) matches old name.
 */
export async function POST() {
  try {
    const data = await runDriverMappFixes();
    return NextResponse.json(data);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

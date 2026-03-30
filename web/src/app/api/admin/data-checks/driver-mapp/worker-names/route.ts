import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { ensureDriverMappSchema } from '@/lib/ensure-driver-mapp-schema';

/** GET: distinct worker from tbl_vworkjobs (non-empty). */
export async function GET() {
  try {
    await ensureDriverMappSchema();
    const rows = await query<{ worker: string | null }>(
      `SELECT DISTINCT worker FROM tbl_vworkjobs
       WHERE worker IS NOT NULL AND trim(worker) != ''
       ORDER BY worker`
    );
    const list = rows.map((r) => r.worker).filter(Boolean) as string[];
    return NextResponse.json({ rows: list });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

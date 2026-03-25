import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/** GET: distinct vineyard_name from tbl_vworkjobs (non-empty). */
export async function GET() {
  try {
    const rows = await query<{ vineyard_name: string | null }>(
      `SELECT DISTINCT vineyard_name FROM tbl_vworkjobs
       WHERE vineyard_name IS NOT NULL AND trim(vineyard_name) != ''
       ORDER BY vineyard_name`
    );
    const list = rows.map((r) => r.vineyard_name).filter(Boolean) as string[];
    return NextResponse.json({ rows: list });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

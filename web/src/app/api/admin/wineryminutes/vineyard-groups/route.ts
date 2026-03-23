import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/** GET: distinct vineyard_group from tbl_vworkjobs for dropdowns */
export async function GET() {
  try {
    const rows = await query<{ vineyard_group: string | null }>(
      `SELECT DISTINCT vineyard_group FROM tbl_vworkjobs
       WHERE vineyard_group IS NOT NULL AND trim(vineyard_group) != ''
       ORDER BY vineyard_group`
    );
    const list = rows.map((r) => r.vineyard_group).filter(Boolean) as string[];
    return NextResponse.json({ rows: list });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export type VineyardGroupRow = {
  id: number;
  winery_name: string | null;
  vineyard_name: string;
  vineyard_group: string;
  created_at: string;
};

/** GET: list all vineyard group mappings */
export async function GET() {
  try {
    const rows = await query<VineyardGroupRow>(
      `SELECT id, winery_name, vineyard_name, vineyard_group, created_at::text
       FROM tbl_vineyardgroups
       ORDER BY winery_name NULLS LAST, vineyard_name`
    );
    return NextResponse.json({ rows });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** POST: create a new vineyard group mapping */
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const winery_name = (body.winery_name ?? '').trim() || null;
    const vineyard_name = (body.vineyard_name ?? '').trim();
    const vineyard_group = (body.vineyard_group ?? '').trim();
    if (!vineyard_name || !vineyard_group) {
      return NextResponse.json(
        { error: 'vineyard_name and vineyard_group are required' },
        { status: 400 }
      );
    }

    const result = await query<{ id: number }>(
      `INSERT INTO tbl_vineyardgroups (winery_name, vineyard_name, vineyard_group)
       VALUES ($1, $2, $3)
       RETURNING id`,
      [winery_name, vineyard_name, vineyard_group]
    );
    const id = result[0]?.id;
    if (id == null) return NextResponse.json({ error: 'Insert failed' }, { status: 500 });
    return NextResponse.json({ id, ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

type VineyardGroupRow = {
  id: number;
  winery_name: string | null;
  vineyard_name: string;
  vineyard_group: string;
  created_at: string;
};

/** GET: single vineyard group mapping by id */
export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const rows = await query<VineyardGroupRow>(
      `SELECT id, winery_name, vineyard_name, vineyard_group, created_at::text
       FROM tbl_vineyardgroups WHERE id = $1`,
      [idNum]
    );
    if (rows.length === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json(rows[0]);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** PUT: update vineyard group mapping */
export async function PUT(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

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

    const n = await execute(
      `UPDATE tbl_vineyardgroups SET winery_name = $1, vineyard_name = $2, vineyard_group = $3 WHERE id = $4`,
      [winery_name, vineyard_name, vineyard_group, idNum]
    );
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** DELETE: remove vineyard group mapping */
export async function DELETE(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const n = await execute(`DELETE FROM tbl_vineyardgroups WHERE id = $1`, [idNum]);
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

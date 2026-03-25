import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

type VineMappRow = {
  id: number;
  oldvworkname: string;
  newvworkname: string;
  created_at: string;
};

/** GET: single vineyard name fix by id */
export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const rows = await query<VineMappRow>(
      `SELECT id, oldvworkname, newvworkname, created_at::text
       FROM tbl_vine_mapp WHERE id = $1`,
      [idNum]
    );
    if (rows.length === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json(rows[0]);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** PUT: update vineyard name fix */
export async function PUT(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const body = await request.json();
    const oldvworkname = (body.oldvworkname ?? body.old_work_name ?? '').trim();
    const newvworkname = (body.newvworkname ?? body.new_work_name ?? '').trim();
    if (!oldvworkname || !newvworkname) {
      return NextResponse.json(
        { error: 'oldvworkname and newvworkname are required' },
        { status: 400 }
      );
    }

    const n = await execute(
      `UPDATE tbl_vine_mapp SET oldvworkname = $1, newvworkname = $2 WHERE id = $3`,
      [oldvworkname, newvworkname, idNum]
    );
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** DELETE: remove vineyard name fix */
export async function DELETE(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const n = await execute(`DELETE FROM tbl_vine_mapp WHERE id = $1`, [idNum]);
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export type VineMappRow = {
  id: number;
  oldvworkname: string;
  newvworkname: string;
  created_at: string;
};

/** GET: list all vineyard name fixes */
export async function GET() {
  try {
    const rows = await query<VineMappRow>(
      `SELECT id, oldvworkname, newvworkname, created_at::text
       FROM tbl_vine_mapp
       ORDER BY oldvworkname`
    );
    return NextResponse.json({ rows });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** POST: create a new vineyard name fix */
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const oldvworkname = (body.oldvworkname ?? body.old_work_name ?? '').trim();
    const newvworkname = (body.newvworkname ?? body.new_work_name ?? '').trim();
    if (!oldvworkname || !newvworkname) {
      return NextResponse.json(
        { error: 'oldvworkname and newvworkname are required' },
        { status: 400 }
      );
    }

    const result = await query<{ id: number }>(
      `INSERT INTO tbl_vine_mapp (oldvworkname, newvworkname)
       VALUES ($1, $2)
       RETURNING id`,
      [oldvworkname, newvworkname]
    );
    const id = result[0]?.id;
    if (id == null) return NextResponse.json({ error: 'Insert failed' }, { status: 500 });
    return NextResponse.json({ id, ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

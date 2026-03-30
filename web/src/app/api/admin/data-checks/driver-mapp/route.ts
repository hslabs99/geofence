import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { ensureDriverMappSchema } from '@/lib/ensure-driver-mapp-schema';

export type DriverMappRow = {
  id: number;
  oldvworkname: string;
  newvworkname: string;
  created_at: string;
};

/** GET: list all driver name fixes */
export async function GET() {
  try {
    await ensureDriverMappSchema();
    const rows = await query<DriverMappRow>(
      `SELECT id, oldvworkname, newvworkname, created_at::text
       FROM tbl_driver_mapp
       ORDER BY oldvworkname`
    );
    return NextResponse.json({ rows });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** POST: create a new driver name fix */
export async function POST(request: Request) {
  try {
    await ensureDriverMappSchema();
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
      `INSERT INTO tbl_driver_mapp (oldvworkname, newvworkname)
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

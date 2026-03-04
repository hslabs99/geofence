import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

export async function PUT(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const rowId = parseInt(id, 10);
    if (isNaN(rowId)) {
      return NextResponse.json({ error: 'Invalid id' }, { status: 400 });
    }
    const body = await request.json();
    const { type, vwname, gpsname } = body as {
      type?: string;
      vwname?: string;
      gpsname?: string;
    };
    const data: { type?: string; vwname?: string; gpsname?: string } = {};
    if (type !== undefined) data.type = String(type).trim();
    if (vwname !== undefined) data.vwname = String(vwname).trim();
    if (gpsname !== undefined) data.gpsname = String(gpsname).trim();
    if (Object.keys(data).length === 0) {
      return NextResponse.json({ error: 'Nothing to update' }, { status: 400 });
    }
    const setClause = Object.keys(data).map((k, i) => `"${k}" = $${i + 1}`).join(', ');
    const values: unknown[] = Object.values(data);
    values.push(rowId);
    const updated = await query(
      `UPDATE tbl_gpsmappings SET ${setClause} WHERE id = $${values.length} RETURNING *`,
      values
    );
    const row = updated[0];
    if (!row) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ row });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function DELETE(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const rowId = parseInt(id, 10);
    if (isNaN(rowId)) {
      return NextResponse.json({ error: 'Invalid id' }, { status: 400 });
    }
    const n = await execute('DELETE FROM tbl_gpsmappings WHERE id = $1', [rowId]);
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

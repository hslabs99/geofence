import { NextResponse } from 'next/server';
import { execute, query } from '@/lib/db';
import type { SqlRunRow } from '../route';

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
    const { sql_name, sql_command, sql_description } = body as {
      sql_name?: string;
      sql_command?: string;
      sql_description?: string | null;
    };
    const data: {
      sql_name?: string;
      sql_command?: string;
      sql_description?: string | null;
    } = {};
    if (sql_name !== undefined) {
      const s = String(sql_name).trim();
      if (!s) return NextResponse.json({ error: 'sql_name cannot be empty' }, { status: 400 });
      data.sql_name = s;
    }
    if (sql_command !== undefined) {
      const s = String(sql_command).trim();
      if (!s) return NextResponse.json({ error: 'sql_command cannot be empty' }, { status: 400 });
      data.sql_command = s;
    }
    if (sql_description !== undefined) {
      data.sql_description =
        sql_description != null && String(sql_description).trim() !== ''
          ? String(sql_description).trim()
          : null;
    }
    if (Object.keys(data).length === 0) {
      return NextResponse.json({ error: 'Nothing to update' }, { status: 400 });
    }
    const keys = Object.keys(data) as (keyof typeof data)[];
    const setParts = keys.map((k, i) => `${k} = $${i + 1}`);
    setParts.push(`updated_at = now()`);
    const values: unknown[] = keys.map((k) => data[k] as unknown);
    values.push(rowId);
    const updated = await query<SqlRunRow>(
      `UPDATE tbl_sqlruns SET ${setParts.join(', ')} WHERE id = $${values.length}
       RETURNING id, sql_name, sql_command, sql_description, run_order, created_at, updated_at`,
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
    const n = await execute('DELETE FROM tbl_sqlruns WHERE id = $1', [rowId]);
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

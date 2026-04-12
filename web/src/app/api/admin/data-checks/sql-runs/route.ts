import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export type SqlRunRow = {
  id: number;
  sql_name: string;
  sql_command: string;
  sql_description: string | null;
  run_order: number;
  created_at: string;
  updated_at: string;
};

export async function GET() {
  try {
    const rows = await query<SqlRunRow>(
      `SELECT id, sql_name, sql_command, sql_description, run_order, created_at, updated_at
       FROM tbl_sqlruns
       ORDER BY run_order ASC, id ASC`
    );
    return NextResponse.json({ rows });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { sql_name, sql_command, sql_description } = body as {
      sql_name?: string;
      sql_command?: string;
      sql_description?: string;
    };
    if (!sql_name || typeof sql_name !== 'string' || !sql_name.trim()) {
      return NextResponse.json({ error: 'sql_name required' }, { status: 400 });
    }
    if (!sql_command || typeof sql_command !== 'string' || !sql_command.trim()) {
      return NextResponse.json({ error: 'sql_command required' }, { status: 400 });
    }
    const maxRow = await query<{ m: string | null }>(
      `SELECT MAX(run_order)::text AS m FROM tbl_sqlruns`
    );
    const nextOrder =
      maxRow[0]?.m != null && maxRow[0].m !== '' ? parseInt(maxRow[0].m, 10) + 1 : 0;

    const inserted = await query<SqlRunRow>(
      `INSERT INTO tbl_sqlruns (sql_name, sql_command, sql_description, run_order)
       VALUES ($1, $2, $3, $4)
       RETURNING id, sql_name, sql_command, sql_description, run_order, created_at, updated_at`,
      [
        sql_name.trim(),
        sql_command.trim(),
        sql_description != null && String(sql_description).trim() !== ''
          ? String(sql_description).trim()
          : null,
        Number.isFinite(nextOrder) ? nextOrder : 0,
      ]
    );
    const row = inserted[0];
    if (!row) return NextResponse.json({ error: 'Insert failed' }, { status: 500 });
    return NextResponse.json({ row });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

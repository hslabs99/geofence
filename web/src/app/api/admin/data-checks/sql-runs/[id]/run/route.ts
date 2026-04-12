import { NextResponse } from 'next/server';
import { getClient, query } from '@/lib/db';
import { summarizePgResult } from '@/lib/pg-result-summary';

type Row = { id: number; sql_name: string; sql_command: string };

/**
 * Execute one stored SQL by id.
 */
export async function POST(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  let client: Awaited<ReturnType<typeof getClient>> | undefined;
  try {
    const { id } = await params;
    const rowId = parseInt(id, 10);
    if (isNaN(rowId)) {
      return NextResponse.json({ error: 'Invalid id' }, { status: 400 });
    }
    const rows = await query<Row>(
      `SELECT id, sql_name, sql_command FROM tbl_sqlruns WHERE id = $1`,
      [rowId]
    );
    const row = rows[0];
    if (!row) {
      return NextResponse.json({ error: 'Not found' }, { status: 404 });
    }

    client = await getClient();
    const ranAt = new Date().toISOString();
    try {
      const r = await client.query(row.sql_command);
      const summary = summarizePgResult(r);
      return NextResponse.json({
        ok: true,
        ranAt,
        mode: 'single' as const,
        summary: `Ran "${row.sql_name}" successfully.`,
        step: {
          id: row.id,
          sql_name: row.sql_name,
          ok: true,
          command: r.command,
          rowCount: r.rowCount,
          summary,
        },
      });
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      return NextResponse.json(
        {
          ok: false,
          ranAt,
          mode: 'single' as const,
          summary: `Failed: "${row.sql_name}".`,
          step: {
            id: row.id,
            sql_name: row.sql_name,
            ok: false,
            error: message,
          },
        },
        { status: 422 }
      );
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  } finally {
    if (client) client.release();
  }
}

import { NextResponse } from 'next/server';
import { getClient, query } from '@/lib/db';
import { summarizePgResult } from '@/lib/pg-result-summary';

type SqlRunExecRow = {
  id: number;
  sql_name: string;
  sql_command: string;
};

/**
 * Execute each stored SQL in run_order. Stops at the first error; prior statements are already committed.
 */
export async function POST() {
  let client: Awaited<ReturnType<typeof getClient>> | undefined;
  try {
    const rows = await query<SqlRunExecRow>(
      `SELECT id, sql_name, sql_command FROM tbl_sqlruns ORDER BY run_order ASC, id ASC`
    );
    const ranAt = new Date().toISOString();
    if (rows.length === 0) {
      return NextResponse.json({
        ok: true,
        ranAt,
        mode: 'all' as const,
        summary: 'No statements configured.',
        steps: [] as Array<{
          id: number;
          sql_name: string;
          ok: boolean;
          command?: string | null;
          rowCount?: number | null;
          summary?: string;
          error?: string;
        }>,
      });
    }

    client = await getClient();
    const steps: Array<{
      id: number;
      sql_name: string;
      ok: boolean;
      command?: string | null;
      rowCount?: number | null;
      summary?: string;
      error?: string;
    }> = [];

    for (const row of rows) {
      try {
        const r = await client.query(row.sql_command);
        steps.push({
          id: row.id,
          sql_name: row.sql_name,
          ok: true,
          command: r.command,
          rowCount: r.rowCount,
          summary: summarizePgResult(r),
        });
      } catch (e) {
        const message = e instanceof Error ? e.message : String(e);
        steps.push({
          id: row.id,
          sql_name: row.sql_name,
          ok: false,
          error: message,
        });
        return NextResponse.json(
          {
            ok: false,
            ranAt,
            mode: 'all' as const,
            summary: `Stopped after ${steps.length} statement(s): error on "${row.sql_name}".`,
            stoppedAt: row.id,
            steps,
          },
          { status: 422 }
        );
      }
    }

    return NextResponse.json({
      ok: true,
      ranAt,
      mode: 'all' as const,
      summary: `All ${steps.length} statement(s) ran successfully.`,
      steps,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  } finally {
    if (client) client.release();
  }
}

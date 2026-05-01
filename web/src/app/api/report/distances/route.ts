import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { buildWhereAndParams } from '@/lib/vworkjobs-build-where';

/** Cap rows returned (full table scan still possible on COUNT). Raise if needed. */
const MAX_ROWS = 25_000;

/**
 * GET: jobs for the Distances report — same filter params as /api/vworkjobs (customer and template optional).
 * Returns a slim column set plus total count and truncation flag.
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const { conditions, values, debug } = buildWhereAndParams(searchParams);
    const whereClause = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';

    const countRows = await query<{ cnt: string | number }>(
      `SELECT COUNT(*) AS cnt FROM tbl_vworkjobs t${whereClause}`,
      values,
    );
    const rawCnt = countRows[0]?.cnt;
    const total =
      typeof rawCnt === 'number' ? rawCnt : rawCnt != null ? parseInt(String(rawCnt), 10) : 0;

    const dataSql = `
      SELECT
        t.job_id,
        t.customer AS customer,
        t.template,
        t.delivery_winery,
        t.vineyard_name,
        t.truck_id,
        t.worker,
        t.trailermode,
        t.distance,
        t.excluded
      FROM tbl_vworkjobs t
      ${whereClause}
      ORDER BY
        trim(COALESCE(t.customer::text, '')),
        trim(COALESCE(t.template::text, '')),
        trim(COALESCE(t.delivery_winery::text, '')),
        trim(COALESCE(t.truck_id::text, '')),
        trim(COALESCE(t.vineyard_name::text, '')),
        trim(t.job_id::text)
      LIMIT ${MAX_ROWS}`;

    const rows = await query(dataSql, values);
    const truncated = total > (Array.isArray(rows) ? rows.length : 0);

    return NextResponse.json({
      ok: true,
      rows: Array.isArray(rows) ? rows : [],
      total,
      rowCount: Array.isArray(rows) ? rows.length : 0,
      truncated,
      maxRows: MAX_ROWS,
      debug,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

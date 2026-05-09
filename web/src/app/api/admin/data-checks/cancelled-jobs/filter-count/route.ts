import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { buildWhereAndParams } from '@/lib/vworkjobs-build-where';

/**
 * GET: COUNT(*) of tbl_vworkjobs rows matching the same filter params as Diverted Jobs (tab 14) / Inspect
 * (customer, template, winery, vineyard, dateFrom/dateTo, etc. via buildWhereAndParams).
 * Does not run detour detection — cheap for live UI while adjusting filters.
 */
export async function GET(request: Request) {
  try {
    const { conditions, values } = buildWhereAndParams(new URL(request.url).searchParams);
    const whereClause = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';
    const countRows = await query<{ c: string }>(
      `SELECT COUNT(*)::text AS c FROM tbl_vworkjobs t${whereClause}`,
      values,
    );
    const count = parseInt(countRows[0]?.c ?? '0', 10) || 0;
    return NextResponse.json({ ok: true, count });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 400 });
  }
}

import { NextResponse } from 'next/server';
import { getClient } from '@/lib/db';
import { getDayRangeUTC } from '@/lib/tracksolid';

function dateRange(from: string, to: string): string[] {
  const fromDate = new Date(from + 'T00:00:00');
  const toDate = new Date(to + 'T00:00:00');
  if (fromDate.getTime() > toDate.getTime()) return [];
  const out: string[] = [];
  const pad = (n: number) => String(n).padStart(2, '0');
  const cur = new Date(fromDate);
  while (cur.getTime() <= toDate.getTime()) {
    out.push(`${cur.getFullYear()}-${pad(cur.getMonth() + 1)}-${pad(cur.getDate())}`);
    cur.setDate(cur.getDate() + 1);
  }
  return out;
}

export type DbCheckSimpleRow =
  | { date: string; status: 'empty' }
  | { date: string; status: 'data'; count: number; minTime: string; maxTime: string };

export type DbCheckSimpleResponse = {
  dates: string[];
  rows: DbCheckSimpleRow[];
};

/** POST: tbl_tracking only — one row per UTC calendar day: total count and min/max position_time (all devices). */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as { dateFrom?: string; dateTo?: string };
    const dateFrom = (body?.dateFrom ?? '').trim();
    const dateTo = (body?.dateTo ?? '').trim();
    if (!dateFrom || !dateTo) {
      return NextResponse.json(
        { error: 'dateFrom and dateTo (YYYY-MM-DD) required' },
        { status: 400 }
      );
    }
    const dates = dateRange(dateFrom, dateTo);
    if (dates.length === 0) {
      return NextResponse.json({ error: 'dateFrom must be <= dateTo' }, { status: 400 });
    }

    const rangeStart = getDayRangeUTC(dateFrom).begin;
    const rangeEnd = getDayRangeUTC(dateTo).end;

    const client = await getClient();
    try {
      await client.query("SET LOCAL timezone = 'UTC'");
      const res = await client.query(
        `SELECT to_char((position_time)::date, 'YYYY-MM-DD') AS d,
                COUNT(*)::bigint AS cnt,
                MIN(position_time)::text AS min_t,
                MAX(position_time)::text AS max_t
         FROM tbl_tracking
         WHERE position_time >= $1::timestamp AND position_time <= $2::timestamp
         GROUP BY (position_time)::date
         ORDER BY d`,
        [rangeStart, rangeEnd]
      );

      const byDay = new Map<string, { count: number; minTime: string; maxTime: string }>();
      for (const row of res.rows as {
        d: string;
        cnt: string;
        min_t: string | null;
        max_t: string | null;
      }[]) {
        byDay.set(row.d, {
          count: parseInt(row.cnt, 10),
          minTime: row.min_t?.trim() ?? '',
          maxTime: row.max_t?.trim() ?? '',
        });
      }

      const rows: DbCheckSimpleRow[] = dates.map((date) => {
        const v = byDay.get(date);
        if (!v) return { date, status: 'empty' as const };
        return {
          date,
          status: 'data' as const,
          count: v.count,
          minTime: v.minTime,
          maxTime: v.maxTime,
        };
      });

      return NextResponse.json({ dates, rows } satisfies DbCheckSimpleResponse);
    } finally {
      client.release();
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[db-check-simple]', message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

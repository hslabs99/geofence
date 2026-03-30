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

export type DbCheckCell =
  | { status: 'empty' }
  | { status: 'data'; count: number; minTime: string; maxTime: string };

export type DbCheckGridResponse = {
  dates: string[];
  devices: string[];
  rows: Array<{ device: string; cells: DbCheckCell[] }>;
};

/** POST: tbl_tracking only — count and min/max position_time per device per UTC calendar day. */
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
        `SELECT device_name::text AS device_name,
                to_char((position_time)::date, 'YYYY-MM-DD') AS d,
                COUNT(*)::bigint AS cnt,
                MIN(position_time)::text AS min_t,
                MAX(position_time)::text AS max_t
         FROM tbl_tracking
         WHERE position_time >= $1::timestamp AND position_time <= $2::timestamp
         GROUP BY device_name, (position_time)::date
         ORDER BY device_name, d`,
        [rangeStart, rangeEnd]
      );

      const map = new Map<string, { count: number; minTime: string; maxTime: string }>();
      const deviceSet = new Set<string>();
      for (const row of res.rows as {
        device_name: string;
        d: string;
        cnt: string;
        min_t: string | null;
        max_t: string | null;
      }[]) {
        deviceSet.add(row.device_name);
        map.set(`${row.device_name}\t${row.d}`, {
          count: parseInt(row.cnt, 10),
          minTime: row.min_t?.trim() ?? '',
          maxTime: row.max_t?.trim() ?? '',
        });
      }
      const devices = [...deviceSet].sort();
      const rows: DbCheckGridResponse['rows'] = devices.map((device) => ({
        device,
        cells: dates.map((d) => {
          const v = map.get(`${device}\t${d}`);
          if (!v) return { status: 'empty' as const };
          return {
            status: 'data' as const,
            count: v.count,
            minTime: v.minTime,
            maxTime: v.maxTime,
          };
        }),
      }));

      return NextResponse.json({
        dates,
        devices,
        rows,
      } satisfies DbCheckGridResponse);
    } finally {
      client.release();
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[db-check]', message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

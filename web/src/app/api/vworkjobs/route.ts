import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';
import { dateToLiteralUTC } from '@/lib/utils';

/** JSON serialization — no toISOString. */
function jsonSafe<T>(obj: T): T {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'bigint') return String(obj) as T;
  if (typeof obj === 'object' && obj instanceof Date) return dateToLiteralUTC(obj) as T;
  if (Array.isArray(obj)) return obj.map(jsonSafe) as T;
  if (typeof obj === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj)) {
      out[k] = jsonSafe(v);
    }
    return out as T;
  }
  return obj;
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const dateParam = searchParams.get('date'); // YYYY-MM-DD: jobs with actual_start_time on this date
    const stepsFetchedParam = searchParams.get('stepsFetched'); // 'true' | 'false': filter by steps_fetched

    let rows: unknown[];
    let debug: { date?: string; stepsFetchedFilter?: string; stepsFetchedFilterApplied: boolean; stepsFetchedFilterSkipped?: boolean; whereHint?: string } = { stepsFetchedFilterApplied: false };
    if (dateParam || stepsFetchedParam === 'false' || stepsFetchedParam === 'true') {
      const conditions: string[] = [];
      const values: unknown[] = [];
      let idx = 1;
      if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
        conditions.push(`(actual_start_time >= $${idx}::timestamp AND actual_start_time < $${idx}::timestamp + interval '1 day')`);
        values.push(`${dateParam}T00:00:00`);
        idx++;
        debug.date = dateParam;
      }
      if (stepsFetchedParam === 'false') {
        conditions.push(`(steps_fetched = false OR steps_fetched IS NULL)`);
        debug.stepsFetchedFilter = 'false';
      } else if (stepsFetchedParam === 'true') {
        conditions.push(`steps_fetched = true`);
        debug.stepsFetchedFilter = 'true';
      }
      const whereClause = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';
      debug.whereHint = whereClause || '(none)';
      try {
        rows = await prisma.$queryRawUnsafe(
          `SELECT * FROM tbl_vworkjobs${whereClause} ORDER BY job_id LIMIT 500`,
          ...values
        );
        debug.stepsFetchedFilterApplied = Boolean(debug.stepsFetchedFilter);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if ((msg.includes('42703') || msg.includes('does not exist')) && (stepsFetchedParam === 'false' || stepsFetchedParam === 'true')) {
          const dateOnlyConditions: string[] = [];
          const dateOnlyValues: unknown[] = [];
          let i = 1;
          if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
            dateOnlyConditions.push(`(actual_start_time >= $${i}::timestamp AND actual_start_time < $${i}::timestamp + interval '1 day')`);
            dateOnlyValues.push(`${dateParam}T00:00:00`);
          }
          const w = dateOnlyConditions.length ? ` WHERE ${dateOnlyConditions.join(' AND ')}` : '';
          rows = await prisma.$queryRawUnsafe(`SELECT * FROM tbl_vworkjobs${w} ORDER BY job_id LIMIT 500`, ...dateOnlyValues);
          debug.stepsFetchedFilterSkipped = true;
          debug.stepsFetchedFilterApplied = false;
          debug.whereHint = `(steps_fetched column missing) ${w || '(none)'}`;
        } else {
          throw err;
        }
      }
    } else {
      rows = await prisma.$queryRaw`SELECT * FROM tbl_vworkjobs ORDER BY job_id LIMIT 500`;
    }
    return NextResponse.json({ rows: jsonSafe(rows), debug });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

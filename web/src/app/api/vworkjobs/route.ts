import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { dateToLiteral } from '@/lib/utils';

/** Raw timestamp columns in tbl_vworkjobs: SELECT as to_char so API returns exact DB digits. Only columns that exist in tbl_vworkjobs (no gps_start_time/gps_end_time — those are not in this table). */
const RAW_TIMESTAMP_COLS = [
  'actual_start_time', 'actual_end_time', 'planned_start_time',
  'step_1_completed_at', 'step_2_completed_at', 'step_3_completed_at', 'step_4_completed_at', 'step_5_completed_at',
  'step_1_gps_completed_at', 'step_2_gps_completed_at', 'step_3_gps_completed_at', 'step_4_gps_completed_at', 'step_5_gps_completed_at',
  'step_1_actual_time', 'step_2_actual_time', 'step_3_actual_time', 'step_4_actual_time', 'step_5_actual_time',
] as const;
const RAW_SELECT_FRAGMENT = RAW_TIMESTAMP_COLS.map((c) => `to_char(t.${c}, 'YYYY-MM-DD HH24:MI:SS') AS ${c}_raw`).join(', ');

/** JSON serialization — no UTC; dates as raw so NZ-stored values display correctly. */
function jsonSafe<T>(obj: T): T {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'bigint') return String(obj) as T;
  if (typeof obj === 'object' && obj instanceof Date) return dateToLiteral(obj) as T;
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
    const dateParam = searchParams.get('date');
    const stepsFetchedParam = searchParams.get('stepsFetched');

    let rows: unknown[];
    const debug: { date?: string; stepsFetchedFilter?: string; stepsFetchedFilterApplied: boolean; stepsFetchedFilterSkipped?: boolean; whereHint?: string } = { stepsFetchedFilterApplied: false };
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
        rows = await query(
          `SELECT t.*, ${RAW_SELECT_FRAGMENT} FROM tbl_vworkjobs t${whereClause} ORDER BY t.job_id LIMIT 500`,
          values
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
          rows = await query(`SELECT t.*, ${RAW_SELECT_FRAGMENT} FROM tbl_vworkjobs t${w} ORDER BY t.job_id LIMIT 500`, dateOnlyValues);
          debug.stepsFetchedFilterSkipped = true;
          debug.stepsFetchedFilterApplied = false;
          debug.whereHint = `(steps_fetched column missing) ${w || '(none)'}`;
        } else {
          throw err;
        }
      }
    } else {
      rows = await query(`SELECT t.*, ${RAW_SELECT_FRAGMENT} FROM tbl_vworkjobs t ORDER BY t.job_id LIMIT 500`);
    }
    // Use raw timestamp strings from to_char — exact DB digits, no Date, no timezone, no add/subtract.
    const normalized = (rows as Record<string, unknown>[]).map((row) => {
      const out = { ...row };
      for (const col of RAW_TIMESTAMP_COLS) {
        const rawKey = `${col}_raw`;
        const rawVal = out[rawKey];
        if (typeof rawVal === 'string') {
          out[col] = rawVal;
          delete out[rawKey];
        }
      }
      return out;
    });
    return NextResponse.json({ rows: jsonSafe(normalized), debug });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

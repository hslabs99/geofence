import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { dateToLiteral } from '@/lib/utils';

/** Raw timestamp columns in tbl_vworkjobs: SELECT as to_char so API returns exact DB digits. Only columns that exist in tbl_vworkjobs (no gps_start_time/gps_end_time — those are not in this table). */
const RAW_TIMESTAMP_COLS = [
  'actual_start_time', 'actual_end_time', 'planned_start_time',
  'step_1_completed_at', 'step_2_completed_at', 'step_3_completed_at', 'step_4_completed_at', 'step_5_completed_at',
  'step_1_gps_completed_at', 'step_2_gps_completed_at', 'step_3_gps_completed_at', 'step_4_gps_completed_at', 'step_5_gps_completed_at',
  'step_1_actual_time', 'step_2_actual_time', 'step_3_actual_time', 'step_4_actual_time', 'step_5_actual_time',
  'step1oride', 'step2oride', 'step3oride', 'step4oride', 'step5oride',
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

/** Apply filters (customer, template, date range, etc.) and pagination. Uses "Customer" / "Template" if present, else customer / template. */
function buildWhereAndParams(searchParams: URLSearchParams): { conditions: string[]; values: unknown[]; debug: Record<string, unknown> } {
  const conditions: string[] = [];
  const values: unknown[] = [];
  const debug: Record<string, unknown> = {};
  let idx = 1;

  const dateFromParam = searchParams.get('dateFrom');
  const dateToParam = searchParams.get('dateTo');
  const dateParam = searchParams.get('date');
  if (dateFromParam && dateToParam && /^\d{4}-\d{2}-\d{2}$/.test(dateFromParam) && /^\d{4}-\d{2}-\d{2}$/.test(dateToParam)) {
    conditions.push(`(actual_start_time >= $${idx}::date AND actual_start_time < $${idx + 1}::date + interval '1 day')`);
    values.push(dateFromParam, dateToParam);
    idx += 2;
    debug.dateFrom = dateFromParam;
    debug.dateTo = dateToParam;
  } else if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    conditions.push(`(actual_start_time >= $${idx}::timestamp AND actual_start_time < $${idx}::timestamp + interval '1 day')`);
    values.push(`${dateParam}T00:00:00`);
    idx++;
    debug.date = dateParam;
  }

  const customerParam = searchParams.get('customer')?.trim();
  if (customerParam) {
    conditions.push(`(trim(t.customer) = $${idx})`);
    values.push(customerParam);
    idx++;
    debug.customer = customerParam;
  }

  const templateParam = searchParams.get('template')?.trim();
  if (templateParam) {
    conditions.push(`(trim(t.template) = $${idx})`);
    values.push(templateParam);
    idx++;
    debug.template = templateParam;
  }

  const wineryParam = searchParams.get('winery')?.trim();
  if (wineryParam) {
    conditions.push(`(trim(t.delivery_winery) = $${idx})`);
    values.push(wineryParam);
    idx++;
    debug.winery = wineryParam;
  }

  const vineyardParam = searchParams.get('vineyard')?.trim();
  if (vineyardParam) {
    conditions.push(`(trim(t.vineyard_name) = $${idx})`);
    values.push(vineyardParam);
    idx++;
    debug.vineyard = vineyardParam;
  }

  const vineyardGroupParam = searchParams.get('vineyard_group')?.trim();
  if (vineyardGroupParam) {
    conditions.push(`(trim(COALESCE(t.vineyard_group, '')) = $${idx})`);
    values.push(vineyardGroupParam);
    idx++;
    debug.vineyard_group = vineyardGroupParam;
  }

  const truckParam = searchParams.get('truck_id')?.trim();
  if (truckParam) {
    conditions.push(`(trim(t.truck_id) = $${idx})`);
    values.push(truckParam);
    idx++;
    debug.truck_id = truckParam;
  }

  const workerParam = searchParams.get('worker')?.trim();
  if (workerParam) {
    conditions.push(`(t.worker IS NOT NULL AND trim(t.worker) ILIKE $${idx})`);
    values.push(`%${workerParam}%`);
    idx++;
    debug.worker = workerParam;
  }

  const trailermodeParam = searchParams.get('trailermode')?.trim();
  if (trailermodeParam) {
    conditions.push(`(trim(COALESCE(t.trailermode::text, t.trailertype::text)) = $${idx})`);
    values.push(trailermodeParam);
    idx++;
    debug.trailermode = trailermodeParam;
  }

  const stepsFetchedParam = searchParams.get('stepsFetched');
  if (stepsFetchedParam === 'false') {
    conditions.push(`(t.steps_fetched = false OR t.steps_fetched IS NULL)`);
    debug.stepsFetchedFilter = 'false';
  } else if (stepsFetchedParam === 'true') {
    conditions.push(`t.steps_fetched = true`);
    debug.stepsFetchedFilter = 'true';
  }

  return { conditions, values, debug };
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const limitParam = searchParams.get('limit');
    const offsetParam = searchParams.get('offset');
    const limit = Math.min(Math.max(1, parseInt(limitParam ?? '500', 10) || 500), 2000);
    const offset = Math.max(0, parseInt(offsetParam ?? '0', 10) || 0);

    const { conditions, values, debug } = buildWhereAndParams(searchParams);
    const whereClause = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';
    const orderLimitOffset = ` ORDER BY t.job_id LIMIT ${limit} OFFSET ${offset}`;

    // Get total count and page of rows. Use COUNT(*) OVER () so we get total from first row.
    let rows: unknown[];
    let total = 0;
    const selectList = `t.*, ${RAW_SELECT_FRAGMENT}`;
    rows = await query(
      `SELECT ${selectList}, COUNT(*) OVER () AS _total FROM tbl_vworkjobs t${whereClause}${orderLimitOffset}`,
      values
    );
    if (Array.isArray(rows) && rows.length > 0 && typeof (rows[0] as Record<string, unknown>)._total === 'number') {
      total = (rows[0] as Record<string, unknown>)._total as number;
    } else if (Array.isArray(rows)) {
      total = rows.length;
      if (rows.length === limit) total = limit + offset; // underestimate if we got a full page
    }

    // Use raw timestamp strings from to_char — exact DB digits, no Date, no timezone, no add/subtract.
    const normalized = (rows as Record<string, unknown>[]).map((row) => {
      const out = { ...row };
      delete out._total;
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
    return NextResponse.json({ rows: jsonSafe(normalized), total, debug });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

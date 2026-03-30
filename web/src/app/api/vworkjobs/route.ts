import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { dateToLiteral } from '@/lib/utils';

/** Raw timestamp columns in tbl_vworkjobs: SELECT as to_char so API returns exact DB digits. */
const RAW_TIMESTAMP_COLS = [
  'actual_start_time', 'actual_end_time', 'planned_start_time',
  'step_1_completed_at', 'step_2_completed_at', 'step_3_completed_at', 'step_4_completed_at', 'step_5_completed_at',
  'step_1_gps_completed_at', 'step_2_gps_completed_at', 'step_3_gps_completed_at', 'step_4_gps_completed_at', 'step_5_gps_completed_at',
  'step_1_actual_time', 'step_2_actual_time', 'step_3_actual_time', 'step_4_actual_time', 'step_5_actual_time',
  'step1oride', 'step2oride', 'step3oride', 'step4oride', 'step5oride',
] as const;
const RAW_SELECT_FRAGMENT = RAW_TIMESTAMP_COLS.map((c) => `to_char(t.${c}, 'YYYY-MM-DD HH24:MI:SS') AS ${c}_raw`).join(', ');

/** Whitelist for ORDER BY (Inspect / Summary). */
const SORT_COLUMNS = new Set([
  'job_id',
  'actual_start_time',
  'planned_start_time',
  'actual_end_time',
  'worker',
  'truck_id',
  'customer',
]);

/** Optional date filter on a single timestamp column (columnDateCol). */
const COLUMN_DATE_WHITELIST = new Set([
  'planned_start_time',
  'actual_start_time',
  'actual_end_time',
]);

function buildOrderBy(searchParams: URLSearchParams): { sql: string; debug: Record<string, unknown> } {
  const colRaw = searchParams.get('sortColumn')?.trim() ?? '';
  const dirRaw = searchParams.get('sortDir')?.trim().toLowerCase() ?? '';
  const dir = dirRaw === 'desc' ? 'DESC' : 'ASC';
  const col = SORT_COLUMNS.has(colRaw) ? colRaw : 'job_id';
  return {
    sql: `ORDER BY t.${col} ${dir} NULLS LAST`,
    debug: { sortColumn: col, sortDir: dir.toLowerCase() },
  };
}

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

/** Apply filters (customer, template, date range, etc.) and pagination. Uses trim(t.customer) for customer. */
function buildWhereAndParams(searchParams: URLSearchParams): { conditions: string[]; values: unknown[]; debug: Record<string, unknown> } {
  const conditions: string[] = [];
  const values: unknown[] = [];
  const debug: Record<string, unknown> = {};
  let idx = 1;

  const dateFromParam = searchParams.get('dateFrom')?.trim();
  const dateToParam = searchParams.get('dateTo')?.trim();
  const dateParam = searchParams.get('date')?.trim();
  if (dateFromParam && dateToParam && /^\d{4}-\d{2}-\d{2}$/.test(dateFromParam) && /^\d{4}-\d{2}-\d{2}$/.test(dateToParam)) {
    conditions.push(`(actual_start_time >= $${idx}::date AND actual_start_time < $${idx + 1}::date + interval '1 day')`);
    values.push(dateFromParam, dateToParam);
    idx += 2;
    debug.dateFrom = dateFromParam;
    debug.dateTo = dateToParam;
  } else if (dateFromParam && /^\d{4}-\d{2}-\d{2}$/.test(dateFromParam)) {
    conditions.push(`(actual_start_time >= $${idx}::date)`);
    values.push(dateFromParam);
    idx++;
    debug.dateFrom = dateFromParam;
  } else if (dateToParam && /^\d{4}-\d{2}-\d{2}$/.test(dateToParam)) {
    conditions.push(`(actual_start_time < $${idx}::date + interval '1 day')`);
    values.push(dateToParam);
    idx++;
    debug.dateTo = dateToParam;
  } else if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    conditions.push(`(actual_start_time >= $${idx}::timestamp AND actual_start_time < $${idx}::timestamp + interval '1 day')`);
    values.push(`${dateParam}T00:00:00`);
    idx++;
    debug.date = dateParam;
  }

  const plannedFrom = searchParams.get('plannedDateFrom')?.trim();
  const plannedTo = searchParams.get('plannedDateTo')?.trim();
  if (plannedFrom && plannedTo && /^\d{4}-\d{2}-\d{2}$/.test(plannedFrom) && /^\d{4}-\d{2}-\d{2}$/.test(plannedTo)) {
    conditions.push(`(t.planned_start_time IS NOT NULL AND t.planned_start_time::date >= $${idx}::date AND t.planned_start_time::date <= $${idx + 1}::date)`);
    values.push(plannedFrom, plannedTo);
    idx += 2;
    debug.plannedDateFrom = plannedFrom;
    debug.plannedDateTo = plannedTo;
  } else if (plannedFrom && /^\d{4}-\d{2}-\d{2}$/.test(plannedFrom)) {
    conditions.push(`(t.planned_start_time IS NOT NULL AND t.planned_start_time::date >= $${idx}::date)`);
    values.push(plannedFrom);
    idx++;
    debug.plannedDateFrom = plannedFrom;
  } else if (plannedTo && /^\d{4}-\d{2}-\d{2}$/.test(plannedTo)) {
    conditions.push(`(t.planned_start_time IS NOT NULL AND t.planned_start_time::date <= $${idx}::date)`);
    values.push(plannedTo);
    idx++;
    debug.plannedDateTo = plannedTo;
  }

  const columnDateCol = searchParams.get('columnDateCol')?.trim();
  const columnDateFrom = searchParams.get('columnDateFrom')?.trim().slice(0, 10);
  const columnDateTo = searchParams.get('columnDateTo')?.trim().slice(0, 10);
  if (columnDateCol && COLUMN_DATE_WHITELIST.has(columnDateCol)) {
    if (columnDateFrom && columnDateTo && /^\d{4}-\d{2}-\d{2}$/.test(columnDateFrom) && /^\d{4}-\d{2}-\d{2}$/.test(columnDateTo)) {
      conditions.push(`(t.${columnDateCol} IS NOT NULL AND t.${columnDateCol}::date >= $${idx}::date AND t.${columnDateCol}::date <= $${idx + 1}::date)`);
      values.push(columnDateFrom, columnDateTo);
      idx += 2;
      debug.columnDateCol = columnDateCol;
      debug.columnDateFrom = columnDateFrom;
      debug.columnDateTo = columnDateTo;
    } else if (columnDateFrom && /^\d{4}-\d{2}-\d{2}$/.test(columnDateFrom)) {
      conditions.push(`(t.${columnDateCol} IS NOT NULL AND t.${columnDateCol}::date >= $${idx}::date)`);
      values.push(columnDateFrom);
      idx++;
      debug.columnDateCol = columnDateCol;
      debug.columnDateFrom = columnDateFrom;
    } else if (columnDateTo && /^\d{4}-\d{2}-\d{2}$/.test(columnDateTo)) {
      conditions.push(`(t.${columnDateCol} IS NOT NULL AND t.${columnDateCol}::date <= $${idx}::date)`);
      values.push(columnDateTo);
      idx++;
      debug.columnDateCol = columnDateCol;
      debug.columnDateTo = columnDateTo;
    }
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

  /** Exact match on worker (e.g. tagging page device dropdown). Mutually exclusive with fuzzy `worker`. */
  const deviceParam = searchParams.get('device')?.trim();
  const workerParam = searchParams.get('worker')?.trim();
  if (deviceParam) {
    conditions.push(`(t.worker IS NOT NULL AND trim(t.worker) = $${idx})`);
    values.push(deviceParam);
    idx++;
    debug.device = deviceParam;
  } else if (workerParam) {
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

  const jobIdContains = searchParams.get('jobIdContains')?.trim();
  const jobIdExact = searchParams.get('jobIdExact')?.trim();
  if (jobIdExact) {
    conditions.push(`trim(t.job_id::text) = trim($${idx}::text)`);
    values.push(jobIdExact);
    idx++;
    debug.jobIdExact = jobIdExact;
  } else if (jobIdContains) {
    conditions.push(`(trim(t.job_id::text) ILIKE $${idx})`);
    values.push(`%${jobIdContains}%`);
    idx++;
    debug.jobIdContains = jobIdContains;
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

/** For API debug: substitute $n from highest index first so $10 is not broken by replacing $1. */
function interpolateSqlPlaceholders(sql: string, values: unknown[]): string {
  let out = sql;
  for (let i = values.length; i >= 1; i--) {
    const v = values[i - 1];
    const rep =
      v === null || v === undefined
        ? 'NULL'
        : typeof v === 'string'
          ? `'${String(v).replace(/'/g, "''")}'`
          : typeof v === 'number' || typeof v === 'bigint'
            ? String(v)
            : `'${String(v).replace(/'/g, "''")}'`;
    out = out.split(`$${i}`).join(rep);
  }
  return out;
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const countOnly = searchParams.get('countOnly') === 'true' || searchParams.get('countOnly') === '1';
    if (countOnly) {
      const { conditions, values, debug } = buildWhereAndParams(searchParams);
      const whereClause = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';
      const countSqlForDebug = `SELECT COUNT(*) AS cnt FROM tbl_vworkjobs t${whereClause}`;
      const countRows = await query<{ cnt: string | number }>(countSqlForDebug, values);
      const raw = countRows[0]?.cnt;
      const count =
        typeof raw === 'number'
          ? raw
          : raw != null
            ? parseInt(String(raw), 10)
            : 0;
      const debugSqlWithParams =
        values.length > 0 ? interpolateSqlPlaceholders(countSqlForDebug, values) : countSqlForDebug;
      return NextResponse.json({
        count,
        ok: true,
        debug,
        debugSql: countSqlForDebug,
        debugSqlParams: values,
        debugSqlLiteral: debugSqlWithParams,
      });
    }

    const limitParam = searchParams.get('limit');
    const offsetParam = searchParams.get('offset');
    const hasExplicitLimit = limitParam != null && String(limitParam).trim() !== '';
    const limit = hasExplicitLimit
      ? Math.min(Math.max(1, parseInt(String(limitParam), 10) || 500), 20000)
      : null;
    let offset = Math.max(0, parseInt(offsetParam ?? '0', 10) || 0);

    const { conditions, values, debug } = buildWhereAndParams(searchParams);
    const { sql: orderBySql, debug: orderDebug } = buildOrderBy(searchParams);
    Object.assign(debug, orderDebug);

    const whereClause = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';
    const resolveJob = searchParams.get('resolvePageForJob')?.trim();

    let resolvePage: number | null = null;
    let jobNotFound = false;

    if (resolveJob && limit != null) {
      const rankSql = `
        SELECT sub.idx FROM (
          SELECT trim(t.job_id::text) AS jid, ROW_NUMBER() OVER (${orderBySql}) - 1 AS idx
          FROM tbl_vworkjobs t
          ${whereClause}
        ) sub
        WHERE sub.jid = $${values.length + 1}
      `;
      const rankValues = [...values, resolveJob];
      const rankRows = await query<{ idx: string }>(rankSql, rankValues);
      const jobIdx = rankRows[0]?.idx != null ? parseInt(String(rankRows[0].idx), 10) : NaN;
      if (!Number.isFinite(jobIdx)) {
        jobNotFound = true;
        resolvePage = null;
        offset = 0;
      } else {
        resolvePage = Math.floor(jobIdx / limit);
        offset = resolvePage * limit;
        debug.resolvePageForJob = resolveJob;
        debug.resolvePage = resolvePage;
        debug.resolveJobIndex = jobIdx;
      }
    }

    const orderLimitOffset =
      limit != null
        ? ` ${orderBySql} LIMIT ${limit} OFFSET ${offset}`
        : ` ${orderBySql} LIMIT ALL OFFSET ${offset}`;

    let rows: unknown[];
    let total = 0;
    const selectList = `t.*, ${RAW_SELECT_FRAGMENT}`;
    rows = await query(
      `SELECT ${selectList}, COUNT(*) OVER () AS _total FROM tbl_vworkjobs t${whereClause}${orderLimitOffset}`,
      values,
    );
    if (Array.isArray(rows) && rows.length > 0 && typeof (rows[0] as Record<string, unknown>)._total === 'number') {
      total = (rows[0] as Record<string, unknown>)._total as number;
    } else if (Array.isArray(rows)) {
      total = rows.length;
      if (limit != null && rows.length === limit) total = limit + offset;
    }

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

    const countSqlForDebug = `SELECT COUNT(*) AS cnt FROM tbl_vworkjobs t${whereClause}`;
    const debugSqlWithParams =
      values.length > 0 ? interpolateSqlPlaceholders(countSqlForDebug, values) : countSqlForDebug;

    const payload: Record<string, unknown> = {
      rows: jsonSafe(normalized),
      total,
      debug,
      debugSql: countSqlForDebug,
      debugSqlParams: values,
      debugSqlLiteral: debugSqlWithParams,
    };
    if (resolveJob && limit != null) {
      payload.resolvePage = resolvePage;
      payload.jobNotFound = jobNotFound;
    }

    return NextResponse.json(payload);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

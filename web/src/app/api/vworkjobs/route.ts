import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { buildWhereAndParams } from '@/lib/vworkjobs-build-where';
import { dateToLiteral } from '@/lib/utils';

/** Raw timestamp columns in tbl_vworkjobs: SELECT as to_char so API returns exact DB digits. */
const RAW_TIMESTAMP_COLS = [
  'actual_start_time', 'actual_end_time', 'planned_start_time',
  'step_1_completed_at', 'step_1_safe', 'step_2_completed_at', 'step_3_completed_at', 'step_4_completed_at', 'step_5_completed_at',
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

function buildOrderBy(searchParams: URLSearchParams): { sql: string; debug: Record<string, unknown> } {
  const dirRaw = searchParams.get('sortDir')?.trim().toLowerCase() ?? '';
  const dir = dirRaw === 'desc' ? 'DESC' : 'ASC';

  const requested = [
    searchParams.get('sortColumn')?.trim() ?? '',
    searchParams.get('sortColumn2')?.trim() ?? '',
    searchParams.get('sortColumn3')?.trim() ?? '',
  ];
  const cols: string[] = [];
  const seen = new Set<string>();
  for (const raw of requested) {
    if (!raw || !SORT_COLUMNS.has(raw) || seen.has(raw)) continue;
    cols.push(raw);
    seen.add(raw);
  }
  const orderCols = cols.length > 0 ? cols : ['job_id'];
  const parts = orderCols.map((c) => `t.${c} ${dir} NULLS LAST`);
  return {
    sql: `ORDER BY ${parts.join(', ')}`,
    debug: { sortColumns: orderCols, sortDir: dir.toLowerCase() },
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

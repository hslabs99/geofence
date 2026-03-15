import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { dateToLiteral } from '@/lib/utils';

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
    const limit = Math.min(parseInt(searchParams.get('limit') ?? '200', 10), 1000);
    const type = searchParams.get('type')?.trim() || undefined;
    const cat1 = searchParams.get('cat1')?.trim() || undefined;
    const cat2 = searchParams.get('cat2')?.trim() || undefined;
    const sort = (searchParams.get('sort') ?? 'logid').toLowerCase();
    const dir = (searchParams.get('dir') ?? 'desc').toLowerCase();
    const dateFrom = searchParams.get('dateFrom')?.trim() || undefined;
    const dateTo = searchParams.get('dateTo')?.trim() || undefined;

    const validSort = ['logid', 'logdatetime'].includes(sort) ? sort : 'logid';
    const validDir = dir === 'asc' ? 'ASC' : 'DESC';

    let sql = 'SELECT * FROM tbl_logs WHERE 1=1';
    const params: (string | number)[] = [];
    let i = 1;
    if (type) {
      sql += ` AND logtype = $${i++}`;
      params.push(type);
    }
    if (cat1) {
      sql += ` AND logcat1 = $${i++}`;
      params.push(cat1);
    }
    if (cat2) {
      sql += ` AND logcat2 = $${i++}`;
      params.push(cat2);
    }
    if (dateFrom) {
      sql += ` AND logdatetime >= $${i++}::timestamp`;
      params.push(dateFrom.length === 10 ? `${dateFrom}T00:00:00` : dateFrom);
    }
    if (dateTo) {
      sql += ` AND logdatetime <= $${i++}::timestamp`;
      params.push(dateTo.length === 10 ? `${dateTo}T23:59:59` : dateTo);
    }
    sql += ` ORDER BY ${validSort} ${validDir} LIMIT $${i}`;
    params.push(limit);

    const logs = await query(sql, params);
    return NextResponse.json({ logs: jsonSafe(logs) });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

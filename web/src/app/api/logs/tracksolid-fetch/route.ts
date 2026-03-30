import { NextResponse } from 'next/server';
import { execute, query } from '@/lib/db';

const LOGTYPE = 'tracksolid_fetch';

type StartBody = {
  action: 'start';
  deviceName: string;
  imei: string;
  date: string;
  endpoint: string;
  requestUrl: string;
};

type CompleteBody = {
  action: 'complete';
  logId: number;
  outcome: 'success' | 'failed' | 'error' | 'client_abort';
  rowcount?: number | null;
  error?: string | null;
  httpStatus?: number | null;
};

function parseLogDetails(raw: string | null): Record<string, unknown> {
  if (!raw?.trim()) return {};
  try {
    const o = JSON.parse(raw) as unknown;
    return typeof o === 'object' && o !== null && !Array.isArray(o) ? (o as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

/**
 * Client-initiated TrackSolid GET /api/tracksolid/track lifecycle:
 * - start: insert row with status "attempting" (incomplete until complete)
 * - complete: merge status success | failed | error | client_abort + ended_at
 */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as StartBody | CompleteBody;
    if (body?.action === 'start') {
      const deviceName = (body as StartBody).deviceName?.trim();
      const imei = (body as StartBody).imei?.trim();
      const date = (body as StartBody).date?.trim();
      const endpoint = (body as StartBody).endpoint?.trim() ?? '';
      const requestUrl = (body as StartBody).requestUrl?.trim() ?? '';
      if (!deviceName || !imei || !date) {
        return NextResponse.json({ ok: false, error: 'deviceName, imei, date required' }, { status: 400 });
      }
      const startedAt = new Date().toISOString();
      const logdetails = JSON.stringify({
        phase: 'tracksolid_gettrack',
        status: 'attempting',
        started_at: startedAt,
        ended_at: null as string | null,
        device_name: deviceName,
        imei,
        date,
        endpoint: endpoint || 'default',
        request_url: requestUrl,
        rowcount: null as number | null,
        http_status: null as number | null,
        error: null as string | null,
      });
      const rows = await query<{ logid: string | number }>(
        'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4) RETURNING logid',
        [LOGTYPE, deviceName, date, logdetails]
      );
      const logid = rows[0]?.logid;
      const logId = logid != null ? (typeof logid === 'bigint' ? Number(logid) : Number(logid)) : null;
      if (logId == null || Number.isNaN(logId)) {
        return NextResponse.json({ ok: false, error: 'Failed to create log row' }, { status: 500 });
      }
      return NextResponse.json({ ok: true, logId });
    }

    if (body?.action === 'complete') {
      const logId = Number((body as CompleteBody).logId);
      const outcome = (body as CompleteBody).outcome;
      if (!Number.isFinite(logId) || logId <= 0) {
        return NextResponse.json({ ok: false, error: 'logId required' }, { status: 400 });
      }
      if (!['success', 'failed', 'error', 'client_abort'].includes(outcome)) {
        return NextResponse.json({ ok: false, error: 'invalid outcome' }, { status: 400 });
      }
      const rowcount = (body as CompleteBody).rowcount;
      const error = (body as CompleteBody).error ?? null;
      const httpStatus = (body as CompleteBody).httpStatus ?? null;

      const existingRows = await query<{ logdetails: string | null }>(
        'SELECT logdetails FROM tbl_logs WHERE logid = $1 AND logtype = $2',
        [logId, LOGTYPE]
      );
      const existing = existingRows[0];
      if (!existing) {
        return NextResponse.json({ ok: false, error: 'log row not found' }, { status: 404 });
      }
      const base = parseLogDetails(existing.logdetails);
      const endedAt = new Date().toISOString();
      const statusMap: Record<string, string> = {
        success: 'success',
        failed: 'failed',
        error: 'error',
        client_abort: 'client_abort',
      };
      const merged = {
        ...base,
        status: statusMap[outcome] ?? outcome,
        ended_at: endedAt,
        rowcount: rowcount != null ? rowcount : (base.rowcount as number | null) ?? null,
        http_status: httpStatus != null ? httpStatus : (base.http_status as number | null) ?? null,
        error: error != null ? error : (base.error as string | null) ?? null,
      };
      const n = await execute('UPDATE tbl_logs SET logdetails = $1::text WHERE logid = $2 AND logtype = $3', [
        JSON.stringify(merged),
        logId,
        LOGTYPE,
      ]);
      if (n === 0) {
        return NextResponse.json({ ok: false, error: 'update failed' }, { status: 500 });
      }
      return NextResponse.json({ ok: true, logId });
    }

    return NextResponse.json({ ok: false, error: 'action must be start or complete' }, { status: 400 });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/logs/tracksolid-fetch]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

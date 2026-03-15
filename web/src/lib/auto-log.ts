import { execute } from '@/lib/db';

const LOGTYPE_AUTO = 'auto';

/**
 * Insert a row into tbl_logs for auto endpoint runs.
 * logdatetime is set by DB default. Use logtype='auto', logcat1=endpoint name, logcat2=stage/context, logdetails=JSON or message.
 */
export async function logAuto(
  endpoint: string,
  stage: string,
  details: string | Record<string, unknown>
): Promise<void> {
  const logdetails = typeof details === 'string' ? details : JSON.stringify(details);
  try {
    await execute(
      'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4)',
      [LOGTYPE_AUTO, endpoint, stage, logdetails]
    );
  } catch (err) {
    console.error('[auto-log] Failed to write tbl_logs:', err);
  }
}

/** Return [callDate-1, callDate, callDate+1] as YYYY-MM-DD in UTC. */
export function getAutoThreeDayRangeUTC(): { dateFrom: string; dateTo: string; dates: [string, string, string] } {
  const now = new Date();
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth();
  const d = now.getUTCDate();
  const pad = (n: number) => String(n).padStart(2, '0');
  const callDate = `${y}-${pad(m + 1)}-${pad(d)}`;
  const prev = new Date(Date.UTC(y, m, d - 1));
  const next = new Date(Date.UTC(y, m, d + 1));
  const dateFrom = `${prev.getUTCFullYear()}-${pad(prev.getUTCMonth() + 1)}-${pad(prev.getUTCDate())}`;
  const dateTo = `${next.getUTCFullYear()}-${pad(next.getUTCMonth() + 1)}-${pad(next.getUTCDate())}`;
  const dates: [string, string, string] = [dateFrom, callDate, dateTo];
  return { dateFrom, dateTo, dates };
}

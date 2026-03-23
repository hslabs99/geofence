import { NextResponse } from 'next/server';
import { getClient } from '@/lib/db';

/**
 * VERBATIM TIMES — This route builds delete bounds from the REQUEST date (YYYY-MM-DD) only.
 * FORBIDDEN: Do NOT use this function or any Date formatting for API-sourced timestamps (gpsTime).
 * For API data, use positionTimeForStorage / positionTimeBoundForQuery from @/lib/verbatim-time only.
 */
function formatDateForDebug(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

const DELETE_APIFEED_SQL =
  'DELETE FROM tbl_apifeed WHERE device_name = $1 AND position_time >= $2 AND position_time <= $3';
const DELETE_TRACKING_SQL =
  'DELETE FROM tbl_tracking WHERE device_name = $1 AND position_time >= $2 AND position_time <= $3';

/**
 * POST: Clear both tbl_apifeed and tbl_tracking for a device and UTC day (ground zero for debugging).
 * Body: { deviceName: string, date: string } with date = YYYY-MM-DD.
 * Deletes where device_name = deviceName and position_time in [date 00:00:00 UTC, date 23:59:59 UTC].
 * Uses SET LOCAL timezone = 'UTC' so bounds are interpreted as UTC (avoids session TZ making 00:00 into 05:00 etc).
 * Returns exact SQL and params in response for debug.
 */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as { deviceName?: string; date?: string };
    const deviceName = body?.deviceName?.trim();
    const dateStr = body?.date?.trim();
    if (!deviceName || !dateStr) {
      return NextResponse.json(
        { ok: false, error: 'deviceName and date (YYYY-MM-DD) required' },
        { status: 400 }
      );
    }
    const parts = dateStr.split(/[-/]/);
    const y = parseInt(parts[0], 10);
    const m = parseInt(parts[1], 10) - 1;
    const d = parseInt(parts[2], 10);
    if (Number.isNaN(y) || Number.isNaN(m) || Number.isNaN(d)) {
      return NextResponse.json(
        { ok: false, error: 'Invalid date. Use YYYY-MM-DD.' },
        { status: 400 }
      );
    }
    const dayStart = new Date(Date.UTC(y, m, d, 0, 0, 0));
    const dayEnd = new Date(Date.UTC(y, m, d, 23, 59, 59));
    const dayStartStr = formatDateForDebug(dayStart);
    const dayEndStr = formatDateForDebug(dayEnd);
    const params = [deviceName, dayStartStr, dayEndStr];

    const client = await getClient();
    try {
      await client.query('BEGIN');
      await client.query("SET LOCAL timezone = 'UTC'");
      const resApifeed = await client.query(DELETE_APIFEED_SQL, params);
      const resTracking = await client.query(DELETE_TRACKING_SQL, params);
      await client.query('COMMIT');
      const deletedApifeed = resApifeed.rowCount ?? 0;
      const deletedTracking = resTracking.rowCount ?? 0;

      return NextResponse.json({
        ok: true,
        deletedApifeed,
        deletedTracking,
        debug: {
          deleteSqlApifeed: DELETE_APIFEED_SQL,
          deleteParamsApifeed: [deviceName, dayStartStr, dayEndStr],
          deleteSqlTracking: DELETE_TRACKING_SQL,
          deleteParamsTracking: [deviceName, dayStartStr, dayEndStr],
        },
      });
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

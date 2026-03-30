import { NextResponse } from 'next/server';
import { PoolClient } from 'pg';
import { getAccessToken, listDevices, getTrackForDevice, getDayRangeUTC } from '@/lib/tracksolid';
import { getClient } from '@/lib/db';

const HK_ENDPOINT = 'hk';

const DATE_API_CONCURRENCY = 6;

/** Take first, middle, and last point that day for the 3 checks. */
function firstMiddleLast<T>(arr: T[]): [T, T, T] {
  if (arr.length === 0) throw new Error('empty');
  const first = arr[0];
  const last = arr[arr.length - 1];
  const midIdx = Math.floor((arr.length - 1) / 2);
  const middle = arr[midIdx];
  return [first, middle, last];
}

/** Expand date range to array of YYYY-MM-DD. */
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

async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let nextIndex = 0;
  async function worker() {
    while (true) {
      const i = nextIndex++;
      if (i >= items.length) break;
      results[i] = await fn(items[i], i);
    }
  }
  const n = Math.max(1, Math.min(limit, items.length || 1));
  await Promise.all(Array.from({ length: n }, () => worker()));
  return results;
}

/**
 * Count, min/max position_time, and whether exact first/last API times exist in tbl_tracking.
 * Must be called with client that has SET LOCAL timezone = 'UTC'.
 */
async function getTrackingStatsAndPresence(
  client: PoolClient,
  deviceName: string,
  beginStr: string,
  endStr: string,
  firstTime: string,
  lastTime: string
): Promise<{
  count: number;
  minTime: string | null;
  maxTime: string | null;
  firstOk: boolean;
  lastOk: boolean;
}> {
  const res = await client.query(
    `SELECT
       COUNT(*)::bigint AS cnt,
       MIN(position_time)::text AS min_t,
       MAX(position_time)::text AS max_t,
       EXISTS (
         SELECT 1 FROM tbl_tracking t
         WHERE t.device_name = $1 AND t.position_time = $4::timestamp
         LIMIT 1
       ) AS has_first,
       EXISTS (
         SELECT 1 FROM tbl_tracking t
         WHERE t.device_name = $1 AND t.position_time = $5::timestamp
         LIMIT 1
       ) AS has_last
     FROM tbl_tracking
     WHERE device_name = $1 AND position_time >= $2::timestamp AND position_time <= $3::timestamp`,
    [deviceName.trim(), beginStr.trim(), endStr.trim(), firstTime.trim(), lastTime.trim()]
  );
  const row = res.rows[0] as
    | { cnt: string; min_t: string | null; max_t: string | null; has_first: boolean; has_last: boolean }
    | undefined;
  if (!row) {
    return { count: 0, minTime: null, maxTime: null, firstOk: false, lastOk: false };
  }
  return {
    count: parseInt(row.cnt, 10),
    minTime: row.min_t?.trim() || null,
    maxTime: row.max_t?.trim() || null,
    firstOk: row.has_first === true,
    lastOk: row.has_last === true,
  };
}

/**
 * Check if tbl_tracking has a row with exact device_name and position_time.
 * Must be called with same client that has SET LOCAL timezone = 'UTC'.
 */
async function hasPositionInTracking(
  client: PoolClient,
  deviceName: string,
  positionTimeStr: string
): Promise<boolean> {
  const res = await client.query(
    `SELECT 1 AS found FROM tbl_tracking
     WHERE device_name = $1 AND position_time = $2::timestamp
     LIMIT 1`,
    [deviceName.trim(), positionTimeStr.trim()]
  );
  return (res.rows[0] as { found?: number } | undefined)?.found === 1;
}

export type GpsIntegrityLogEntry = {
  date: string;
  device: string;
  outcomes: ('true' | 'not found')[];
  sampleTimes: string[];
};

/** Cell for grid: one per (date, device). Three checks: 1st time, last time, row count (API vs tbl_tracking). */
export type GpsIntegrityCell =
  | { status: 'no_data' }
  | {
      status: 'data';
      firstOk: boolean;
      lastOk: boolean;
      countOk: boolean;
      firstTime: string;
      lastTime: string;
      apiCount: number;
      dbCount: number;
      dbFirstTime: string | null;
      dbLastTime: string | null;
    };

/** Rows = devices, columns = dates (cells[i] aligns with dates[i]). */
export type GpsIntegrityGridResponse = {
  dates: string[];
  devices: string[];
  rows: Array<{ device: string; cells: GpsIntegrityCell[] }>;
};

async function buildCellForDeviceDate(
  client: PoolClient,
  token: string,
  imeiByDevice: Record<string, string>,
  deviceName: string,
  date: string
): Promise<GpsIntegrityCell> {
  const imei = imeiByDevice[deviceName];
  if (!imei) return { status: 'no_data' };

  const { begin, end } = getDayRangeUTC(date);
  const trackResult = await getTrackForDevice(token, imei, begin, end, HK_ENDPOINT);
  const points = trackResult.points ?? [];
  if (points.length < 1) return { status: 'no_data' };

  const firstTime = String(points[0].gpsTime ?? '').trim();
  const lastTime = String(points[points.length - 1].gpsTime ?? '').trim();
  if (!firstTime || !lastTime) return { status: 'no_data' };

  const stats = await getTrackingStatsAndPresence(client, deviceName, begin, end, firstTime, lastTime);
  const apiCount = points.length;
  const dbCount = stats.count;
  return {
    status: 'data',
    firstOk: stats.firstOk,
    lastOk: stats.lastOk,
    countOk: apiCount === dbCount,
    firstTime,
    lastTime,
    apiCount,
    dbCount,
    dbFirstTime: stats.minTime,
    dbLastTime: stats.maxTime,
  };
}

/** GET: Return list of device names from HK endpoint (for manual device selection). */
export async function GET() {
  try {
    const tokenResult = await getAccessToken(HK_ENDPOINT, { skipCache: false });
    const listResult = await listDevices(tokenResult.token, HK_ENDPOINT);
    const devices = listResult.devices ?? [];
    const names: string[] = [];
    for (const d of devices) {
      const name = (d.deviceName ?? '').trim();
      const imei = (d.imei ?? '').trim();
      if (name && imei) names.push(name);
    }
    names.sort();
    return NextResponse.json({ devices: names });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[gps-integrity GET]', message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/**
 * POST: Two modes.
 * - grid: true → rows = devices, columns = dates; stream: true → NDJSON lines (meta, deviceRow × N, done).
 * - else → legacy log.
 */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as {
      dateFrom?: string;
      dateTo?: string;
      deviceName?: string;
      grid?: boolean;
      stream?: boolean;
    };
    const dateFrom = (body?.dateFrom ?? '').trim();
    const dateTo = (body?.dateTo ?? '').trim();
    const deviceFilter = (body?.deviceName ?? '').trim();
    const isGrid = body?.grid === true;
    const wantStream = body?.stream === true;

    if (!dateFrom || !dateTo) {
      return NextResponse.json(
        { error: 'dateFrom and dateTo (YYYY-MM-DD) required' },
        { status: 400 }
      );
    }

    const dates = dateRange(dateFrom, dateTo);
    if (dates.length === 0) {
      return NextResponse.json(
        { error: 'dateFrom must be <= dateTo' },
        { status: 400 }
      );
    }

    const tokenResult = await getAccessToken(HK_ENDPOINT, { skipCache: false });
    const listResult = await listDevices(tokenResult.token, HK_ENDPOINT);
    const devices = listResult.devices ?? [];
    const imeiByDevice: Record<string, string> = {};
    for (const d of devices) {
      const name = (d.deviceName ?? '').trim();
      const imei = (d.imei ?? '').trim();
      if (name && imei) imeiByDevice[name] = imei;
    }
    const deviceNames = Object.keys(imeiByDevice).sort();
    const deviceNamesFiltered = deviceFilter && imeiByDevice[deviceFilter] ? [deviceFilter] : deviceNames;

    if (deviceFilter && !isGrid && deviceNamesFiltered.length === 0) {
      return NextResponse.json(
        { error: `Device "${deviceFilter}" not found in HK endpoint device list` },
        { status: 400 }
      );
    }

    if (isGrid && wantStream) {
      const encoder = new TextEncoder();
      const token = tokenResult.token;
      const stream = new ReadableStream({
        async start(controller) {
          const send = (obj: unknown) => {
            controller.enqueue(encoder.encode(`${JSON.stringify(obj)}\n`));
          };
          const client = await getClient();
          try {
            await client.query("SET LOCAL timezone = 'UTC'");
            send({ type: 'meta', dates, devices: deviceNames });
            for (const deviceName of deviceNames) {
              const cells = await mapWithConcurrency(dates, DATE_API_CONCURRENCY, (date) =>
                buildCellForDeviceDate(client, token, imeiByDevice, deviceName, date)
              );
              send({ type: 'deviceRow', device: deviceName, cells });
            }
            send({ type: 'done' });
          } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            console.error('[gps-integrity stream]', message);
            send({ type: 'error', message });
          } finally {
            client.release();
            controller.close();
          }
        },
      });
      return new Response(stream, {
        headers: {
          'Content-Type': 'application/x-ndjson; charset=utf-8',
          'Cache-Control': 'no-store',
        },
      });
    }

    const client = await getClient();

    try {
      await client.query("SET LOCAL timezone = 'UTC'");

      if (isGrid) {
        const rows: Array<{ device: string; cells: GpsIntegrityCell[] }> = [];
        const token = tokenResult.token;
        for (const deviceName of deviceNames) {
          const cells = await mapWithConcurrency(dates, DATE_API_CONCURRENCY, (date) =>
            buildCellForDeviceDate(client, token, imeiByDevice, deviceName, date)
          );
          rows.push({ device: deviceName, cells });
        }
        return NextResponse.json({
          dates,
          devices: deviceNames,
          rows,
        } as GpsIntegrityGridResponse);
      }

      const log: GpsIntegrityLogEntry[] = [];
      for (const date of dates) {
        const { begin, end } = getDayRangeUTC(date);
        let foundDeviceWithData = false;

        for (const deviceName of deviceNamesFiltered) {
          const imei = imeiByDevice[deviceName];
          if (!imei) continue;

          const trackResult = await getTrackForDevice(
            tokenResult.token,
            imei,
            begin,
            end,
            HK_ENDPOINT
          );
          const points = trackResult.points ?? [];
          if (points.length < 1) {
            if (deviceFilter) {
              log.push({
                date,
                device: deviceName,
                outcomes: [],
                sampleTimes: [],
              });
              foundDeviceWithData = true;
            }
            continue;
          }

          foundDeviceWithData = true;
          const [firstPt, middlePt, lastPt] = firstMiddleLast(points);
          const sampleTimes = [firstPt, middlePt, lastPt].map((p) => String(p.gpsTime ?? '').trim());
          const outcomes: ('true' | 'not found')[] = [];

          for (const gpsTime of sampleTimes) {
            if (!gpsTime) {
              outcomes.push('not found');
              continue;
            }
            const found = await hasPositionInTracking(client, deviceName, gpsTime);
            outcomes.push(found ? 'true' : 'not found');
          }

          log.push({
            date,
            device: deviceName,
            outcomes,
            sampleTimes,
          });
          break;
        }

        if (!foundDeviceWithData) {
          log.push({
            date,
            device: deviceFilter ? `"${deviceFilter}" (no data)` : '(no device with data)',
            outcomes: [],
            sampleTimes: [],
          });
        }
      }

      return NextResponse.json({ log });
    } finally {
      client.release();
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[gps-integrity]', message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

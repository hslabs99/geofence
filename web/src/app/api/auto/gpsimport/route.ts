import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { fillPositionTimeNzNulls } from '@/lib/fill-position-time-nz';
import { getAccessToken, listDevices, getTrackForDevice, getDayRangeUTC, getBaseUrl } from '@/lib/tracksolid';
import { logAuto, getAutoThreeDayRangeUTC } from '@/lib/auto-log';

/**
 * VERBATIM TIMES — NEVER ALTER API DATA.
 * We fetch points (gpsTime from TrackSolid) and pass them to apifeed/import and merge-tracking as-is.
 * We do NOT modify gpsTime. What the API returns must arrive in tbl_tracking.position_time 100% literally.
 * If we ever get timezone info from the API, ignore it; treat all dates/times as verbatim.
 */
const ENDPOINT = 'gpsimport';

/** App base URL for internal fetch calls. */
function getAppBaseUrl(): string {
  const u = process.env.APP_URL ?? process.env.VERCEL_URL;
  if (u) return u.startsWith('http') ? u : `https://${u}`;
  return 'http://localhost:3000';
}

const TRACKSOLID_KEYS = ['global', 'hk', 'eu', 'us'] as const;

/** Resolve TrackSolid endpoint: request (query/body) > TRACKSOLID_ENDPOINT env > default "hk" (same as device list / track APIs). */
async function resolveTracksolidEndpoint(request: Request): Promise<string> {
  let key: string | null = null;
  try {
    const url = new URL(request.url);
    key = url.searchParams.get('endpoint')?.trim().toLowerCase() ?? null;
    if (!key && request.method === 'POST') {
      const body = await request.clone().json().catch(() => ({}));
      key = (body?.endpoint != null ? String(body.endpoint).trim().toLowerCase() : null) || null;
    }
  } catch {
    // ignore
  }
  if (key && TRACKSOLID_KEYS.includes(key as (typeof TRACKSOLID_KEYS)[number])) return key;
  const envKey = process.env.TRACKSOLID_ENDPOINT?.trim().toLowerCase();
  if (envKey && TRACKSOLID_KEYS.includes(envKey as (typeof TRACKSOLID_KEYS)[number])) return envKey;
  return 'hk';
}

export async function GET(request: Request) {
  const base = getAppBaseUrl();
  const tsEndpoint = await resolveTracksolidEndpoint(request);
  try {
    const { dateFrom, dateTo, dates } = getAutoThreeDayRangeUTC();
    await logAuto(ENDPOINT, 'start', {
      dateFrom,
      dateTo,
      dates,
      tracksolidEndpoint: tsEndpoint,
      tracksolidBaseUrl: getBaseUrl(tsEndpoint),
    });

    const tokenResult = await getAccessToken(tsEndpoint, { skipCache: false });
    await logAuto(ENDPOINT, 'token', { ok: true });

    const listResult = await listDevices(tokenResult.token, tsEndpoint);
    const devices = listResult.devices ?? [];
    const imeiByDevice: Record<string, string> = {};
    for (const d of devices) {
      const name = (d.deviceName ?? '').trim();
      const imei = (d.imei ?? '').trim();
      if (name && imei) imeiByDevice[name] = imei;
    }
    await logAuto(ENDPOINT, 'devices', { total: devices.length, matchedNames: Object.keys(imeiByDevice).length });

    const workerRows = await query<{ worker: string | null }>(
      `SELECT DISTINCT trim(w.worker) AS worker
       FROM tbl_vworkjobs w
       WHERE w.worker IS NOT NULL AND trim(w.worker) <> ''
       ORDER BY 1`
    );
    const workers = workerRows.map((r) => r.worker?.trim() ?? '').filter(Boolean);
    await logAuto(ENDPOINT, 'workers', { count: workers.length, workers });

    const devicesToRun = workers.filter((w) => imeiByDevice[w]);
    const skipped = workers.filter((w) => !imeiByDevice[w]);
    if (skipped.length) {
      await logAuto(ENDPOINT, 'skipped_no_imei', { devices: skipped });
    }

    const dayLog: { date: string; device: string; apiRows: number; inserted?: number }[] = [];
    let totalInserted = 0;

    for (const date of dates) {
      const { begin, end } = getDayRangeUTC(date);
      for (let i = 0; i < devicesToRun.length; i++) {
        const deviceName = devicesToRun[i];
        const imei = imeiByDevice[deviceName];
        if (!imei) continue;
        if (i > 0) {
          await new Promise((r) => setTimeout(r, 300));
        }

        try {
          const clearRes = await fetch(`${base}/api/apifeed/clear-for-device-date`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ deviceName, date }),
          });
          const clearData = await clearRes.json();
          if (!clearData.ok) {
            await logAuto(ENDPOINT, 'clear_error', { date, deviceName, error: clearData.error });
            continue;
          }

          const trackResult = await getTrackForDevice(tokenResult.token, imei, begin, end, tsEndpoint);
          const points = trackResult.points ?? [];

          if (points.length === 0) continue;

          const saveRes = await fetch(`${base}/api/apifeed/import`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              deviceName,
              imei,
              points: points.map((p) => ({ lat: p.lat, lng: p.lng, gpsTime: p.gpsTime })),
            }),
          });
          const saveData = await saveRes.json();
          if (!saveData.ok) {
            await logAuto(ENDPOINT, 'save_error', { date, deviceName, error: saveData.error });
            continue;
          }

          const times = points.map((p) => p.gpsTime).filter((t) => t != null && String(t).trim() !== '') as string[];
          const sorted = times.length > 0 ? [...times].map((t) => String(t).trim()).sort() : [];
          const minTime = sorted[0];
          const maxTime = sorted.length > 0 ? sorted[sorted.length - 1] : null;
          if (minTime == null || maxTime == null) {
            await logAuto(ENDPOINT, 'merge_skip', { date, deviceName, reason: 'no valid gpsTime in payload' });
            continue;
          }
          const mergeRes = await fetch(`${base}/api/apifeed/merge-tracking`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ deviceName, minTime, maxTime }),
          });
          const mergeData = await mergeRes.json();
          if (!mergeRes.ok || !mergeData?.ok) {
            await logAuto(ENDPOINT, 'merge_error', { date, deviceName, error: mergeData?.error ?? mergeRes.statusText });
            continue;
          }
          const inserted = mergeData.inserted ?? 0;
          totalInserted += inserted;
          dayLog.push({ date, device: deviceName, apiRows: points.length, inserted });
          await logAuto(ENDPOINT, 'device_day', { date, deviceName, apiRows: points.length, inserted });
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          await logAuto(ENDPOINT, 'device_error', { date, deviceName, error: msg });
        }
      }
    }

    let positionTimeNzBackfillUpdated = 0;
    let positionTimeNzBackfillError: string | null = null;
    try {
      positionTimeNzBackfillUpdated = await fillPositionTimeNzNulls();
    } catch (e) {
      positionTimeNzBackfillError = e instanceof Error ? e.message : String(e);
      await logAuto(ENDPOINT, 'position_time_nz_backfill_error', { error: positionTimeNzBackfillError });
    }
    await logAuto(ENDPOINT, 'done', {
      totalInserted,
      dayLog,
      positionTimeNzBackfillUpdated,
      positionTimeNzBackfillError,
    });
    return NextResponse.json({
      ok: true,
      dateFrom,
      dateTo,
      totalInserted,
      dayLog,
      positionTimeNzBackfillUpdated,
      positionTimeNzBackfillError,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await logAuto(ENDPOINT, 'error', { message });
    console.error('[auto/gpsimport]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

export async function POST(request: Request) {
  return GET(request);
}

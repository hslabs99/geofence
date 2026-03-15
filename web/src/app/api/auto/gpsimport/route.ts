import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { getAccessToken, listDevices, getTrackForDevice, getDayRangeUTC } from '@/lib/tracksolid';
import { logAuto, getAutoThreeDayRangeUTC } from '@/lib/auto-log';

const ENDPOINT = 'gpsimport';

function getBaseUrl(): string {
  const u = process.env.APP_URL ?? process.env.VERCEL_URL;
  if (u) return u.startsWith('http') ? u : `https://${u}`;
  return 'http://localhost:3000';
}

export async function GET() {
  const base = getBaseUrl();
  try {
    const { dateFrom, dateTo, dates } = getAutoThreeDayRangeUTC();
    await logAuto(ENDPOINT, 'start', { dateFrom, dateTo, dates });

    const tokenResult = await getAccessToken(undefined, { skipCache: false });
    await logAuto(ENDPOINT, 'token', { ok: true });

    const listResult = await listDevices(tokenResult.token, undefined);
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
      for (const deviceName of devicesToRun) {
        const imei = imeiByDevice[deviceName];
        if (!imei) continue;

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

          const trackResult = await getTrackForDevice(tokenResult.token, imei, begin, end, undefined);
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

          const times = points.map((p) => p.gpsTime).filter(Boolean) as string[];
          const sorted = [...times].sort();
          const minTime = sorted[0];
          const maxTime = sorted[sorted.length - 1];
          const mergeRes = await fetch(`${base}/api/apifeed/merge-tracking`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ deviceName, minTime, maxTime }),
          });
          const mergeData = await mergeRes.json();
          const inserted = mergeData?.inserted ?? 0;
          totalInserted += inserted;
          dayLog.push({ date, device: deviceName, apiRows: points.length, inserted });
          await logAuto(ENDPOINT, 'device_day', { date, deviceName, apiRows: points.length, inserted });
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          await logAuto(ENDPOINT, 'device_error', { date, deviceName, error: msg });
        }
      }
    }

    await logAuto(ENDPOINT, 'done', { totalInserted, dayLog });
    return NextResponse.json({ ok: true, dateFrom, dateTo, totalInserted, dayLog });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await logAuto(ENDPOINT, 'error', { message });
    console.error('[auto/gpsimport]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

export async function POST() {
  return GET();
}

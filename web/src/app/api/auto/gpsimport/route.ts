import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { fillPositionTimeNzNulls } from '@/lib/fill-position-time-nz';
import { getAccessToken, listDevices, getTrackForDevice, getDayRangeUTC, getBaseUrl } from '@/lib/tracksolid';
import { logAuto, getAutoThreeDayRangeUTC } from '@/lib/auto-log';
import { mergeTracksolidDevicesIntoTblDevices, mergeVworkjobsIntoTblDevices } from '@/lib/tbl-devices-sync';
import { upsertTracksolidPointsToTracking } from '@/lib/upsert-tracking-direct';

/**
 * VERBATIM TIMES — NEVER ALTER API DATA.
 * We fetch points (gpsTime from TrackSolid) and upsert directly to tbl_tracking as-is.
 * We do NOT modify gpsTime. What the API returns must arrive in tbl_tracking.position_time 100% literally.
 * If we ever get timezone info from the API, ignore it; treat all dates/times as verbatim.
 */
const ENDPOINT = 'gpsimport';

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

    const tsMerge = await mergeTracksolidDevicesIntoTblDevices(devices);
    const vjMerge = await mergeVworkjobsIntoTblDevices();
    await logAuto(ENDPOINT, 'tbl_devices_merge', {
      tracksolidInserted: tsMerge.inserted,
      tracksolidImeiUpdates: tsMerge.imeiUpdates,
      vworkjobsInserted: vjMerge.inserted,
      totalDevicesInTable: vjMerge.totalDevices,
    });

    const deviceRows = await query<{ device_name: string | null; imei: string | null }>(
      `SELECT trim(device_name) AS device_name, nullif(trim(imei), '') AS imei
       FROM tbl_devices
       ORDER BY 1`
    );

    const resolved: { deviceName: string; imei: string }[] = [];
    const skippedNoImei: string[] = [];
    for (const r of deviceRows) {
      const deviceName = (r.device_name ?? '').trim();
      if (!deviceName) continue;
      const imei = (r.imei ?? '').trim() || imeiByDevice[deviceName] || '';
      if (!imei) {
        skippedNoImei.push(deviceName);
        continue;
      }
      resolved.push({ deviceName, imei });
    }
    await logAuto(ENDPOINT, 'poll_list', {
      tblDevicesRows: deviceRows.length,
      withImei: resolved.length,
      skippedNoImei: skippedNoImei.length,
      skippedNamesSample: skippedNoImei.slice(0, 80),
    });
    if (skippedNoImei.length) {
      await logAuto(ENDPOINT, 'skipped_no_imei', { devices: skippedNoImei });
    }

    const dayLog: { date: string; device: string; apiRows: number; inserted?: number }[] = [];
    let totalInserted = 0;

    for (const date of dates) {
      const { begin, end } = getDayRangeUTC(date);
      for (let i = 0; i < resolved.length; i++) {
        const { deviceName, imei } = resolved[i];
        if (i > 0) {
          await new Promise((r) => setTimeout(r, 300));
        }

        try {
          const trackResult = await getTrackForDevice(tokenResult.token, imei, begin, end, tsEndpoint);
          const points = trackResult.points ?? [];

          if (points.length === 0) continue;

          const upsertResult = await upsertTracksolidPointsToTracking(
            deviceName,
            imei,
            points.map((p) => ({ lat: p.lat, lng: p.lng, gpsTime: p.gpsTime }))
          );
          const inserted = upsertResult.inserted ?? 0;
          totalInserted += inserted;
          dayLog.push({ date, device: deviceName, apiRows: points.length, inserted });
          await logAuto(ENDPOINT, 'device_day', {
            date,
            deviceName,
            apiRows: points.length,
            inserted,
            skippedInvalid: upsertResult.skipped,
          });
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

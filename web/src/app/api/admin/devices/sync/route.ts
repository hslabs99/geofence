import { NextResponse } from 'next/server';
import { getAccessToken, listDevices, getBaseUrl } from '@/lib/tracksolid';
import { mergeTracksolidDevicesIntoTblDevices, mergeVworkjobsIntoTblDevices } from '@/lib/tbl-devices-sync';

const TRACKSOLID_KEYS = ['global', 'hk', 'eu', 'us'] as const;

function resolveEndpoint(body: { endpoint?: string } | null, request: Request): string {
  let key: string | null = null;
  try {
    const url = new URL(request.url);
    key = url.searchParams.get('endpoint')?.trim().toLowerCase() ?? null;
    if (!key && body?.endpoint != null) {
      key = String(body.endpoint).trim().toLowerCase() || null;
    }
  } catch {
    // ignore
  }
  if (key && TRACKSOLID_KEYS.includes(key as (typeof TRACKSOLID_KEYS)[number])) return key;
  const envKey = process.env.TRACKSOLID_ENDPOINT?.trim().toLowerCase();
  if (envKey && TRACKSOLID_KEYS.includes(envKey as (typeof TRACKSOLID_KEYS)[number])) return envKey;
  return 'hk';
}

/**
 * Full device harvest: merge TrackSolid account devices (device_name + imei) and distinct vworkjobs workers into tbl_devices.
 */
export async function POST(request: Request) {
  try {
    const body = (await request.json().catch(() => ({}))) as { endpoint?: string };
    const endpoint = resolveEndpoint(body, request);
    const tokenResult = await getAccessToken(endpoint, { skipCache: false });
    const listResult = await listDevices(tokenResult.token, endpoint);
    const devices = listResult.devices ?? [];

    const tracksolid = await mergeTracksolidDevicesIntoTblDevices(devices);
    const vworkjobs = await mergeVworkjobsIntoTblDevices();

    return NextResponse.json({
      ok: true,
      tracksolidEndpoint: endpoint,
      tracksolidBaseUrl: getBaseUrl(endpoint),
      tracksolidDeviceCount: devices.length,
      tracksolidInserted: tracksolid.inserted,
      tracksolidImeiUpdates: tracksolid.imeiUpdates,
      vworkjobsInserted: vworkjobs.inserted,
      totalDevices: vworkjobs.totalDevices,
      totalWorkersDistinct: vworkjobs.totalWorkers,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/devices/sync]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { ensureTblDevicesSchema } from '@/lib/ensure-tbl-devices-schema';

/**
 * List devices from tbl_devices (union of TrackSolid + vworkjobs harvest). Includes imei when known.
 */
export async function GET() {
  try {
    await ensureTblDevicesSchema();
    const rows = await query<{ device_name: string | null; imei: string | null }>(
      `SELECT trim(device_name) AS device_name, nullif(trim(imei), '') AS imei
       FROM tbl_devices
       ORDER BY 1`
    );
    const devices = rows
      .map((r) => ({
        deviceName: (r.device_name ?? '').trim(),
        imei: r.imei?.trim() || null,
      }))
      .filter((d) => d.deviceName);
    return NextResponse.json({ ok: true, devices });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/devices GET]', message);
    return NextResponse.json(
      { ok: false, error: message },
      { status: 500 }
    );
  }
}

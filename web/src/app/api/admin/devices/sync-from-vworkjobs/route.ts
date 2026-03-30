import { NextResponse } from 'next/server';
import { mergeVworkjobsIntoTblDevices } from '@/lib/tbl-devices-sync';

/**
 * Sync devices from tbl_vworkjobs.worker into tbl_devices:
 * - SELECT DISTINCT worker FROM tbl_vworkjobs
 * - Insert any worker not already in tbl_devices (device_name), with "!group" = 'Newt' if column exists
 */
export async function POST() {
  try {
    const { inserted, totalDevices, totalWorkers } = await mergeVworkjobsIntoTblDevices();

    return NextResponse.json({
      ok: true,
      inserted: Number(inserted),
      totalDevices,
      totalWorkers,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/devices/sync-from-vworkjobs]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

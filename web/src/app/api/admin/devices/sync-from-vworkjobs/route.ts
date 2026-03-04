import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

/**
 * Sync devices from tbl_vworkjobs.worker into tbl_devices:
 * - SELECT DISTINCT worker FROM tbl_vworkjobs
 * - Insert any worker not already in tbl_devices (device_name), with "!group" = 'Newt' if column exists
 */
export async function POST() {
  try {
    const withGroup = `
      INSERT INTO tbl_devices (device_name, "!group")
      SELECT DISTINCT trim(w.worker), 'Newt'
      FROM tbl_vworkjobs w
      WHERE w.worker IS NOT NULL AND trim(w.worker) <> ''
      AND NOT EXISTS (SELECT 1 FROM tbl_devices d WHERE d.device_name = trim(w.worker))
    `;
    const withoutGroup = `
      INSERT INTO tbl_devices (device_name)
      SELECT DISTINCT trim(w.worker)
      FROM tbl_vworkjobs w
      WHERE w.worker IS NOT NULL AND trim(w.worker) <> ''
      AND NOT EXISTS (SELECT 1 FROM tbl_devices d WHERE d.device_name = trim(w.worker))
    `;
    let inserted = 0;
    try {
      inserted = await prisma.$executeRawUnsafe(withGroup);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes('"!group"') || msg.includes('!group') || msg.includes('column')) {
        inserted = await prisma.$executeRawUnsafe(withoutGroup);
      } else {
        throw e;
      }
    }

    const countResult = await prisma.$queryRaw<{ count: bigint }[]>`SELECT COUNT(*) AS count FROM tbl_devices`;
    const totalDevices = Number(countResult[0]?.count ?? 0);
    const workersResult = await prisma.$queryRaw<{ count: bigint }[]>`
      SELECT COUNT(DISTINCT trim(worker)) AS count FROM tbl_vworkjobs WHERE worker IS NOT NULL AND trim(worker) <> ''
    `;
    const totalWorkers = Number(workersResult[0]?.count ?? 0);

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

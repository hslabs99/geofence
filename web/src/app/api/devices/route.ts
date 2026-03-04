import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

/**
 * List devices from tbl_devices (subset we want to track).
 * Table has device_name only; IMEI is resolved from Tracksolid device list (Step 2) when calling the API.
 */
export async function GET() {
  try {
    const rows = await prisma.$queryRaw<
      Array<{ device_name: string | null }>
    >`
      SELECT device_name FROM tbl_devices ORDER BY device_name
    `;
    const devices = rows
      .map((r) => ({ deviceName: r.device_name?.trim() ?? '' }))
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

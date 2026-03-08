import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/**
 * List devices (workers) from tbl_vworkjobs: SELECT DISTINCT worker.
 * Use this for any device list or table with checkboxes (e.g. tagging). No CRUD on tbl_devices.
 */
export async function GET() {
  try {
    const rows = await query<{ worker: string | null }>(
      `SELECT DISTINCT trim(w.worker) AS worker
       FROM tbl_vworkjobs w
       WHERE w.worker IS NOT NULL AND trim(w.worker) <> ''
       ORDER BY 1`
    );
    const devices = rows
      .map((r) => ({ deviceName: r.worker?.trim() ?? '' }))
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

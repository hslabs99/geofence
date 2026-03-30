import { NextResponse } from 'next/server';
import { upsertTracksolidPointsToTracking } from '@/lib/upsert-tracking-direct';
import { execute } from '@/lib/db';

const LOGTYPE = 'import_direct';

async function logImportDirect(
  deviceName: string,
  date: string | null,
  details: Record<string, unknown>
): Promise<void> {
  try {
    await execute(
      'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4)',
      [LOGTYPE, deviceName, date ?? '', JSON.stringify(details)]
    );
  } catch (logErr) {
    console.error('[api/tracking/import-direct] Failed to write tbl_logs:', logErr);
  }
}

export async function POST(request: Request) {
  let deviceNameForLog = '(missing device)';
  let dateForLog: string | null = null;
  try {
    const body = (await request.json()) as {
      deviceName?: string;
      imei?: string | null;
      date?: string | null;
      points?: Array<{ lat: number; lng: number; gpsTime: string }>;
    };
    const deviceName = body?.deviceName?.trim();
    const imei = body?.imei != null ? String(body.imei).trim() : null;
    const date = body?.date != null ? String(body.date).trim() : null;
    const points = Array.isArray(body?.points) ? body.points : [];
    deviceNameForLog = deviceName || deviceNameForLog;
    dateForLog = date || null;

    if (!deviceName) {
      await logImportDirect(deviceNameForLog, dateForLog, {
        ok: false,
        stage: 'validate',
        error: 'deviceName required',
      });
      return NextResponse.json({ ok: false, error: 'deviceName required' }, { status: 400 });
    }
    if (!Array.isArray(points)) {
      await logImportDirect(deviceNameForLog, dateForLog, {
        ok: false,
        stage: 'validate',
        error: 'points array required',
      });
      return NextResponse.json({ ok: false, error: 'points array required' }, { status: 400 });
    }

    const out = await upsertTracksolidPointsToTracking(deviceName, imei, points);
    return NextResponse.json({
      ok: true,
      inserted: out.inserted,
      total: out.total,
      skipped: out.skipped,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await logImportDirect(deviceNameForLog, dateForLog, {
      ok: false,
      stage: 'upsert',
      error: message,
    });
    console.error('[api/tracking/import-direct]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

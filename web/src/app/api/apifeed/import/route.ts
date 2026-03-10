import { NextResponse } from 'next/server';
import { getClient } from '@/lib/db';

const SOURCE = 'tracksolid';
const BATCH_SIZE = 500;

/** Format Date for debug display (UTC). */
function formatDateForDebug(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

/**
 * Parse gpsTime for insert. Returns the Date or null with a reason for debugging.
 */
function parsePositionTimeWithReason(
  gpsTime: string
): { date: Date } | { reason: string } {
  const raw = gpsTime;
  const s = String(gpsTime ?? '').trim();
  if (!s) return { reason: 'empty or whitespace' };
  const normalized = s.includes('T') ? s : `${s.replace(' ', 'T')}Z`;
  const d = new Date(normalized);
  if (Number.isNaN(d.getTime())) return { reason: `invalid date (normalized: "${normalized}")` };
  return { date: d };
}

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as {
      deviceName?: string;
      imei?: string;
      points?: Array<{ lat: number; lng: number; gpsTime: string }>;
    };
    const deviceName = body?.deviceName?.trim();
    const imei = typeof body?.imei === 'string' ? body.imei.trim() : null;
    const points = Array.isArray(body?.points) ? body.points : [];
    if (!deviceName) {
      return NextResponse.json({ ok: false, error: 'deviceName required' }, { status: 400 });
    }
    if (points.length === 0) {
      return NextResponse.json({ ok: false, error: 'points array required and non-empty' }, { status: 400 });
    }

    const rows: { positionTime: Date; location: string; payloadJson: string }[] = [];
    const skippedRows: { gpsTime: string; lat: unknown; lng: unknown; reason: string }[] = [];
    const imeiVal = imei ?? null;
    for (const p of points) {
      const parsed = parsePositionTimeWithReason(p.gpsTime);
      if ('reason' in parsed) {
        skippedRows.push({
          gpsTime: String(p.gpsTime ?? ''),
          lat: p.lat,
          lng: p.lng,
          reason: parsed.reason,
        });
        continue;
      }
      rows.push({
        positionTime: parsed.date,
        location: `${Number(p.lat) ?? 0},${Number(p.lng) ?? 0}`,
        payloadJson: JSON.stringify({ lat: p.lat, lng: p.lng, gpsTime: p.gpsTime }),
      });
    }

    const DELETE_SQL =
      'DELETE FROM tbl_apifeed WHERE device_name = $1 AND position_time >= $2 AND position_time <= $3';

    if (rows.length === 0) {
      return NextResponse.json({
        ok: true,
        inserted: 0,
        skipped: skippedRows.length,
        total: points.length,
        deleted: 0,
        deleteSql: `${DELETE_SQL} -- N/A (no rows passed parse, so no delete ran)`,
        deleteParams: [deviceName, '—', '—'],
        skippedRows: skippedRows.slice(0, 1500),
      });
    }

    const minTime = new Date(Math.min(...rows.map((r) => r.positionTime.getTime())));
    const maxTime = new Date(Math.max(...rows.map((r) => r.positionTime.getTime())));
    // Delete full UTC day so range is 00:00:00–23:59:59, not just min/max of points
    const dayStart = new Date(Date.UTC(
      minTime.getUTCFullYear(),
      minTime.getUTCMonth(),
      minTime.getUTCDate(),
      0,
      0,
      0
    ));
    const dayEnd = new Date(Date.UTC(
      minTime.getUTCFullYear(),
      minTime.getUTCMonth(),
      minTime.getUTCDate(),
      23,
      59,
      59
    ));

    // Detect duplicate position_time (same device) — if DB has UNIQUE(device_name, position_time), only first per time would persist
    const positionTimeKeys = new Set(rows.map((r) => r.positionTime.getTime()));
    const uniquePositionTimes = positionTimeKeys.size;
    const duplicatePositionTimeCount = rows.length - uniquePositionTimes;

    // Bounds and position_time are UTC strings only; session set to UTC so no TZ shift. Never pass Date to DB.
    const dayStartStr = formatDateForDebug(dayStart);
    const dayEndStr = formatDateForDebug(dayEnd);
    let deleted: number;
    let inserted = 0;
    const client = await getClient();
    try {
      await client.query('BEGIN');
      await client.query("SET LOCAL timezone = 'UTC'");
      const res = await client.query(DELETE_SQL, [deviceName, dayStartStr, dayEndStr]);
      deleted = res.rowCount ?? 0;
      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        const values: unknown[] = [];
        const placeholders: string[] = [];
        let paramIdx = 1;
        for (const r of batch) {
          placeholders.push(`($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2}, $${paramIdx + 3}, $${paramIdx + 4}, $${paramIdx + 5}::jsonb, now())`);
          values.push(deviceName, imeiVal, r.location, formatDateForDebug(r.positionTime), SOURCE, r.payloadJson);
          paramIdx += 6;
        }
        const insRes = await client.query(
          `INSERT INTO tbl_apifeed (device_name, imei, location, position_time, source, payload, importedwhen) VALUES ${placeholders.join(', ')}`,
          values
        );
        inserted += insRes.rowCount ?? 0;
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }

    return NextResponse.json({
      ok: true,
      inserted,
      skipped: skippedRows.length,
      total: points.length,
      deleted,
      uniquePositionTimes,
      duplicatePositionTimeInPayload: duplicatePositionTimeCount,
      deleteSql: DELETE_SQL,
      deleteParams: [deviceName, formatDateForDebug(dayStart), formatDateForDebug(dayEnd)],
      skippedRows: skippedRows.slice(0, 1500),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.error('[api/apifeed/import POST] Error:', message, err instanceof Error ? err.stack : '');
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

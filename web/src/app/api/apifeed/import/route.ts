import { NextResponse } from 'next/server';
import { getClient } from '@/lib/db';
import { positionTimeForStorage, positionTimeBoundForQuery } from '@/lib/verbatim-time';

/**
 * VERBATIM TIMES — NEVER ALTER API DATA FOR TIMEZONES.
 * What we get from the API (gpsTime) must arrive in position_time 100% as received.
 * All position_time values MUST go through positionTimeForStorage( rawString ) — never pass a Date.
 * FORBIDDEN: formatDateForDebug, toISOString, getUTC*, appending Z, any timezone conversion.
 * See web/docs/VERBATIM_TIMES.md
 */
const SOURCE = 'tracksolid';
const BATCH_SIZE = 500;

/**
 * Validate gpsTime is a parseable timestamp. Used only to skip invalid rows.
 * We never use the parsed Date for storage — position_time is stored as the raw string.
 */
function parsePositionTimeWithReason(
  gpsTime: string
): { ok: true } | { reason: string } {
  const s = String(gpsTime ?? '').trim();
  if (!s) return { reason: 'empty or whitespace' };
  // Accept space or T; do not append Z — we only check parseability
  const normalized = s.includes('T') ? s : s.replace(' ', 'T');
  const d = new Date(normalized);
  if (Number.isNaN(d.getTime())) return { reason: `invalid date (normalized: "${normalized}")` };
  return { ok: true };
}

/** Derive day bounds from raw timestamp strings. No timezone conversion. */
function dayBoundsFromRawTimes(rawTimes: string[]): { dayStartStr: string; dayEndStr: string } {
  if (rawTimes.length === 0) return { dayStartStr: '1970-01-01 00:00:00', dayEndStr: '1970-01-01 23:59:59' };
  const sorted = [...rawTimes].sort();
  const minStr = sorted[0];
  const maxStr = sorted[sorted.length - 1];
  const looksLikeDate = (s: string) => /^\d{4}-\d{2}-\d{2}/.test(s);
  if (looksLikeDate(minStr) && looksLikeDate(maxStr)) {
    return {
      dayStartStr: `${minStr.slice(0, 10)} 00:00:00`,
      dayEndStr: `${maxStr.slice(0, 10)} 23:59:59`,
    };
  }
  return { dayStartStr: minStr, dayEndStr: maxStr };
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

    const rows: { positionTimeRaw: string; location: string; payloadJson: string }[] = [];
    const skippedRows: { gpsTime: string; lat: unknown; lng: unknown; reason: string }[] = [];
    const imeiVal = imei ?? null;
    for (const p of points) {
      const raw = positionTimeForStorage(p.gpsTime ?? '');
      if (!raw) {
        skippedRows.push({
          gpsTime: String(p.gpsTime ?? ''),
          lat: p.lat,
          lng: p.lng,
          reason: 'empty or whitespace',
        });
        continue;
      }
      rows.push({
        positionTimeRaw: raw,
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

    const rawTimes = rows.map((r) => r.positionTimeRaw);
    const { dayStartStr, dayEndStr } = dayBoundsFromRawTimes(rawTimes);
    const dayStartForDb = positionTimeBoundForQuery(dayStartStr);
    const dayEndForDb = positionTimeBoundForQuery(dayEndStr);

    // Detect duplicate position_time (same device) — if DB has UNIQUE(device_name, position_time), only first per time would persist
    const positionTimeKeys = new Set(rawTimes);
    const uniquePositionTimes = positionTimeKeys.size;
    const duplicatePositionTimeCount = rows.length - uniquePositionTimes;
    let deleted: number;
    let inserted = 0;
    const client = await getClient();
    try {
      await client.query('BEGIN');
      await client.query("SET LOCAL timezone = 'UTC'");
      const res = await client.query(DELETE_SQL, [deviceName, dayStartForDb, dayEndForDb]);
      deleted = res.rowCount ?? 0;
      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        const values: unknown[] = [];
        const placeholders: string[] = [];
        let paramIdx = 1;
        for (const r of batch) {
          placeholders.push(`($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2}, $${paramIdx + 3}, $${paramIdx + 4}, $${paramIdx + 5}::jsonb, now())`);
          // position_time = raw API string only. NEVER alter for timezone or re-format.
          values.push(deviceName, imeiVal, r.location, r.positionTimeRaw, SOURCE, r.payloadJson);
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
      deleteParams: [deviceName, dayStartForDb, dayEndForDb],
      skippedRows: skippedRows.slice(0, 1500),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.error('[api/apifeed/import POST] Error:', message, err instanceof Error ? err.stack : '');
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

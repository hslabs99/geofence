import { NextResponse } from 'next/server';
import { execute } from '@/lib/db';

const DELETE_SQL = `
  DELETE FROM tbl_tracking
  WHERE device_name = $1
    AND position_time >= $2
    AND position_time <= $3
`;

// Column order matches gpsdata insert (src/db.ts): device_no..address, then created_at, apirow
const INSERT_SQL = `
  INSERT INTO tbl_tracking (
    device_no,
    device_name,
    imei,
    model,
    ignition,
    position_time,
    speed_raw,
    speed_kmh,
    azimuth,
    position_type,
    satellites,
    data_type,
    lat,
    lon,
    geom,
    address,
    created_at,
    apirow,
    geofence_mapped
  )
  SELECT
    NULL,
    a.device_name,
    a.imei,
    NULL,
    NULL,
    a.position_time,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    (a.payload->>'lat')::double precision,
    (a.payload->>'lng')::double precision,
    ST_SetSRID(ST_MakePoint((a.payload->>'lng')::double precision, (a.payload->>'lat')::double precision), 4326),
    NULL,
    now(),
    a.apifeed_id,
    false
  FROM tbl_apifeed a
  WHERE a.device_name = $1
    AND a.position_time >= $2
    AND a.position_time <= $3
`;

/**
 * Merge tbl_apifeed into tbl_tracking for a device and time range:
 * 1. DELETE from tbl_tracking where device_name and position_time in range
 * 2. INSERT into tbl_tracking from tbl_apifeed (same range), with created_at = now(), apirow = apifeed_id, imei from tbl_apifeed.
 * GEOM is built from lat/lon: PostGIS point via ST_MakePoint(lon, lat) with SRID 4326 (WGS84). geofence_mapped = false so store_fences() will pick these rows in the mid-loop prep.
 */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as {
      deviceName?: string;
      minTime?: string;
      maxTime?: string;
    };
    const deviceName = body?.deviceName?.trim();
    const minTime = body?.minTime?.trim();
    const maxTime = body?.maxTime?.trim();
    if (!deviceName || !minTime || !maxTime) {
      return NextResponse.json(
        { ok: false, error: 'deviceName, minTime and maxTime required' },
        { status: 400 }
      );
    }

    const minDate = new Date(minTime.includes('T') ? minTime : `${minTime.replace(' ', 'T')}Z`);
    const maxDate = new Date(maxTime.includes('T') ? maxTime : `${maxTime.replace(' ', 'T')}Z`);
    if (Number.isNaN(minDate.getTime()) || Number.isNaN(maxDate.getTime())) {
      return NextResponse.json({ ok: false, error: 'Invalid minTime or maxTime' }, { status: 400 });
    }

    const params = [deviceName, minDate, maxDate];

    let deleted: number;
    try {
      deleted = await execute(DELETE_SQL, params);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      // eslint-disable-next-line no-console
      console.error('[api/apifeed/merge-tracking] DELETE failed:', message);
      return NextResponse.json(
        {
          ok: false,
          error: message,
          failedStep: 'delete',
          rawSql: DELETE_SQL,
          params: { deviceName, minTime, maxTime },
        },
        { status: 500 }
      );
    }

    let inserted: number;
    try {
      inserted = await execute(INSERT_SQL, params);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      // eslint-disable-next-line no-console
      console.error('[api/apifeed/merge-tracking] INSERT failed:', message);
      return NextResponse.json(
        {
          ok: false,
          error: message,
          failedStep: 'insert',
          rawSql: INSERT_SQL,
          params: { deviceName, minTime, maxTime },
        },
        { status: 500 }
      );
    }

    return NextResponse.json({
      ok: true,
      deleted: Number(deleted),
      inserted: Number(inserted),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.error('[api/apifeed/merge-tracking POST] Error:', message, err instanceof Error ? err.stack : '');
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export const dynamic = 'force-dynamic';

type Row = {
  fence_name: string | null;
  center_lat: string | null;
  center_lon: string | null;
};

/**
 * Point inside a geofence, looked up by the name stored on the driver job.
 * Same PostGIS point the other “View on map” links use (ST_PointOnSurface).
 */
export async function GET(request: Request) {
  try {
    const fence = new URL(request.url).searchParams.get('fence')?.trim() ?? '';
    if (!fence) return NextResponse.json({ error: 'Geofence name is required' }, { status: 400 });

    const rows = await query<Row>(
      `SELECT btrim(g.fence_name) AS fence_name,
              CASE
                WHEN g.geom IS NULL OR ST_IsEmpty(g.geom) THEN NULL
                ELSE ST_Y(ST_PointOnSurface(g.geom))::text
              END AS center_lat,
              CASE
                WHEN g.geom IS NULL OR ST_IsEmpty(g.geom) THEN NULL
                ELSE ST_X(ST_PointOnSurface(g.geom))::text
              END AS center_lon
       FROM tbl_geofences g
       WHERE lower(btrim(g.fence_name)) = lower(btrim($1))
       ORDER BY
         CASE WHEN g.geom IS NULL OR ST_IsEmpty(g.geom) THEN 1 ELSE 0 END,
         g.fence_id
       LIMIT 1`,
      [fence],
    );
    const row = rows[0];
    if (!row) return NextResponse.json({ error: 'Geofence not found' }, { status: 404 });

    const lat = row.center_lat == null ? NaN : parseFloat(row.center_lat);
    const lon = row.center_lon == null ? NaN : parseFloat(row.center_lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return NextResponse.json({ error: 'No map coordinates for this geofence' }, { status: 404 });
    }

    return NextResponse.json({
      fence_name: row.fence_name ?? fence,
      center_lat: lat,
      center_lon: lon,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export const dynamic = 'force-dynamic';

type Row = {
  fence_id: number;
  fence_name: string | null;
  geojson: string;
  center_lat: string;
  center_lon: string;
};

/** GET: single geofence by id with geometry as GeoJSON for map display */
export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const rows = await query<Row>(
      `SELECT fence_id, fence_name, ST_AsGeoJSON(geom)::text AS geojson,
              ST_Y(ST_PointOnSurface(geom))::text AS center_lat,
              ST_X(ST_PointOnSurface(geom))::text AS center_lon
       FROM tbl_geofences WHERE fence_id = $1`,
      [idNum]
    );
    if (rows.length === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });

    const r = rows[0];
    let geometry: unknown = null;
    try {
      geometry = JSON.parse(r.geojson);
    } catch {
      // leave null if invalid json
    }
    const lat = parseFloat(r.center_lat);
    const lon = parseFloat(r.center_lon);
    return NextResponse.json({
      fence_id: r.fence_id,
      fence_name: r.fence_name ?? '',
      geometry,
      center_lat: Number.isFinite(lat) ? lat : null,
      center_lon: Number.isFinite(lon) ? lon : null,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

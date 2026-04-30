import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const sortBy = (searchParams.get('sort') ?? 'fence_name').toLowerCase();
    const order = (searchParams.get('order') ?? 'asc').toLowerCase();
    const orderDir = order === 'desc' ? 'DESC' : 'ASC';
    const allowedSort = ['fence_name', 'fence_id'];
    const sortColumn = allowedSort.includes(sortBy) ? sortBy : 'fence_name';

    const rows = await query<{
      fence_id: number;
      fence_name: string | null;
      map_lat: string | null;
      map_lon: string | null;
    }>(
      `SELECT fence_id, fence_name,
        ST_Y(ST_PointOnSurface(geom))::text AS map_lat,
        ST_X(ST_PointOnSurface(geom))::text AS map_lon
       FROM tbl_geofences ORDER BY ${sortColumn} ${orderDir}`
    );

    return NextResponse.json(
      rows.map((r) => {
        const lat = r.map_lat != null ? parseFloat(String(r.map_lat)) : NaN;
        const lon = r.map_lon != null ? parseFloat(String(r.map_lon)) : NaN;
        return {
          fence_id: r.fence_id,
          fence_name: r.fence_name ?? '',
          map_lat: Number.isFinite(lat) ? lat : null,
          map_lon: Number.isFinite(lon) ? lon : null,
        };
      })
    );
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

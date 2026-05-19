import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export const dynamic = 'force-dynamic';

/** List tbl_geofences with PostGIS-derived geometry only (no joins to jobs or mappings). */
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
      area_m2: string | null;
      bbox_west: string | null;
      bbox_east: string | null;
      bbox_south: string | null;
      bbox_north: string | null;
    }>(
      `SELECT g.fence_id,
        g.fence_name,
        CASE
          WHEN g.geom IS NULL OR ST_IsEmpty(g.geom) THEN NULL
          ELSE ST_Y(ST_PointOnSurface(g.geom))::text
        END AS map_lat,
        CASE
          WHEN g.geom IS NULL OR ST_IsEmpty(g.geom) THEN NULL
          ELSE ST_X(ST_PointOnSurface(g.geom))::text
        END AS map_lon,
        CASE
          WHEN g.geom IS NULL OR ST_IsEmpty(g.geom) THEN NULL
          ELSE ST_Area(g.geom::geography)::text
        END AS area_m2,
        CASE
          WHEN g.geom IS NULL OR ST_IsEmpty(g.geom) THEN NULL
          ELSE ST_XMin(g.geom)::text
        END AS bbox_west,
        CASE
          WHEN g.geom IS NULL OR ST_IsEmpty(g.geom) THEN NULL
          ELSE ST_XMax(g.geom)::text
        END AS bbox_east,
        CASE
          WHEN g.geom IS NULL OR ST_IsEmpty(g.geom) THEN NULL
          ELSE ST_YMin(g.geom)::text
        END AS bbox_south,
        CASE
          WHEN g.geom IS NULL OR ST_IsEmpty(g.geom) THEN NULL
          ELSE ST_YMax(g.geom)::text
        END AS bbox_north
       FROM tbl_geofences g
       ORDER BY g.${sortColumn} ${orderDir}`
    );

    const parseCoord = (v: string | null): number | null => {
      if (v == null || String(v).trim() === '') return null;
      const n = parseFloat(String(v));
      return Number.isFinite(n) ? n : null;
    };

    return NextResponse.json(
      rows.map((r) => {
        const lat = r.map_lat != null ? parseFloat(String(r.map_lat)) : NaN;
        const lon = r.map_lon != null ? parseFloat(String(r.map_lon)) : NaN;
        const area =
          r.area_m2 != null && String(r.area_m2).trim() !== ''
            ? parseFloat(String(r.area_m2))
            : NaN;
        return {
          fence_id: r.fence_id,
          fence_name: r.fence_name ?? '',
          map_lat: Number.isFinite(lat) ? lat : null,
          map_lon: Number.isFinite(lon) ? lon : null,
          area_m2: Number.isFinite(area) ? area : null,
          bbox_west: parseCoord(r.bbox_west),
          bbox_east: parseCoord(r.bbox_east),
          bbox_south: parseCoord(r.bbox_south),
          bbox_north: parseCoord(r.bbox_north),
        };
      })
    );
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

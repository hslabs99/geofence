import { NextResponse } from 'next/server';
import { execute } from '@/lib/db';

type GeoJSONPolygon = { type: 'Polygon'; coordinates: number[][][] };
type GeoJSONMultiPolygon = { type: 'MultiPolygon'; coordinates: number[][][][] };

function extractPlacemarks(kml: string): { name: string; coordinates: string }[] {
  const placemarks: { name: string; coordinates: string }[] = [];
  const placemarkRe = /<Placemark[^>]*>([\s\S]*?)<\/Placemark>/gi;
  let m: RegExpExecArray | null;
  while ((m = placemarkRe.exec(kml)) !== null) {
    const block = m[1];
    const nameMatch = block.match(/<name[^>]*>([\s\S]*?)<\/name>/i);
    const name = (nameMatch ? nameMatch[1].trim() : 'Unnamed').replace(/<[^>]+>/g, '') || 'Unnamed';
    const coordMatch = block.match(/<coordinates[^>]*>([\s\S]*?)<\/coordinates>/i);
    const coordinates = coordMatch ? coordMatch[1].trim() : '';
    if (coordinates) placemarks.push({ name, coordinates });
  }
  return placemarks;
}

function parseCoordinateString(coords: string): number[][] {
  const points: number[][] = [];
  const tokens = coords.split(/[\s\n\r]+/).filter(Boolean);
  for (const t of tokens) {
    const parts = t.split(',');
    const lng = parseFloat(parts[0]);
    const lat = parseFloat(parts[1]);
    if (Number.isFinite(lng) && Number.isFinite(lat)) points.push([lng, lat]);
  }
  if (points.length > 0 && (points[0][0] !== points[points.length - 1][0] || points[0][1] !== points[points.length - 1][1])) {
    points.push([points[0][0], points[0][1]]);
  }
  return points;
}

function coordinatesToMultiPolygon(coords: string): GeoJSONMultiPolygon | null {
  const ring = parseCoordinateString(coords);
  if (ring.length < 4) return null;
  const polygon: GeoJSONPolygon = { type: 'Polygon', coordinates: [ring] };
  return { type: 'MultiPolygon', coordinates: [polygon.coordinates] };
}

export async function POST(request: Request) {
  try {
    const contentType = request.headers.get('content-type') ?? '';
    let kml: string;

    if (contentType.includes('multipart/form-data')) {
      const formData = await request.formData();
      const file = formData.get('file') as File | null;
      if (!file) {
        return NextResponse.json({ error: 'No file provided' }, { status: 400 });
      }
      kml = await file.text();
    } else if (contentType.includes('application/json')) {
      const body = await request.json();
      kml = typeof body.kml === 'string' ? body.kml : '';
      if (!kml) return NextResponse.json({ error: 'Missing or invalid "kml" in body' }, { status: 400 });
    } else if (contentType.includes('text/') || contentType.includes('application/xml')) {
      kml = await request.text();
    } else {
      return NextResponse.json(
        { error: 'Send KML as: multipart/form-data (file), application/json ({ kml }), or text/xml body' },
        { status: 400 }
      );
    }

    const placemarks = extractPlacemarks(kml);
    const fences: { name: string; geom: GeoJSONMultiPolygon }[] = [];

    for (const pm of placemarks) {
      const geom = coordinatesToMultiPolygon(pm.coordinates);
      if (geom) fences.push({ name: pm.name, geom });
    }

    if (fences.length === 0) {
      return NextResponse.json({ error: 'No valid polygon placemarks found in KML', imported: 0 }, { status: 400 });
    }

    let imported = 0;
    const names: string[] = [];

    for (const fence of fences) {
      try {
        await execute('DELETE FROM tbl_geofences WHERE fence_name = $1', [fence.name]);
        await execute(
          'INSERT INTO tbl_geofences (fence_name, geom) VALUES ($1, ST_SetSRID(ST_GeomFromGeoJSON($2), 4326))',
          [fence.name, JSON.stringify(fence.geom)]
        );
        imported += 1;
        names.push(fence.name);
      } catch (err) {
        console.error('Import fence failed:', fence.name, err);
      }
    }

    return NextResponse.json({ imported, total: fences.length, names });
  } catch (err) {
    console.error('Import geofences error:', err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : 'Import failed' },
      { status: 500 }
    );
  }
}

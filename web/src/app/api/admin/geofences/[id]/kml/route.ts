import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export const dynamic = 'force-dynamic';

type Row = {
  fence_name: string | null;
  kml_geom: string;
};

/**
 * GET: returns the geofence as KML (download / My Maps). Do not rely on
 * https://www.google.com/maps?q=<this-url> — Google often cannot fetch app-hosted KML.
 * Use lat/lon from the geofences API (ST_PointOnSurface) for “View on map” instead.
 */
export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) {
      return new NextResponse('Invalid id', { status: 400 });
    }

    const rows = await query<Row>(
      `SELECT fence_name, ST_AsKML(geom, 6) AS kml_geom
       FROM tbl_geofences WHERE fence_id = $1`,
      [idNum]
    );
    if (rows.length === 0) {
      return new NextResponse('Not found', { status: 404 });
    }

    const r = rows[0];
    const name = (r.fence_name ?? 'Geofence').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const kml = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <Placemark>
      <name>${name}</name>
      ${r.kml_geom}
    </Placemark>
  </Document>
</kml>`;

    return new NextResponse(kml, {
      status: 200,
      headers: {
        'Content-Type': 'application/vnd.google-earth.kml+xml',
        'Content-Disposition': `inline; filename="geofence-${idNum}.kml"`,
      },
    });
  } catch (err) {
    console.error('KML export error:', err);
    return new NextResponse('Export failed', { status: 500 });
  }
}

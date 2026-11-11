import { NextResponse } from 'next/server';
import { latestStoredCoordinates } from '@/lib/current-location-poll';

function miss(message: string, status: number) {
  const safe = message.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  return new NextResponse(
    `<!doctype html><html><head><meta charset="utf-8"><title>Current location</title></head><body style="font-family:system-ui,sans-serif;padding:2rem;max-width:36rem"><h1 style="font-size:1.25rem">Current location</h1><p>${safe}</p></body></html>`,
    {
      status,
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    },
  );
}

/** GET: open the latest stored GPS fix for this IMEI in Google Maps. */
export async function GET(request: Request) {
  try {
    const params = new URL(request.url).searchParams;
    const imei = params.get('imei')?.trim() ?? '';
    const truckId = params.get('truck')?.trim() ?? '';
    if (!imei && !truckId) return miss('This job has no IMEI.', 400);
    const coordinates = await latestStoredCoordinates({ imei, truckId });
    if (!coordinates) {
      return miss(
        imei ? `No current location is stored for IMEI ${imei}.` : `No current location is stored for truck ${truckId}.`,
        404,
      );
    }
    const response = NextResponse.redirect(`https://www.google.com/maps?q=${coordinates}`);
    response.headers.set('Cache-Control', 'no-store');
    return response;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return miss(message, 500);
  }
}

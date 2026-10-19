import { NextResponse } from 'next/server';
import { listCurrentLocations, loadCurrentLocationSettings } from '@/lib/current-location-poll';

/** GET: rows from tbl_current_locations, newest first. Used by the driver phone app. */
export async function GET(request: Request) {
  try {
    const limit = Number(new URL(request.url).searchParams.get('limit') ?? '100');
    const [rows, settings] = await Promise.all([
      listCurrentLocations(Number.isFinite(limit) ? limit : 100),
      loadCurrentLocationSettings(),
    ]);
    return NextResponse.json({ rows, driverAppPollSeconds: settings.driverAppPollSeconds });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

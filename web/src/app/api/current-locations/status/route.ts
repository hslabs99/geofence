import { NextResponse } from 'next/server';
import { currentLocationPollStatus, latestCurrentLocation, loadCurrentLocationSettings } from '@/lib/current-location-poll';

/** GET ?truck= — whether the location poll is running, plus the latest rows. */
export async function GET(request: Request) {
  try {
    const truck = new URL(request.url).searchParams.get('truck');
    const settings = await loadCurrentLocationSettings();
    const status = currentLocationPollStatus();
    const rows = await latestCurrentLocation(truck);
    return NextResponse.json({
      running: status.running,
      intervalSeconds: status.settings.pollIntervalSeconds,
      driverAppPollSeconds: settings.driverAppPollSeconds,
      lastPolltime: status.lastPolltime,
      truckId: truck?.trim() || null,
      truck: rows.truck,
      latest: rows.latest,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

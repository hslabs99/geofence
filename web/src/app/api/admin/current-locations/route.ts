import { NextResponse } from 'next/server';
import {
  currentLocationPollStatus,
  listCurrentLocationLogs,
  listCurrentLocations,
  loadCurrentLocationSettings,
  currentLocationCallBudget,
  currentLocationTokenStatus,
  emptyCurrentLocations,
  pollCurrentLocationsOnce,
  saveCurrentLocationSettings,
  startCurrentLocationPoll,
  stopCurrentLocationPoll,
  type CurrentLocationSettings,
} from '@/lib/current-location-poll';
import {
  loadLocationAlertSettings,
  locationAlertStatus,
  notifyPrimaryStarted,
  notifyPrimaryStopped,
  saveLocationAlertSettings,
  startLocationServiceChecker,
  stopLocationServiceChecker,
  type LocationAlertSettings,
} from '@/lib/location-service-alerts';

async function snapshot() {
  await loadCurrentLocationSettings();
  await loadLocationAlertSettings();
  const status = currentLocationPollStatus();
  const alerts = locationAlertStatus();
  const [rows, logs, token, calls] = await Promise.all([
    listCurrentLocations(500),
    listCurrentLocationLogs(80),
    currentLocationTokenStatus(),
    currentLocationCallBudget(),
  ]);
  return { ...status, alerts, token, calls, rows, logs };
}

function settingsFromBody(body: Partial<CurrentLocationSettings>): Partial<CurrentLocationSettings> {
  return {
    pollIntervalSeconds: body.pollIntervalSeconds,
    flushOlderThanHours: body.flushOlderThanHours,
    flushEveryMinutes: body.flushEveryMinutes,
    driverAppPollSeconds: body.driverAppPollSeconds,
    confirmAwayMeters: body.confirmAwayMeters,
  };
}

/** GET: poll status, recent location rows, and poll logs. */
export async function GET() {
  try {
    return NextResponse.json(await snapshot());
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** POST: start | stop | once | settings | empty | alert-settings | checker-start | checker-stop */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as { action?: string } & Partial<CurrentLocationSettings> &
      Partial<LocationAlertSettings>;
    const action = (body.action ?? '').trim();
    const settings = settingsFromBody(body);
    if (action === 'start') {
      const wasRunning = currentLocationPollStatus().running;
      const started = await startCurrentLocationPoll(settings);
      const email = wasRunning ? undefined : await notifyPrimaryStarted();
      return NextResponse.json({ ok: true, ...started, email, ...(await snapshot()) });
    }
    if (action === 'stop') {
      const wasRunning = currentLocationPollStatus().running;
      const stopped = stopCurrentLocationPoll();
      const email = wasRunning ? await notifyPrimaryStopped() : undefined;
      return NextResponse.json({ ok: true, ...stopped, email, ...(await snapshot()) });
    }
    if (action === 'once') {
      const result = await pollCurrentLocationsOnce();
      return NextResponse.json({ ok: true, ...result, ...(await snapshot()) });
    }
    if (action === 'settings') {
      const saved = await saveCurrentLocationSettings(settings);
      return NextResponse.json({ ok: true, ...(await snapshot()), settings: saved });
    }
    if (action === 'empty') {
      const cleared = await emptyCurrentLocations();
      return NextResponse.json({ ok: true, ...cleared, ...(await snapshot()) });
    }
    if (action === 'alert-settings') {
      const saved = await saveLocationAlertSettings({
        alertEmails: body.alertEmails,
        checkerIntervalSeconds: body.checkerIntervalSeconds,
      });
      return NextResponse.json({ ...(await snapshot()), ok: true, alerts: { ...locationAlertStatus(), ...saved } });
    }
    if (action === 'checker-start') {
      const alerts = await startLocationServiceChecker({
        alertEmails: body.alertEmails,
        checkerIntervalSeconds: body.checkerIntervalSeconds,
      });
      return NextResponse.json({ ...(await snapshot()), ok: true, alerts });
    }
    if (action === 'checker-stop') {
      const alerts = stopLocationServiceChecker();
      return NextResponse.json({ ...(await snapshot()), ok: true, alerts });
    }
    return NextResponse.json(
      { error: 'action must be start, stop, once, settings, empty, alert-settings, checker-start, or checker-stop' },
      { status: 400 },
    );
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const status = message.startsWith('Invalid email') ? 400 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

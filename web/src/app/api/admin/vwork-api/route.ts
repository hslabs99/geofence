import { NextResponse } from 'next/server';
import {
  listVworkApiLogs,
  loadVworkApiSettings,
  pollVworkApiOnce,
  saveVworkApiSettings,
  startVworkApiPoll,
  stopVworkApiPoll,
  vworkApiPollStatus,
  type VworkApiSettings,
} from '@/lib/vwork-api-poll';
import { syncVworkDrivers } from '@/lib/vwork-driver-sync';
import { linkQueuedJobsToVwork } from '@/lib/vwork-job-steps';

async function snapshot() {
  await loadVworkApiSettings();
  const [status, logs] = await Promise.all([Promise.resolve(vworkApiPollStatus()), listVworkApiLogs(40)]);
  return { ...status, logs };
}

/** GET: VWork API poll status and recent poll logs. */
export async function GET() {
  try {
    return NextResponse.json(await snapshot());
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** POST: start | stop | once | settings */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as { action?: string } & Partial<VworkApiSettings>;
    const action = (body.action ?? '').trim();
    const settings = {
      pollIntervalSeconds: body.pollIntervalSeconds,
      lookbackDays: body.lookbackDays,
    };
    if (action === 'start') {
      await startVworkApiPoll(settings);
      return NextResponse.json({ ok: true, ...(await snapshot()) });
    }
    if (action === 'stop') {
      stopVworkApiPoll();
      return NextResponse.json({ ok: true, ...(await snapshot()) });
    }
    if (action === 'once') {
      const result = await pollVworkApiOnce();
      return NextResponse.json({ ok: true, ...result, ...(await snapshot()) });
    }
    if (action === 'settings') {
      const saved = await saveVworkApiSettings(settings);
      return NextResponse.json({ ok: true, ...(await snapshot()), settings: saved });
    }
    if (action === 'sync-drivers') {
      await linkQueuedJobsToVwork();
      const drivers = await syncVworkDrivers();
      return NextResponse.json({ ok: true, drivers, ...(await snapshot()) });
    }
    return NextResponse.json({ error: 'action must be start, stop, once, settings, or sync-drivers' }, { status: 400 });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

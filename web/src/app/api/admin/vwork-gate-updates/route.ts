import { NextResponse } from 'next/server';
import {
  drainVworkGateUpdates,
  listVworkGateUpdates,
  loadVworkGateUpdateSettings,
  retryFailedVworkGateUpdates,
  saveVworkGateUpdateSettings,
  startVworkGateUpdates,
  stopVworkGateUpdates,
  vworkGateUpdateStatus,
} from '@/lib/vwork-gate-queue';

async function snapshot() {
  await loadVworkGateUpdateSettings();
  const listed = await listVworkGateUpdates(100);
  return { ...vworkGateUpdateStatus(), ...listed, settings: { intervalSeconds: vworkGateUpdateStatus().intervalSeconds } };
}

/** GET: queued driver stage updates and whether the sender is running. */
export async function GET() {
  try {
    return NextResponse.json(await snapshot());
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** POST: start | stop | once | settings | retry-failed */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as { action?: string; intervalSeconds?: number };
    const action = (body.action ?? '').trim();
    if (action === 'start') {
      await startVworkGateUpdates({ intervalSeconds: body.intervalSeconds });
      return NextResponse.json({ ok: true, ...(await snapshot()) });
    }
    if (action === 'stop') {
      stopVworkGateUpdates();
      return NextResponse.json({ ok: true, ...(await snapshot()) });
    }
    if (action === 'once') {
      const result = await drainVworkGateUpdates({ untilEmpty: true });
      return NextResponse.json({ ok: true, ...result, ...(await snapshot()) });
    }
    if (action === 'settings') {
      const saved = await saveVworkGateUpdateSettings({ intervalSeconds: body.intervalSeconds });
      return NextResponse.json({ ok: true, ...(await snapshot()), settings: saved });
    }
    if (action === 'retry-failed') {
      const retried = await retryFailedVworkGateUpdates();
      return NextResponse.json({ ok: true, retried, ...(await snapshot()) });
    }
    return NextResponse.json({ error: 'action must be start, stop, once, settings, or retry-failed' }, { status: 400 });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

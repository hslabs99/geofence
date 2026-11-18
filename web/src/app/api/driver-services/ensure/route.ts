import { NextResponse } from 'next/server';
import { driverServiceStatus, ensureDriverServices } from '@/lib/ensure-background-services';

/** GET: whether the driver services are running. Does not start them. */
export async function GET() {
  try {
    return NextResponse.json({ ok: true, services: driverServiceStatus() });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** POST: the driver is waiting. Start any polls that are stopped. */
export async function POST() {
  try {
    const result = await ensureDriverServices();
    return NextResponse.json({ ok: result.errors.length === 0, ...result });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

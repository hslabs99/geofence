import { NextResponse } from 'next/server';
import { fillMissingDriverImeis, listDriverLookups } from '@/lib/driver-imei-sync';

/** GET: driver users and the TrackSolid IMEI stored for each driver ID. */
export async function GET() {
  try {
    return NextResponse.json(await listDriverLookups());
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** POST: fill driver IMEIs that are still blank from the TrackSolid device list. */
export async function POST() {
  try {
    const result = await fillMissingDriverImeis();
    const list = await listDriverLookups();
    return NextResponse.json({ ok: true, ...result, ...list });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

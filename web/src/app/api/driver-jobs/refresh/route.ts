import { NextResponse } from 'next/server';
import { forceVworkJobRefresh } from '@/lib/vwork-api-poll';

/** POST: the driver asked for new jobs. Pull VWork now, then the phone reads the queue. */
export async function POST() {
  try {
    const result = await forceVworkJobRefresh();
    return NextResponse.json({ ok: result.error == null, ...result });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

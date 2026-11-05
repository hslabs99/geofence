import { NextResponse } from 'next/server';
import { loadLiveDriverJobs } from '@/lib/live-driver-jobs';

/** GET: every driver job, including finished ones, with vineyard and winery metres. */
export async function GET() {
  try {
    const data = await loadLiveDriverJobs();
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

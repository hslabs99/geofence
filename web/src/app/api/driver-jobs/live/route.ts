import { NextResponse } from 'next/server';
import { loadLiveDriverJobs } from '@/lib/live-driver-jobs';

/** GET: open driver jobs (step 1 recorded, step 5 not yet) with vineyard and winery metres. */
export async function GET() {
  try {
    const data = await loadLiveDriverJobs();
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { enqueueVworkFeedJobs } from '@/lib/vwork-feed-queue';
import { checkVworkJobFeed, insertVworkFeedTestRows, listVworkJobFeed } from '@/lib/vwork-job-feed';

/** GET: recent rows in vwork_job_feed. */
export async function GET() {
  try {
    return NextResponse.json(await listVworkJobFeed(80));
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** POST: { action: "check" | "test", lookbackDays } */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as { action?: string; lookbackDays?: number };
    const action = (body.action ?? 'check').trim();
    if (action === 'test') {
      const seeded = await insertVworkFeedTestRows();
      const queue = await enqueueVworkFeedJobs();
      const list = await listVworkJobFeed(80);
      return NextResponse.json({ ok: true, ...seeded, queue, ...list });
    }
    if (action !== 'check') {
      return NextResponse.json({ error: 'action must be check or test' }, { status: 400 });
    }
    const result = await checkVworkJobFeed(Number(body.lookbackDays ?? 14));
    const queue = await enqueueVworkFeedJobs();
    const list = await listVworkJobFeed(80);
    return NextResponse.json({ ok: true, ...result, queue, ...list });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const status = message.includes('VWORK_API_KEY') ? 400 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

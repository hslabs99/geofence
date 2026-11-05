import { NextResponse } from 'next/server';
import { listOpenVworkFenceIssues, resolveVworkFenceIssue } from '@/lib/vwork-fence-resolve';

/** GET: open VWork fence names the job controller still has to map. */
export async function GET() {
  try {
    return NextResponse.json(await listOpenVworkFenceIssues());
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/** POST: { id, fenceName } saves the fence for this VWork name and fills waiting jobs. */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as { id?: number; fenceName?: string };
    const id = Number(body.id);
    if (!Number.isInteger(id) || id <= 0) {
      return NextResponse.json({ error: 'Issue not found' }, { status: 400 });
    }
    await resolveVworkFenceIssue(id, body.fenceName ?? '');
    return NextResponse.json({ ok: true, ...(await listOpenVworkFenceIssues()) });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const status = message.includes('existing geo fence') || message.includes('no longer open') || message.includes('TrackSolid IMEI') ? 400 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

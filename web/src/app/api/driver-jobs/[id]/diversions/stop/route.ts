import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { stopJobDiversion } from '@/lib/driver-diversions';
import { ensureTblDriverJobsSchema } from '@/lib/ensure-tbl-driver-jobs-schema';
import { isPhoneLocalStamp } from '@/lib/driver-jobs';

export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    await ensureTblDriverJobsSchema();
    const { id } = await params;
    const jobId = Number(id);
    if (!Number.isInteger(jobId) || jobId <= 0) return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    const jobs = await query<{ id: number }>(`SELECT id FROM tbl_driver_jobs WHERE id = $1`, [jobId]);
    if (!jobs[0]) return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    const body = (await request.json()) as { at?: unknown };
    const at = typeof body.at === 'string' ? body.at.trim() : '';
    if (!isPhoneLocalStamp(at)) {
      return NextResponse.json({ error: 'Time must be YYYY-MM-DD HH:MM:SS' }, { status: 400 });
    }
    const diversion = await stopJobDiversion(jobId, at);
    if (!diversion) return NextResponse.json({ error: 'No diversion is running' }, { status: 409 });
    return NextResponse.json({ diversion });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

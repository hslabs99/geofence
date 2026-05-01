import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

type Row = { status: string; cnt: number };

export async function GET() {
  try {
    const rows = await query<Row>(
      `SELECT COALESCE(status, 'unknown') AS status, COUNT(*)::int AS cnt
       FROM tbl_step4to5_audit_queue
       GROUP BY COALESCE(status, 'unknown')`,
    );
    const counts: Record<string, number> = {};
    for (const r of rows) {
      counts[String(r.status)] = Number(r.cnt ?? 0);
    }
    return NextResponse.json({
      ok: true,
      counts,
      pending: counts.pending ?? 0,
      processing: counts.processing ?? 0,
      done: counts.done ?? 0,
      skipped: counts.skipped ?? 0,
      error: counts.error ?? 0,
      total: Object.values(counts).reduce((a, b) => a + (Number.isFinite(b) ? b : 0), 0),
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}


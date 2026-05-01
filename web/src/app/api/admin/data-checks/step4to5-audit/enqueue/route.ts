import { NextResponse } from 'next/server';
import { execute, query } from '@/lib/db';

type Body = {
  jobIds?: string[];
  reason?: string;
};

function normJobId(v: unknown): string {
  const s = v == null ? '' : String(v).trim();
  return s;
}

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as Body;
    const reason = body?.reason != null && String(body.reason).trim() !== '' ? String(body.reason).trim() : null;
    const ids = Array.isArray(body?.jobIds) ? body.jobIds.map(normJobId).filter(Boolean) : [];
    const unique = [...new Set(ids)];
    if (unique.length === 0) {
      return NextResponse.json({ error: 'jobIds is required' }, { status: 400 });
    }
    if (unique.length > 5000) {
      return NextResponse.json({ error: 'Too many jobIds (max 5000 per request)' }, { status: 400 });
    }

    // Snapshot current job metadata for UI; ignore missing jobs.
    const jobRows = await query<{
      job_id: string;
      customer: string | null;
      template: string | null;
      delivery_winery: string | null;
      vineyard_group: string | null;
      trailermode: string | null;
    }>(
      `SELECT trim(job_id::text) AS job_id, customer, template, delivery_winery, vineyard_group, trailermode
       FROM tbl_vworkjobs
       WHERE trim(job_id::text) = ANY($1::text[])`,
      [unique],
    );

    let inserted = 0;
    for (const r of jobRows) {
      const jid = normJobId(r.job_id);
      if (!jid) continue;
      const n = await execute(
        `INSERT INTO tbl_step4to5_audit_queue (job_id, customer, template, delivery_winery, vineyard_group, trailermode, reason, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')
         ON CONFLICT (job_id) DO UPDATE SET
           updated_at = now(),
           reason = COALESCE(EXCLUDED.reason, tbl_step4to5_audit_queue.reason),
           status = CASE
             WHEN tbl_step4to5_audit_queue.status IN ('done','skipped') THEN tbl_step4to5_audit_queue.status
             ELSE 'pending'
           END`,
        [
          jid,
          r.customer ?? null,
          r.template ?? null,
          r.delivery_winery ?? null,
          r.vineyard_group ?? null,
          r.trailermode ?? null,
          reason,
        ],
      );
      // execute returns affected rows: insert=1, upsert=1
      inserted += n > 0 ? 1 : 0;
    }

    return NextResponse.json({ ok: true, requested: unique.length, found: jobRows.length, upserted: inserted });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}


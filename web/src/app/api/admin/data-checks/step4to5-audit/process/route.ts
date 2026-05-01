import { NextResponse } from 'next/server';
import { execute, query } from '@/lib/db';
import { synthStep4ActualTimeSql } from '@/lib/step4to5-fix';

type QueueRow = {
  id: number;
  job_id: string;
};

function toInt(v: string | null | undefined, d: number): number {
  const n = v != null ? parseInt(String(v), 10) : NaN;
  return Number.isFinite(n) ? n : d;
}

/**
 * POST /api/admin/data-checks/step4to5-audit/process?limit=100
 *
 * Low-volume loop: for pending queue rows, populate `step_4_actual_time` using derived actual steps 1–3,
 * capped by `COALESCE(step_5_actual_time, step_5_completed_at, step_4_safe, actual_end_time)`.
 */
export async function POST(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const limit = Math.max(1, Math.min(toInt(searchParams.get('limit'), 100), 1000));

    const rows = await query<QueueRow>(
      `SELECT id, job_id
       FROM tbl_step4to5_audit_queue
       WHERE status = 'pending'
       ORDER BY id
       LIMIT ${limit}`,
    );

    let done = 0;
    let skipped = 0;
    let errored = 0;

    const perJob: { job_id: string; status: string; updated: number; error?: string }[] = [];

    for (const r of rows) {
      const jid = String(r.job_id ?? '').trim();
      if (!jid) continue;
      try {
        await execute(
          `UPDATE tbl_step4to5_audit_queue
           SET status = 'processing', attempts = attempts + 1, updated_at = now()
           WHERE id = $1`,
          [r.id],
        );

        const cap = `COALESCE(step_5_actual_time, step_5_completed_at, step_4_safe, actual_end_time)`;
        const expr = synthStep4ActualTimeSql(cap);

        const updated = await execute(
          `UPDATE tbl_vworkjobs
           SET step_4_actual_time = ${expr},
               step_4_via = 'RULE'
           WHERE trim(job_id::text) = $1
             AND step_4_actual_time IS NULL`,
          [jid],
        );

        if (updated > 0) {
          done++;
          perJob.push({ job_id: jid, status: 'done', updated });
          await execute(
            `UPDATE tbl_step4to5_audit_queue
             SET status = 'done', processed_at = now(), updated_at = now(), last_error = NULL
             WHERE id = $1`,
            [r.id],
          );
        } else {
          // Either job doesn't exist, or it didn't need updating, or preconditions made expr NULL.
          skipped++;
          perJob.push({ job_id: jid, status: 'skipped', updated });
          await execute(
            `UPDATE tbl_step4to5_audit_queue
             SET status = 'skipped', processed_at = now(), updated_at = now()
             WHERE id = $1`,
            [r.id],
          );
        }
      } catch (e) {
        errored++;
        const msg = e instanceof Error ? e.message : String(e);
        perJob.push({ job_id: jid, status: 'error', updated: 0, error: msg });
        await execute(
          `UPDATE tbl_step4to5_audit_queue
           SET status = 'error', last_error = $2, updated_at = now()
           WHERE id = $1`,
          [r.id, msg],
        );
      }
    }

    return NextResponse.json({
      ok: true,
      fetched: rows.length,
      done,
      skipped,
      errored,
      perJob,
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}


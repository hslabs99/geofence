import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

type JobRow = {
  job_id: string;
  worker: string | null;
  truck_id: string | null;
  actual_start_time: string | null;
  actual_end_time: string | null;
};

/**
 * GET /api/admin/tagging/steps-from-step4to5-queue
 * Optional:
 * - status=pending|error|skipped|done|processing (can repeat). Default: all statuses
 * - limit (default 5000, max 20000)
 *
 * Returns a minimal job shape sufficient for `runFetchStepsForJobs`.
 * Ignores all other filters (customer/template/date/etc) by design.
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const statusesRaw = searchParams.getAll('status').map((s) => s.trim()).filter(Boolean);
    const statuses = statusesRaw.length > 0 ? statusesRaw : null;
    const limRaw = parseInt(searchParams.get('limit') ?? '5000', 10);
    const limit = Math.max(1, Math.min(Number.isFinite(limRaw) ? limRaw : 5000, 20000));

    const ids =
      statuses && statuses.length > 0
        ? await query<{ job_id: string }>(
            `SELECT job_id
             FROM tbl_step4to5_audit_queue
             WHERE status = ANY($1::text[])
             ORDER BY id
             LIMIT ${limit}`,
            [statuses],
          )
        : await query<{ job_id: string }>(
            `SELECT job_id
             FROM tbl_step4to5_audit_queue
             ORDER BY id
             LIMIT ${limit}`,
          );
    const jobIds = ids.map((r) => String(r.job_id ?? '').trim()).filter(Boolean);
    if (jobIds.length === 0) {
      return NextResponse.json({ ok: true, statuses: statuses ?? 'all', total: 0, jobIds: [], jobs: [] });
    }

    const jobs = await query<JobRow>(
      `SELECT
         trim(job_id::text) AS job_id,
         worker,
         truck_id,
         to_char(actual_start_time, 'YYYY-MM-DD HH24:MI:SS') AS actual_start_time,
         to_char(actual_end_time, 'YYYY-MM-DD HH24:MI:SS') AS actual_end_time
       FROM tbl_vworkjobs
       WHERE trim(job_id::text) = ANY($1::text[])
       ORDER BY trim(job_id::text)`,
      [jobIds],
    );

    return NextResponse.json({ ok: true, statuses: statuses ?? 'all', total: jobIds.length, jobIds, jobs });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}


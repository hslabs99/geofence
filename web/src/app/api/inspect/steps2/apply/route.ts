/**
 * POST /api/inspect/steps2/apply — apply a **Steps+** stay to VWork step 2/3 fields (legacy path name "steps2").
 * Sets step_2_actual_time, step_3_actual_time, step_2_via, step_3_via, and appends "VineFence+:" to calcnotes.
 * Body: { jobId: string, enter_time: string, exit_time: string }
 */

import { NextResponse } from 'next/server';
import { execute } from '@/lib/db';

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => ({}));
    const jobId = body?.jobId != null ? String(body.jobId).trim() : '';
    const enterTime = body?.enter_time != null ? String(body.enter_time).trim() : '';
    const exitTime = body?.exit_time != null ? String(body.exit_time).trim() : '';

    if (!jobId) {
      return NextResponse.json({ error: 'jobId is required' }, { status: 400 });
    }
    if (!enterTime || !exitTime) {
      return NextResponse.json({ error: 'enter_time and exit_time are required' }, { status: 400 });
    }

    await execute(
      `UPDATE tbl_vworkjobs SET
        step_2_actual_time = $1::timestamp,
        step_2_via = 'VineFence+',
        step_3_actual_time = $2::timestamp,
        step_3_via = 'VineFence+'
      WHERE job_id::text = $3`,
      [enterTime, exitTime, jobId]
    );

    try {
      await execute(
        `UPDATE tbl_vworkjobs SET calcnotes = COALESCE(TRIM(calcnotes) || ' ', '') || 'VineFence+:'
         WHERE job_id::text = $1`,
        [jobId]
      );
      /* Stored in tbl_vworkjobs.calcnotes */
    } catch {
      /* calcnotes column may not exist yet; run add_calcnotes_tbl_vworkjobs.sql */
    }

    return NextResponse.json({ ok: true, job_id: jobId });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

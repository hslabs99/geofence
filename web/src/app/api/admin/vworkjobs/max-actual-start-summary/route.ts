import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/**
 * GET: tbl_vworkjobs coverage for Steps planning — no worker filter.
 * - maxActualStartTime: latest actual_start_time among all jobs with a start.
 * - maxActualStartWithStepData: latest actual_start_time among jobs that have
 *   at least one of step_1..5_actual_time set (derived steps written).
 */
export async function GET() {
  try {
    const rows = await query<{
      max_all: string | null;
      max_with_steps: string | null;
    }>(
      `SELECT
         to_char(max(v.actual_start_time), 'YYYY-MM-DD HH24:MI:SS') AS max_all,
         to_char(
           max(v.actual_start_time) FILTER (
             WHERE v.step_1_actual_time IS NOT NULL
                OR v.step_2_actual_time IS NOT NULL
                OR v.step_3_actual_time IS NOT NULL
                OR v.step_4_actual_time IS NOT NULL
                OR v.step_5_actual_time IS NOT NULL
           ),
           'YYYY-MM-DD HH24:MI:SS'
         ) AS max_with_steps
       FROM tbl_vworkjobs v
       WHERE v.actual_start_time IS NOT NULL`
    );
    const row = rows[0];
    return NextResponse.json({
      ok: true,
      maxActualStartTime: row?.max_all ?? null,
      maxActualStartWithStepData: row?.max_with_steps ?? null,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

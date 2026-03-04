import { NextResponse } from 'next/server';
import { execute } from '@/lib/db';

/**
 * POST: Set steps_fetched = false and steps_fetched_when = NULL for all rows in tbl_vworkjobs.
 * Use this to re-open all jobs for a fresh "Fetch GPS steps" run (Step 4).
 * If columns do not exist yet, returns ok: true, updated: 0 (run migrations/add_steps_fetched_tbl_vworkjobs.sql).
 */
export async function POST() {
  try {
    const count = await execute(`
      UPDATE tbl_vworkjobs
      SET steps_fetched = false, steps_fetched_when = NULL
    `);
    return NextResponse.json({ ok: true, updated: Number(count) });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (message.includes('42703') || message.includes('does not exist')) {
      return NextResponse.json({
        ok: true,
        updated: 0,
        skipped: 'steps_fetched columns not in DB; run migrations/add_steps_fetched_tbl_vworkjobs.sql',
      });
    }
    console.error('[api/admin/vworkjobs/reset-steps]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

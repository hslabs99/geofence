import { NextResponse } from 'next/server';
import { runStep4to5NormalAllEligible } from '@/lib/run-vwork-data-fixes';

/**
 * POST: Normal Step4→5 fix on every tbl_vworkjobs row that matches eligibility
 * (step4to5=0, step_4 Job Completed, step_5 not Job Completed, step_5_completed_at null, step_4_completed_at set).
 * Same logic as the last step of POST /api/admin/data-checks/run-all-vwork-fixes.
 */
export async function POST() {
  try {
    const data = await runStep4to5NormalAllEligible();
    return NextResponse.json(data);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

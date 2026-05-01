import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

type AuditRow = {
  job_id: string;
  customer: string | null;
  template: string | null;
  worker: string | null;
  delivery_winery: string | null;
  vineyard_group: string | null;
  trailermode: string | null;
  step4to5: number | null;
  step_3_completed_at: string | null;
  step_4_completed_at: string | null;
  step_4_actual_time: string | null;
  step_4_safe: string | null;
  step_5_completed_at: string | null;
  step_5_actual_time: string | null;
};

function parseBool(v: string | null | undefined, defaultValue: boolean): boolean {
  if (v == null) return defaultValue;
  const s = String(v).trim().toLowerCase();
  if (s === '1' || s === 'true' || s === 'yes' || s === 'y') return true;
  if (s === '0' || s === 'false' || s === 'no' || s === 'n') return false;
  return defaultValue;
}

/**
 * GET /api/admin/data-checks/step4to5-audit
 * Optional query params:
 * - customer, template (trim match)
 * - step4to5Only=1 (default true): only rows with step4to5=1
 * - limit (default 5000, max 20000)
 *
 * Returns jobs where step 4 is missing in either:
 * - step_4_completed_at (VWork/original) OR
 * - step_4_actual_time (derived actuals)
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const customer = searchParams.get('customer')?.trim() ?? '';
    const template = searchParams.get('template')?.trim() ?? '';
    const step4to5Only = parseBool(searchParams.get('step4to5Only'), true);
    const limRaw = parseInt(searchParams.get('limit') ?? '5000', 10);
    const limit = Math.max(1, Math.min(Number.isFinite(limRaw) ? limRaw : 5000, 20000));

    const conditions: string[] = [];
    const values: unknown[] = [];
    if (customer) {
      values.push(customer);
      conditions.push(`trim(customer) = $${values.length}`);
    }
    if (template) {
      values.push(template);
      conditions.push(`trim(template) = $${values.length}`);
    }
    if (step4to5Only) {
      conditions.push(`COALESCE(step4to5, 0) = 1`);
    }
    conditions.push(`(step_4_completed_at IS NULL OR step_4_actual_time IS NULL)`);

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const rows = await query<AuditRow>(
      `SELECT
         trim(job_id::text) AS job_id,
         customer,
         template,
         worker,
         delivery_winery,
         vineyard_group,
         trailermode,
         step4to5,
         to_char(step_3_completed_at, 'YYYY-MM-DD HH24:MI:SS') AS step_3_completed_at,
         to_char(step_4_completed_at, 'YYYY-MM-DD HH24:MI:SS') AS step_4_completed_at,
         to_char(step_4_actual_time, 'YYYY-MM-DD HH24:MI:SS') AS step_4_actual_time,
         to_char(step_4_safe, 'YYYY-MM-DD HH24:MI:SS') AS step_4_safe,
         to_char(step_5_completed_at, 'YYYY-MM-DD HH24:MI:SS') AS step_5_completed_at,
         to_char(step_5_actual_time, 'YYYY-MM-DD HH24:MI:SS') AS step_5_actual_time
       FROM tbl_vworkjobs
       ${where}
       ORDER BY customer NULLS LAST, template NULLS LAST, delivery_winery NULLS LAST, job_id
       LIMIT ${limit}`,
      values,
    );

    const countRows = await query<{ cnt: number }>(
      `SELECT COUNT(*)::int AS cnt FROM tbl_vworkjobs ${where}`,
      values,
    );
    const total = Number(countRows[0]?.cnt ?? 0);

    return NextResponse.json({ ok: true, total, rows });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}


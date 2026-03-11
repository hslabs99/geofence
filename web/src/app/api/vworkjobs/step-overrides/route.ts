import { NextResponse } from 'next/server';
import { execute } from '@/lib/db';

/**
 * POST: Save manual step overrides (orides) and recalc actuals.
 * Body: job_id, step1oride..step5oride (nullable string), steporidecomment (nullable string).
 * Updates oride columns; for each step where oride is present, sets step_N_actual_time = oride and step_N_via = 'ORIDE'.
 * Steps with no oride keep existing step_N_actual_time and step_N_via.
 */
export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => ({}));
    const jobId = body?.job_id != null ? String(body.job_id).trim() : '';
    if (!jobId) {
      return NextResponse.json({ error: 'job_id is required' }, { status: 400 });
    }
    const step1oride = body.step1oride != null && body.step1oride !== '' ? String(body.step1oride).trim() : null;
    const step2oride = body.step2oride != null && body.step2oride !== '' ? String(body.step2oride).trim() : null;
    const step3oride = body.step3oride != null && body.step3oride !== '' ? String(body.step3oride).trim() : null;
    const step4oride = body.step4oride != null && body.step4oride !== '' ? String(body.step4oride).trim() : null;
    const step5oride = body.step5oride != null && body.step5oride !== '' ? String(body.step5oride).trim() : null;
    const steporidecomment = body.steporidecomment != null ? String(body.steporidecomment) : null;

    // Pass orides as text so PostgreSQL deduces one type; use ::text in CASE and cast to timestamp only when assigning.
    await execute(
      `UPDATE tbl_vworkjobs SET
        step1oride = CASE WHEN $1::text IS NOT NULL AND trim($1::text) <> '' THEN ($1::text)::timestamp ELSE NULL END,
        step2oride = CASE WHEN $2::text IS NOT NULL AND trim($2::text) <> '' THEN ($2::text)::timestamp ELSE NULL END,
        step3oride = CASE WHEN $3::text IS NOT NULL AND trim($3::text) <> '' THEN ($3::text)::timestamp ELSE NULL END,
        step4oride = CASE WHEN $4::text IS NOT NULL AND trim($4::text) <> '' THEN ($4::text)::timestamp ELSE NULL END,
        step5oride = CASE WHEN $5::text IS NOT NULL AND trim($5::text) <> '' THEN ($5::text)::timestamp ELSE NULL END,
        steporidecomment = $6,
        step_1_actual_time = CASE WHEN $1::text IS NOT NULL AND trim($1::text) <> '' THEN ($1::text)::timestamp ELSE step_1_actual_time END,
        step_1_via = CASE WHEN $1::text IS NOT NULL AND trim($1::text) <> '' THEN 'ORIDE' ELSE step_1_via END,
        step_2_actual_time = CASE WHEN $2::text IS NOT NULL AND trim($2::text) <> '' THEN ($2::text)::timestamp ELSE step_2_actual_time END,
        step_2_via = CASE WHEN $2::text IS NOT NULL AND trim($2::text) <> '' THEN 'ORIDE' ELSE step_2_via END,
        step_3_actual_time = CASE WHEN $3::text IS NOT NULL AND trim($3::text) <> '' THEN ($3::text)::timestamp ELSE step_3_actual_time END,
        step_3_via = CASE WHEN $3::text IS NOT NULL AND trim($3::text) <> '' THEN 'ORIDE' ELSE step_3_via END,
        step_4_actual_time = CASE WHEN $4::text IS NOT NULL AND trim($4::text) <> '' THEN ($4::text)::timestamp ELSE step_4_actual_time END,
        step_4_via = CASE WHEN $4::text IS NOT NULL AND trim($4::text) <> '' THEN 'ORIDE' ELSE step_4_via END,
        step_5_actual_time = CASE WHEN $5::text IS NOT NULL AND trim($5::text) <> '' THEN ($5::text)::timestamp ELSE step_5_actual_time END,
        step_5_via = CASE WHEN $5::text IS NOT NULL AND trim($5::text) <> '' THEN 'ORIDE' ELSE step_5_via END
      WHERE job_id::text = $7`,
      [step1oride ?? null, step2oride ?? null, step3oride ?? null, step4oride ?? null, step5oride ?? null, steporidecomment ?? null, jobId]
    );

    return NextResponse.json({ ok: true, job_id: jobId });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

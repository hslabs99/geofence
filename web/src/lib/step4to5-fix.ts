import { execute } from '@/lib/db';

/**
 * Synthetic step_4_completed_at from VWork steps 1–3, capped by `cap` (rerun: step_5_completed_at; normal migrate: old step_4).
 * 1) step_3 + (step_2 − step_1) when that instant is strictly before `cap`.
 * 2) Else if outbound is valid and step_3 < `cap`, midpoint between step_3 and `cap` (when travel would land on/after `cap`).
 * Otherwise NULL. All operands cast to timestamp so comparisons cannot silently mis-order mixed types.
 */
export function synthStep4CompletedAtSql(capColumnExpr: string): string {
  const cap = capColumnExpr.trim();
  const s1 = `(step_1_completed_at)::timestamp`;
  const s2 = `(step_2_completed_at)::timestamp`;
  const s3 = `(step_3_completed_at)::timestamp`;
  const c = `(${cap})::timestamp`;
  const travel = `${s3} + (${s2} - ${s1})`;
  return `CASE
  WHEN ${c} IS NULL THEN NULL
  WHEN step_1_completed_at IS NOT NULL
    AND step_2_completed_at IS NOT NULL
    AND step_3_completed_at IS NOT NULL
    AND ${s2} > ${s1}
  THEN (
    CASE
    WHEN ${travel} < ${c} THEN ${travel}
    WHEN ${s3} < ${c} THEN ${s3} + (${c} - ${s3}) / 2
    ELSE NULL
    END
  )
  ELSE NULL
END`;
}

/**
 * Synthetic step_4_actual_time from derived actual steps 1–3, capped by `cap`.
 * Mirrors synthStep4CompletedAtSql but operates on `step_n_actual_time`.
 */
export function synthStep4ActualTimeSql(capColumnExpr: string): string {
  const cap = capColumnExpr.trim();
  const s1 = `(step_1_actual_time)::timestamp`;
  const s2 = `(step_2_actual_time)::timestamp`;
  const s3 = `(step_3_actual_time)::timestamp`;
  const c = `(${cap})::timestamp`;
  const travel = `${s3} + (${s2} - ${s1})`;
  return `CASE
  WHEN ${c} IS NULL THEN NULL
  WHEN step_1_actual_time IS NOT NULL
    AND step_2_actual_time IS NOT NULL
    AND step_3_actual_time IS NOT NULL
    AND ${s2} > ${s1}
  THEN (
    CASE
    WHEN ${travel} < ${c} THEN ${travel}
    WHEN ${s3} < ${c} THEN ${s3} + (${c} - ${s3}) / 2
    ELSE NULL
    END
  )
  ELSE NULL
END`;
}

/** During normal 4→5 migrate: cap = old step_4_completed_at (same instant copied to step 5). */
export const SYNTH_STEP4_AT_NORMAL = synthStep4CompletedAtSql('step_4_completed_at');

/** Rerun / order-fix: cap = `step_5_completed_at` (VWork job complete on step 5). */
export const SYNTH_STEP4_AT_BEFORE_STEP5 = synthStep4CompletedAtSql('step_5_completed_at');

/** @deprecated Use SYNTH_STEP4_AT_NORMAL or synthStep4CompletedAtSql — was uncapped. */
export const SYNTH_STEP4_AT = SYNTH_STEP4_AT_NORMAL;

/**
 * Normal Step4→5 eligibility (no customer/template). Rows with step4to5=1 or wrong step shape are excluded.
 */
export function eligiblePredicateNoAliasNormal(): string {
  return `COALESCE(step4to5, 0) = 0
  AND trim(COALESCE(step_4_name, '')) = 'Job Completed'
  AND (step_5_name IS NULL OR trim(COALESCE(step_5_name, '')) <> 'Job Completed')
  AND step_5_completed_at IS NULL
  AND step_4_completed_at IS NOT NULL`;
}

export function buildUpdateSqlNormalWhere(whereNoAliasMultiline: string): string {
  const where = whereNoAliasMultiline.replace(/\s+/g, ' ').trim();
  return `UPDATE tbl_vworkjobs SET
  step_5_name = step_4_name,
  step_5_completed_at = step_4_completed_at,
  step_5_address = step_4_address,
  step_4_name = 'Arrive Winery',
  step_4_completed_at = ${SYNTH_STEP4_AT_NORMAL},
  step_4_address = NULL,
  step4to5 = 1,
  step_1_gps_completed_at = NULL, step1_gps_id = NULL,
  step_2_gps_completed_at = NULL, step2_gps_id = NULL,
  step_3_gps_completed_at = NULL, step3_gps_id = NULL,
  step_4_gps_completed_at = NULL, step4_gps_id = NULL,
  step_5_gps_completed_at = NULL, step5_gps_id = NULL,
  step_1_actual_time = NULL, step_1_via = NULL,
  step_2_actual_time = NULL, step_2_via = NULL,
  step_3_actual_time = NULL, step_3_via = NULL,
  step_4_actual_time = NULL, step_4_via = NULL,
  step_5_actual_time = NULL, step_5_via = NULL,
  step1oride = NULL, step2oride = NULL, step3oride = NULL, step4oride = NULL, step5oride = NULL,
  steps_fetched = false, steps_fetched_when = NULL
WHERE ${where}`;
}

/**
 * Normal Step4→5 UPDATE for scoped customer/template ($1, $2) or global (empty params).
 */
export async function executeNormalStep4to5Update(
  updateSql: string,
  params: unknown[] = []
): Promise<number> {
  try {
    return await execute(updateSql, params);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (msg.includes('steps_fetched') && (msg.includes('42703') || msg.includes('does not exist'))) {
      const stripped = updateSql.replace(
        /,\s*steps_fetched\s*=\s*false\s*,\s*steps_fetched_when\s*=\s*NULL/,
        ''
      );
      return execute(stripped, params);
    }
    throw e;
  }
}

export type Step4to5NormalAllResult = {
  ok: true;
  updated: number;
};

/**
 * AutoRun / batch: normal Step4→5 on every tbl_vworkjobs row matching eligibility (no customer/template).
 */
export async function runStep4to5NormalAllEligible(): Promise<Step4to5NormalAllResult> {
  const sql = buildUpdateSqlNormalWhere(eligiblePredicateNoAliasNormal());
  const updated = await executeNormalStep4to5Update(sql, []);
  return { ok: true, updated };
}

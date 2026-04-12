import { execute } from '@/lib/db';

/** Synthetic step_4_completed_at from steps 1–3 (same as Data Checks Step4→5). */
export const SYNTH_STEP4_AT = `CASE
    WHEN step_1_completed_at IS NOT NULL
      AND step_2_completed_at IS NOT NULL
      AND step_3_completed_at IS NOT NULL
      AND step_2_completed_at > step_1_completed_at
    THEN step_3_completed_at + (step_2_completed_at - step_1_completed_at)
    ELSE NULL
  END`;

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
  step_4_completed_at = ${SYNTH_STEP4_AT},
  step_4_address = NULL,
  step4to5 = 1,
  Step_1_GPS_completed_at = NULL, Step1_gps_id = NULL,
  Step_2_GPS_completed_at = NULL, Step2_gps_id = NULL,
  Step_3_GPS_completed_at = NULL, Step3_gps_id = NULL,
  Step_4_GPS_completed_at = NULL, Step4_gps_id = NULL,
  Step_5_GPS_completed_at = NULL, Step5_gps_id = NULL,
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

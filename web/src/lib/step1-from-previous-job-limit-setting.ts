import { query } from '@/lib/db';
import {
  STEP1_FROM_PREVIOUS_JOB_LIMIT_DEFAULT_MINUTES,
  STEP1_FROM_PREVIOUS_JOB_LIMIT_SETTING_NAME,
  STEP1_FROM_PREVIOUS_JOB_LIMIT_SETTINGS_TYPE,
} from '@/lib/step1-from-previous-job-limit-setting-names';

export {
  STEP1_FROM_PREVIOUS_JOB_LIMIT_DEFAULT_MINUTES,
  STEP1_FROM_PREVIOUS_JOB_LIMIT_SETTING_NAME,
  STEP1_FROM_PREVIOUS_JOB_LIMIT_SETTINGS_TYPE,
} from '@/lib/step1-from-previous-job-limit-setting-names';

/**
 * Step1(lastJobEnd): ignore the previous job if its Step 5 time is more than this many minutes before this job’s actual_start_time.
 */
export async function getStep1FromPreviousJobLimitMinutes(): Promise<number> {
  const rows = await query<{ settingvalue: string | null }>(
    `SELECT settingvalue FROM tbl_settings WHERE type = $1 AND settingname = $2 LIMIT 1`,
    [STEP1_FROM_PREVIOUS_JOB_LIMIT_SETTINGS_TYPE, STEP1_FROM_PREVIOUS_JOB_LIMIT_SETTING_NAME]
  );
  const raw = rows[0]?.settingvalue?.trim() ?? '';
  if (raw === '') return STEP1_FROM_PREVIOUS_JOB_LIMIT_DEFAULT_MINUTES;
  const n = parseInt(raw, 10);
  if (Number.isNaN(n) || n < 0) return STEP1_FROM_PREVIOUS_JOB_LIMIT_DEFAULT_MINUTES;
  return Math.min(24 * 60, n);
}

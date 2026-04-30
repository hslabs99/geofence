import { query } from '@/lib/db';
import {
  JOB_END_CEILING_BUFFER_DEFAULT_MINUTES,
  JOB_END_CEILING_BUFFER_SETTING_NAME,
  JOB_END_CEILING_BUFFER_SETTINGS_TYPE,
} from '@/lib/job-end-ceiling-buffer-setting-names';

export {
  JOB_END_CEILING_BUFFER_DEFAULT_MINUTES,
  JOB_END_CEILING_BUFFER_SETTING_NAME,
  JOB_END_CEILING_BUFFER_SETTINGS_TYPE,
} from '@/lib/job-end-ceiling-buffer-setting-names';

/**
 * Minutes of slack after `step_5_completed_at` / `actual_end_time` when deciding whether to keep GPS vineyard EXIT
 * as step 3 (see `applyGpsGuardrails` in derived-steps).
 */
export async function getJobEndCeilingBufferMinutes(): Promise<number> {
  const rows = await query<{ settingvalue: string | null }>(
    `SELECT settingvalue FROM tbl_settings WHERE type = $1 AND settingname = $2 LIMIT 1`,
    [JOB_END_CEILING_BUFFER_SETTINGS_TYPE, JOB_END_CEILING_BUFFER_SETTING_NAME]
  );
  const raw = rows[0]?.settingvalue?.trim() ?? '';
  if (raw === '') return JOB_END_CEILING_BUFFER_DEFAULT_MINUTES;
  const n = parseInt(raw, 10);
  if (Number.isNaN(n) || n < 0) return JOB_END_CEILING_BUFFER_DEFAULT_MINUTES;
  return Math.min(24 * 60, n);
}

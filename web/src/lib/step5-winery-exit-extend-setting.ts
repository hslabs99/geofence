import { query } from '@/lib/db';
import {
  STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES,
  STEP5_EXTEND_WINERY_EXIT_SETTING_NAME,
  STEP5_EXTEND_WINERY_EXIT_SETTINGS_TYPE,
} from '@/lib/step5-winery-exit-extend-setting-names';

export {
  STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES,
  STEP5_EXTEND_WINERY_EXIT_SETTING_NAME,
  STEP5_EXTEND_WINERY_EXIT_SETTINGS_TYPE,
} from '@/lib/step5-winery-exit-extend-setting-names';

/**
 * Minutes after VWork job end (`step_5_completed_at` / `actual_end_time`) to search for and accept winery EXIT
 * as GPS step 5 when the driver tapped “job complete” before physically leaving (see `decideFinalSteps` / Part 1 fetch).
 */
export async function getStep5ExtendWineryExitMinutes(): Promise<number> {
  const rows = await query<{ settingvalue: string | null }>(
    `SELECT settingvalue FROM tbl_settings WHERE type = $1 AND settingname = $2 LIMIT 1`,
    [STEP5_EXTEND_WINERY_EXIT_SETTINGS_TYPE, STEP5_EXTEND_WINERY_EXIT_SETTING_NAME]
  );
  const raw = rows[0]?.settingvalue?.trim() ?? '';
  if (raw === '') return STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;
  const n = parseInt(raw, 10);
  if (Number.isNaN(n) || n < 0) return STEP5_EXTEND_WINERY_EXIT_DEFAULT_MINUTES;
  return Math.min(24 * 60, n);
}

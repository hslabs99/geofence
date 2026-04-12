import { query } from '@/lib/db';
import {
  VWORK_TT_LOAD_SIZE_SETTINGS_TYPE,
  VWORK_TT_LOAD_SIZE_SETTING_NAME,
  VWORK_TT_LOAD_SIZE_DEFAULT,
} from '@/lib/vwork-tt-load-size-setting-names';

/**
 * Numeric threshold for jobs that have loadsize > 0: loadsize strictly greater than this
 * gets trailermode TT; otherwise T. Rows with null or zero loadsize are not changed by the fix.
 */
export async function getTtLoadSizeThreshold(): Promise<number> {
  const rows = await query<{ settingvalue: string | null }>(
    `SELECT settingvalue FROM tbl_settings WHERE type = $1 AND settingname = $2 LIMIT 1`,
    [VWORK_TT_LOAD_SIZE_SETTINGS_TYPE, VWORK_TT_LOAD_SIZE_SETTING_NAME]
  );
  const raw = rows[0]?.settingvalue?.trim();
  if (raw == null || raw === '') return VWORK_TT_LOAD_SIZE_DEFAULT;
  const n = Number.parseFloat(raw.replace(',', '.'));
  if (!Number.isFinite(n) || n < 0) return VWORK_TT_LOAD_SIZE_DEFAULT;
  return n;
}

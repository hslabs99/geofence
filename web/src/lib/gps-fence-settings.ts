import { query } from '@/lib/db';
import {
  GPS_FENCE_SETTINGS_TYPE,
  GPS_STD_TIME_NAME,
  GPSPLUS_GROWTH_NAME,
  GPSPLUS_TIME_NAME,
} from './gps-fence-settings-names';

export type StepsPlusSettings = {
  bufferMeters: number;
  minDurationSeconds: number;
};

const DEFAULT_GROWTH_M = 10;

/** null = row missing or invalid (configure in Settings). */
export async function getGpsStdTimeSeconds(): Promise<number | null> {
  const rows = await query<{ settingvalue: string | null }>(
    `SELECT settingvalue FROM tbl_settings WHERE type = $1 AND settingname = $2 LIMIT 1`,
    [GPS_FENCE_SETTINGS_TYPE, GPS_STD_TIME_NAME]
  );
  const raw = rows[0]?.settingvalue?.trim() ?? '';
  if (raw === '') return null;
  const n = parseInt(raw, 10);
  if (Number.isNaN(n) || n < 0) return null;
  return Math.min(86400, n);
}

/**
 * Steps+ buffer and minimum segment duration from tbl_settings.
 * When GPSplusTime is unset, minDurationSeconds is 0 (all segments pass the filter until you set GPS+ time).
 */
export async function getStepsPlusSettings(): Promise<StepsPlusSettings> {
  const rows = await query<{ settingname: string; settingvalue: string | null }>(
    `SELECT settingname, settingvalue FROM tbl_settings
     WHERE type = $1 AND settingname IN ($2, $3)`,
    [GPS_FENCE_SETTINGS_TYPE, GPSPLUS_GROWTH_NAME, GPSPLUS_TIME_NAME]
  );

  let bufferMeters = DEFAULT_GROWTH_M;
  let minDurationSeconds = 0;

  for (const r of rows) {
    const raw = r.settingvalue?.trim() ?? '';
    if (raw === '') continue;
    const n = parseInt(raw, 10);
    if (Number.isNaN(n) || n < 0) continue;
    if (r.settingname === GPSPLUS_GROWTH_NAME) bufferMeters = n;
    if (r.settingname === GPSPLUS_TIME_NAME) minDurationSeconds = n;
  }

  return { bufferMeters, minDurationSeconds };
}

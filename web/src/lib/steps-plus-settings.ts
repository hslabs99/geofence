import { query } from '@/lib/db';

export const STEPS_PLUS_SETTING_TYPE = 'System';
/** PostGIS ST_Buffer distance (meters) for Steps+ / GPS+ vineyard fence expansion. */
export const GPSPLUS_GROWTH_NAME = 'GPSplusGrowth';
/** Minimum segment duration (seconds) to count as a stay for Steps+ / GPS+. */
export const GPSPLUS_TIME_NAME = 'GPSplusTime';

const DEFAULT_GROWTH_M = 10;
const DEFAULT_TIME_SEC = 300;

export type StepsPlusSettings = {
  bufferMeters: number;
  minDurationSeconds: number;
};

export async function getStepsPlusSettings(): Promise<StepsPlusSettings> {
  const rows = await query<{ settingname: string; settingvalue: string | null }>(
    `SELECT settingname, settingvalue FROM tbl_settings
     WHERE type = $1 AND settingname IN ($2, $3)`,
    [STEPS_PLUS_SETTING_TYPE, GPSPLUS_GROWTH_NAME, GPSPLUS_TIME_NAME]
  );

  let bufferMeters = DEFAULT_GROWTH_M;
  let minDurationSeconds = DEFAULT_TIME_SEC;

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

/**
 * Steps+ settings from tbl_settings (server-side).
 * Setting names: see gps-fence-settings-names.ts
 */
export {
  GPS_FENCE_SETTINGS_TYPE as STEPS_PLUS_SETTING_TYPE,
  GPSPLUS_GROWTH_NAME,
  GPSPLUS_TIME_NAME,
  GPS_STD_TIME_NAME,
} from './gps-fence-settings-names';

export { getGpsStdTimeSeconds, getStepsPlusSettings, type StepsPlusSettings } from './gps-fence-settings';

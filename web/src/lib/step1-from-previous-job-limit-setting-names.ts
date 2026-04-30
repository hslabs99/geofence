/** tbl_settings.type — safe for client (Admin → Settings). */
export const STEP1_FROM_PREVIOUS_JOB_LIMIT_SETTINGS_TYPE = 'System';

/**
 * Max minutes between previous job Step 5 time and this job’s actual_start_time for Step1(lastJobEnd) to apply.
 * tbl_settings.settingname — human label as stored in DB.
 */
export const STEP1_FROM_PREVIOUS_JOB_LIMIT_SETTING_NAME = 'Step 1 from previous Job Limit';

/** Default: 2 hours. */
export const STEP1_FROM_PREVIOUS_JOB_LIMIT_DEFAULT_MINUTES = 120;

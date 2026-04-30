export const SUMMARY_COLUMN_COLOR_SETTINGS_TYPE = 'System';

export const SUMMARY_COLUMN_COLOR_SETTING_NAMES = {
  step2: 'SummaryColumnColorStep2',
  step3: 'SummaryColumnColorStep3',
  step4: 'SummaryColumnColorStep4',
  step5: 'SummaryColumnColorStep5',
  travel: 'SummaryColumnColorTravel',
  inVineyard: 'SummaryColumnColorInVineyard',
  inWinery: 'SummaryColumnColorInWinery',
  total: 'SummaryColumnColorTotal',
} as const;

export type SummaryColumnColorKey = keyof typeof SUMMARY_COLUMN_COLOR_SETTING_NAMES;

export const SUMMARY_COLUMN_COLOR_DEFAULTS: Record<SummaryColumnColorKey, string> = {
  step2: '',
  step3: '',
  step4: '',
  step5: '',
  travel: '',
  inVineyard: '',
  inWinery: '',
  total: '',
};


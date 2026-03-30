-- Optional one-time seed: default ENTER/EXIT grace (GPSstdTime) and Steps+ min stay (GPSplusTime).
-- Run only if these keys are missing; ON CONFLICT preserves existing values.
INSERT INTO tbl_settings (type, settingname, settingvalue)
VALUES
  ('System', 'GPSstdTime', '300'),
  ('System', 'GPSplusTime', '300')
ON CONFLICT (type, settingname) DO NOTHING;

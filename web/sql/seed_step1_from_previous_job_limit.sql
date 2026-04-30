-- Step1(lastJobEnd): max minutes from previous job Step 5 to this job actual_start_time (default 2 hours).
INSERT INTO tbl_settings (type, settingname, settingvalue)
VALUES ('System', 'Step 1 from previous Job Limit', '120')
ON CONFLICT (type, settingname) DO NOTHING;

-- Step 5 GPS: search/accept winery EXIT up to this many minutes after VWork job completion (early “complete” tap).
INSERT INTO tbl_settings (type, settingname, settingvalue)
VALUES ('System', 'Step5ExtendWineryExit', '30')
ON CONFLICT (type, settingname) DO NOTHING;

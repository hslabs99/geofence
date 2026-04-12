-- Default TT Load Size threshold for trailermode data fix (tbl_settings).
-- Jobs with loadsize > this value → trailermode TT; otherwise T.

INSERT INTO tbl_settings (type, settingname, settingvalue)
VALUES ('System', 'TTLoadSize', '25.5')
ON CONFLICT (type, settingname) DO NOTHING;

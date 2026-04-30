-- Default: GPS step 3 (vineyard EXIT) may be kept up to this many minutes after VWork job end.
-- See derived-steps applyGpsGuardrails / Admin → Settings → Job end ceiling buffer (minutes).

INSERT INTO tbl_settings (type, settingname, settingvalue)
VALUES ('System', 'JobEndCeilingBuffer', '30')
ON CONFLICT (type, settingname) DO NOTHING;

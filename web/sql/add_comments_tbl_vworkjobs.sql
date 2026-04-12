-- vWork export "Comments" / "joblaml" → tbl_vworkjobs.comments (matches tbl_mappings VW → comments, dbmaxlength 1000).

ALTER TABLE tbl_vworkjobs ADD COLUMN IF NOT EXISTS comments VARCHAR(1000);

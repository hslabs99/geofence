-- Step4→5 migration flag and step 5 text fields (run if not already applied).
-- step4to5: 0 = not migrated, 1 = migrated (even if synthetic step_4_completed_at stayed null).

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS step4to5 smallint NOT NULL DEFAULT 0;

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS step_5_name varchar(100);

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS step_5_address varchar(500);

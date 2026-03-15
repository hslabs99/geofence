-- Add calc_notes to tbl_vworkjobs for method notes (e.g. VineFence+ when step 2/3 from buffered fence).
-- Run once. Safe to run if column already exists (IF NOT EXISTS not supported for columns in older PG; use DO block or ignore error).

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS calc_notes text;

COMMENT ON COLUMN tbl_vworkjobs.calc_notes IS 'Appended method notes e.g. VineFence+: when step 2/3 were set from buffered vineyard fence.';

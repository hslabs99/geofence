-- Add calcnotes to tbl_vworkjobs for method notes (e.g. VineFence+ when step 2/3 from buffered fence).
-- Column name is calcnotes (no underscore). Run once.
-- If you previously added calc_notes, rename: ALTER TABLE tbl_vworkjobs RENAME COLUMN calc_notes TO calcnotes;

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS calcnotes text;

COMMENT ON COLUMN tbl_vworkjobs.calcnotes IS 'Appended method notes e.g. VineFence+: when step 2/3 were set from buffered vineyard fence.';

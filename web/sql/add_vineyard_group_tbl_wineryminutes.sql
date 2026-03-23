-- Add vineyardgroup to tbl_wineryminutes (after Template). From tbl_vworkjobs.vineyard_group.
-- Limits are now by customer, template, vineyardgroup, winery, TT.
-- Run once if the column is missing.

ALTER TABLE tbl_wineryminutes
  ADD COLUMN IF NOT EXISTS vineyardgroup VARCHAR(255);

COMMENT ON COLUMN tbl_wineryminutes.vineyardgroup IS 'Vineyard group from tbl_vworkjobs.vineyard_group; distinct values for dropdown. Match with job.vineyard_group when evaluating limits in Summary.';

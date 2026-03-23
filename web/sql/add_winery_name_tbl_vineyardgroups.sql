-- Add winery_name to tbl_vineyardgroups (from tbl_vworkjobs.delivery_winery).
-- Run once if the table was created without this column.

ALTER TABLE tbl_vineyardgroups
  ADD COLUMN IF NOT EXISTS winery_name text;

COMMENT ON COLUMN tbl_vineyardgroups.winery_name IS 'Winery (delivery_winery from tbl_vworkjobs).';

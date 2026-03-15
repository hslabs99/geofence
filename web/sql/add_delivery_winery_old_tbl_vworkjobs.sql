-- Add delivery_winery_old to tbl_vworkjobs so "Run Fixes" (wine name fixes) can store the previous value before updating delivery_winery.
-- Run once. Safe to run if column already exists.

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS delivery_winery_old text;

COMMENT ON COLUMN tbl_vworkjobs.delivery_winery_old IS 'Previous delivery_winery before last Run Fixes (winery name fix) application.';

-- Add Template column to tbl_wineryminutes (between Customer and Winery)
ALTER TABLE tbl_wineryminutes
  ADD COLUMN IF NOT EXISTS "Template" VARCHAR(255);

COMMENT ON COLUMN tbl_wineryminutes."Template" IS 'Template from tbl_vworkjobs; dropdown filters by Customer then Template then delivery_winery.';

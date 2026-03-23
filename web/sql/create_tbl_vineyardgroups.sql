-- Vineyard groups: map vineyard (and optional winery) to a group for reporting/aggregation.
-- Run in your SQL client against the same DB as the app.
-- If you already created the table without winery_name, run add_winery_name_tbl_vineyardgroups.sql instead.

CREATE TABLE IF NOT EXISTS tbl_vineyardgroups (
  id             serial PRIMARY KEY,
  winery_name    text,
  vineyard_name  text NOT NULL,
  vineyard_group text NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tbl_vineyardgroups_vineyard_name
  ON tbl_vineyardgroups (vineyard_name);

CREATE INDEX IF NOT EXISTS idx_tbl_vineyardgroups_winery_name
  ON tbl_vineyardgroups (winery_name);

COMMENT ON TABLE tbl_vineyardgroups IS 'Vineyard grouping: map winery/vineyard to vineyard_group (winery from tbl_vworkjobs.delivery_winery).';

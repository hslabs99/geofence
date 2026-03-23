-- Add vineyard_group to tbl_vworkjobs. Populated by Data Checks > Varied > Update Vineyard_Group
-- (set to NA then overwritten from tbl_vineyardgroups where winery/vineyard match).
-- Run once if the column is missing.

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS vineyard_group text;

COMMENT ON COLUMN tbl_vworkjobs.vineyard_group IS 'Group from tbl_vineyardgroups (winery+vineyard match); NA when no match. Set via Data Checks > Update Vineyard_Group.';

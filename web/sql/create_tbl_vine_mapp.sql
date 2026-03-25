-- Vineyard name mapping: map old vwork vineyard name to new name (normalise tbl_vworkjobs.vineyard_name).
-- Run in your SQL client against the same DB as the app.

CREATE TABLE IF NOT EXISTS tbl_vine_mapp (
  id            serial PRIMARY KEY,
  oldvworkname  text NOT NULL,
  newvworkname  text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tbl_vine_mapp_oldvworkname
  ON tbl_vine_mapp (oldvworkname);

COMMENT ON TABLE tbl_vine_mapp IS 'Vineyard name fixes: map old work vineyard name to new name.';

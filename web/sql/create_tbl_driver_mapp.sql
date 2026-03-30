-- Driver (worker) name mapping: map old vwork worker name to new name (normalise tbl_vworkjobs.worker).
-- Run in your SQL client against the same DB as the app.

CREATE TABLE IF NOT EXISTS tbl_driver_mapp (
  id            serial PRIMARY KEY,
  oldvworkname  text NOT NULL,
  newvworkname  text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tbl_driver_mapp_oldvworkname
  ON tbl_driver_mapp (oldvworkname);

COMMENT ON TABLE tbl_driver_mapp IS 'Driver name fixes: map old work worker name to new name.';

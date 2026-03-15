-- Winery name mapping: map old work name to new work name (e.g. for normalising winery names).
-- Run in your SQL client against the same DB as the app.

CREATE TABLE IF NOT EXISTS tbl_wine_mapp (
  id            serial PRIMARY KEY,
  oldvworkname  text NOT NULL,
  newvworkname  text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tbl_wine_mapp_oldvworkname
  ON tbl_wine_mapp (oldvworkname);

COMMENT ON TABLE tbl_wine_mapp IS 'Winery name fixes: map old work name to new work name (usage TBD).';

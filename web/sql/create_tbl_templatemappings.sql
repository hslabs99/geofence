-- NA template display labels for Summary → Time Limits and By Job vineyard group column.
-- When tbl_vworkjobs.vineyard_group is NA, Summary shows textmask for the row's template if a row exists here.
-- Run once to create the table.

CREATE TABLE IF NOT EXISTS tbl_templatemappings (
  id SERIAL PRIMARY KEY,
  template VARCHAR(512) NOT NULL,
  textmask VARCHAR(1024) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_tbl_templatemappings_template UNIQUE (template)
);

COMMENT ON TABLE tbl_templatemappings IS 'Map vWork template name to display text when vineyard_group is NA in Summary.';
COMMENT ON COLUMN tbl_templatemappings.template IS 'tbl_vworkjobs.template value (trimmed match).';
COMMENT ON COLUMN tbl_templatemappings.textmask IS 'Label to show instead of NA in Summary Time Limits / By Job.';

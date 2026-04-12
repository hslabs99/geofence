-- Admin Data Checks → SQL Updates: named SQL snippets with explicit run order.
-- Apply in your DB before using /admin/data-checks?tab=sql-updates

CREATE TABLE IF NOT EXISTS tbl_sqlruns (
  id               serial PRIMARY KEY,
  sql_name         text NOT NULL,
  sql_command      text NOT NULL,
  sql_description  text,
  run_order        integer NOT NULL DEFAULT 0,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tbl_sqlruns_run_order ON tbl_sqlruns (run_order);

COMMENT ON TABLE tbl_sqlruns IS 'Configurable SQL snippets for admin data checks (order = run_order).';
COMMENT ON COLUMN tbl_sqlruns.sql_name IS 'Short label for the snippet.';
COMMENT ON COLUMN tbl_sqlruns.sql_command IS 'SQL executed by the app when runs are invoked.';
COMMENT ON COLUMN tbl_sqlruns.run_order IS 'Lower numbers run first; reorder via admin UI.';

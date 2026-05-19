-- Inspect job history: append-only pin rows (note IS NOT NULL) plus rolling last 10 unpinned opens (note IS NULL).
-- Display fields (winery, vineyard, worker, actual_start_time) come from tbl_vworkjobs via job_id.
-- Run in your SQL client against the same DB as the app.

CREATE TABLE IF NOT EXISTS tbl_inspect_history (
  id         serial PRIMARY KEY,
  job_id     text NOT NULL,
  note       text NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tbl_inspect_history_created_at
  ON tbl_inspect_history (created_at DESC);

COMMENT ON TABLE tbl_inspect_history IS 'Append-only pin rows (non-null note) plus last 10 unpinned opens in Inspect; join to tbl_vworkjobs for delivery_winery, vineyard_name, worker, actual_start_time.';

-- Optional memo when pinning a job from Inspect ("Pin job"); shown in sidebar Recent jobs.
-- Run against the same DB as the app (safe to run once).

ALTER TABLE tbl_inspect_history ADD COLUMN IF NOT EXISTS note text;

COMMENT ON COLUMN tbl_inspect_history.note IS 'Optional pin memo from Inspect; shown under the job in Recent jobs.';

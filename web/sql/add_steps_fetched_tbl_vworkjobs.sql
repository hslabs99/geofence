-- Add steps_fetched and steps_fetched_when so Step 4 (Fetch GPS steps) can skip already-fetched jobs and Reset Steps can clear the flag.
-- Run in your SQL client (e.g. Heidi) against the same DB as the app.

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS steps_fetched boolean DEFAULT false,
  ADD COLUMN IF NOT EXISTS steps_fetched_when timestamp;

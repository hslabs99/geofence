-- Add updated_at to tbl_geofences so we can see when a fence was last updated (e.g. on KML re-import).
-- Run in your SQL client against the same DB as the app.

ALTER TABLE tbl_geofences
  ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

-- Backfill existing rows so they have a value
UPDATE tbl_geofences SET updated_at = now() WHERE updated_at IS NULL;

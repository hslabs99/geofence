-- Tracksolid platform geofence sync: tbl_geofences.source_name and stg_tracksolid_geofences (with radius_meters for circles).
-- Run in your SQL client against the same DB as the app.

-- 1) Allow distinguishing KML vs Tracksolid fences; match by (source_name, fence_name) in merge.
ALTER TABLE tbl_geofences
  ADD COLUMN IF NOT EXISTS source_name TEXT NULL;

-- 2) Staging table for Tracksolid fence list before merge. radius_meters used to buffer circle center to polygon.
CREATE TABLE IF NOT EXISTS public.stg_tracksolid_geofences (
  source_name TEXT NOT NULL,
  source_fence_id TEXT NOT NULL,
  fence_name TEXT NOT NULL,
  fence_type TEXT NOT NULL,
  color TEXT,
  description TEXT,
  geom_wkt TEXT NOT NULL,
  radius_meters NUMERIC NULL,
  PRIMARY KEY (source_name, source_fence_id)
);

-- If table already existed without radius_meters, add it.
ALTER TABLE public.stg_tracksolid_geofences
  ADD COLUMN IF NOT EXISTS radius_meters NUMERIC NULL;

-- 3) Platform last-modified time when API provides it (update_time / last_modified). Used to avoid overwriting with older data.
ALTER TABLE public.stg_tracksolid_geofences
  ADD COLUMN IF NOT EXISTS source_updated_at TIMESTAMPTZ NULL;

ALTER TABLE tbl_geofences
  ADD COLUMN IF NOT EXISTS source_updated_at TIMESTAMPTZ NULL;

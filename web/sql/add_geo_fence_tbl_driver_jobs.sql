-- Geofence names on a driver job. Later these map to tbl_geofences lookups.
ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS winery_geo_fence text;
ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS vineyard_geo_fence text;

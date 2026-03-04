-- Fix tbl_tracking.geom: set proper PostGIS type and allow NULL
-- Run manually in your DB (psql or SQL client). Requires: CREATE EXTENSION IF NOT EXISTS postgis;

-- 1) Allow NULL (drop NOT NULL constraint)
ALTER TABLE tbl_tracking
  ALTER COLUMN geom DROP NOT NULL;

-- 2) Set explicit geometry type so it no longer shows as UNKNOWN
ALTER TABLE tbl_tracking
  ALTER COLUMN geom TYPE geometry(Point, 4326)
  USING (
    CASE
      WHEN geom IS NOT NULL THEN ST_SetSRID(ST_Force2D(geom::geometry), 4326)
      ELSE NULL
    END
  );

-- If step 2 fails (e.g. "cannot cast type unknown to geometry"):
-- Option A: Add new column, backfill from lat/lon, drop old, rename:
--   ALTER TABLE tbl_tracking ADD COLUMN geom_new geometry(Point, 4326);
--   UPDATE tbl_tracking SET geom_new = ST_SetSRID(ST_MakePoint(lon, lat), 4326) WHERE lat IS NOT NULL AND lon IS NOT NULL;
--   ALTER TABLE tbl_tracking DROP COLUMN geom;
--   ALTER TABLE tbl_tracking RENAME COLUMN geom_new TO geom;

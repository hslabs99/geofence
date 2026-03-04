-- Fix tbl_tracking.geom: set proper PostGIS type and allow NULL
-- Run this in your DB (e.g. psql or your SQL client). Requires: CREATE EXTENSION IF NOT EXISTS postgis;

-- 1) Allow NULL (drop NOT NULL constraint)
ALTER TABLE tbl_tracking
  ALTER COLUMN geom DROP NOT NULL;

-- 2) Set explicit geometry type so it no longer shows as UNKNOWN
--    Use ST_Force2D if you have 3D/4D geometries; otherwise ST_SetSRID(geom, 4326) or just geom
ALTER TABLE tbl_tracking
  ALTER COLUMN geom TYPE geometry(Point, 4326)
  USING (
    CASE
      WHEN geom IS NOT NULL THEN ST_SetSRID(ST_Force2D(geom::geometry), 4326)
      ELSE NULL
    END
  );

-- If the USING above fails (e.g. "cannot cast type unknown to geometry"), try:
-- ALTER TABLE tbl_tracking
--   ALTER COLUMN geom TYPE geometry(Point, 4326)
--   USING ST_SetSRID(ST_MakePoint(0, 0), 4326);  -- only if you're okay overwriting existing geom
-- Then run the DROP NOT NULL in step 1 first.
-- Fix store_fences_for_date: attempted=true but mapped=false when GPS is inside geofences.
-- Common causes: SRID mismatch (geom with no/wrong SRID), or 3D/4D geoms. We force 4326 and 2D for the containment check.
--
-- DIAGNOSTIC (run once to verify): pick one tracking row that should be inside a fence and check SRID + containment.
-- Replace the id and run:
/*
SELECT
  t.id,
  ST_AsText(t.geom) AS tracking_point,
  ST_SRID(t.geom) AS tracking_srid,
  ST_GeometryType(t.geom) AS tracking_type,
  g.fence_id,
  g.fence_name,
  ST_SRID(g.geom) AS fence_srid,
  ST_GeometryType(g.geom) AS fence_type,
  ST_Contains(g.geom, t.geom) AS contains_raw,
  ST_Within(ST_Force2D(ST_SetSRID(t.geom, 4326)), ST_Force2D(ST_SetSRID(g.geom, 4326))) AS within_normalized
FROM tbl_tracking t
CROSS JOIN tbl_geofences g
WHERE t.id = 12345
  AND t.geom IS NOT NULL
LIMIT 5;
*/

CREATE OR REPLACE FUNCTION store_fences_for_date(
  p_date              date,
  p_only_unattempted  boolean DEFAULT true
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_updated integer;
BEGIN
  WITH day_rows AS (
    SELECT t.id,
           ( SELECT g.fence_id
             FROM tbl_geofences g
             WHERE ST_Within(
               ST_Force2D(ST_SetSRID(t.geom, 4326)),
               ST_Force2D(ST_SetSRID(g.geom, 4326))
             )
             ORDER BY ST_Area(g.geom) ASC
             LIMIT 1
           ) AS assigned_fence_id
    FROM tbl_tracking t
    WHERE t.geom IS NOT NULL
      AND (t.position_time_nz::date = p_date)
      AND (NOT p_only_unattempted OR t.geofence_attempted IS NULL OR t.geofence_attempted = false)
  )
  UPDATE tbl_tracking t
  SET
    geofence_id       = d.assigned_fence_id,
    geofence_mapped   = (d.assigned_fence_id IS NOT NULL),
    geofence_attempted = true
  FROM day_rows d
  WHERE t.id = d.id;

  GET DIAGNOSTICS v_updated = ROW_COUNT;
  RETURN v_updated;
END;
$$;

-- store_fences_for_date_scoped: same as store_fences_for_date but with scope control.
-- p_force_update: true = all rows for the date; false = respect p_only_missed.
-- p_only_missed: when true (and p_force_update false), only rows where geofence_attempted = true and geofence_mapped = false (pick up new fence hits).

CREATE OR REPLACE FUNCTION store_fences_for_date_scoped(
  p_date         date,
  p_force_update boolean DEFAULT false,
  p_only_missed  boolean DEFAULT false
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_updated INTEGER;
BEGIN
  WITH day_rows AS (
    SELECT t.id,
           (
             SELECT g.fence_id
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
      AND t.position_time_nz::date = p_date
      AND (
        p_force_update
        OR (p_only_missed AND t.geofence_attempted = true AND (t.geofence_id IS NULL OR t.geofence_mapped = false))
        OR (NOT p_force_update AND NOT p_only_missed AND (t.geofence_attempted IS NULL OR t.geofence_attempted = false))
      )
  )
  UPDATE tbl_tracking t
  SET
    geofence_id       = COALESCE(d.assigned_fence_id, t.geofence_id),
    geofence_mapped   = (d.assigned_fence_id IS NOT NULL),
    geofence_attempted = true
  FROM day_rows d
  WHERE t.id = d.id;

  GET DIAGNOSTICS v_updated = ROW_COUNT;
  RETURN v_updated;
END;
$$;

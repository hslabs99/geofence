-- Run this script once to create the function (e.g. in Cloud SQL or local Postgres).
-- check_buffered_fence_stays: read-only. Finds contiguous periods (any duration) that device
-- was inside a buffered geofence. Uses tbl_tracking (position_time_nz, lon, lat) and tbl_geofences.
-- Caller filters duration_seconds >= 300 for "stays" and aggregates max(duration_seconds) per fence for log.
-- Does not modify any table.

CREATE OR REPLACE FUNCTION check_buffered_fence_stays(
  p_device_id     text,
  p_start_time     timestamptz,
  p_end_time       timestamptz,
  p_fence_names    text[],
  p_buffer_meters  numeric DEFAULT 10
)
RETURNS TABLE(
  fence_name       text,
  enter_time       timestamptz,
  exit_time        timestamptz,
  duration_seconds numeric
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN QUERY
  WITH pts AS (
    SELECT t.position_time_nz AS t, t.lon, t.lat
    FROM tbl_tracking t
    WHERE t.device_name = p_device_id
      AND t.position_time_nz > p_start_time
      AND t.position_time_nz < p_end_time
      AND t.lon IS NOT NULL
      AND t.lat IS NOT NULL
  ),
  fence_buffered AS (
    SELECT g.fence_name, ST_Buffer(ST_Transform(ST_Force2D(g.geom), 3857), p_buffer_meters) AS buf
    FROM tbl_geofences g
    WHERE g.fence_name = ANY(p_fence_names)
      AND g.geom IS NOT NULL
  ),
  points_with_inside AS (
    SELECT p.t, f.fence_name,
      ST_Within(
        ST_Transform(ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 3857),
        f.buf
      ) AS inside
    FROM pts p
    CROSS JOIN fence_buffered f
  ),
  with_grp AS (
    SELECT t, fence_name, inside,
      sum(CASE WHEN NOT inside THEN 1 ELSE 0 END) OVER (PARTITION BY fence_name ORDER BY t) AS grp
    FROM points_with_inside
  ),
  segments AS (
    SELECT
      fence_name,
      min(t) AS enter_time,
      max(t) AS exit_time,
      EXTRACT(EPOCH FROM (max(t) - min(t)))::numeric AS duration_seconds
    FROM with_grp
    WHERE inside
    GROUP BY fence_name, grp
  )
  SELECT s.fence_name, s.enter_time, s.exit_time, s.duration_seconds
  FROM segments s;
END;
$$;

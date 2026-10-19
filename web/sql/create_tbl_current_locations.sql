-- Append-only current GPS reads. One row per truck per TrackSolid poll.
-- gpstime is the API gpsTime string, stored verbatim. polltime is the clock time of that API call.

CREATE TABLE IF NOT EXISTS tbl_current_locations (
  id           serial PRIMARY KEY,
  truck_id     text,
  location     text,
  gpstime      text,
  polltime     text,
  coordinates  text,
  fencename    text,
  softfencename text,
  imei         text,
  gpstime_nz   timestamp
);

CREATE INDEX IF NOT EXISTS idx_tbl_current_locations_polltime
  ON tbl_current_locations (polltime DESC);

CREATE INDEX IF NOT EXISTS idx_tbl_current_locations_truck_lower
  ON tbl_current_locations (lower(truck_id), id DESC);

COMMENT ON TABLE tbl_current_locations IS 'Latest TrackSolid location reads, one new row per truck each poll.';

-- One row per TrackSolid HTTP call. The 24-hour window is stored in tbl_settings
-- (type TracksolidCallWindow). HK/EU/US accounts allow 8,640 calls in that window.
CREATE TABLE IF NOT EXISTS tbl_tracksolid_api_calls (
  id            bigserial PRIMARY KEY,
  called_at_ms  bigint NOT NULL,
  method        text NOT NULL,
  endpoint      text NOT NULL,
  ok            boolean NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tbl_tracksolid_api_calls_endpoint_at
  ON tbl_tracksolid_api_calls (endpoint, called_at_ms);

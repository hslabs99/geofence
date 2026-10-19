-- TrackSolid device IMEI for each current-location reading.
ALTER TABLE tbl_current_locations ADD COLUMN IF NOT EXISTS imei text;

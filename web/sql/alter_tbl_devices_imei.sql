-- Optional manual migration: IMEI from TrackSolid is merged at runtime by ensure + tbl-devices-sync.
ALTER TABLE tbl_devices ADD COLUMN IF NOT EXISTS imei text;

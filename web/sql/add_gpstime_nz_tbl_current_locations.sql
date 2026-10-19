-- NZ display time for current-location GPS readings.
-- gpstime stays the raw API string. gpstime_nz is filled only by SQL:
-- (left(gpstime, 19)::timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific/Auckland')
-- where gpstime_nz is null.
ALTER TABLE tbl_current_locations ADD COLUMN IF NOT EXISTS gpstime_nz timestamp;

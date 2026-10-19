-- GPS device IMEI for a driver job. truck_id stays the device name.
ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS imei text;

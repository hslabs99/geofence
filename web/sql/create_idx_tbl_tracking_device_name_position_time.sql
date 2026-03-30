-- Conflict key for idempotent GPS imports (insert-only, no deletes).
CREATE UNIQUE INDEX IF NOT EXISTS idx_tbl_tracking_device_name_position_time
  ON tbl_tracking (device_name, position_time);

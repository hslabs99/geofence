-- When the driver taps a gate, store that phone clock time. Reset job clears these.
ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_1_click timestamp without time zone;
ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_2_click timestamp without time zone;
ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_3_click timestamp without time zone;
ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_4_click timestamp without time zone;
ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_5_click timestamp without time zone;

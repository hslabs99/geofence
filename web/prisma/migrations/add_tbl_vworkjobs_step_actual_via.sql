-- Add calculated columns: Step_N_Actual_Time (single source of truth) and step_N_via ('GPS' or 'VW').
-- Run in Heidi (or any SQL client). Unquoted names become lowercase in PostgreSQL.

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS step_1_actual_time timestamp,
  ADD COLUMN IF NOT EXISTS step_1_via varchar(10),
  ADD COLUMN IF NOT EXISTS step_2_actual_time timestamp,
  ADD COLUMN IF NOT EXISTS step_2_via varchar(10),
  ADD COLUMN IF NOT EXISTS step_3_actual_time timestamp,
  ADD COLUMN IF NOT EXISTS step_3_via varchar(10),
  ADD COLUMN IF NOT EXISTS step_4_actual_time timestamp,
  ADD COLUMN IF NOT EXISTS step_4_via varchar(10),
  ADD COLUMN IF NOT EXISTS step_5_actual_time timestamp,
  ADD COLUMN IF NOT EXISTS step_5_via varchar(10);

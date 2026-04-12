-- One-shot: add vWork export columns that imports expect (matches `COLUMN_MAX_LENGTHS` in `src/db.ts` + seed `tbl_mappings`).
-- Safe to re-run: every column uses IF NOT EXISTS.
-- Run against `geodata` on Cloud SQL (e.g. Cloud Console SQL workspace or psql).

-- --- VARCHAR / text (mapped job fields) ---
ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS customer VARCHAR(100),
  ADD COLUMN IF NOT EXISTS template VARCHAR(50),
  ADD COLUMN IF NOT EXISTS worker VARCHAR(50),
  ADD COLUMN IF NOT EXISTS proof_of_delivery VARCHAR(100),
  ADD COLUMN IF NOT EXISTS progress_state VARCHAR(50),
  ADD COLUMN IF NOT EXISTS step_1_name VARCHAR(100),
  ADD COLUMN IF NOT EXISTS step_2_name VARCHAR(100),
  ADD COLUMN IF NOT EXISTS step_3_name VARCHAR(100),
  ADD COLUMN IF NOT EXISTS step_4_name VARCHAR(100),
  ADD COLUMN IF NOT EXISTS step_1_address VARCHAR(500),
  ADD COLUMN IF NOT EXISTS step_2_address VARCHAR(500),
  ADD COLUMN IF NOT EXISTS step_3_address VARCHAR(500),
  ADD COLUMN IF NOT EXISTS step_4_address VARCHAR(500),
  ADD COLUMN IF NOT EXISTS contains_mog VARCHAR(50),
  ADD COLUMN IF NOT EXISTS delivery_location_map VARCHAR(500),
  ADD COLUMN IF NOT EXISTS delivery_winery VARCHAR(200),
  ADD COLUMN IF NOT EXISTS vinename VARCHAR(100),
  ADD COLUMN IF NOT EXISTS driver_notes VARCHAR(1000),
  ADD COLUMN IF NOT EXISTS field_docket_number VARCHAR(50),
  ADD COLUMN IF NOT EXISTS field_notes VARCHAR(1000),
  ADD COLUMN IF NOT EXISTS pickup_location_map VARCHAR(500),
  ADD COLUMN IF NOT EXISTS trailer_rego VARCHAR(20),
  ADD COLUMN IF NOT EXISTS truck_id VARCHAR(20),
  ADD COLUMN IF NOT EXISTS truck_rego VARCHAR(20),
  ADD COLUMN IF NOT EXISTS comments VARCHAR(1000);

-- --- Numeric (Load size export) ---
ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS loadsize NUMERIC;

-- --- Timestamps (verbatim-style; no TZ conversion in app) ---
ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS planned_start_time TIMESTAMP WITHOUT TIME ZONE,
  ADD COLUMN IF NOT EXISTS gps_start_time TIMESTAMP WITHOUT TIME ZONE,
  ADD COLUMN IF NOT EXISTS gps_end_time TIMESTAMP WITHOUT TIME ZONE,
  ADD COLUMN IF NOT EXISTS step_1_completed_at TIMESTAMP WITHOUT TIME ZONE,
  ADD COLUMN IF NOT EXISTS step_2_completed_at TIMESTAMP WITHOUT TIME ZONE,
  ADD COLUMN IF NOT EXISTS step_3_completed_at TIMESTAMP WITHOUT TIME ZONE,
  ADD COLUMN IF NOT EXISTS step_4_completed_at TIMESTAMP WITHOUT TIME ZONE;

-- --- Integers / numeric used by importer (if missing) ---
ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS planned_duration_mins INTEGER,
  ADD COLUMN IF NOT EXISTS gps_duration_mins INTEGER,
  ADD COLUMN IF NOT EXISTS worker_duration_mins INTEGER,
  ADD COLUMN IF NOT EXISTS number_of_steps INTEGER,
  ADD COLUMN IF NOT EXISTS number_of_loads INTEGER,
  ADD COLUMN IF NOT EXISTS booking_id INTEGER;

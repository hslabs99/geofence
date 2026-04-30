-- Manual driving distance / minutes for winery–vineyard pairs (same pair key as tbl_distances).
--
-- tbl_distances       — main distances-module table (GPS harvest, rollups, Populate vWork baseline).
-- tbl_distances_manual — optional overrides: user-entered road distance/minutes when GPS did not yield data.
-- Merge rule in app: COALESCE(tbl_distances_manual.*, tbl_distances.*) per pair (lower(trim) match).
--
-- Run once against the same DB as the app.

CREATE TABLE IF NOT EXISTS tbl_distances_manual (
  id                serial PRIMARY KEY,
  delivery_winery   text NOT NULL,
  vineyard_name     text NOT NULL,
  distance_m        integer NOT NULL,
  duration_min      numeric,
  notes             text,
  updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tbl_distances_manual_pair
  ON tbl_distances_manual (lower(trim(delivery_winery)), lower(trim(vineyard_name)));

COMMENT ON TABLE tbl_distances_manual IS 'Manual overrides for tbl_distances pairs: user-entered drive m/min; merged over tbl_distances in Admin → Distances and Populate vWork.';
COMMENT ON COLUMN tbl_distances_manual.distance_m IS 'Driving distance in metres (e.g. from Google Maps directions).';
COMMENT ON COLUMN tbl_distances_manual.duration_min IS 'Optional drive time in minutes.';
COMMENT ON COLUMN tbl_distances_manual.notes IS 'Free text (e.g. source URL, date measured).';

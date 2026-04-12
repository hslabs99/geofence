-- Master winery → vineyard driving distance (m) and duration (min).
-- One row per (delivery_winery, vineyard_name). Seed from Admin → Distances.

CREATE TABLE IF NOT EXISTS tbl_distances (
  id              serial PRIMARY KEY,
  delivery_winery text NOT NULL,
  vineyard_name   text NOT NULL,
  distance_m      integer,
  duration_min    numeric(10, 2),
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (delivery_winery, vineyard_name)
);

CREATE INDEX IF NOT EXISTS idx_tbl_distances_winery
  ON tbl_distances (delivery_winery);

CREATE INDEX IF NOT EXISTS idx_tbl_distances_vineyard
  ON tbl_distances (vineyard_name);

COMMENT ON TABLE tbl_distances IS 'Driving distance (m) and duration (min) for winery–vineyard pairs that occur together on tbl_vworkjobs (seed), not all winery×vineyard combinations.';
COMMENT ON COLUMN tbl_distances.delivery_winery IS 'From tbl_vworkjobs.delivery_winery (trimmed when seeded).';
COMMENT ON COLUMN tbl_distances.vineyard_name IS 'From tbl_vworkjobs.vineyard_name (trimmed when seeded).';

-- Truck ids seen on a VWork account. The same truck id on Delegat and Others is two rows.
-- The app also creates this from ensure-vwork-job-feed-schema.ts.

CREATE TABLE IF NOT EXISTS vwork_truck_sources (
  id             bigserial PRIMARY KEY,
  account        varchar NOT NULL,
  truck_id       varchar NOT NULL,
  first_seen_at  text NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_vwork_truck_sources_account_truck
  ON vwork_truck_sources (account, lower(btrim(truck_id)));

COMMENT ON TABLE vwork_truck_sources IS 'Which VWork API a truck id came from: delegat or others.';

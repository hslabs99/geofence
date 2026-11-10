import { execute } from '@/lib/db';

let ensured = false;

/** Creates vwork_job_feed if missing. Safe to call repeatedly. */
export async function ensureVworkJobFeedSchema(): Promise<void> {
  if (ensured) return;
  await execute(`
    CREATE TABLE IF NOT EXISTS vwork_job_feed (
      id                       bigserial PRIMARY KEY,
      job_id                   varchar NOT NULL UNIQUE,
      customer                 varchar,
      template                 varchar,
      worker                   varchar,
      status                   varchar,
      third_party_id           varchar,
      planned_start_time       text,
      planned_duration         varchar,
      actual_start_time        text,
      worker_duration_mins     varchar,
      proof_of_delivery        varchar,
      number_of_steps          varchar,
      pod_signed_by            varchar,
      pod_signed_at            text,
      asset_id                 varchar,
      asset_name               varchar,
      customer_third_party_id  varchar,
      groups                   varchar,
      create_date              text,
      source_updated_at        text,
      step_1_name              varchar,
      step_1_completed_at      text,
      step_1_address           text,
      step_2_name              varchar,
      step_2_completed_at      text,
      step_2_address           text,
      step_3_name              varchar,
      step_3_completed_at      text,
      step_3_address           text,
      step_4_name              varchar,
      step_4_completed_at      text,
      step_4_address           text,
      step_5_name              varchar,
      step_5_completed_at      text,
      step_5_address           text,
      truck_id                 varchar,
      truck_rego               varchar,
      trailer_rego             varchar,
      driver_name              varchar,
      vineyard_name            varchar,
      pickup_location_map      text,
      field_docket_number      varchar,
      delivery_winery          varchar,
      delivery_location_map    text,
      weighbridge_weight       varchar,
      driver_notes             text,
      comments                 varchar,
      field_notes              varchar,
      contains_mog             varchar,
      number_of_loads          varchar,
      loadsize                 varchar,
      custom_fields            text,
      fetched_at               text NOT NULL
    )`);
  await execute(`ALTER TABLE vwork_job_feed ADD COLUMN IF NOT EXISTS account varchar`);
  await execute(`UPDATE vwork_job_feed SET account = 'delegat' WHERE account IS NULL OR btrim(account) = ''`);
  await execute(`ALTER TABLE vwork_job_feed ALTER COLUMN account SET DEFAULT 'delegat'`);
  await execute(`ALTER TABLE vwork_job_feed ALTER COLUMN account SET NOT NULL`);
  await execute(`ALTER TABLE vwork_job_feed ADD COLUMN IF NOT EXISTS group_name varchar`);
  await execute(`
    DELETE FROM vwork_job_feed a
    USING vwork_job_feed b
    WHERE a.job_id = b.job_id AND a.id > b.id`);
  await execute(`DROP INDEX IF EXISTS uq_vwork_job_feed_account_job_id`);
  await execute(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_vwork_job_feed_job_id
      ON vwork_job_feed (job_id)`);
  await execute(`
    CREATE INDEX IF NOT EXISTS idx_vwork_job_feed_fetched_at ON vwork_job_feed (fetched_at DESC)`);
  await execute(`
    CREATE TABLE IF NOT EXISTS vwork_truck_sources (
      id             bigserial PRIMARY KEY,
      account        varchar NOT NULL,
      truck_id       varchar NOT NULL,
      first_seen_at  text NOT NULL
    )`);
  await execute(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_vwork_truck_sources_account_truck
      ON vwork_truck_sources (account, lower(btrim(truck_id)))`);
  await execute(`
    INSERT INTO vwork_truck_sources (account, truck_id, first_seen_at)
    SELECT DISTINCT ON (account, lower(btrim(truck_id)))
           account,
           btrim(truck_id),
           fetched_at
    FROM vwork_job_feed
    WHERE truck_id IS NOT NULL AND btrim(truck_id) <> ''
    ORDER BY account, lower(btrim(truck_id)), id
    ON CONFLICT DO NOTHING`);
  await execute(`
    DELETE FROM vwork_truck_sources o
    USING vwork_truck_sources d
    WHERE o.account = 'others'
      AND d.account = 'delegat'
      AND lower(btrim(o.truck_id)) = lower(btrim(d.truck_id))
      AND NOT EXISTS (
        SELECT 1 FROM vwork_job_feed f
        WHERE f.account = 'others'
          AND lower(btrim(f.truck_id)) = lower(btrim(o.truck_id))
      )`);
  ensured = true;
}

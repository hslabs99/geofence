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
  await execute(`
    CREATE INDEX IF NOT EXISTS idx_vwork_job_feed_fetched_at ON vwork_job_feed (fetched_at DESC)`);
  ensured = true;
}

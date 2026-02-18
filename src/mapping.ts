/**
 * Maps normalized header names to tbl_vworkjobs column names.
 * Aligned with vwork1.xlsx headers.
 * Only maps to columns that exist in tbl_vworkjobs.
 */
export const HEADER_TO_COLUMN: Record<string, string> = {
  // Job & client
  'job id': 'job_id',
  'client name': 'customer',
  'when': 'planned_start_time',
  // Planned
  'planned start (d/m/y)': 'planned_start_time',
  'planned duration (hrs)': 'planned_duration_mins', // converted hrs->mins in parser
  'planned duration (mins)': 'planned_duration_mins',
  // GPS
  'gps start time': 'gps_start_time',
  'gps end time': 'gps_end_time',
  'gps duration (mins)': 'gps_duration_mins',
  // Progress
  'proof of delivery': 'proof_of_delivery',
  'progress of job': 'progress_state',
  'progress state': 'progress_state',
  'number of days': 'number_of_steps',
  'number of steps': 'number_of_steps',
  // Sign-off
  'signed by': 'worker',
  // Step 1-4
  'step 1 planner': 'step_1_name',
  'step 1 name': 'step_1_name',
  'step 1 completed at': 'step_1_completed_at',
  'step 1 address': 'step_1_address',
  'step 2 planner': 'step_2_name',
  'step 2 name': 'step_2_name',
  'step 2 completed at': 'step_2_completed_at',
  'step 2 address': 'step_2_address',
  'step 3 planner': 'step_3_name',
  'step 3 name': 'step_3_name',
  'step 3 completed at': 'step_3_completed_at',
  'step 3 address': 'step_3_address',
  'step 4 planner': 'step_4_name',
  'step 4 name': 'step_4_name',
  'step 4 completed at': 'step_4_completed_at',
  'step 4 address': 'step_4_address',
  // Audit & booking
  'audit_id': 'booking_id',
  'bookingid': 'booking_id',
  // Container & delivery
  'container mod': 'contains_mog',
  'contains mog': 'contains_mog',
  'delivery location (map)': 'delivery_location_map',
  'delivery location map': 'delivery_location_map',
  'delivery (when)': 'delivery_winery',
  'delivery winery': 'delivery_winery',
  // Notes & dockets
  'joblaml': 'comments',
  'driver notes': 'driver_notes',
  'help docket number': 'field_docket_number',
  'field docket number': 'field_docket_number',
  'pickup location (map)': 'pickup_location_map',
  'pickup location map': 'pickup_location_map',
  'further reqs': 'field_notes',
  'field notes': 'field_notes',
  'comments': 'comments',
  // Vehicle
  'vinename': 'vinename',
  'load size': 'load_size',
  'number of loads': 'number_of_loads',
  'trailer rego': 'trailer_rego',
  'truck id': 'truck_id',
  'truck rego': 'truck_rego',
  // Legacy
  'template': 'template',
  'worker': 'worker',
  'worker duration (mins)': 'worker_duration_mins',
};

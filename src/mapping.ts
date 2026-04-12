/**
 * Maps normalized header names to tbl_vworkjobs column names.
 * Aligned with vwork1.xlsx headers.
 * Only maps to columns that exist in tbl_vworkjobs.
 */
export const HEADER_TO_COLUMN: Record<string, string> = {
  // Job & client
  'job id': 'job_id',
  'client name': 'customer',
  /** Same as "Client name" — exports often use "Customer" (case-insensitive via normalizeHeader). */
  customer: 'customer',
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
  'load size': 'loadsize',
  'number of loads': 'number_of_loads',
  'trailer rego': 'trailer_rego',
  'truck id': 'truck_id',
  'truck rego': 'truck_rego',
  // Legacy
  'template': 'template',
  'worker': 'worker',
  'worker duration (mins)': 'worker_duration_mins',
};

/**
 * Merge hardcoded defaults with DB `tbl_mappings` (DB wins on key). Used only for ad-hoc tools;
 * **Drive vWork import uses `mappings.headerToColumn` from DB only** — no merge — so new CSV columns
 * never pick up stray hardcoded targets.
 */
export function mergeVworkHeaderToColumn(dbHeaderToColumn: Record<string, string>): Record<string, string> {
  return { ...HEADER_TO_COLUMN, ...dbHeaderToColumn };
}

/**
 * Keep only header→column pairs whose target `dbcolumnname` exists in `tbl_mappings` (VW active rows).
 * Stops hardcoded HEADER_TO_COLUMN targets from being used when that column was removed from `tbl_mappings`.
 */
export function filterHeaderToColumnByMappingTargets(
  headerToColumn: Record<string, string>,
  mappingDbcolumnNames: Set<string>
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [headerKey, col] of Object.entries(headerToColumn)) {
    const c = col.trim().toLowerCase().replace(/\s+/g, ' ').trim();
    if (c && mappingDbcolumnNames.has(c)) out[headerKey] = col;
  }
  return out;
}

/**
 * Maps normalized header names to tbl_gpsdata column names.
 * Handles truncated Excel headers (e.g. "evice nam" -> device_name, "ence nam" -> fence_name).
 */
export const GPS_HEADER_TO_COLUMN: Record<string, string> = {
  'no.': 'no',
  'no': 'no',
  'evice nam': 'device_name',
  'device name': 'device_name',
  'imei': 'imei',
  'model': 'model',
  'driver name': 'driver_name',
  'number plate': 'number_plate',
  'sim': 'sim',
  'phone': 'phone',
  'ence nam': 'fence_name',
  'fence name': 'fence_name',
  'enter time': 'enter_time',
  'outer time': 'outer_time',
  'out time': 'outer_time',
  'exit time': 'outer_time',
  'stay time': 'stay_time',
};

/**
 * Maps normalized header names to tbl_tracking column names.
 * Hardcoded for Track Details XLS (e.g. Track Details_20260223095117.xls).
 */
export const TRACKING_HEADER_TO_COLUMN: Record<string, string> = {
  'no.': 'device_no',
  'no': 'device_no',
  'device name': 'device_name',
  'imei': 'imei',
  'model': 'model',
  'ignition': 'ignition',
  'position time': 'position_time',
  'speed': 'speed_raw',
  'azimuth': 'azimuth',
  'position type': 'position_type',
  'no. of satellites': 'satellites',
  'data type': 'data_type',
  'coordinates': 'coordinates', // special: parsed to lat, lon, geom
  'address': 'address',
};

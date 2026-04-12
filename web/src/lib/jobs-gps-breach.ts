/**
 * GPS job row breach check — pure logic only (no DB / pg). Safe to import from Client Components.
 */

export type JobsGpsReportRowBreachInput = {
  incomplete: boolean;
  critical_null_nz: boolean;
  gap_missing: boolean;
  gps_from: string | null;
  minutes_after_gps_from_to_first_fix: number | null;
  min_gps_nz: string | null;
  gps_to: string | null;
  minutes_from_last_fix_to_gps_to: number | null;
  max_gps_nz: string | null;
};

/**
 * True when the job row breaches GPS coverage limits (edge x/y vs threshold, internal gap, critical NZ null)
 * or has an incomplete window — same rule as the server report.
 */
export function jobsGpsRowBreachesLimits(r: JobsGpsReportRowBreachInput, thr: number): boolean {
  if (r.incomplete) return true;
  if (r.critical_null_nz) return true;
  if (r.gap_missing) return true;
  const x = r.minutes_after_gps_from_to_first_fix;
  const fromBreach = r.gps_from != null && (x != null ? x > thr : r.min_gps_nz == null);
  const y = r.minutes_from_last_fix_to_gps_to;
  const toBreach = r.gps_to != null && (y != null ? y > thr : r.max_gps_nz == null);
  return fromBreach || toBreach;
}

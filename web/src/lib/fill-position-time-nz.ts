import { execute } from '@/lib/db';

/**
 * VERBATIM TIMES — allowed derivation for display/ordering only.
 * Fills position_time_nz from position_time (UTC-naive → Pacific/Auckland, DST-aware) where nz is still null (e.g. after merge).
 */
export async function fillPositionTimeNzNulls(): Promise<number> {
  return execute(`
    UPDATE public.tbl_tracking
    SET position_time_nz = (position_time AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific/Auckland')
    WHERE position_time IS NOT NULL AND position_time_nz IS NULL
  `);
}

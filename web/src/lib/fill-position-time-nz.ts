import { execute } from '@/lib/db';

/**
 * VERBATIM TIMES — allowed derivation for display/ordering only.
 * Fills position_time_nz from position_time (+13h) where nz is still null (e.g. after merge).
 */
export async function fillPositionTimeNzNulls(): Promise<number> {
  return execute(`
    UPDATE public.tbl_tracking
    SET position_time_nz = position_time + interval '13 hours'
    WHERE position_time IS NOT NULL AND position_time_nz IS NULL
  `);
}

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';
import { findMatchingWineryMinuteRow, type WineryMinuteLimitRow } from '@/lib/wineryminutes-limit-match';

const LIMIT_COLS = `id, "Customer", "Template", vineyardgroup, "Winery", "TT", "ToVineMins", "InVineMins", "ToWineMins", "InWineMins", "TotalMins"`;

type ComboRow = {
  customer: string;
  template: string;
  delivery_winery: string;
  vineyard_group: string;
  job_tt: string;
  job_count: number;
};

export type LimitException = ComboRow;

/**
 * GET: vworkjobs key combinations (customer, template, winery, vineyard_group, TT) that have no matching tbl_wineryminutes row
 * (same rules as Summary: exact vineyard group; TT or TTT fallback for T/TT).
 */
export async function GET() {
  try {
    const limits = await query<WineryMinuteLimitRow>(`SELECT ${LIMIT_COLS} FROM tbl_wineryminutes`);

    const combos = await query<ComboRow>(
      `SELECT
         trim(customer) AS customer,
         trim(COALESCE(template, '')) AS template,
         trim(delivery_winery) AS delivery_winery,
         trim(COALESCE(vineyard_group, '')) AS vineyard_group,
         trim(COALESCE(trailermode::text, '')) AS job_tt,
         COUNT(*)::int AS job_count
       FROM tbl_vworkjobs
       WHERE trim(COALESCE(delivery_winery, '')) <> ''
       GROUP BY 1, 2, 3, 4, 5`
    );

    const exceptions: LimitException[] = [];
    for (const c of combos) {
      const m = findMatchingWineryMinuteRow(limits, {
        customer: c.customer,
        template: c.template,
        winery: c.delivery_winery,
        vineyardGroup: c.vineyard_group,
        jobTT: c.job_tt,
      });
      if (!m) exceptions.push(c);
    }

    exceptions.sort((a, b) => b.job_count - a.job_count);

    return NextResponse.json({
      exceptions,
      comboCount: combos.length,
      exceptionCount: exceptions.length,
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

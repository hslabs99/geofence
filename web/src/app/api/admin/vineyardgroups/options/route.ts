import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/** GET: distinct delivery_winery and vineyard_name from tbl_vworkjobs for dropdowns/datalists */
export async function GET() {
  try {
    const [deliveryWineries, vineyardNames] = await Promise.all([
      query<{ delivery_winery: string | null }>(
        `SELECT DISTINCT delivery_winery FROM tbl_vworkjobs
         WHERE delivery_winery IS NOT NULL AND trim(delivery_winery) != ''
         ORDER BY delivery_winery`
      ),
      query<{ vineyard_name: string | null }>(
        `SELECT DISTINCT vineyard_name FROM tbl_vworkjobs
         WHERE vineyard_name IS NOT NULL AND trim(vineyard_name) != ''
         ORDER BY vineyard_name`
      ),
    ]);
    return NextResponse.json({
      deliveryWineries: deliveryWineries.map((r) => r.delivery_winery).filter(Boolean) as string[],
      vineyardNames: vineyardNames.map((r) => r.vineyard_name).filter(Boolean) as string[],
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

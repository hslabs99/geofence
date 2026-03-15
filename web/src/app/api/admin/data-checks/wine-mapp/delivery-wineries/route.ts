import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/** GET: distinct delivery_winery from tbl_vworkjobs. Optional ?customer=X&template=Y to filter by Customer and Template. */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const customer = searchParams.get('customer')?.trim() ?? '';
    const template = searchParams.get('template')?.trim() ?? '';

    let rows: { delivery_winery: string | null }[];

    if (customer && template) {
      try {
        rows = await query<{ delivery_winery: string | null }>(
          `SELECT DISTINCT delivery_winery FROM tbl_vworkjobs
           WHERE "Customer" IS NOT NULL AND trim("Customer") = $1 AND "Template" IS NOT NULL AND trim("Template") = $2
             AND delivery_winery IS NOT NULL AND trim(delivery_winery) != ''
           ORDER BY delivery_winery`,
          [customer, template]
        );
      } catch {
        rows = await query<{ delivery_winery: string | null }>(
          `SELECT DISTINCT delivery_winery FROM tbl_vworkjobs
           WHERE trim(customer) = $1 AND trim(template) = $2
             AND delivery_winery IS NOT NULL AND trim(delivery_winery) != ''
           ORDER BY delivery_winery`,
          [customer, template]
        );
      }
    } else if (customer) {
      try {
        rows = await query<{ delivery_winery: string | null }>(
          `SELECT DISTINCT delivery_winery FROM tbl_vworkjobs
           WHERE "Customer" IS NOT NULL AND trim("Customer") = $1
             AND delivery_winery IS NOT NULL AND trim(delivery_winery) != ''
           ORDER BY delivery_winery`,
          [customer]
        );
      } catch {
        rows = await query<{ delivery_winery: string | null }>(
          `SELECT DISTINCT delivery_winery FROM tbl_vworkjobs
           WHERE trim(customer) = $1 AND delivery_winery IS NOT NULL AND trim(delivery_winery) != ''
           ORDER BY delivery_winery`,
          [customer]
        );
      }
    } else {
      rows = await query<{ delivery_winery: string | null }>(
        `SELECT DISTINCT delivery_winery FROM tbl_vworkjobs
         WHERE delivery_winery IS NOT NULL AND trim(delivery_winery) != ''
         ORDER BY delivery_winery`
      );
    }

    const list = rows.map((r) => r.delivery_winery).filter(Boolean) as string[];
    return NextResponse.json({ rows: list });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

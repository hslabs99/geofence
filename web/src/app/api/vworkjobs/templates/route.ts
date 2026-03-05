import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/** GET: distinct Template values from tbl_vworkjobs. Optional ?customer=X to filter by customer (same as summary page). */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const customer = searchParams.get('customer')?.trim() ?? '';

    type Row = { template: string | null };
    let rows: Row[];

    if (customer) {
      try {
        rows = await query<Row>(
          `SELECT DISTINCT trim("Template") AS template
           FROM tbl_vworkjobs
           WHERE "Customer" IS NOT NULL AND trim("Customer") = $1 AND "Template" IS NOT NULL AND trim("Template") <> ''
           ORDER BY 1`,
          [customer]
        );
      } catch {
        try {
          rows = await query<Row>(
            `SELECT DISTINCT trim(template) AS template
             FROM tbl_vworkjobs
             WHERE customer IS NOT NULL AND trim(customer) = $1 AND template IS NOT NULL AND trim(template) <> ''
             ORDER BY 1`,
            [customer]
          );
        } catch {
          rows = [];
        }
      }
    } else {
      try {
        rows = await query<Row>(
          `SELECT DISTINCT trim("Template") AS template
           FROM tbl_vworkjobs
           WHERE "Template" IS NOT NULL AND trim("Template") <> ''
           ORDER BY 1`
        );
      } catch {
        try {
          rows = await query<Row>(
            `SELECT DISTINCT trim(template) AS template
             FROM tbl_vworkjobs
             WHERE template IS NOT NULL AND trim(template) <> ''
             ORDER BY 1`
          );
        } catch {
          rows = [];
        }
      }
    }

    const templates = rows.map((r) => r.template).filter(Boolean) as string[];
    return NextResponse.json({ templates });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

/** GET: distinct Customer values from tbl_vworkjobs for assigning to Client users. */
export async function GET() {
  try {
    const rows = await prisma.$queryRaw<{ customer: string | null }[]>`
      SELECT DISTINCT trim("Customer") AS customer
      FROM tbl_vworkjobs
      WHERE "Customer" IS NOT NULL AND trim("Customer") <> ''
      ORDER BY 1
    `;
    const customers = rows.map((r) => r.customer).filter(Boolean) as string[];
    return NextResponse.json({ customers });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (msg.includes('42703') || msg.includes('does not exist')) {
      try {
        const rowsLower = await prisma.$queryRaw<{ customer: string | null }[]>`
          SELECT DISTINCT trim(customer) AS customer
          FROM tbl_vworkjobs
          WHERE customer IS NOT NULL AND trim(customer) <> ''
          ORDER BY 1
        `;
        const customers = rowsLower.map((r) => r.customer).filter(Boolean) as string[];
        return NextResponse.json({ customers });
      } catch {
        /* fallthrough */
      }
    }
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

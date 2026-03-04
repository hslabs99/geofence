import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

export async function GET() {
  try {
    const [fenceNames, deliveryWineries, vineyardNames] = await Promise.all([
      prisma.$queryRaw<{ fence_name: string | null }[]>`
        SELECT DISTINCT fence_name FROM tbl_gpsdata
        WHERE fence_name IS NOT NULL AND fence_name != ''
        ORDER BY fence_name
      `,
      prisma.$queryRaw<{ delivery_winery: string | null }[]>`
        SELECT DISTINCT delivery_winery FROM tbl_vworkjobs
        WHERE delivery_winery IS NOT NULL AND delivery_winery != ''
        ORDER BY delivery_winery
      `,
      prisma.$queryRaw<{ vineyard_name: string | null }[]>`
        SELECT DISTINCT vineyard_name FROM tbl_vworkjobs
        WHERE vineyard_name IS NOT NULL AND vineyard_name != ''
        ORDER BY vineyard_name
      `,
    ]);
    return NextResponse.json({
      fenceNames: fenceNames.map((r) => r.fence_name).filter(Boolean) as string[],
      deliveryWineries: deliveryWineries.map((r) => r.delivery_winery).filter(Boolean) as string[],
      vineyardNames: vineyardNames.map((r) => r.vineyard_name).filter(Boolean) as string[],
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

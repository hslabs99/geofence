import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const sortBy = (searchParams.get('sort') ?? 'fence_name').toLowerCase();
    const order = (searchParams.get('order') ?? 'asc').toLowerCase();
    const orderDir = order === 'desc' ? 'DESC' : 'ASC';
    const allowedSort = ['fence_name', 'fence_id'];
    const sortColumn = allowedSort.includes(sortBy) ? sortBy : 'fence_name';

    const rows = await query<{ fence_id: number; fence_name: string | null }>(
      `SELECT fence_id, fence_name FROM tbl_geofences ORDER BY ${sortColumn} ${orderDir}`
    );

    return NextResponse.json(
      rows.map((r) => ({
        fence_id: r.fence_id,
        fence_name: r.fence_name ?? '',
      }))
    );
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

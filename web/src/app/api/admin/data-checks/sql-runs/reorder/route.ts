import { NextResponse } from 'next/server';
import { getClient } from '@/lib/db';

/**
 * Move one row up or down in run_order by renumbering 0..n-1 in sort order.
 */
export async function POST(request: Request) {
  const client = await getClient();
  try {
    const body = await request.json();
    const { id, direction } = body as { id?: unknown; direction?: unknown };
    const rowId = typeof id === 'number' ? id : parseInt(String(id ?? ''), 10);
    if (isNaN(rowId)) {
      return NextResponse.json({ error: 'id required' }, { status: 400 });
    }
    if (direction !== 'up' && direction !== 'down') {
      return NextResponse.json({ error: 'direction must be up or down' }, { status: 400 });
    }

    await client.query('BEGIN');
    const list = await client.query<{ id: number }>(
      `SELECT id FROM tbl_sqlruns ORDER BY run_order ASC, id ASC`
    );
    const ids = list.rows.map((r) => r.id);
    const idx = ids.indexOf(rowId);
    if (idx === -1) {
      await client.query('ROLLBACK');
      return NextResponse.json({ error: 'Not found' }, { status: 404 });
    }
    const j = direction === 'up' ? idx - 1 : idx + 1;
    if (j < 0 || j >= ids.length) {
      await client.query('ROLLBACK');
      return NextResponse.json({ ok: true, unchanged: true });
    }
    const next = [...ids];
    [next[idx], next[j]] = [next[j], next[idx]];
    for (let i = 0; i < next.length; i++) {
      await client.query(`UPDATE tbl_sqlruns SET run_order = $1, updated_at = now() WHERE id = $2`, [
        i,
        next[i],
      ]);
    }
    await client.query('COMMIT');
    return NextResponse.json({ ok: true });
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* ignore */
    }
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  } finally {
    client.release();
  }
}

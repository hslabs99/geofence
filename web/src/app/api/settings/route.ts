import { NextResponse } from 'next/server';
import { query, getClient } from '@/lib/db';

/**
 * Settings API
 * - GET ?type=&name=         → newest single value for (type, settingname) + duplicate count.
 * - GET ?type=&name=&all=1   → ALL rows for (type, settingname) (diagnostic — Settings inspector).
 * - PUT body {type, settingname, settingvalue}
 *     → atomic DELETE-then-INSERT inside a transaction, so any pre-existing
 *       duplicate rows are removed and exactly one row remains. This is
 *       robust whether or not tbl_settings has a UNIQUE(type, settingname)
 *       constraint — previous implementations used ON CONFLICT which only
 *       works when the constraint exists and silently no-ops when not.
 */

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const type = searchParams.get('type');
    const name = searchParams.get('name');
    const all = searchParams.get('all') === '1' || searchParams.get('all') === 'true';
    if (!type || !name) {
      return NextResponse.json({ error: 'type and name required' }, { status: 400 });
    }

    if (all) {
      // Return every matching row so the admin Settings inspector can spot
      // duplicates. ctid::text exposes Postgres' physical row id (always present)
      // so users can identify individual rows.
      const rows = await query<{ settingvalue: string | null; ctid: string }>(
        `SELECT settingvalue, ctid::text AS ctid
         FROM tbl_settings
         WHERE type = $1 AND settingname = $2
         ORDER BY ctid DESC`,
        [type, name]
      );
      return NextResponse.json({ rows });
    }

    // Newest row wins. Without an updated_at column we rely on ctid DESC which
    // tracks Postgres physical insertion order — combined with PUT's
    // DELETE-then-INSERT this gives a deterministic latest-write-wins result.
    const rows = await query<{ settingvalue: string | null }>(
      `SELECT settingvalue
       FROM tbl_settings
       WHERE type = $1 AND settingname = $2
       ORDER BY ctid DESC
       LIMIT 1`,
      [type, name]
    );
    const countRows = await query<{ cnt: string | number }>(
      `SELECT COUNT(*)::text AS cnt FROM tbl_settings WHERE type = $1 AND settingname = $2`,
      [type, name]
    );
    const raw = countRows[0]?.cnt;
    const duplicateCount = typeof raw === 'number' ? raw : parseInt(String(raw ?? '0'), 10) || 0;

    const row = rows[0];
    if (!row || row.settingvalue == null) {
      return NextResponse.json({ settingvalue: null, duplicateCount });
    }
    return NextResponse.json({ settingvalue: row.settingvalue, duplicateCount });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.error('[api/settings GET] Error:', message, err instanceof Error ? err.stack : '');
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function PUT(request: Request) {
  try {
    const body = await request.json().catch(() => ({}));
    const type = (body as { type?: string }).type;
    const name = (body as { settingname?: string }).settingname;
    const value = (body as { settingvalue?: string | null }).settingvalue ?? null;
    if (!type || typeof type !== 'string' || !name || typeof name !== 'string') {
      return NextResponse.json(
        { error: 'type and settingname required', received: { type, settingname: name } },
        { status: 400 }
      );
    }
    const valueStr = value == null ? null : String(value);

    // DELETE-then-INSERT in a transaction so the table can never end up with
    // multiple rows for the same (type, settingname). Fixes the "saved value
    // does not persist after refresh" bug, which was caused by duplicate
    // rows + a GET that returned an arbitrary one.
    const client = await getClient();
    let deletedCount = 0;
    try {
      await client.query('BEGIN');
      const del = await client.query(
        'DELETE FROM tbl_settings WHERE type = $1 AND settingname = $2',
        [type, name]
      );
      deletedCount = del.rowCount ?? 0;
      await client.query(
        'INSERT INTO tbl_settings (type, settingname, settingvalue) VALUES ($1, $2, $3)',
        [type, name, valueStr]
      );
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }

    return NextResponse.json({ ok: true, deletedDuplicates: Math.max(0, deletedCount - 1) });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.error('[api/settings PUT] Error:', message, err instanceof Error ? err.stack : '');
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

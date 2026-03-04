import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const type = searchParams.get('type');
    const name = searchParams.get('name');
    if (!type || !name) {
      return NextResponse.json({ error: 'type and name required' }, { status: 400 });
    }
    const rows = await query<{ settingvalue: string | null }>(
      'SELECT settingvalue FROM tbl_settings WHERE type = $1 AND settingname = $2 LIMIT 1',
      [type, name]
    );
    const row = rows[0];
    if (!row || row.settingvalue == null) {
      return NextResponse.json({ settingvalue: null });
    }
    return NextResponse.json({ settingvalue: row.settingvalue });
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
      return NextResponse.json({ error: 'type and settingname required', received: { type, settingname: name } }, { status: 400 });
    }
    const valueStr = value == null ? null : String(value);
    await execute(
      `INSERT INTO tbl_settings (type, settingname, settingvalue)
       VALUES ($1, $2, $3)
       ON CONFLICT (type, settingname) DO UPDATE SET settingvalue = EXCLUDED.settingvalue`,
      [type, name, valueStr]
    );
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.error('[api/settings PUT] Error:', message, err instanceof Error ? err.stack : '');
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

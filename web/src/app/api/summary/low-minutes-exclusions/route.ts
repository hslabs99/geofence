import { NextResponse } from 'next/server';
import { execute, query } from '@/lib/db';

type Row = { vineyard_name: string };

export async function GET() {
  try {
    const rows = await query<Row>(
      `SELECT vineyard_name
       FROM tbl_low_minutes_highlight_exclusions
       ORDER BY vineyard_name ASC`,
    );
    return NextResponse.json({ vineyards: rows.map((r) => r.vineyard_name) });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.error('[api/summary/low-minutes-exclusions GET] Error:', message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function PUT(request: Request) {
  try {
    const body = await request.json().catch(() => ({}));
    const vineyards = (body as { vineyards?: unknown }).vineyards;
    if (!Array.isArray(vineyards) || vineyards.some((v) => typeof v !== 'string')) {
      return NextResponse.json({ error: 'vineyards must be a string[]' }, { status: 400 });
    }
    const cleaned = Array.from(
      new Set(
        (vineyards as string[])
          .map((v) => v.trim())
          .filter((v) => v.length > 0)
          .slice(0, 5000),
      ),
    );

    await execute('DELETE FROM tbl_low_minutes_highlight_exclusions');
    for (const v of cleaned) {
      await execute(
        `INSERT INTO tbl_low_minutes_highlight_exclusions (vineyard_name)
         VALUES ($1)
         ON CONFLICT (vineyard_name) DO NOTHING`,
        [v],
      );
    }
    return NextResponse.json({ ok: true, count: cleaned.length });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // eslint-disable-next-line no-console
    console.error('[api/summary/low-minutes-exclusions PUT] Error:', message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}


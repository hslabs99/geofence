import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

const TABLE_NAME = 'tbl_vworkjobs';

function parseOrder(body: unknown): string[] | null {
  if (!body || typeof body !== 'object' || !Array.isArray((body as { columnOrder?: unknown }).columnOrder))
    return null;
  const arr = (body as { columnOrder: unknown[] }).columnOrder;
  if (!arr.every((x) => typeof x === 'string')) return null;
  return arr as string[];
}

function parseHidden(body: unknown): string[] | null {
  if (!body || typeof body !== 'object') return null;
  const arr = (body as { hiddenColumns?: unknown }).hiddenColumns;
  if (!Array.isArray(arr) || !arr.every((x) => typeof x === 'string')) return null;
  return arr as string[];
}

function parseStored(raw: string): { columnOrder: string[]; hiddenColumns: string[] } {
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return {
        columnOrder: parsed.every((x: unknown) => typeof x === 'string') ? parsed : [],
        hiddenColumns: [],
      };
    }
    if (parsed && typeof parsed === 'object') {
      const order = Array.isArray(parsed.columnOrder) && parsed.columnOrder.every((x: unknown) => typeof x === 'string')
        ? parsed.columnOrder
        : [];
      const hidden = Array.isArray(parsed.hiddenColumns) && parsed.hiddenColumns.every((x: unknown) => typeof x === 'string')
        ? parsed.hiddenColumns
        : [];
      return { columnOrder: order, hiddenColumns: hidden };
    }
  } catch {}
  return { columnOrder: [], hiddenColumns: [] };
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const table = searchParams.get('table') ?? TABLE_NAME;
    const rows = await query<{ column_order: string }>(
      'SELECT column_order FROM tbl_table_column_order WHERE table_name = $1',
      [table]
    );
    const row = rows[0];
    if (!row) return NextResponse.json({ columnOrder: null, hiddenColumns: null });
    const { columnOrder, hiddenColumns } = parseStored(row.column_order);
    return NextResponse.json({
      columnOrder: columnOrder.length ? columnOrder : null,
      hiddenColumns: hiddenColumns.length ? hiddenColumns : null,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function PUT(request: Request) {
  try {
    const body = await request.json().catch(() => ({}));
    const table = (body as { table?: string }).table ?? TABLE_NAME;
    const columnOrder = parseOrder(body);
    if (!columnOrder || columnOrder.length === 0)
      return NextResponse.json({ error: 'columnOrder must be a non-empty string array' }, { status: 400 });
    const hiddenColumns = parseHidden(body) ?? [];
    const stored = JSON.stringify({ columnOrder, hiddenColumns });
    await execute(
      `INSERT INTO tbl_table_column_order (table_name, column_order)
       VALUES ($1, $2)
       ON CONFLICT (table_name) DO UPDATE SET column_order = EXCLUDED.column_order, updated_at = NOW()`,
      [table, stored]
    );
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

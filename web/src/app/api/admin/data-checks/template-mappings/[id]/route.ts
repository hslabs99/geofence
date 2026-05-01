import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

type TemplateMappingRow = {
  id: number;
  template: string;
  textmask: string;
  created_at: string;
};

export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const rows = await query<TemplateMappingRow>(
      `SELECT id, template, textmask, created_at::text FROM tbl_templatemappings WHERE id = $1`,
      [idNum]
    );
    if (rows.length === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json(rows[0]);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

export async function PUT(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const body = await request.json();
    const template = (body.template ?? '').toString().trim();
    const textmask = (body.textmask ?? '').toString().trim();
    if (!template || !textmask) {
      return NextResponse.json({ error: 'template and textmask are required' }, { status: 400 });
    }

    const n = await execute(
      `UPDATE tbl_templatemappings SET template = $1, textmask = $2 WHERE id = $3`,
      [template, textmask, idNum]
    );
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/unique|duplicate/i.test(msg)) {
      return NextResponse.json({ error: 'A mapping for this template already exists' }, { status: 409 });
    }
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

export async function DELETE(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const n = await execute(`DELETE FROM tbl_templatemappings WHERE id = $1`, [idNum]);
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

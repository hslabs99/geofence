import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export type TemplateMappingRow = {
  id: number;
  template: string;
  textmask: string;
  created_at: string;
};

/** GET: list all template → NA display mappings */
export async function GET() {
  try {
    const rows = await query<TemplateMappingRow>(
      `SELECT id, template, textmask, created_at::text
       FROM tbl_templatemappings
       ORDER BY template`
    );
    return NextResponse.json({ rows });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** POST: create mapping */
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const template = (body.template ?? '').toString().trim();
    const textmask = (body.textmask ?? '').toString().trim();
    if (!template || !textmask) {
      return NextResponse.json({ error: 'template and textmask are required' }, { status: 400 });
    }

    const result = await query<{ id: number }>(
      `INSERT INTO tbl_templatemappings (template, textmask)
       VALUES ($1, $2)
       RETURNING id`,
      [template, textmask]
    );
    const id = result[0]?.id;
    if (id == null) return NextResponse.json({ error: 'Insert failed' }, { status: 500 });
    return NextResponse.json({ id, ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/unique|duplicate/i.test(msg)) {
      return NextResponse.json({ error: 'A mapping for this template already exists' }, { status: 409 });
    }
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

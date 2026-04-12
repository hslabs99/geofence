import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export type StepRuleNoteRow = {
  id: number;
  ruled: string;
  ruledesc: string | null;
  level: string;
  rulenotes: string | null;
  created_at: string;
  updated_at: string;
};

export async function GET() {
  try {
    const rows = await query<StepRuleNoteRow>(
      `SELECT id, ruled, ruledesc, level, rulenotes, created_at, updated_at
       FROM tbl_step_rule_notes
       ORDER BY level ASC, ruled ASC`
    );
    return NextResponse.json({ rows });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { ruled, ruledesc, level, rulenotes } = body as {
      ruled?: string;
      ruledesc?: string;
      level?: string;
      rulenotes?: string;
    };
    if (!ruled || typeof ruled !== 'string' || !ruled.trim()) {
      return NextResponse.json({ error: 'ruled required' }, { status: 400 });
    }
    if (!level || (level !== 'Winery' && level !== 'Vineyard')) {
      return NextResponse.json({ error: 'level must be Winery or Vineyard' }, { status: 400 });
    }
    const inserted = await query<StepRuleNoteRow>(
      `INSERT INTO tbl_step_rule_notes (ruled, ruledesc, level, rulenotes)
       VALUES ($1, $2, $3, $4)
       RETURNING id, ruled, ruledesc, level, rulenotes, created_at, updated_at`,
      [
        ruled.trim(),
        ruledesc != null && String(ruledesc).trim() !== '' ? String(ruledesc).trim() : null,
        level,
        rulenotes != null && String(rulenotes).trim() !== '' ? String(rulenotes).trim() : null,
      ]
    );
    const row = inserted[0];
    if (!row) return NextResponse.json({ error: 'Insert failed' }, { status: 500 });
    return NextResponse.json({ row });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

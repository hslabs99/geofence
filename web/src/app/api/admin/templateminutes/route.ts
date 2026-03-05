import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export type TemplateMinutesRow = {
  id: number;
  Customer: string | null;
  Template: string | null;
  ToVineMins: number | null;
  InVineMins: number | null;
  ToWineMins: number | null;
  InWineMins: number | null;
  TotalMins: number | null;
};

/** GET: list all rows, or single row when ?customer=X&template=Y */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const customer = searchParams.get('customer')?.trim() ?? '';
    const template = searchParams.get('template')?.trim() ?? '';

    if (customer && template) {
      const rows = await query<TemplateMinutesRow>(
        `SELECT id, "Customer", "Template", "ToVineMins", "InVineMins", "ToWineMins", "InWineMins", "TotalMins"
         FROM tbl_templateminutes
         WHERE trim("Customer") = $1 AND trim("Template") = $2
         LIMIT 1`,
        [customer, template]
      );
      return NextResponse.json(rows[0] ?? null);
    }

    const rows = await query<TemplateMinutesRow>(
      `SELECT id, "Customer", "Template", "ToVineMins", "InVineMins", "ToWineMins", "InWineMins", "TotalMins"
       FROM tbl_templateminutes
       ORDER BY "Customer", "Template"`
    );
    return NextResponse.json({ rows });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** POST: create a new record */
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const Customer = (body.Customer ?? body.customer ?? '').trim() || null;
    const Template = (body.Template ?? body.template ?? '').trim() || null;
    const ToVineMins = body.ToVineMins != null ? Number(body.ToVineMins) : null;
    const InVineMins = body.InVineMins != null ? Number(body.InVineMins) : null;
    const ToWineMins = body.ToWineMins != null ? Number(body.ToWineMins) : null;
    const InWineMins = body.InWineMins != null ? Number(body.InWineMins) : null;
    const TotalMins = body.TotalMins != null ? Number(body.TotalMins) : null;

    const result = await query<{ id: number }>(
      `INSERT INTO tbl_templateminutes ("Customer", "Template", "ToVineMins", "InVineMins", "ToWineMins", "InWineMins", "TotalMins")
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING id`,
      [Customer, Template, ToVineMins, InVineMins, ToWineMins, InWineMins, TotalMins]
    );
    const id = result[0]?.id;
    if (id == null) return NextResponse.json({ error: 'Insert failed' }, { status: 500 });
    return NextResponse.json({ id, ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

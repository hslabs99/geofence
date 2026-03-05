import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

type TemplateMinutesRow = {
  id: number;
  Customer: string | null;
  Template: string | null;
  ToVineMins: number | null;
  InVineMins: number | null;
  ToWineMins: number | null;
  InWineMins: number | null;
  TotalMins: number | null;
};

/** GET: single record by id */
export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const rows = await query<TemplateMinutesRow>(
      `SELECT id, "Customer", "Template", "ToVineMins", "InVineMins", "ToWineMins", "InWineMins", "TotalMins"
       FROM tbl_templateminutes WHERE id = $1`,
      [idNum]
    );
    if (rows.length === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json(rows[0]);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** PUT: update record */
export async function PUT(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const body = await request.json();
    const Customer = (body.Customer ?? body.customer ?? '').trim() || null;
    const Template = (body.Template ?? body.template ?? '').trim() || null;
    const ToVineMins = body.ToVineMins != null ? Number(body.ToVineMins) : null;
    const InVineMins = body.InVineMins != null ? Number(body.InVineMins) : null;
    const ToWineMins = body.ToWineMins != null ? Number(body.ToWineMins) : null;
    const InWineMins = body.InWineMins != null ? Number(body.InWineMins) : null;
    const TotalMins = body.TotalMins != null ? Number(body.TotalMins) : null;

    const n = await execute(
      `UPDATE tbl_templateminutes
       SET "Customer" = $1, "Template" = $2, "ToVineMins" = $3, "InVineMins" = $4, "ToWineMins" = $5, "InWineMins" = $6, "TotalMins" = $7
       WHERE id = $8`,
      [Customer, Template, ToVineMins, InVineMins, ToWineMins, InWineMins, TotalMins, idNum]
    );
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** DELETE: remove record */
export async function DELETE(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const n = await execute(`DELETE FROM tbl_templateminutes WHERE id = $1`, [idNum]);
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

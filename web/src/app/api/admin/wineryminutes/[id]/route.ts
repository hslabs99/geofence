import { NextResponse } from 'next/server';
import { query, execute } from '@/lib/db';

type WineryMinutesRow = {
  id: number;
  Customer: string | null;
  Template: string | null;
  Winery: string | null;
  TT: string | null;
  ToVineMins: number | null;
  InVineMins: number | null;
  ToWineMins: number | null;
  InWineMins: number | null;
  TotalMins: number | null;
};

const SELECT_COLS = `id, "Customer", "Template", "Winery", "TT", "ToVineMins", "InVineMins", "ToWineMins", "InWineMins", "TotalMins"`;

/** GET: single record by id */
export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const idNum = parseInt(id, 10);
    if (Number.isNaN(idNum)) return NextResponse.json({ error: 'Invalid id' }, { status: 400 });

    const rows = await query<WineryMinutesRow>(
      `SELECT ${SELECT_COLS} FROM tbl_wineryminutes WHERE id = $1`,
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
    const Winery = (body.Winery ?? body.winery ?? '').trim() || null;
    const TT = (body.TT ?? body.tt ?? '').trim() || null;
    if (TT !== null && TT !== '' && TT !== 'T' && TT !== 'TT' && TT !== 'TTT') {
      return NextResponse.json({ error: 'TT must be T, TT, or TTT' }, { status: 400 });
    }
    const ToVineMins = body.ToVineMins != null ? Number(body.ToVineMins) : null;
    const InVineMins = body.InVineMins != null ? Number(body.InVineMins) : null;
    const ToWineMins = body.ToWineMins != null ? Number(body.ToWineMins) : null;
    const InWineMins = body.InWineMins != null ? Number(body.InWineMins) : null;
    const TotalMins = body.TotalMins != null ? Number(body.TotalMins) : null;

    const n = await execute(
      `UPDATE tbl_wineryminutes
       SET "Customer" = $1, "Template" = $2, "Winery" = $3, "TT" = $4, "ToVineMins" = $5, "InVineMins" = $6, "ToWineMins" = $7, "InWineMins" = $8, "TotalMins" = $9
       WHERE id = $10`,
      [Customer, Template, Winery, TT, ToVineMins, InVineMins, ToWineMins, InWineMins, TotalMins, idNum]
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

    const n = await execute(`DELETE FROM tbl_wineryminutes WHERE id = $1`, [idNum]);
    if (n === 0) return NextResponse.json({ error: 'Not found' }, { status: 404 });
    return NextResponse.json({ ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export type WineryMinutesRow = {
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

/** GET: list all rows; or rows for customer when ?customer=X; or single row when ?customer=X&template=Y&winery=Z */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const customer = searchParams.get('customer')?.trim() ?? '';
    const template = searchParams.get('template')?.trim() ?? '';
    const winery = searchParams.get('winery')?.trim() ?? '';

    if (customer && template && winery) {
      const rows = await query<WineryMinutesRow>(
        `SELECT ${SELECT_COLS} FROM tbl_wineryminutes
         WHERE trim("Customer") = $1 AND trim(COALESCE("Template", '')) = $2 AND trim("Winery") = $3
         LIMIT 1`,
        [customer, template, winery]
      );
      return NextResponse.json(rows[0] ?? null);
    }

    if (customer) {
      if (template) {
        const rows = await query<WineryMinutesRow>(
          `SELECT ${SELECT_COLS} FROM tbl_wineryminutes
           WHERE trim("Customer") = $1 AND trim(COALESCE("Template", '')) = $2
           ORDER BY "Winery", "TT"`,
          [customer, template]
        );
        return NextResponse.json({ rows });
      }
      const rows = await query<WineryMinutesRow>(
        `SELECT ${SELECT_COLS} FROM tbl_wineryminutes
         WHERE trim("Customer") = $1
         ORDER BY "Template", "Winery", "TT"`,
        [customer]
      );
      return NextResponse.json({ rows });
    }

    const rows = await query<WineryMinutesRow>(
      `SELECT ${SELECT_COLS} FROM tbl_wineryminutes
       ORDER BY "Customer", "Template", "Winery"`
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

    const result = await query<{ id: number }>(
      `INSERT INTO tbl_wineryminutes ("Customer", "Template", "Winery", "TT", "ToVineMins", "InVineMins", "ToWineMins", "InWineMins", "TotalMins")
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       RETURNING id`,
      [Customer, Template, Winery, TT, ToVineMins, InVineMins, ToWineMins, InWineMins, TotalMins]
    );
    const id = result[0]?.id;
    if (id == null) return NextResponse.json({ error: 'Insert failed' }, { status: 500 });
    return NextResponse.json({ id, ok: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

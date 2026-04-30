import { NextResponse } from 'next/server';
import * as XLSX from 'xlsx';
import { query } from '@/lib/db';

export const runtime = 'nodejs';

type Row = {
  Customer: string | null;
  Template: string | null;
  vineyardgroup: string | null;
  Winery: string | null;
  TT: string | null;
  ToVineMins: number | null;
  InVineMins: number | null;
  ToWineMins: number | null;
  InWineMins: number | null;
  TotalMins: number | null;
};

const EXPORT_COLS = `"Customer", "Template", vineyardgroup, "Winery", "TT", "ToVineMins", "InVineMins", "ToWineMins", "InWineMins", "TotalMins"`;

/** GET: download all winery minutes as .xlsx (same column names as import). */
export async function GET() {
  try {
    const rows = await query<Row>(
      `SELECT ${EXPORT_COLS} FROM tbl_wineryminutes
       ORDER BY "Customer", "Template", "Winery", vineyardgroup, "TT"`
    );

    const sheetRows = rows.map((r) => ({
      Customer: r.Customer ?? '',
      Template: r.Template ?? '',
      Winery: r.Winery ?? '',
      'Vineyard group': r.vineyardgroup ?? '',
      TT: r.TT ?? '',
      ToVineMins: r.ToVineMins ?? '',
      InVineMins: r.InVineMins ?? '',
      ToWineMins: r.ToWineMins ?? '',
      InWineMins: r.InWineMins ?? '',
      TotalMins: r.TotalMins ?? '',
    }));

    const ws = XLSX.utils.json_to_sheet(sheetRows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Winery minutes');
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' }) as Buffer;

    const day = new Date().toISOString().slice(0, 10);
    return new NextResponse(new Uint8Array(buf), {
      status: 200,
      headers: {
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': `attachment; filename="winery-minutes-${day}.xlsx"`,
      },
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

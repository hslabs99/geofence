import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export async function GET() {
  try {
    const rows = await query('SELECT * FROM tbl_gpsmappings ORDER BY id ASC');
    return NextResponse.json({ rows });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { type, vwname, gpsname } = body as {
      type?: string;
      vwname?: string;
      gpsname?: string;
    };
    if (!type || typeof type !== 'string' || !type.trim()) {
      return NextResponse.json({ error: 'type required' }, { status: 400 });
    }
    if (!vwname || typeof vwname !== 'string' || !vwname.trim()) {
      return NextResponse.json({ error: 'vwname required' }, { status: 400 });
    }
    if (!gpsname || typeof gpsname !== 'string' || !gpsname.trim()) {
      return NextResponse.json({ error: 'gpsname required' }, { status: 400 });
    }
    const inserted = await query(
      `INSERT INTO tbl_gpsmappings (type, vwname, gpsname) VALUES ($1, $2, $3) RETURNING *`,
      [type.trim(), vwname.trim(), gpsname.trim()]
    );
    const row = inserted[0];
    if (!row) return NextResponse.json({ error: 'Insert failed' }, { status: 500 });
    return NextResponse.json({ row });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

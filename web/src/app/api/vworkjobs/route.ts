import { NextResponse } from 'next/server';
import { getPool } from '@/lib/db';

export async function GET() {
  try {
    const pool = getPool();
    const res = await pool.query('SELECT * FROM tbl_vworkjobs ORDER BY job_id LIMIT 500');
    return NextResponse.json({ rows: res.rows });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

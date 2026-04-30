import { NextResponse } from 'next/server';
import { execute, query } from '@/lib/db';

type PutBody = {
  delivery_winery?: string;
  vineyard_name?: string;
  distance_m?: number;
  duration_min?: number | string | null;
  notes?: string | null;
};

function normPair(w: string, v: string) {
  return {
    dw: w.trim(),
    vn: v.trim(),
  };
}

/** PUT: upsert manual distance for a winery–vineyard pair (trimmed names). */
export async function PUT(request: Request) {
  try {
    const body = (await request.json()) as PutBody;
    const { dw, vn } = normPair(String(body.delivery_winery ?? ''), String(body.vineyard_name ?? ''));
    if (!dw || !vn) {
      return NextResponse.json({ error: 'delivery_winery and vineyard_name are required' }, { status: 400 });
    }
    const dm = body.distance_m != null ? Number(body.distance_m) : NaN;
    if (!Number.isFinite(dm) || dm <= 0) {
      return NextResponse.json({ error: 'distance_m must be a positive number' }, { status: 400 });
    }
    const roundedM = Math.round(dm);
    let dur: number | null = null;
    const durRaw = body.duration_min;
    if (durRaw != null && durRaw !== '') {
      const t = Number(durRaw);
      if (Number.isNaN(t) || t < 0) {
        return NextResponse.json({ error: 'duration_min must be a non-negative number or empty' }, { status: 400 });
      }
      dur = t;
    }
    const notes = body.notes != null ? String(body.notes) : null;

    const updated = await execute(
      `UPDATE tbl_distances_manual
       SET distance_m = $3,
           duration_min = $4,
           notes = $5,
           updated_at = now()
       WHERE lower(trim(delivery_winery)) = lower(trim($1))
         AND lower(trim(vineyard_name)) = lower(trim($2))`,
      [dw, vn, roundedM, dur, notes]
    );
    if (updated === 0) {
      await execute(
        `INSERT INTO tbl_distances_manual (delivery_winery, vineyard_name, distance_m, duration_min, notes, updated_at)
         VALUES ($1, $2, $3, $4, $5, now())`,
        [dw, vn, roundedM, dur, notes]
      );
    }

    const rows = await query<{
      id: number;
      delivery_winery: string;
      vineyard_name: string;
      distance_m: number;
      duration_min: string | null;
      notes: string | null;
      updated_at: string;
    }>(
      `SELECT id, delivery_winery, vineyard_name, distance_m,
              duration_min::text AS duration_min, notes,
              to_char(updated_at, 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
       FROM tbl_distances_manual
       WHERE lower(trim(delivery_winery)) = lower(trim($1))
         AND lower(trim(vineyard_name)) = lower(trim($2))`,
      [dw, vn]
    );
    return NextResponse.json({ ok: true, row: rows[0] ?? null });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/relation "tbl_distances_manual" does not exist/i.test(msg)) {
      return NextResponse.json(
        {
          error:
            'Table tbl_distances_manual does not exist. Run web/sql/create_tbl_distances_manual.sql on your database.',
        },
        { status: 500 }
      );
    }
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

/** DELETE: remove manual override for a pair (body: delivery_winery, vineyard_name). */
export async function DELETE(request: Request) {
  try {
    const body = (await request.json().catch(() => ({}))) as PutBody;
    const { dw, vn } = normPair(String(body.delivery_winery ?? ''), String(body.vineyard_name ?? ''));
    if (!dw || !vn) {
      return NextResponse.json({ error: 'delivery_winery and vineyard_name are required' }, { status: 400 });
    }
    const n = await execute(
      `DELETE FROM tbl_distances_manual
       WHERE lower(trim(delivery_winery)) = lower(trim($1))
         AND lower(trim(vineyard_name)) = lower(trim($2))`,
      [dw, vn]
    );
    return NextResponse.json({ ok: true, deleted: n });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/relation "tbl_distances_manual" does not exist/i.test(msg)) {
      return NextResponse.json(
        {
          error:
            'Table tbl_distances_manual does not exist. Run web/sql/create_tbl_distances_manual.sql on your database.',
        },
        { status: 500 }
      );
    }
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

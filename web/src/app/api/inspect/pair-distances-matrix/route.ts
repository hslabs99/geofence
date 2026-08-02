import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

const MAX_PAIRS = 400;

function numOrNull(v: unknown): number | null {
  if (v == null || v === '') return null;
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseDurationText(v: unknown): number | null {
  if (v == null) return null;
  const n = Number(String(v).trim());
  return Number.isFinite(n) ? n : null;
}

export type PairDistMatrixCell = {
  winery: string;
  vineyard: string;
  distance_m: number | null;
  duration_min: number | null;
  manual_override: boolean;
};

/**
 * POST body: { pairs: { winery: string; vineyard: string }[] }
 * Returns effective metres / minutes (tbl_distances_manual overrides when present), same merge as Populate vWork.
 */
export async function POST(request: Request) {
  try {
    const body = (await request.json()) as { pairs?: unknown };
    const raw = Array.isArray(body.pairs) ? body.pairs : [];
    const pairs: { winery: string; vineyard: string }[] = [];
    const seen = new Set<string>();
    for (const p of raw) {
      if (pairs.length >= MAX_PAIRS) break;
      if (p == null || typeof p !== 'object') continue;
      const o = p as { winery?: unknown; vineyard?: unknown };
      const w = typeof o.winery === 'string' ? o.winery.trim() : '';
      const v = typeof o.vineyard === 'string' ? o.vineyard.trim() : '';
      if (!w || !v) continue;
      const key = `${w.toLowerCase()}\x00${v.toLowerCase()}`;
      if (seen.has(key)) continue;
      seen.add(key);
      pairs.push({ winery: w, vineyard: v });
    }

    if (pairs.length === 0) {
      return NextResponse.json({ pairs: [] as PairDistMatrixCell[] });
    }

    const jsonPayload = JSON.stringify(pairs);

    const sqlWithManual = `
      WITH requested AS (
        SELECT winery, vineyard
        FROM jsonb_to_recordset($1::jsonb) AS x(winery text, vineyard text)
      )
      SELECT
        r.winery AS req_winery,
        r.vineyard AS req_vineyard,
        (m.id IS NOT NULL) AS manual_override,
        CASE WHEN m.id IS NOT NULL THEN m.distance_m ELSE d.distance_m END AS distance_m,
        CASE
          WHEN m.id IS NOT NULL AND m.duration_min IS NOT NULL THEN m.duration_min::text
          ELSE d.duration_min::text
        END AS duration_min_t
      FROM requested r
      LEFT JOIN tbl_distances d
        ON lower(trim(d.delivery_winery)) = lower(trim(r.winery))
       AND lower(trim(d.vineyard_name)) = lower(trim(r.vineyard))
      LEFT JOIN tbl_distances_manual m
        ON lower(trim(m.delivery_winery)) = lower(trim(r.winery))
       AND lower(trim(m.vineyard_name)) = lower(trim(r.vineyard))`;

    type Row = {
      req_winery: string;
      req_vineyard: string;
      /** pg driver may return boolean or smallint */
      manual_override: boolean | number;
      distance_m: unknown;
      duration_min_t: unknown;
    };

    let rows: Row[];
    try {
      rows = await query<Row>(sqlWithManual, [jsonPayload]);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (!/tbl_distances_manual/i.test(msg) || !/does not exist/i.test(msg)) throw e;
      const sqlNoManual = `
        WITH requested AS (
          SELECT winery, vineyard
          FROM jsonb_to_recordset($1::jsonb) AS x(winery text, vineyard text)
        )
        SELECT
          r.winery AS req_winery,
          r.vineyard AS req_vineyard,
          false AS manual_override,
          d.distance_m AS distance_m,
          d.duration_min::text AS duration_min_t
        FROM requested r
        LEFT JOIN tbl_distances d
          ON lower(trim(d.delivery_winery)) = lower(trim(r.winery))
         AND lower(trim(d.vineyard_name)) = lower(trim(r.vineyard))`;
      rows = await query<Row>(sqlNoManual, [jsonPayload]);
    }

    const out: PairDistMatrixCell[] = rows.map((r) => ({
      winery: r.req_winery,
      vineyard: r.req_vineyard,
      distance_m: numOrNull(r.distance_m),
      duration_min: parseDurationText(r.duration_min_t),
      manual_override: r.manual_override === true || r.manual_override === 1,
    }));

    return NextResponse.json({ pairs: out });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (/relation "tbl_distances" does not exist/i.test(msg)) {
      return NextResponse.json(
        { error: 'Table tbl_distances does not exist.', pairs: [] },
        { status: 503 },
      );
    }
    console.error('[pair-distances-matrix]', e);
    const friendly = /max clients reached|EMAXCONNSESSION/i.test(msg)
      ? 'Database connection pool full (Supabase session limit). Wait a few seconds and retry, or restart the dev server.'
      : msg;
    return NextResponse.json({ error: friendly }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import * as XLSX from 'xlsx';
import type { PoolClient } from 'pg';
import { getClient } from '@/lib/db';

export const runtime = 'nodejs';

function normHeader(s: unknown): string {
  return String(s ?? '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

/** Map normalized header -> logical field name */
function headerToField(norm: string): string | null {
  const m: Record<string, string> = {
    customer: 'customer',
    template: 'template',
    winery: 'winery',
    'delivery winery': 'winery',
    delivery_winery: 'winery',
    deliverywinery: 'winery',
    'vineyard group': 'vineyardgroup',
    vineyardgroup: 'vineyardgroup',
    vineyard_group: 'vineyardgroup',
    tt: 'tt',
    trailermode: 'tt',
    tovinemins: 'toVineMins',
    'to vine': 'toVineMins',
    'to vine mins': 'toVineMins',
    invinemins: 'inVineMins',
    'in vine': 'inVineMins',
    'in vine mins': 'inVineMins',
    towinemins: 'toWineMins',
    'to wine': 'toWineMins',
    'to wine mins': 'toWineMins',
    inwinemins: 'inWineMins',
    'in wine': 'inWineMins',
    'in wine mins': 'inWineMins',
    totalmins: 'totalMins',
    total: 'totalMins',
  };
  return m[norm] ?? null;
}

function cellStr(v: unknown): string {
  if (v == null || v === '') return '';
  if (typeof v === 'number' && Number.isFinite(v)) return String(v);
  return String(v).trim();
}

function cellNum(v: unknown): number | null {
  if (v === '' || v == null) return null;
  if (typeof v === 'number' && Number.isFinite(v)) return Math.round(v * 100) / 100;
  const s = String(v).trim();
  if (!s) return null;
  const n = parseFloat(s.replace(/,/g, ''));
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 100) / 100;
}

function normMatchPart(s: string): string {
  return s.trim();
}

async function findIds(
  client: PoolClient,
  args: {
    customer: string | null;
    template: string;
    winery: string;
    vg: string;
    tt: string;
  }
): Promise<number[]> {
  const c = args.customer?.trim() ?? '';
  if (c !== '') {
    const res = await client.query<{ id: number }>(
      `SELECT id FROM tbl_wineryminutes
       WHERE trim(coalesce("Customer", '')) = $1
         AND trim(coalesce("Template", '')) = $2
         AND trim(coalesce("Winery", '')) = $3
         AND trim(coalesce(vineyardgroup, '')) = $4
         AND trim(coalesce("TT", '')) = $5`,
      [c, args.template, args.winery, args.vg, args.tt]
    );
    return res.rows.map((r) => r.id);
  }
  const res = await client.query<{ id: number }>(
    `SELECT id FROM tbl_wineryminutes
     WHERE trim(coalesce("Template", '')) = $1
       AND trim(coalesce("Winery", '')) = $2
       AND trim(coalesce(vineyardgroup, '')) = $3
       AND trim(coalesce("TT", '')) = $4`,
    [args.template, args.winery, args.vg, args.tt]
  );
  return res.rows.map((r) => r.id);
}

/** POST multipart/form-data field `file`: .xlsx from export; updates minute columns on match, else inserts if Customer present. */
export async function POST(request: Request) {
  try {
    const ct = request.headers.get('content-type') ?? '';
    if (!ct.includes('multipart/form-data')) {
      return NextResponse.json({ error: 'Expected multipart/form-data with file field' }, { status: 400 });
    }
    const form = await request.formData();
    const file = form.get('file');
    if (!file || typeof file === 'string') {
      return NextResponse.json({ error: 'Missing file' }, { status: 400 });
    }
    const buf = Buffer.from(await file.arrayBuffer());
    const wb = XLSX.read(buf, { type: 'buffer' });
    const sheetName = wb.SheetNames[0];
    if (!sheetName) {
      return NextResponse.json({ error: 'Workbook has no sheets' }, { status: 400 });
    }
    const sheet = wb.Sheets[sheetName];
    const objects = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, { defval: '' });

    let updated = 0;
    let inserted = 0;
    let skipped = 0;
    const errors: { row: number; message: string }[] = [];

    const client = await getClient();
    try {
      await client.query('BEGIN');

      for (let i = 0; i < objects.length; i++) {
        const raw = objects[i];
        const sheetRow = i + 2;

        const fields: Record<string, unknown> = {};
        for (const [k, val] of Object.entries(raw)) {
          const field = headerToField(normHeader(k));
          if (field) fields[field] = val;
        }

        const template = normMatchPart(cellStr(fields.template));
        const winery = normMatchPart(cellStr(fields.winery));
        const vg = normMatchPart(cellStr(fields.vineyardgroup));
        const ttRaw = normMatchPart(cellStr(fields.tt));
        const customerRaw = normMatchPart(cellStr(fields.customer));

        if (template === '' && winery === '' && ttRaw === '') {
          skipped++;
          continue;
        }

        if (template === '' || winery === '' || ttRaw === '') {
          errors.push({ row: sheetRow, message: 'Template, Winery, and TT are required for each non-blank row.' });
          continue;
        }

        if (ttRaw !== 'T' && ttRaw !== 'TT' && ttRaw !== 'TTT') {
          errors.push({ row: sheetRow, message: `TT must be T, TT, or TTT (got "${ttRaw}")` });
          continue;
        }

        const customer = customerRaw === '' ? null : customerRaw;

        const toVine = cellNum(fields.toVineMins);
        const inVine = cellNum(fields.inVineMins);
        const toWine = cellNum(fields.toWineMins);
        const inWine = cellNum(fields.inWineMins);
        const total = cellNum(fields.totalMins);

        const ids = await findIds(client, {
          customer,
          template,
          winery,
          vg,
          tt: ttRaw,
        });

        if (ids.length > 1) {
          errors.push({
            row: sheetRow,
            message:
              'Multiple DB rows match this Template + Winery + Vineyard group + TT' +
              (customer ? ' and Customer' : '') +
              '; fix duplicates or set Customer in the sheet.',
          });
          continue;
        }

        if (ids.length === 1) {
          await client.query(
            `UPDATE tbl_wineryminutes
             SET "ToVineMins" = $1, "InVineMins" = $2, "ToWineMins" = $3, "InWineMins" = $4, "TotalMins" = $5
             WHERE id = $6`,
            [toVine, inVine, toWine, inWine, total, ids[0]]
          );
          updated++;
          continue;
        }

        if (!customer) {
          errors.push({
            row: sheetRow,
            message: 'No matching row; Customer is required on the sheet to insert a new row.',
          });
          continue;
        }

        await client.query(
          `INSERT INTO tbl_wineryminutes ("Customer", "Template", vineyardgroup, "Winery", "TT", "ToVineMins", "InVineMins", "ToWineMins", "InWineMins", "TotalMins")
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
          [customer, template, vg || null, winery, ttRaw, toVine, inVine, toWine, inWine, total]
        );
        inserted++;
      }

      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    return NextResponse.json({
      ok: true,
      updated,
      inserted,
      skipped,
      errors,
      sheetName,
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

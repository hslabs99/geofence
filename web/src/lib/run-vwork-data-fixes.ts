import { query, execute } from '@/lib/db';
import { ensureDriverMappSchema } from '@/lib/ensure-driver-mapp-schema';

type MappRow = {
  id: number;
  oldvworkname: string;
  newvworkname: string;
};

export type WineMappFixesResult = {
  ok: true;
  totalUpdated: number;
  perMapping: { id: number; oldvworkname: string; newvworkname: string; updated: number }[];
};

export async function runWineMappFixes(): Promise<WineMappFixesResult> {
  const mappings = await query<MappRow>(
    `SELECT id, oldvworkname, newvworkname FROM tbl_wine_mapp ORDER BY id`
  );
  if (mappings.length === 0) {
    return { ok: true, totalUpdated: 0, perMapping: [] };
  }
  const perMapping: WineMappFixesResult['perMapping'] = [];
  let totalUpdated = 0;
  for (const m of mappings) {
    const n = await execute(
      `UPDATE tbl_vworkjobs
       SET delivery_winery_old = delivery_winery, delivery_winery = $1
       WHERE trim(delivery_winery) = trim($2)`,
      [m.newvworkname, m.oldvworkname]
    );
    perMapping.push({
      id: m.id,
      oldvworkname: m.oldvworkname,
      newvworkname: m.newvworkname,
      updated: n,
    });
    totalUpdated += n;
  }
  return { ok: true, totalUpdated, perMapping };
}

export type VineMappFixesResult = WineMappFixesResult;

export async function runVineMappFixes(): Promise<VineMappFixesResult> {
  const mappings = await query<MappRow>(
    `SELECT id, oldvworkname, newvworkname FROM tbl_vine_mapp ORDER BY id`
  );
  if (mappings.length === 0) {
    return { ok: true, totalUpdated: 0, perMapping: [] };
  }
  const perMapping: VineMappFixesResult['perMapping'] = [];
  let totalUpdated = 0;
  for (const m of mappings) {
    const n = await execute(
      `UPDATE tbl_vworkjobs
       SET vineyard_name_old = vineyard_name, vineyard_name = $1
       WHERE trim(vineyard_name) = trim($2)`,
      [m.newvworkname, m.oldvworkname]
    );
    perMapping.push({
      id: m.id,
      oldvworkname: m.oldvworkname,
      newvworkname: m.newvworkname,
      updated: n,
    });
    totalUpdated += n;
  }
  return { ok: true, totalUpdated, perMapping };
}

export type DriverMappFixesResult = WineMappFixesResult;

export async function runDriverMappFixes(): Promise<DriverMappFixesResult> {
  await ensureDriverMappSchema();
  const mappings = await query<MappRow>(
    `SELECT id, oldvworkname, newvworkname FROM tbl_driver_mapp ORDER BY id`
  );
  if (mappings.length === 0) {
    return { ok: true, totalUpdated: 0, perMapping: [] };
  }
  const perMapping: DriverMappFixesResult['perMapping'] = [];
  let totalUpdated = 0;
  for (const m of mappings) {
    const n = await execute(
      `UPDATE tbl_vworkjobs
       SET worker_old = worker, worker = $1
       WHERE trim(worker) = trim($2)`,
      [m.newvworkname, m.oldvworkname]
    );
    perMapping.push({
      id: m.id,
      oldvworkname: m.oldvworkname,
      newvworkname: m.newvworkname,
      updated: n,
    });
    totalUpdated += n;
  }
  return { ok: true, totalUpdated, perMapping };
}

export type UpdateVineyardGroupResult = {
  ok: true;
  setToNa: number;
  matched: number;
  totalRows: number;
};

export async function runUpdateVineyardGroup(): Promise<UpdateVineyardGroupResult> {
  const setToNa = await execute(`UPDATE tbl_vworkjobs SET vineyard_group = 'NA'`);
  const matched = await execute(
    `UPDATE tbl_vworkjobs v
     SET vineyard_group = (
       SELECT vg.vineyard_group
       FROM tbl_vineyardgroups vg
       WHERE trim(v.vineyard_name) = trim(vg.vineyard_name)
         AND (vg.winery_name IS NULL OR trim(v.delivery_winery) = trim(vg.winery_name))
       ORDER BY vg.winery_name NULLS LAST
       LIMIT 1
     )
     WHERE EXISTS (
       SELECT 1 FROM tbl_vineyardgroups vg
       WHERE trim(v.vineyard_name) = trim(vg.vineyard_name)
         AND (vg.winery_name IS NULL OR trim(v.delivery_winery) = trim(vg.winery_name))
     )`
  );
  const totalRows = await query<{ n: string }>(`SELECT count(*)::text AS n FROM tbl_vworkjobs`);
  const total = parseInt(totalRows[0]?.n ?? '0', 10) || 0;
  return { ok: true, setToNa, matched, totalRows: total };
}

export type SetTrailerTypeResult = {
  ok: true;
  updatedTT: number;
  updatedT: number;
  totalUpdated: number;
};

export async function runSetTrailerType(): Promise<SetTrailerTypeResult> {
  const updatedTT = await execute(
    `UPDATE tbl_vworkjobs SET trailermode = 'TT'
     WHERE trailer_rego IS NOT NULL AND TRIM(trailer_rego) <> ''
       AND LOWER(TRIM(trailer_rego)) <> 'no trailer required'
       AND NOT (
         TRIM(trailer_rego) !~ '[0-9]'
         AND (LOWER(TRIM(trailer_rego)) LIKE '%n/a%' OR LOWER(TRIM(trailer_rego)) LIKE '%na%')
       )`
  );
  const updatedT = await execute(
    `UPDATE tbl_vworkjobs SET trailermode = 'T'
     WHERE trailer_rego IS NULL OR TRIM(trailer_rego) = ''
       OR LOWER(TRIM(trailer_rego)) = 'no trailer required'
       OR (
         TRIM(trailer_rego) !~ '[0-9]'
         AND (LOWER(TRIM(trailer_rego)) LIKE '%n/a%' OR LOWER(TRIM(trailer_rego)) LIKE '%na%')
       )`
  );
  return { ok: true, updatedTT, updatedT, totalUpdated: updatedTT + updatedT };
}

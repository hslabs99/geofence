import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/** Read first string column from each row (handles pg lowercase keys). */
function rowsToSortedDistinct(rows: unknown): string[] {
  if (!Array.isArray(rows)) return [];
  const seen = new Set<string>();
  for (const r of rows) {
    const row = r as Record<string, unknown>;
    const raw = row.val ?? row.v;
    if (raw == null) continue;
    const s = String(raw).trim();
    if (s) seen.add(s);
  }
  return [...seen].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
}

async function distinctTextColumn(qualifiedCol: string): Promise<string[]> {
  const sql = `
    SELECT DISTINCT btrim(${qualifiedCol}::text) AS val
    FROM tbl_vworkjobs
    WHERE ${qualifiedCol} IS NOT NULL AND btrim(${qualifiedCol}::text) <> ''
    ORDER BY 1`;
  try {
    const rows = await query(sql);
    return rowsToSortedDistinct(rows);
  } catch {
    return [];
  }
}

/** Distinct values for one column, scoped to customer (tries quoted "Customer" then lowercase customer). */
async function distinctForCustomer(sqlQuoted: string, sqlLower: string, customer: string): Promise<string[]> {
  try {
    const rows = await query(sqlQuoted, [customer]);
    return rowsToSortedDistinct(rows);
  } catch {
    try {
      const rows = await query(sqlLower, [customer]);
      return rowsToSortedDistinct(rows);
    } catch {
      return [];
    }
  }
}

type ScopedResult = {
  truckIds: string[];
  trailermodes: string[];
  trailerTypes: string[];
  loadSizes: string[];
  vineyardNames: string[];
  deliveryWineries: string[];
  workers: string[];
  vineyardGroups: string[];
};

async function distinctAllForCustomer(customer: string): Promise<ScopedResult> {
  const truckQuoted = `SELECT DISTINCT btrim(truck_id::text) AS val FROM tbl_vworkjobs
    WHERE truck_id IS NOT NULL AND btrim(truck_id::text) <> '' AND trim("Customer") = $1 ORDER BY 1`;
  const truckLower = `SELECT DISTINCT btrim(truck_id::text) AS val FROM tbl_vworkjobs
    WHERE truck_id IS NOT NULL AND btrim(truck_id::text) <> '' AND trim(customer) = $1 ORDER BY 1`;

  const tmQuoted = `SELECT DISTINCT btrim(COALESCE(trailermode::text, trailertype::text, '')) AS val FROM tbl_vworkjobs
    WHERE btrim(COALESCE(trailermode::text, trailertype::text, '')) <> '' AND trim("Customer") = $1 ORDER BY 1`;
  const tmLower = `SELECT DISTINCT btrim(COALESCE(trailermode::text, trailertype::text, '')) AS val FROM tbl_vworkjobs
    WHERE btrim(COALESCE(trailermode::text, trailertype::text, '')) <> '' AND trim(customer) = $1 ORDER BY 1`;

  const vineQuoted = `SELECT DISTINCT btrim(vineyard_name::text) AS val FROM tbl_vworkjobs
    WHERE vineyard_name IS NOT NULL AND btrim(vineyard_name::text) <> '' AND trim("Customer") = $1 ORDER BY 1`;
  const vineLower = `SELECT DISTINCT btrim(vineyard_name::text) AS val FROM tbl_vworkjobs
    WHERE vineyard_name IS NOT NULL AND btrim(vineyard_name::text) <> '' AND trim(customer) = $1 ORDER BY 1`;

  const winQuoted = `SELECT DISTINCT btrim(delivery_winery::text) AS val FROM tbl_vworkjobs
    WHERE delivery_winery IS NOT NULL AND btrim(delivery_winery::text) <> '' AND trim("Customer") = $1 ORDER BY 1`;
  const winLower = `SELECT DISTINCT btrim(delivery_winery::text) AS val FROM tbl_vworkjobs
    WHERE delivery_winery IS NOT NULL AND btrim(delivery_winery::text) <> '' AND trim(customer) = $1 ORDER BY 1`;

  const workerQuoted = `SELECT DISTINCT btrim(worker::text) AS val FROM tbl_vworkjobs
    WHERE worker IS NOT NULL AND btrim(worker::text) <> '' AND trim("Customer") = $1 ORDER BY 1`;
  const workerLower = `SELECT DISTINCT btrim(worker::text) AS val FROM tbl_vworkjobs
    WHERE worker IS NOT NULL AND btrim(worker::text) <> '' AND trim(customer) = $1 ORDER BY 1`;

  const vgQuoted = `SELECT DISTINCT btrim(vineyard_group::text) AS val FROM tbl_vworkjobs
    WHERE vineyard_group IS NOT NULL AND btrim(vineyard_group::text) <> '' AND trim("Customer") = $1 ORDER BY 1`;
  const vgLower = `SELECT DISTINCT btrim(vineyard_group::text) AS val FROM tbl_vworkjobs
    WHERE vineyard_group IS NOT NULL AND btrim(vineyard_group::text) <> '' AND trim(customer) = $1 ORDER BY 1`;

  const ttOnlyQuoted = `SELECT DISTINCT btrim(trailertype::text) AS val FROM tbl_vworkjobs
    WHERE trailertype IS NOT NULL AND btrim(trailertype::text) <> '' AND trim("Customer") = $1 ORDER BY 1`;
  const ttOnlyLower = `SELECT DISTINCT btrim(trailertype::text) AS val FROM tbl_vworkjobs
    WHERE trailertype IS NOT NULL AND btrim(trailertype::text) <> '' AND trim(customer) = $1 ORDER BY 1`;

  const lsQuoted = `SELECT DISTINCT trim(both from loadsize::text) AS val FROM tbl_vworkjobs
    WHERE loadsize IS NOT NULL AND trim("Customer") = $1 ORDER BY 1`;
  const lsLower = `SELECT DISTINCT trim(both from loadsize::text) AS val FROM tbl_vworkjobs
    WHERE loadsize IS NOT NULL AND trim(customer) = $1 ORDER BY 1`;

  const [truckIds, trailermodes, trailerTypes, loadSizes, vineyardNames, deliveryWineries, workers, vineyardGroups] = await Promise.all([
    distinctForCustomer(truckQuoted, truckLower, customer),
    distinctForCustomer(tmQuoted, tmLower, customer),
    distinctForCustomer(ttOnlyQuoted, ttOnlyLower, customer),
    distinctForCustomer(lsQuoted, lsLower, customer),
    distinctForCustomer(vineQuoted, vineLower, customer),
    distinctForCustomer(winQuoted, winLower, customer),
    distinctForCustomer(workerQuoted, workerLower, customer),
    distinctForCustomer(vgQuoted, vgLower, customer),
  ]);

  return { truckIds, trailermodes, trailerTypes, loadSizes, vineyardNames, deliveryWineries, workers, vineyardGroups };
}

/**
 * GET: distinct filter values for Inspect / Summary.
 * Optional ?customer=X — when set, all lists are limited to that customer (for Summary filter bar without loading jobs).
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const customer = searchParams.get('customer')?.trim() ?? '';

    if (customer) {
      const scoped = await distinctAllForCustomer(customer);
      return NextResponse.json({
        truckIds: scoped.truckIds,
        trailermodes: scoped.trailermodes,
        trailerTypes: scoped.trailerTypes,
        loadSizes: scoped.loadSizes,
        vineyardNames: scoped.vineyardNames,
        deliveryWineries: scoped.deliveryWineries,
        workers: scoped.workers,
        vineyardGroups: scoped.vineyardGroups,
      });
    }

    const [truckRows, tmRows, trailerTypeRows, loadSizeRows, vineyardNames, deliveryWineries, workers, vineyardGroups] = await Promise.all([
      query<{ v: string }>(
        `SELECT DISTINCT btrim(truck_id::text) AS v FROM tbl_vworkjobs
         WHERE truck_id IS NOT NULL AND btrim(truck_id::text) <> ''
         ORDER BY 1`,
      ).catch(() => []),
      query<{ v: string }>(
        `SELECT DISTINCT btrim(COALESCE(trailermode::text, trailertype::text, '')) AS v FROM tbl_vworkjobs
         WHERE btrim(COALESCE(trailermode::text, trailertype::text, '')) <> ''
         ORDER BY 1`,
      ).catch(() => []),
      query<{ v: string }>(
        `SELECT DISTINCT btrim(trailertype::text) AS v FROM tbl_vworkjobs
         WHERE trailertype IS NOT NULL AND btrim(trailertype::text) <> ''
         ORDER BY 1`,
      ).catch(() => []),
      query<{ v: string }>(
        `SELECT DISTINCT trim(both from loadsize::text) AS v FROM tbl_vworkjobs
         WHERE loadsize IS NOT NULL
         ORDER BY 1`,
      ).catch(() => []),
      distinctTextColumn('vineyard_name'),
      distinctTextColumn('delivery_winery'),
      distinctTextColumn('worker'),
      distinctTextColumn('vineyard_group'),
    ]);

    const truckIds = rowsToSortedDistinct(truckRows);
    const trailermodes = rowsToSortedDistinct(tmRows);
    const trailerTypes = rowsToSortedDistinct(trailerTypeRows);
    const loadSizes = rowsToSortedDistinct(loadSizeRows);

    return NextResponse.json({
      truckIds,
      trailermodes,
      trailerTypes,
      loadSizes,
      vineyardNames,
      deliveryWineries,
      workers,
      vineyardGroups,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

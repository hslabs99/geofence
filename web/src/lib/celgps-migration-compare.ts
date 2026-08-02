import type { Pool } from 'pg';
import {
  createMigrationPool,
  getCelgpsMigrationConfig,
  getCloudSqlMigrationConfig,
  safeConnectionSummary,
  type CelgpsMigrationConfig,
} from '@/lib/celgps-migration-config';

export interface TableCompareRow {
  tableName: string;
  cloudSqlCount: number | null;
  supabaseCount: number | null;
  match: boolean;
  cloudSqlError: string | null;
  supabaseError: string | null;
}

export interface TableCompareResult {
  ok: boolean;
  error: string | null;
  durationMs: number;
  cloudSql: ReturnType<typeof safeConnectionSummary> | null;
  supabase: ReturnType<typeof safeConnectionSummary> | null;
  rows: TableCompareRow[];
  summary: {
    tableCount: number;
    matching: number;
    mismatched: number;
    missingOnCloudSql: number;
    missingOnSupabase: number;
    cloudSqlErrors: number;
    supabaseErrors: number;
  };
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

async function listPublicTables(pool: Pool): Promise<string[]> {
  const res = await pool.query<{ table_name: string }>(
    `SELECT table_name
     FROM information_schema.tables
     WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
     ORDER BY table_name`
  );
  return res.rows.map((r) => r.table_name);
}

async function countTableRows(pool: Pool, tableName: string): Promise<number> {
  const res = await pool.query<{ c: string }>(
    `SELECT COUNT(*)::text AS c FROM public.${quoteIdent(tableName)}`
  );
  return Number(res.rows[0]?.c ?? 0);
}

async function countTablesOnPool(
  pool: Pool,
  tableNames: string[],
  onProgress?: (tableName: string, index: number, total: number) => void
): Promise<Map<string, { count: number | null; error: string | null }>> {
  const out = new Map<string, { count: number | null; error: string | null }>();
  const total = tableNames.length;
  for (let i = 0; i < tableNames.length; i++) {
    const tableName = tableNames[i]!;
    onProgress?.(tableName, i + 1, total);
    try {
      const count = await countTableRows(pool, tableName);
      out.set(tableName, { count, error: null });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      out.set(tableName, { count: null, error: message });
    }
  }
  return out;
}

function buildSummary(rows: TableCompareRow[]): TableCompareResult['summary'] {
  let matching = 0;
  let mismatched = 0;
  let missingOnCloudSql = 0;
  let missingOnSupabase = 0;
  let cloudSqlErrors = 0;
  let supabaseErrors = 0;

  for (const row of rows) {
    if (row.cloudSqlError) cloudSqlErrors++;
    if (row.supabaseError) supabaseErrors++;
    if (row.cloudSqlCount == null && row.supabaseCount != null && !row.cloudSqlError) {
      missingOnCloudSql++;
    }
    if (row.supabaseCount == null && row.cloudSqlCount != null && !row.supabaseError) {
      missingOnSupabase++;
    }
    if (row.match) matching++;
    else mismatched++;
  }

  return {
    tableCount: rows.length,
    matching,
    mismatched,
    missingOnCloudSql,
    missingOnSupabase,
    cloudSqlErrors,
    supabaseErrors,
  };
}

/** Compare public-schema table row counts between Cloud SQL and Supabase. */
export async function compareTableRowCounts(): Promise<TableCompareResult> {
  const started = Date.now();
  const cloudCfg = getCloudSqlMigrationConfig();
  const supaCfg = getCelgpsMigrationConfig();

  if (!cloudCfg.ok) {
    return {
      ok: false,
      error: cloudCfg.error,
      durationMs: Date.now() - started,
      cloudSql: null,
      supabase: supaCfg.ok ? safeConnectionSummary(supaCfg) : null,
      rows: [],
      summary: {
        tableCount: 0,
        matching: 0,
        mismatched: 0,
        missingOnCloudSql: 0,
        missingOnSupabase: 0,
        cloudSqlErrors: 0,
        supabaseErrors: 0,
      },
    };
  }
  if (!supaCfg.ok) {
    return {
      ok: false,
      error: supaCfg.error,
      durationMs: Date.now() - started,
      cloudSql: safeConnectionSummary(cloudCfg),
      supabase: null,
      rows: [],
      summary: {
        tableCount: 0,
        matching: 0,
        mismatched: 0,
        missingOnCloudSql: 0,
        missingOnSupabase: 0,
        cloudSqlErrors: 0,
        supabaseErrors: 0,
      },
    };
  }

  const cloudPool = createMigrationPool(cloudCfg as CelgpsMigrationConfig, 1);
  let cloudTables: string[] = [];
  let cloudCounts = new Map<string, { count: number | null; error: string | null }>();
  try {
    cloudTables = await listPublicTables(cloudPool);
    const cloudSet = new Set(cloudTables);
    const allTablesPreview = cloudTables;
    cloudCounts = await countTablesOnPool(cloudPool, allTablesPreview.filter((t) => cloudSet.has(t)));
  } finally {
    await cloudPool.end().catch(() => undefined);
  }

  const supaPool = createMigrationPool(supaCfg as CelgpsMigrationConfig, 1);
  try {
    const supaTables = await listPublicTables(supaPool);
    const allTables = [...new Set([...cloudTables, ...supaTables])].sort((a, b) =>
      a.localeCompare(b)
    );

    const cloudSet = new Set(cloudTables);
    const supaSet = new Set(supaTables);

    const supaCounts = await countTablesOnPool(
      supaPool,
      allTables.filter((t) => supaSet.has(t))
    );

    const rows: TableCompareRow[] = allTables.map((tableName) => {
      const cloud = cloudSet.has(tableName)
        ? cloudCounts.get(tableName) ?? { count: null, error: 'Not counted' }
        : { count: null, error: null };
      const supa = supaSet.has(tableName)
        ? supaCounts.get(tableName) ?? { count: null, error: 'Not counted' }
        : { count: null, error: null };

      const match =
        cloud.count != null &&
        supa.count != null &&
        !cloud.error &&
        !supa.error &&
        cloud.count === supa.count;

      return {
        tableName,
        cloudSqlCount: cloud.count,
        supabaseCount: supa.count,
        match,
        cloudSqlError: cloudSet.has(tableName) ? cloud.error : 'Table missing',
        supabaseError: supaSet.has(tableName) ? supa.error : 'Table missing',
      };
    });

    const summary = buildSummary(rows);

    return {
      ok: true,
      error: null,
      durationMs: Date.now() - started,
      cloudSql: safeConnectionSummary(cloudCfg),
      supabase: safeConnectionSummary(supaCfg),
      rows,
      summary,
    };
  } finally {
    await supaPool.end().catch(() => undefined);
  }
}

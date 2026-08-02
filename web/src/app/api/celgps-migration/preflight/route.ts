import { NextResponse } from 'next/server';
import {
  getActiveDbTarget,
  getCloudSqlMigrationConfig,
  getCelgpsMigrationConfig,
  getDbTargetFileInfo,
  resolveActiveAppDbConfig,
  safeConnectionSummary,
} from '@/lib/celgps-migration-config';
import { resolveCliTool } from '@/lib/celgps-migration-tools';

/**
 * GET /api/celgps-migration/preflight
 * CLI tool check, env/config status, DB target (no secrets).
 */
export async function GET() {
  const psql = resolveCliTool('psql');
  const pgDump = resolveCliTool('pg_dump');
  const configResult = getCelgpsMigrationConfig();
  const cloudCfg = getCloudSqlMigrationConfig();
  const active = resolveActiveAppDbConfig();
  const dbTarget = getActiveDbTarget();
  const targetFile = getDbTargetFileInfo();

  const postImportChecklist = [
    'Table count vs export CREATE TABLE count',
    'Row counts on key tables (tbl_tracking, tbl_gpsdata, etc.)',
    'PostGIS version + store_* functions present',
    'App starts cleanly against Supabase URL',
    'Auth + CRUD smoke tests',
    'Update production DATABASE_URL; remove Cloud SQL connector',
    'Extension verification (postgis, any others in export)',
  ];

  if (!configResult.ok) {
    return NextResponse.json({
      ok: false,
      error: configResult.error,
      hint: configResult.hint ?? null,
      envFilePath: configResult.envFilePath,
      envFileExists: configResult.envFileExists,
      cli: { psql, pg_dump: pgDump },
      postImportChecklist,
      connection: null,
      cloudSql: cloudCfg.ok ? safeConnectionSummary(cloudCfg) : null,
      dbTarget,
      targetFile,
      activeConnection: active.ok ? safeConnectionSummary(active) : null,
    });
  }

  return NextResponse.json({
    ok: true,
    envFilePath: configResult.envFilePath,
    envFileExists: configResult.envFileExists,
    cli: { psql, pg_dump: pgDump },
    connection: safeConnectionSummary(configResult),
    cloudSql: cloudCfg.ok ? safeConnectionSummary(cloudCfg) : { error: cloudCfg.error, hint: cloudCfg.hint },
    dbTarget,
    targetFile,
    activeConnection: active.ok ? safeConnectionSummary(active) : null,
    postImportChecklist,
    note: 'Migration restore (Step 2) is not enabled — connection test, cross-check, and DB toggle only.',
  });
}

export const dynamic = 'force-dynamic';

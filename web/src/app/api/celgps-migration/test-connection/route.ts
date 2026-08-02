import { NextResponse } from 'next/server';
import {
  getCelgpsMigrationConfig,
  safeConnectionSummary,
} from '@/lib/celgps-migration-config';
import { resolveCliTool, testSupabaseConnection } from '@/lib/celgps-migration-tools';

/**
 * POST /api/celgps-migration/test-connection
 * Test Supabase TCP + login; return version, user, database (no password).
 */
export async function POST() {
  const configResult = getCelgpsMigrationConfig();
  if (!configResult.ok) {
    return NextResponse.json(
      {
        ok: false,
        error: configResult.error,
        hint: configResult.hint ?? null,
        envFilePath: configResult.envFilePath,
        envFileExists: configResult.envFileExists,
      },
      { status: 400 }
    );
  }

  const psql = resolveCliTool('psql');
  const test = await testSupabaseConnection(configResult, psql.found ? psql.path : null);

  if (!test.ok) {
    return NextResponse.json(
      {
        ok: false,
        connection: safeConnectionSummary(configResult),
        cli: { psqlFound: psql.found, psqlPath: psql.path },
        test,
      },
      { status: 502 }
    );
  }

  return NextResponse.json({
    ok: true,
    connection: safeConnectionSummary(configResult),
    cli: { psqlFound: psql.found, psqlPath: psql.path, psqlVersion: psql.version },
    test,
  });
}

export const dynamic = 'force-dynamic';

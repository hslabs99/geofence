import { NextResponse } from 'next/server';
import { resetDbFailoverGuard, resetDbPool } from '@/lib/db';
import {
  getActiveDbTarget,
  getCloudSqlMigrationConfig,
  getCelgpsMigrationConfig,
  getDbTargetFileInfo,
  resolveActiveAppDbConfig,
  safeConnectionSummary,
  setActiveDbTarget,
  type CelgpsMigrationConfig,
  type DbTarget,
} from '@/lib/celgps-migration-config';
import { testMigrationConfigConnection, verifyBothDbTargets } from '@/lib/celgps-db-verify';

/**
 * GET /api/celgps-migration/db-target
 * Default: config only. ?verify=1 opens extra test pools (use sparingly — Supabase session limit ~15).
 */
export async function GET(request: Request) {
  const doVerify = new URL(request.url).searchParams.get('verify') === '1';
  const target = getActiveDbTarget();
  const targetFile = getDbTargetFileInfo();
  const active = resolveActiveAppDbConfig();
  const cloudCfg = getCloudSqlMigrationConfig();
  const supaCfg = getCelgpsMigrationConfig();

  if (!doVerify) {
    return NextResponse.json({
      ok: active.ok,
      target,
      targetFile,
      activeConnection: active.ok ? safeConnectionSummary(active) : null,
      activeError: active.ok ? null : active.error,
      activeHint: active.ok ? null : active.hint ?? null,
      cloudSql: cloudCfg.ok ? safeConnectionSummary(cloudCfg) : { error: cloudCfg.error, hint: cloudCfg.hint },
      supabase: supaCfg.ok ? safeConnectionSummary(supaCfg) : { error: supaCfg.error, hint: supaCfg.hint },
      verify: null,
    });
  }

  const verify = await verifyBothDbTargets();

  return NextResponse.json({
    ok: active.ok,
    target,
    targetFile,
    activeConnection: active.ok ? safeConnectionSummary(active) : null,
    activeConnected: verify[target].connected,
    activeError: active.ok ? null : active.error,
    activeHint: active.ok ? null : active.hint ?? null,
    cloudSql: cloudCfgSummary(cloudCfg, verify.cloudsql),
    supabase: cloudCfgSummary(supaCfg, verify.supabase),
    verify,
  });
}

function cloudCfgSummary(
  cfg: ReturnType<typeof getCloudSqlMigrationConfig>,
  verify: { connected: boolean; error: string | null; durationMs: number }
) {
  if (!cfg.ok) return { error: cfg.error, hint: cfg.hint, connected: false };
  return {
    ...safeConnectionSummary(cfg),
    connected: verify.connected,
    connectError: verify.error,
    connectMs: verify.durationMs,
  };
}

/**
 * POST /api/celgps-migration/db-target — switch only after live connection test passes.
 */
export async function POST(request: Request) {
  const body = await request.json().catch(() => ({}));
  const raw = (body as { target?: string }).target?.trim().toLowerCase();
  if (raw !== 'cloudsql' && raw !== 'supabase') {
    return NextResponse.json({ error: 'target must be cloudsql or supabase' }, { status: 400 });
  }
  const target = raw as DbTarget;

  const cfg = target === 'supabase' ? getCelgpsMigrationConfig() : getCloudSqlMigrationConfig();
  if (!cfg.ok) {
    return NextResponse.json(
      { ok: false, error: cfg.error, hint: cfg.hint ?? null },
      { status: 400 }
    );
  }

  const test = await testMigrationConfigConnection(cfg as CelgpsMigrationConfig);
  if (!test.ok) {
    const label = target === 'supabase' ? 'Supabase' : 'Google Cloud SQL';
    return NextResponse.json(
      {
        ok: false,
        error: `${label} is not reachable (${test.durationMs}ms): ${test.error}`,
        hint:
          target === 'cloudsql'
            ? 'Cloud SQL may be stopped. Stay on Supabase or start the Cloud SQL instance first.'
            : 'Check CELGPS_DATABASE_URL in .env.local.',
      },
      { status: 400 }
    );
  }

  const saved = setActiveDbTarget(target);
  resetDbPool();
  resetDbFailoverGuard();

  const active = resolveActiveAppDbConfig();
  return NextResponse.json({
    ok: true,
    target: saved.target,
    targetFile: getDbTargetFileInfo(),
    activeConnection: active.ok ? safeConnectionSummary(active) : null,
    connected: true,
    connectMs: test.durationMs,
    message:
      target === 'supabase'
        ? `Supabase verified (${test.durationMs}ms). App DB switched.`
        : `Google Cloud SQL verified (${test.durationMs}ms). App DB switched.`,
  });
}

export const dynamic = 'force-dynamic';

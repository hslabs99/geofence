import type { Pool } from 'pg';
import {
  createMigrationPool,
  getCloudSqlMigrationConfig,
  getCelgpsMigrationConfig,
  PG_CONNECT_TIMEOUT_MS,
  safeConnectionSummary,
  type CelgpsMigrationConfig,
  type DbTarget,
} from '@/lib/celgps-migration-config';

export interface DbTargetVerifyResult {
  target: DbTarget;
  configured: boolean;
  connected: boolean;
  error: string | null;
  durationMs: number;
  database: string | null;
  host: string | null;
  label: string | null;
}

export async function testMigrationConfigConnection(
  config: CelgpsMigrationConfig,
  timeoutMs = PG_CONNECT_TIMEOUT_MS
): Promise<{ ok: boolean; error: string | null; durationMs: number }> {
  const pool = createMigrationPool(config, 1, timeoutMs);
  const started = Date.now();
  try {
    await pool.query('SELECT 1 AS n');
    return { ok: true, error: null, durationMs: Date.now() - started };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { ok: false, error: message, durationMs: Date.now() - started };
  } finally {
    await pool.end().catch(() => undefined);
  }
}

export async function verifyDbTarget(target: DbTarget): Promise<DbTargetVerifyResult> {
  const cfg = target === 'supabase' ? getCelgpsMigrationConfig() : getCloudSqlMigrationConfig();
  if (!cfg.ok) {
    return {
      target,
      configured: false,
      connected: false,
      error: cfg.error,
      durationMs: 0,
      database: null,
      host: null,
      label: null,
    };
  }
  const summary = safeConnectionSummary(cfg);
  const test = await testMigrationConfigConnection(cfg as CelgpsMigrationConfig);
  return {
    target,
    configured: true,
    connected: test.ok,
    error: test.error,
    durationMs: test.durationMs,
    database: summary.database,
    host: summary.host,
    label: summary.label,
  };
}

/** Ping Cloud SQL then Supabase sequentially (avoids bursting Supabase session pool). */
export async function verifyBothDbTargets(): Promise<{
  cloudsql: DbTargetVerifyResult;
  supabase: DbTargetVerifyResult;
}> {
  const cloudsql = await verifyDbTarget('cloudsql');
  const supabase = await verifyDbTarget('supabase');
  return { cloudsql, supabase };
}

export function isDbUnreachableError(err: unknown): boolean {
  const e = err as { code?: string; message?: string };
  const code = (e?.code ?? '').toUpperCase();
  const msg = (e?.message ?? String(err)).toLowerCase();
  if (['ECONNREFUSED', 'ETIMEDOUT', 'ENOTFOUND', 'EHOSTUNREACH', 'ECONNRESET'].includes(code)) {
    return true;
  }
  return (
    msg.includes('connect econnrefused') ||
    msg.includes('connection terminated') ||
    msg.includes('timeout expired') ||
    msg.includes('timeout exceeded') ||
    msg.includes('could not connect') ||
    msg.includes('getaddrinfo') ||
    msg.includes('connection timeout')
  );
}

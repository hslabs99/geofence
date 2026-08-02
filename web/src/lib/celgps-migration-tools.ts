import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import {
  CELGPS_SUPABASE_DIRECT_HOST,
  createMigrationPool,
  type CelgpsMigrationConfig,
} from '@/lib/celgps-migration-config';

export interface CliToolStatus {
  name: 'psql' | 'pg_dump';
  found: boolean;
  path: string | null;
  version: string | null;
  tried: string[];
}

const WINDOWS_PG_VERSIONS = [18, 17, 16, 15, 14, 13] as const;

function resolveFromPath(exe: string): string | null {
  const cmd = process.platform === 'win32' ? 'where' : 'which';
  const res = spawnSync(cmd, [exe], { encoding: 'utf8', timeout: 5000 });
  if (res.status !== 0 || !res.stdout?.trim()) return null;
  return res.stdout.trim().split(/\r?\n/)[0]?.trim() || null;
}

function resolveWindowsPostgresBin(exe: string, tried: string[]): string | null {
  for (const ver of WINDOWS_PG_VERSIONS) {
    const candidate = `C:\\Program Files\\PostgreSQL\\${ver}\\bin\\${exe}`;
    tried.push(candidate);
    if (fs.existsSync(candidate)) return candidate;
  }
  return null;
}

function readCliVersion(toolPath: string): string | null {
  const res = spawnSync(toolPath, ['--version'], { encoding: 'utf8', timeout: 10000 });
  if (res.status !== 0) return null;
  const line = (res.stdout || res.stderr || '').trim().split(/\r?\n/)[0];
  return line || null;
}

export function resolveCliTool(name: 'psql' | 'pg_dump'): CliToolStatus {
  const tried: string[] = [];
  const exe = process.platform === 'win32' ? `${name}.exe` : name;

  tried.push(`PATH:${name}`);
  let toolPath = resolveFromPath(name);
  if (!toolPath && process.platform === 'win32') {
    toolPath = resolveWindowsPostgresBin(exe, tried);
  }

  if (!toolPath) {
    return { name, found: false, path: null, version: null, tried };
  }

  return {
    name,
    found: true,
    path: toolPath,
    version: readCliVersion(toolPath),
    tried,
  };
}

export interface PgConnectionTestResult {
  ok: boolean;
  method: 'pg' | 'psql';
  version: string | null;
  currentUser: string | null;
  currentDatabase: string | null;
  durationMs: number;
  error: string | null;
  code: string | null;
  hint: string | null;
  psqlStdout: string | null;
}

function connectionHint(message: string, config: CelgpsMigrationConfig): string | null {
  const lower = message.toLowerCase();
  if (
    (lower.includes('enotfound') || lower.includes('getaddrinfo')) &&
    (config.host === CELGPS_SUPABASE_DIRECT_HOST || config.isDirectHost)
  ) {
    return 'Direct Supabase host (db.*.supabase.co) is IPv6-only and often fails on Windows. Use the Session pooler URL in CELGPS_DATABASE_URL instead.';
  }
  if (lower.includes('password') || lower.includes('authentication')) {
    return 'Check CELGPS_DATABASE_URL or CELGPS_SUPABASE_PASSWORD in .env.local.';
  }
  if (lower.includes('ssl') || lower.includes('certificate')) {
    return 'Supabase requires SSL. Connection uses sslmode=require.';
  }
  if (lower.includes('econnrefused') || lower.includes('timeout')) {
    return 'Cannot reach the database host. Confirm Session pooler host and port 5432.';
  }
  return null;
}

/** Test connectivity via node-pg (primary) and optionally psql CLI. */
export async function testSupabaseConnection(
  config: CelgpsMigrationConfig,
  psqlPath: string | null
): Promise<PgConnectionTestResult> {
  const started = Date.now();
  const pool = createMigrationPool(config, 1);

  try {
    const [versionRow, identityRow] = await Promise.all([
      pool.query<{ version: string }>('SELECT version() AS version'),
      pool.query<{ current_user: string; current_database: string }>(
        'SELECT current_user, current_database()'
      ),
    ]);

    const result: PgConnectionTestResult = {
      ok: true,
      method: 'pg',
      version: versionRow.rows[0]?.version ?? null,
      currentUser: identityRow.rows[0]?.current_user ?? null,
      currentDatabase: identityRow.rows[0]?.current_database ?? null,
      durationMs: Date.now() - started,
      error: null,
      code: null,
      hint: null,
      psqlStdout: null,
    };

    if (psqlPath) {
      const psqlResult = runPsqlVersionQuery(psqlPath, config);
      if (psqlResult.ok) {
        result.method = 'psql';
        result.psqlStdout = psqlResult.stdout;
      }
    }

    return result;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const code = err instanceof Error ? (err as { code?: string }).code ?? null : null;
    return {
      ok: false,
      method: 'pg',
      version: null,
      currentUser: null,
      currentDatabase: null,
      durationMs: Date.now() - started,
      error: message,
      code,
      hint: connectionHint(message, config),
      psqlStdout: null,
    };
  } finally {
    await pool.end().catch(() => undefined);
  }
}

function runPsqlVersionQuery(
  psqlPath: string,
  config: CelgpsMigrationConfig
): { ok: boolean; stdout: string | null; stderr: string | null } {
  let password = config.password ?? '';
  if (!password && config.connectionString) {
    try {
      password = decodeURIComponent(new URL(config.connectionString).password || '');
    } catch {
      password = '';
    }
  }
  const res = spawnSync(
    psqlPath,
    [
      '-h',
      config.host,
      '-p',
      String(config.port),
      '-U',
      config.user,
      '-d',
      config.database,
      '-c',
      'SELECT version(), current_user, current_database();',
      '-t',
      '-A',
    ],
    {
      encoding: 'utf8',
      timeout: 20000,
      env: {
        ...process.env,
        PGPASSWORD: password,
        PGSSLMODE: config.sslRequired ? 'require' : 'prefer',
      },
    }
  );

  if (res.status === 0) {
    return { ok: true, stdout: (res.stdout || '').trim() || null, stderr: null };
  }
  return {
    ok: false,
    stdout: null,
    stderr: (res.stderr || res.stdout || 'psql failed').trim(),
  };
}

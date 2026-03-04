import { PrismaClient } from '@prisma/client';

/**
 * Goldilocks connection string for Prisma on Google Cloud Run/App Hosting.
 * Format: postgresql://USER:PASS@localhost/DBNAME?host=PATH (folder only, no trailing slash, no .s.PGSQL.5432).
 */
function getDatabaseUrl(): string {
  if (process.env.DATABASE_URL) return process.env.DATABASE_URL;

  const user = process.env.PGUSER;
  const pass = encodeURIComponent(process.env.PGPASSWORD || '');
  const db = process.env.PGDATABASE || 'geodata';
  const host = process.env.PGHOST; // folder path e.g. /cloudsql/project:region:instance

  if (!host || !user) throw new Error('Missing DATABASE_URL or PGHOST/PGUSER/PGPASSWORD');

  // Unix socket: localhost + ?host= path (folder only, no trailing slash)
  if (host.startsWith('/')) {
    const pathEncoded = encodeURIComponent(host.replace(/\/$/, '')); // ensure no trailing slash
    return `postgresql://${user}:${pass}@localhost/${db}?host=${pathEncoded}`;
  }

  // TCP (e.g. local dev)
  let url = `postgresql://${user}:${pass}@${host}/${db}`;
  if (process.env.PGPORT) url += `?port=${process.env.PGPORT}`;
  return url;
}

const globalForPrisma = globalThis as unknown as { prisma: PrismaClient | undefined };

function getPrisma(): PrismaClient {
  if (globalForPrisma.prisma) return globalForPrisma.prisma;
  const client = new PrismaClient({
    datasources: {
      db: { url: getDatabaseUrl() },
    },
    log: process.env.NODE_ENV === 'development' ? ['error', 'warn'] : ['error'],
  });
  if (process.env.NODE_ENV !== 'production') globalForPrisma.prisma = client;
  return client;
}

// Lazy proxy so URL is built on first use (request-time env in App Hosting)
export const prisma = new Proxy({} as PrismaClient, {
  get(_target, prop) {
    return (getPrisma() as unknown as Record<string | symbol, unknown>)[prop];
  },
});

import { PrismaClient } from '@prisma/client';

/**
 * Build DATABASE_URL from env. For Unix socket (App Hosting), Prisma needs
 * the placeholder host (localhost) before the ?host= path.
 */
function getDatabaseUrl(): string {
  if (process.env.DATABASE_URL) return process.env.DATABASE_URL;
  if (process.env.PGHOST && process.env.PGUSER && process.env.PGPASSWORD) {
    const password = encodeURIComponent(process.env.PGPASSWORD);
    const db = process.env.PGDATABASE ?? 'geodata';
    const host = process.env.PGHOST;
    // Cloud SQL Unix socket: point to the actual socket file so Prisma doesn't append :5432 to the path
    const socketPath = host.startsWith('/') ? `${host}/.s.PGSQL.5432` : host;
    let url = `postgresql://${process.env.PGUSER}:${password}@localhost/${db}?host=${encodeURIComponent(socketPath)}`;
    if (process.env.PGPORT && !host.startsWith('/')) url += `&port=${process.env.PGPORT}`;
    return url;
  }
  throw new Error('Missing DATABASE_URL or PGHOST/PGUSER/PGPASSWORD');
}

const globalForPrisma = globalThis as unknown as { prisma: PrismaClient | undefined };

function getPrisma(): PrismaClient {
  if (globalForPrisma.prisma) return globalForPrisma.prisma;
  const url = getDatabaseUrl();
  const client = new PrismaClient({
    datasources: {
      db: { url },
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

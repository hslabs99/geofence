import { PrismaClient } from '@prisma/client';

/**
 * Build DATABASE_URL from env. LibPQ Unix socket format: postgresql://user:pass@/db?host=SOCKET_DIR
 * (empty host after @ so Prisma/driver does not append :5432 to the path).
 */
function getDatabaseUrl(): string {
  if (process.env.DATABASE_URL) return process.env.DATABASE_URL;
  if (process.env.PGHOST && process.env.PGUSER && process.env.PGPASSWORD) {
    const user = process.env.PGUSER;
    const password = encodeURIComponent(process.env.PGPASSWORD);
    const db = process.env.PGDATABASE ?? 'geodata';
    const socketDir = process.env.PGHOST; // e.g. /cloudsql/project:region:instance
    if (socketDir.startsWith('/')) {
      return `postgresql://${user}:${password}@/${db}?host=${encodeURIComponent(socketDir)}`;
    }
    let url = `postgresql://${user}:${password}@${socketDir}/${db}`;
    if (process.env.PGPORT) url += `?port=${process.env.PGPORT}`;
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

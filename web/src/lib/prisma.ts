import { PrismaClient } from '@prisma/client';

/** Run before first Prisma use so request-time env (e.g. PGHOST from App Hosting) is applied. */
function ensureDatabaseUrl(): void {
  if (process.env.DATABASE_URL) return;
  if (process.env.PGHOST && process.env.PGUSER && process.env.PGPASSWORD) {
    const password = encodeURIComponent(process.env.PGPASSWORD);
    const db = process.env.PGDATABASE ?? 'geodata';
    const host = process.env.PGHOST;
    process.env.DATABASE_URL = `postgresql://${process.env.PGUSER}:${password}@/${db}?host=${encodeURIComponent(host)}`;
    if (process.env.PGPORT) {
      process.env.DATABASE_URL += `&port=${process.env.PGPORT}`;
    }
  }
}

const globalForPrisma = globalThis as unknown as { prisma: PrismaClient | undefined };

function getPrisma(): PrismaClient {
  ensureDatabaseUrl();
  if (globalForPrisma.prisma) return globalForPrisma.prisma;
  const client = new PrismaClient({
    log: process.env.NODE_ENV === 'development' ? ['error', 'warn'] : ['error'],
  });
  if (process.env.NODE_ENV !== 'production') globalForPrisma.prisma = client;
  return client;
}

// Lazy proxy so ensureDatabaseUrl() runs on first use (request-time env in App Hosting)
export const prisma = new Proxy({} as PrismaClient, {
  get(_target, prop) {
    return (getPrisma() as Record<string | symbol, unknown>)[prop];
  },
});

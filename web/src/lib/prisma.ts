import { PrismaClient } from '@prisma/client';

// Production (e.g. Firebase App Hosting): build DATABASE_URL from PG* env if not set
if (!process.env.DATABASE_URL && process.env.PGHOST && process.env.PGUSER && process.env.PGPASSWORD) {
  const password = encodeURIComponent(process.env.PGPASSWORD);
  const db = process.env.PGDATABASE ?? 'geodata';
  const host = process.env.PGHOST;
  process.env.DATABASE_URL = `postgresql://${process.env.PGUSER}:${password}@/${db}?host=${encodeURIComponent(host)}`;
  if (process.env.PGPORT) {
    process.env.DATABASE_URL += `&port=${process.env.PGPORT}`;
  }
}

const globalForPrisma = globalThis as unknown as { prisma: PrismaClient | undefined };

export const prisma =
  globalForPrisma.prisma ??
  new PrismaClient({
    log: process.env.NODE_ENV === 'development' ? ['error', 'warn'] : ['error'],
  });

if (process.env.NODE_ENV !== 'production') globalForPrisma.prisma = prisma;

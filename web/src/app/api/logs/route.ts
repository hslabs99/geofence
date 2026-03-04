import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';
import { dateToLiteralUTC } from '@/lib/utils';

function jsonSafe<T>(obj: T): T {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'bigint') return String(obj) as T;
  if (typeof obj === 'object' && obj instanceof Date) return dateToLiteralUTC(obj) as T;
  if (Array.isArray(obj)) return obj.map(jsonSafe) as T;
  if (typeof obj === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj)) {
      out[k] = jsonSafe(v);
    }
    return out as T;
  }
  return obj;
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const limit = Math.min(parseInt(searchParams.get('limit') ?? '100', 10), 500);
    const type = searchParams.get('type') ?? undefined;
    const cat1 = searchParams.get('cat1') ?? undefined;

    const logs = await prisma.log.findMany({
      where: {
        ...(type && { logtype: type }),
        ...(cat1 && { logcat1: cat1 }),
      },
      orderBy: { logid: 'desc' },
      take: limit,
    });

    return NextResponse.json({ logs: jsonSafe(logs) });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

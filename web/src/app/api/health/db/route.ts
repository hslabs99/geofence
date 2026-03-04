import { NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

function hintFromError(err: unknown): string {
  const msg = String(err instanceof Error ? err.message : err).toLowerCase();
  if (msg.includes('authentication failed') || msg.includes('password') || msg.includes('credentials')) {
    return 'Check username/password. If password has @#!%&, URL-encode it in DATABASE_URL (e.g. @ → %40).';
  }
  if (msg.includes('econnrefused') || msg.includes('connection refused')) {
    return 'Cannot reach host:port. If using proxy, is it running? If direct, is your IP in Cloud SQL authorized networks?';
  }
  if (msg.includes('econnreset') || msg.includes('timeout') || msg.includes('etimedout')) {
    return 'Connection dropped or timed out. Check firewall, VPN, and that the DB host/port are correct.';
  }
  if (msg.includes('getaddrinfo') || msg.includes('enotfound')) {
    return 'Host name could not be resolved. Check DATABASE_URL host (e.g. 127.0.0.1 or 35.197.176.76).';
  }
  if (!process.env.DATABASE_URL) {
    return 'DATABASE_URL is not set. Add it to .env.local (see .env.local.example).';
  }
  return 'See error message and /api/debug for connection details.';
}

/**
 * GET /api/health/db – sanity check: run SELECT 1, return ok:true if connected.
 * On failure returns error, code, and a short hint for debugging.
 */
export async function GET() {
  try {
    await prisma.$queryRaw`SELECT 1`;
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const code = err instanceof Error ? (err as { code?: string }).code : undefined;
    const hint = hintFromError(err);
    return NextResponse.json(
      {
        ok: false,
        error: message,
        code: code ?? null,
        hint,
      },
      { status: 500 }
    );
  }
}

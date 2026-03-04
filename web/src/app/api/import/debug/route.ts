import { NextResponse } from 'next/server';

export async function GET() {
  const base = process.env.IMPORT_SERVICE_URL ?? 'http://localhost:8080';
  try {
    const res = await fetch(`${base}/debug`, { cache: 'no-store' });
    const data = await res.json().catch(() => ({}));
    return NextResponse.json({
      ok: res.ok,
      status: res.status,
      url: `${base}/debug`,
      ...data,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({
      ok: false,
      error: `Cannot reach import service at ${base}`,
      detail: message,
    });
  }
}

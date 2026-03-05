import { getAccessToken, listPlatformFences, TracksolidApiError } from '@/lib/tracksolid';

/**
 * GET or POST: Fetch platform fence list from Tracksolid only (no DB).
 * Query or body: endpoint (optional, e.g. "hk").
 * Returns { fences: Array<{ fenceId, name, type, ... }> }.
 */
export async function GET(request: Request) {
  return handleFetch(request);
}

export async function POST(request: Request) {
  return handleFetch(request);
}

async function handleFetch(request: Request) {
  try {
    const url = new URL(request.url);
    let endpoint: string | null = url.searchParams.get('endpoint');
    if (request.method === 'POST' && request.headers.get('content-type')?.includes('application/json')) {
      try {
        const body = (await request.json()) as { endpoint?: string };
        if (body.endpoint != null) endpoint = body.endpoint;
      } catch {
        /* ignore */
      }
    }
    const { token } = await getAccessToken(endpoint ?? undefined);
    const { fences } = await listPlatformFences(token, endpoint ?? undefined);
    return Response.json({ fences, count: fences.length });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/tracksolid/fences/fetch]', message);
    if (err instanceof TracksolidApiError) {
      return Response.json({ error: message, debug: err.debug }, { status: 500 });
    }
    return Response.json({ error: message }, { status: 500 });
  }
}

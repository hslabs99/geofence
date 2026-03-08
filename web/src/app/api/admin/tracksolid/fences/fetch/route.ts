import { clearTokenCache, getAccessToken, getBaseUrl, listPlatformFences, TracksolidApiError } from '@/lib/tracksolid';

/** Header name for endpoint override (e.g. "hk", "eu"). Checked first, then body, then query. */
const ENDPOINT_HEADER = 'x-tracksolid-endpoint';

/**
 * GET or POST: Fetch platform fence list from Tracksolid only (no DB).
 * Endpoint from (priority): header X-Tracksolid-Endpoint, then body.endpoint, then query endpoint.
 * Returns { fences, count, debug?, requestDebug? }. On error: { error, debug?, requestDebug? }.
 */
export async function GET(request: Request) {
  return handleFetch(request);
}

export async function POST(request: Request) {
  return handleFetch(request);
}

function getEndpointFromRequest(request: Request, parsedBody?: { endpoint?: string } | null): { endpoint: string | null; source: 'header' | 'body' | 'query' | null } {
  const fromHeader = request.headers.get(ENDPOINT_HEADER)?.trim() || null;
  if (fromHeader) return { endpoint: fromHeader, source: 'header' };
  if (parsedBody?.endpoint != null) return { endpoint: String(parsedBody.endpoint).trim() || null, source: 'body' };
  const url = new URL(request.url);
  const fromQuery = url.searchParams.get('endpoint')?.trim() || null;
  return { endpoint: fromQuery, source: fromQuery ? 'query' : null };
}

async function handleFetch(request: Request) {
  let parsedBody: { endpoint?: string } | null = null;
  if (request.method === 'POST' && request.headers.get('content-type')?.includes('application/json')) {
    try {
      parsedBody = (await request.json()) as { endpoint?: string };
    } catch {
      /* ignore */
    }
  }
  const { endpoint, source: endpointSource } = getEndpointFromRequest(request, parsedBody);
  const resolvedBaseUrl = getBaseUrl(endpoint ?? undefined);
  const requestDebug = {
    endpointSource,
    endpointValue: endpoint,
    resolvedBaseUrl,
    note: endpointSource == null && !endpoint ? 'No endpoint in header, body, or query; using default base URL' : undefined,
  };

  try {
    const { token } = await getAccessToken(endpoint ?? undefined);
    const getNewToken = async () => {
      await clearTokenCache(endpoint ?? undefined);
      const { token: t } = await getAccessToken(endpoint ?? undefined, { skipCache: true });
      return t;
    };
    const { fences, debug } = await listPlatformFences(token, endpoint ?? undefined, undefined, getNewToken);
    return Response.json({
      fences,
      count: fences.length,
      debug: { ...debug, requestDebug },
      requestDebug,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[api/admin/tracksolid/fences/fetch]', message, requestDebug);
    if (err instanceof TracksolidApiError) {
      return Response.json({
        error: message,
        debug: { ...err.debug, requestDebug },
        requestDebug,
      }, { status: 500 });
    }
    return Response.json({ error: message, requestDebug }, { status: 500 });
  }
}

import { clearTokenCache, getAccessToken, getBaseUrl, TracksolidApiError } from '@/lib/tracksolid';
import { getClient } from '@/lib/db';
import { syncTracksolidPlatformGeofences } from '@/lib/tracksolid-fences';

const ENDPOINT_HEADER = 'x-tracksolid-endpoint';

function streamLine(controller: ReadableStreamDefaultController<Uint8Array>, obj: object) {
  controller.enqueue(new TextEncoder().encode(JSON.stringify(obj) + '\n'));
}

function getEndpointFromRequest(request: Request, parsedBody?: { endpoint?: string } | null): { endpoint: string | null; source: 'header' | 'body' | 'query' | null } {
  const fromHeader = request.headers.get(ENDPOINT_HEADER)?.trim() || null;
  if (fromHeader) return { endpoint: fromHeader, source: 'header' };
  if (parsedBody?.endpoint != null) return { endpoint: String(parsedBody.endpoint).trim() || null, source: 'body' };
  const url = new URL(request.url);
  const fromQuery = url.searchParams.get('endpoint')?.trim() || null;
  return { endpoint: fromQuery, source: fromQuery ? 'query' : null };
}

/**
 * POST: Sync Tracksolid platform geofences into tbl_geofences.
 * Streams newline-delimited JSON progress events (stage, message, current, total, fetched, inserted, updated, deleted, error).
 * Endpoint from (priority): header X-Tracksolid-Endpoint, then body.endpoint, then query endpoint.
 */
export async function POST(request: Request) {
  let parsedBody: { endpoint?: string } | null = null;
  if (request.headers.get('content-type')?.includes('application/json')) {
    try {
      parsedBody = await request.json() as { endpoint?: string };
    } catch {
      /* ignore */
    }
  }
  const { endpoint, source: endpointSource } = getEndpointFromRequest(request, parsedBody);
  const requestDebug = {
    endpointSource,
    endpointValue: endpoint,
    resolvedBaseUrl: getBaseUrl(endpoint ?? undefined),
    note: endpointSource == null && !endpoint ? 'No endpoint in header, body, or query; using default base URL' : undefined,
  };

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      let client: Awaited<ReturnType<typeof getClient>> | null = null;
      try {
        streamLine(controller, { stage: 'start', message: 'Starting fence sync…', requestDebug });
        const { token } = await getAccessToken(endpoint ?? undefined);
        const getNewToken = async () => {
          await clearTokenCache(endpoint ?? undefined);
          const { token: t } = await getAccessToken(endpoint ?? undefined, { skipCache: true });
          return t;
        };
        client = await getClient();
        await syncTracksolidPlatformGeofences(token, endpoint ?? undefined, client, (e) => {
          streamLine(controller, e);
        }, getNewToken);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        const debug = err instanceof TracksolidApiError ? { ...err.debug, requestDebug } : undefined;
        console.error('[api/admin/tracksolid/fences/sync]', message, requestDebug);
        streamLine(controller, { stage: 'error', message, error: message, requestDebug, ...(debug && { debug }) });
      } finally {
        client?.release?.();
        controller.close();
      }
    },
  });
  return new Response(stream, {
    headers: {
      'Content-Type': 'application/x-ndjson',
      'Cache-Control': 'no-store',
    },
  });
}

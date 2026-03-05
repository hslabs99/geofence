import { getAccessToken, TracksolidApiError } from '@/lib/tracksolid';
import { getClient } from '@/lib/db';
import { syncTracksolidPlatformGeofences } from '@/lib/tracksolid-fences';

function streamLine(controller: ReadableStreamDefaultController<Uint8Array>, obj: object) {
  controller.enqueue(new TextEncoder().encode(JSON.stringify(obj) + '\n'));
}

/**
 * POST: Sync Tracksolid platform geofences into tbl_geofences.
 * Streams newline-delimited JSON progress events (stage, message, current, total, fetched, inserted, updated, deleted, error).
 * Body or query: endpoint (optional, e.g. "hk").
 */
export async function POST(request: Request) {
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      let client: Awaited<ReturnType<typeof getClient>> | null = null;
      try {
        const url = new URL(request.url);
        let endpoint: string | null = url.searchParams.get('endpoint');
        if (request.headers.get('content-type')?.includes('application/json')) {
          try {
            const body = await request.json() as { endpoint?: string };
            if (body.endpoint != null) endpoint = body.endpoint;
          } catch {
            /* ignore */
          }
        }
        streamLine(controller, { stage: 'start', message: 'Starting fence sync…' });
        const { token } = await getAccessToken(endpoint ?? undefined);
        client = await getClient();
        await syncTracksolidPlatformGeofences(token, endpoint ?? undefined, client, (e) => {
          streamLine(controller, e);
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        const debug = err instanceof TracksolidApiError ? err.debug : undefined;
        console.error('[api/admin/tracksolid/fences/sync]', message);
        streamLine(controller, { stage: 'error', message, error: message, ...(debug && { debug }) });
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

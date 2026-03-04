import { NextResponse } from 'next/server';
import { getAccessToken, listDevices, TracksolidApiError } from '@/lib/tracksolid';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const endpoint = searchParams.get('endpoint') || undefined;
    const refreshToken = searchParams.get('refreshToken') === '1' || searchParams.get('skipCache') === '1';
    const tokenResult = await getAccessToken(endpoint, { skipCache: refreshToken });
    const listResult = await listDevices(tokenResult.token, endpoint);
    return NextResponse.json({
      ok: true,
      devices: listResult.devices,
      debug: {
        steps: [
          { step: 'token', debug: tokenResult.debug },
          { step: 'listDevices', debug: listResult.debug },
        ],
      },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const debug = err instanceof TracksolidApiError ? err.debug : undefined;
    const isAppKeyError = /1001|AppKey|app_key|缺少AppKey/i.test(message);
    return NextResponse.json(
      {
        ok: false,
        error: message,
        ...(debug && { debug }),
        ...(isAppKeyError && {
          appKeyHint:
            'Set TRACKSOLID_APP_KEY and TRACKSOLID_APP_SECRET in web/.env.local (get a new key from Tracksolid if needed), then restart the dev server.',
        }),
      },
      { status: 500 }
    );
  }
}

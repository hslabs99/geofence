import { NextResponse } from 'next/server';
import {
  getAccessToken,
  getTrackForDevice,
  getLast24HoursUTC,
  getYesterdayUTC,
  getDayRangeUTC,
  TracksolidApiError,
} from '@/lib/tracksolid';
import { execute } from '@/lib/db';

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const imei = searchParams.get('imei');
  const endpoint = searchParams.get('endpoint') || undefined;
  const period = searchParams.get('period') || undefined;
  const date = searchParams.get('date')?.trim() || undefined; // YYYY-MM-DD = single day 00:00:00–23:59:59 UTC
  const deviceName = searchParams.get('deviceName') || undefined;
  const refreshToken = searchParams.get('refreshToken') === '1' || searchParams.get('skipCache') === '1';

  try {
    if (!imei?.trim()) {
      return NextResponse.json({ ok: false, error: 'imei required' }, { status: 400 });
    }
    const tokenResult = await getAccessToken(endpoint, { skipCache: refreshToken });
    const { begin, end } = date
      ? getDayRangeUTC(date)
      : period === 'last24h'
        ? getLast24HoursUTC()
        : getYesterdayUTC();
    const trackResult = await getTrackForDevice(
      tokenResult.token,
      imei.trim(),
      begin,
      end,
      endpoint
    );
    const getTrackDebug = deviceName
      ? { ...trackResult.debug, requestedDeviceName: deviceName }
      : trackResult.debug;

    // Log single-day API fetch to tbl_logs for auditing
    if (date) {
      try {
        await execute(
          'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4)',
          [
            'APIfetch',
            deviceName || imei.trim(),
            date,
            JSON.stringify({
              status: trackResult.points.length > 0 ? 'ok' : 'ok_empty',
              rowcount: trackResult.points.length,
              imei: imei.trim(),
              endpoint: endpoint ?? 'default',
              period: date ? 'day' : period ?? 'yesterday',
            }),
          ]
        );
      } catch (logErr) {
        // eslint-disable-next-line no-console
        console.error('[tracksolid/track] Failed to write tbl_logs:', logErr);
      }
    }

    return NextResponse.json({
      ok: true,
      points: trackResult.points,
      debug: {
        steps: [
          { step: 'token', debug: tokenResult.debug },
          { step: 'getTrack', debug: getTrackDebug },
        ],
      },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const debug = err instanceof TracksolidApiError ? err.debug : undefined;
    const rateLimitHint =
      err instanceof TracksolidApiError &&
      (debug?.fullResponse as { code?: number } | undefined)?.code === 1006;
    if (date && imei?.trim()) {
      try {
        await execute(
          'INSERT INTO tbl_logs (logtype, logcat1, logcat2, logdetails) VALUES ($1, $2, $3, $4)',
          [
            'APIfetch',
            deviceName || imei.trim(),
            date,
            JSON.stringify({
              status: 'api_fail',
              rowcount: 0,
              imei: imei.trim(),
              endpoint: endpoint ?? 'default',
              period: date ? 'day' : period ?? 'yesterday',
              error: message,
              rateLimitHint,
            }),
          ]
        );
      } catch (logErr) {
        // eslint-disable-next-line no-console
        console.error('[tracksolid/track] Failed to write tbl_logs for failure:', logErr);
      }
    }
    return NextResponse.json(
      {
        ok: false,
        error: message,
        ...(debug && { debug }),
        ...(rateLimitHint && { rateLimitHint: true }),
      },
      { status: 500 }
    );
  }
}

import { NextResponse } from 'next/server';
import { getGateSpeechPlayback } from '@/lib/gate-speech';
import { isGateSpeechKey } from '@/lib/gate-speech-scenarios';

/** The chime and speech file for one fence alert. The phone plays them when that GPS trigger is on. */
export async function GET(request: Request) {
  try {
    const key = new URL(request.url).searchParams.get('key') ?? '';
    if (!isGateSpeechKey(key)) return NextResponse.json({ error: 'That alert was not found.' }, { status: 404 });
    const playback = await getGateSpeechPlayback(key);
    return NextResponse.json(playback);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

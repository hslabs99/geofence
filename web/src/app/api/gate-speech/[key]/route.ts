import { NextResponse } from 'next/server';
import { readGateSpeechAudio } from '@/lib/gate-speech';
import { isGateSpeechKey } from '@/lib/gate-speech-scenarios';

export async function GET(_request: Request, { params }: { params: Promise<{ key: string }> }) {
  try {
    const { key } = await params;
    if (!isGateSpeechKey(key)) return NextResponse.json({ error: 'That alert was not found.' }, { status: 404 });
    const audio = await readGateSpeechAudio(key);
    if (!audio) return NextResponse.json({ error: 'No speech file yet.' }, { status: 404 });
    return new NextResponse(new Uint8Array(audio), {
      headers: {
        'Content-Type': 'audio/mpeg',
        'Content-Length': String(audio.length),
        'Cache-Control': 'no-store',
      },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

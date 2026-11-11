import { NextResponse } from 'next/server';
import { clearGateSpeechAudio, generateGateSpeech, saveGateSpeechChime, saveGateSpeechRepeat, saveGateSpeechSound, saveGateSpeechText } from '@/lib/gate-speech';
import { isGateSpeechKey } from '@/lib/gate-speech-scenarios';

function speechText(body: unknown): string {
  if (!body || typeof body !== 'object' || !('text' in body)) return '';
  const text = (body as { text?: unknown }).text;
  return typeof text === 'string' ? text : '';
}

export async function PATCH(request: Request, { params }: { params: Promise<{ key: string }> }) {
  try {
    const { key } = await params;
    if (!isGateSpeechKey(key)) return NextResponse.json({ error: 'That alert was not found.' }, { status: 404 });
    const body = (await request.json()) as { text?: unknown; chime?: unknown; playCount?: unknown; gapSeconds?: unknown; sound?: unknown };
    const hasText = typeof body.text === 'string';
    const hasChime = typeof body.chime === 'string';
    const hasRepeat = typeof body.playCount === 'number' && typeof body.gapSeconds === 'number';
    const hasSound = typeof body.sound === 'string';
    if (!hasText && !hasChime && !hasRepeat && !hasSound) {
      return NextResponse.json({ error: 'Enter the words to speak.' }, { status: 400 });
    }
    const clip = hasSound
      ? await saveGateSpeechSound(key, body.sound as string)
      : hasRepeat
        ? await saveGateSpeechRepeat(key, body.playCount as number, body.gapSeconds as number)
        : hasChime
          ? await saveGateSpeechChime(key, body.chime as string)
          : await saveGateSpeechText(key, speechText(body));
    return NextResponse.json({ clip });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const status = /enter the words|under \d+ characters|choose a chime|play the alert|pause between|choose text/i.test(message) ? 400 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

export async function POST(request: Request, { params }: { params: Promise<{ key: string }> }) {
  try {
    const { key } = await params;
    if (!isGateSpeechKey(key)) return NextResponse.json({ error: 'That alert was not found.' }, { status: 404 });
    const clip = await generateGateSpeech(key, speechText(await request.json()));
    return NextResponse.json({ clip });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const status = /enter the words|tone prompt|under \d+ characters|not set/i.test(message) ? 400 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

export async function DELETE(_request: Request, { params }: { params: Promise<{ key: string }> }) {
  try {
    const { key } = await params;
    if (!isGateSpeechKey(key)) return NextResponse.json({ error: 'That alert was not found.' }, { status: 404 });
    const clip = await clearGateSpeechAudio(key);
    return NextResponse.json({ clip });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

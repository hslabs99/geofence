import { NextResponse } from 'next/server';
import { getGateSpeechSettings, listGateSpeech, saveGateSpeechSettings } from '@/lib/gate-speech';

export async function GET() {
  try {
    const [clips, settings] = await Promise.all([listGateSpeech(), getGateSpeechSettings()]);
    return NextResponse.json({ clips, ...settings });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function PATCH(request: Request) {
  try {
    const body = (await request.json()) as { prompt?: unknown; voice?: unknown; gender?: unknown; accent?: unknown };
    const settings = await saveGateSpeechSettings(
      typeof body.prompt === 'string' ? body.prompt : '',
      typeof body.voice === 'string' ? body.voice : '',
      typeof body.gender === 'string' ? body.gender : '',
      typeof body.accent === 'string' ? body.accent : '',
    );
    return NextResponse.json(settings);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const status = /tone prompt|choose a speaker|choose male|accent country/i.test(message) ? 400 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

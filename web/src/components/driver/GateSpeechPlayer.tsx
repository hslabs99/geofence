'use client';

import { useEffect } from 'react';
import { playAlert } from '@/lib/gate-speech-playback';
import { gateSpeechKey, gateSpeechParts } from '@/lib/gate-speech-scenarios';

/**
 * Plays the fence alert that the truck's GPS has just raised.
 * Each gate uses its own Play setting: chime, text, chime and text, or nothing.
 * It plays the saved number of times, with the saved pause between plays, then stops.
 * A red reminder to head for the winery stays silent.
 */
export default function GateSpeechPlayer({
  step,
  alert,
  remind = false,
}: {
  step: 2 | 3 | 4 | null | undefined;
  alert: 'yellow' | 'red' | null | undefined;
  remind?: boolean;
}) {
  const key = gateSpeechKey(step, alert, remind);

  useEffect(() => {
    if (!key) return;
    let stopped = false;
    let playing: { stop: () => void } | null = null;
    let timer = 0;
    let played = 0;

    const playOnce = (chimeId: string, speech: boolean, times: number, gapMs: number) => {
      if (stopped || played >= times) return;
      if (chimeId === 'none' && !speech) return;
      played += 1;
      playing = playAlert(speech ? `/api/gate-speech/${key}?t=${Date.now()}` : null, chimeId, {
        onEnded: () => {
          if (stopped || played >= times) return;
          timer = window.setTimeout(() => playOnce(chimeId, speech, times, gapMs), gapMs);
        },
      });
    };

    const start = async () => {
      try {
        const res = await fetch(`/api/gate-speech/chime?key=${encodeURIComponent(key)}`, { cache: 'no-store' });
        const data = (await res.json()) as { chime?: unknown; hasAudio?: unknown; playCount?: unknown; gapSeconds?: unknown; sound?: unknown };
        if (!res.ok) return;
        if (stopped) return;
        const chime = typeof data.chime === 'string' ? data.chime : 'none';
        const hasAudio = data.hasAudio === true;
        const sound = typeof data.sound === 'string' ? data.sound : 'both';
        const parts = gateSpeechParts(sound, chime, hasAudio);
        const times = typeof data.playCount === 'number' && data.playCount >= 1 ? Math.floor(data.playCount) : 1;
        const gapMs = typeof data.gapSeconds === 'number' && data.gapSeconds >= 0 ? data.gapSeconds * 1000 : 30_000;
        playOnce(parts.chimeId, parts.speech, times, gapMs);
      } catch {
        /* A missed fetch does not keep retrying. The next GPS change can play it. */
      }
    };

    void start();

    return () => {
      stopped = true;
      window.clearTimeout(timer);
      playing?.stop();
    };
  }, [key]);

  return null;
}

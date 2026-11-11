import { chimeFileUrl } from '@/lib/gate-speech-chimes';

type AudioNav = Navigator & { audioSession?: { type: string } };

function alertSession() {
  const nav = navigator as AudioNav;
  if (nav.audioSession) nav.audioSession.type = 'playback';
  return () => {
    if (nav.audioSession) nav.audioSession.type = 'auto';
  };
}

let current: { stop: () => void } | null = null;

/**
 * Plays one chime, then the speech file.
 * onEnded runs when that pair finishes. It does not run when stop() cuts it off.
 * A new play stops the previous one.
 */
export function playAlert(
  speechUrl: string | null,
  chimeId: string,
  options?: { onEnded?: () => void },
): { stop: () => void } {
  current?.stop();
  const release = alertSession();
  const chimeSrc = chimeFileUrl(chimeId);
  const chime = chimeSrc ? new Audio(chimeSrc) : null;
  const speech = speechUrl ? new Audio(speechUrl) : null;
  let stopped = false;
  let finished = false;

  const finish = () => {
    if (stopped || finished) return;
    finished = true;
    release();
    options?.onEnded?.();
  };

  const stop = () => {
    if (stopped) return;
    stopped = true;
    chime?.pause();
    speech?.pause();
    release();
    if (current?.stop === stop) current = null;
  };

  let speechStarted = false;
  const startSpeech = () => {
    if (stopped || speechStarted) return;
    speechStarted = true;
    if (!speech) {
      finish();
      return;
    }
    speech.addEventListener('ended', finish);
    speech.addEventListener('error', finish);
    void speech.play().catch(() => finish());
  };

  if (chime) {
    chime.addEventListener('error', startSpeech);
    chime.addEventListener('ended', startSpeech);
    void chime.play().catch(() => startSpeech());
  } else {
    startSpeech();
  }

  current = { stop };
  return current;
}

'use client';

import { memo, useEffect, useRef, useState } from 'react';
import {
  GATE_SPEECH_CHIME_NONE,
  GATE_SPEECH_CHIMES,
  isGateSpeechChime,
  type GateSpeechChime,
} from '@/lib/gate-speech-chimes';
import { playAlert } from '@/lib/gate-speech-playback';
import {
  GATE_SPEECH_ACCENTS,
  GATE_SPEECH_GAP_MAX,
  GATE_SPEECH_GAP_MIN,
  GATE_SPEECH_GENDERS,
  GATE_SPEECH_PLAY_MAX,
  GATE_SPEECH_PLAY_MIN,
  GATE_SPEECH_VOICES,
  GATE_SPEECH_SOUNDS,
  defaultVoiceForGender,
  gateSpeechInstructions,
  gateSpeechParts,
  voicesForGender,
  type GateSpeechAccent,
  type GateSpeechGender,
  type GateSpeechKey,
  type GateSpeechSound,
  type GateSpeechVoice,
} from '@/lib/gate-speech-scenarios';

type Clip = {
  key: GateSpeechKey;
  alert: 'amber' | 'red';
  step: 2 | 3 | 4;
  label: string;
  place: string;
  note: string | null;
  speechText: string;
  spokenText: string | null;
  spokenPrompt: string | null;
  spokenVoice: string | null;
  chime: GateSpeechChime | typeof GATE_SPEECH_CHIME_NONE;
  playCount: number;
  gapSeconds: number;
  sound: GateSpeechSound;
  hasAudio: boolean;
  bytes: number;
  generatedAt: string | null;
};

function fileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  return `${Math.max(1, Math.round(bytes / 1024))} KB`;
}

function playClip(key: string, version: string | null, chimeId: string, sound: string, hasAudio: boolean) {
  const parts = gateSpeechParts(sound, chimeId, hasAudio);
  playAlert(
    parts.speech ? `/api/gate-speech/${key}?v=${encodeURIComponent(version ?? '')}&t=${Date.now()}` : null,
    parts.chimeId,
  );
}

function playChime(chimeId: string) {
  playAlert(null, chimeId);
}

function GateSpeechPanel() {
  const [clips, setClips] = useState<Clip[]>([]);
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [prompt, setPrompt] = useState('');
  const [promptDraft, setPromptDraft] = useState('');
  const [gender, setGender] = useState<GateSpeechGender>('male');
  const [genderDraft, setGenderDraft] = useState<GateSpeechGender>('male');
  const [accent, setAccent] = useState<GateSpeechAccent>('New Zealand');
  const [accentDraft, setAccentDraft] = useState<GateSpeechAccent>('New Zealand');
  const [voice, setVoice] = useState<GateSpeechVoice>('echo');
  const [voiceDraft, setVoiceDraft] = useState<GateSpeechVoice>('echo');
  const [error, setError] = useState<string | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [chimeBusyKey, setChimeBusyKey] = useState<string | null>(null);
  const [repeatDrafts, setRepeatDrafts] = useState<Record<string, { playCount: string; gapSeconds: string }>>({});
  const [promptBusy, setPromptBusy] = useState(false);

  async function load() {
    const res = await fetch('/api/admin/gate-speech', { cache: 'no-store' });
    const data = await res.json();
    if (!res.ok) throw new Error(data?.error ?? res.statusText);
    const rows = Array.isArray(data?.clips) ? (data.clips as Clip[]) : [];
    const savedPrompt = typeof data?.prompt === 'string' ? data.prompt : '';
    const savedGender: GateSpeechGender = data?.gender === 'female' ? 'female' : 'male';
    const savedAccent: GateSpeechAccent = GATE_SPEECH_ACCENTS.includes(data?.accent) ? data.accent : 'New Zealand';
    const savedVoice: GateSpeechVoice = GATE_SPEECH_VOICES.some((item) => item.id === data?.voice)
      ? data.voice
      : defaultVoiceForGender(savedGender);
    setClips(rows);
    setDrafts(Object.fromEntries(rows.map((row) => [row.key, row.speechText])));
    setPrompt(savedPrompt);
    setPromptDraft(savedPrompt);
    setGender(savedGender);
    setGenderDraft(savedGender);
    setAccent(savedAccent);
    setAccentDraft(savedAccent);
    setVoice(savedVoice);
    setVoiceDraft(savedVoice);
  }

  useEffect(() => {
    let cancelled = false;
    load().catch((err) => {
      if (!cancelled) setError(err instanceof Error ? err.message : String(err));
    });
    return () => {
      cancelled = true;
    };
  }, []);

  function replaceClip(clip: Clip) {
    setClips((current) => current.map((row) => (row.key === clip.key ? clip : row)));
    setDrafts((current) => ({ ...current, [clip.key]: clip.speechText }));
  }

  async function savePrompt() {
    setPromptBusy(true);
    setError(null);
    try {
      const res = await fetch('/api/admin/gate-speech', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          prompt: promptDraft,
          voice: voiceDraft,
          gender: genderDraft,
          accent: accentDraft,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      const saved = typeof data?.prompt === 'string' ? data.prompt : promptDraft;
      const savedGender: GateSpeechGender = data?.gender === 'female' ? 'female' : genderDraft;
      const savedAccent: GateSpeechAccent = GATE_SPEECH_ACCENTS.includes(data?.accent) ? data.accent : accentDraft;
      const savedVoice: GateSpeechVoice = GATE_SPEECH_VOICES.some((item) => item.id === data?.voice) ? data.voice : voiceDraft;
      setPrompt(saved);
      setPromptDraft(saved);
      setGender(savedGender);
      setGenderDraft(savedGender);
      setAccent(savedAccent);
      setAccentDraft(savedAccent);
      setVoice(savedVoice);
      setVoiceDraft(savedVoice);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setPromptBusy(false);
    }
  }

  async function saveChime(key: GateSpeechKey, chimeId: string, previous: Clip['chime']) {
    if (!isGateSpeechChime(chimeId)) return;
    setChimeBusyKey(key);
    setError(null);
    setClips((current) => current.map((row) => (row.key === key ? { ...row, chime: chimeId } : row)));
    try {
      const res = await fetch(`/api/admin/gate-speech/${key}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chime: chimeId }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      const saved = data.clip as Clip;
      setClips((current) => current.map((row) => (row.key === key ? { ...row, chime: saved.chime } : row)));
    } catch (err) {
      setClips((current) => current.map((row) => (row.key === key ? { ...row, chime: previous } : row)));
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setChimeBusyKey(null);
    }
  }

  async function saveSound(key: GateSpeechKey, sound: string, previous: Clip['sound']) {
    setError(null);
    setClips((current) => current.map((row) => (row.key === key ? { ...row, sound: sound as GateSpeechSound } : row)));
    try {
      const res = await fetch(`/api/admin/gate-speech/${key}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sound }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      const saved = data.clip as Clip;
      setClips((current) => current.map((row) => (row.key === key ? { ...row, sound: saved.sound } : row)));
    } catch (err) {
      setClips((current) => current.map((row) => (row.key === key ? { ...row, sound: previous } : row)));
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function saveRepeat(key: GateSpeechKey, playCount: number, gapSeconds: number, saved: { playCount: number; gapSeconds: number }) {
    if (playCount === saved.playCount && gapSeconds === saved.gapSeconds) {
      setRepeatDrafts((current) => {
        const next = { ...current };
        delete next[key];
        return next;
      });
      return;
    }
    setError(null);
    try {
      const res = await fetch(`/api/admin/gate-speech/${key}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playCount, gapSeconds }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      const clip = data.clip as Clip;
      setClips((current) => current.map((row) => (row.key === key ? { ...row, playCount: clip.playCount, gapSeconds: clip.gapSeconds } : row)));
      setRepeatDrafts((current) => {
        const next = { ...current };
        delete next[key];
        return next;
      });
    } catch (err) {
      setRepeatDrafts((current) => {
        const next = { ...current };
        delete next[key];
        return next;
      });
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  const saveRepeatRef = useRef(saveRepeat);
  saveRepeatRef.current = saveRepeat;

  useEffect(() => {
    const handles: number[] = [];
    for (const [key, draft] of Object.entries(repeatDrafts)) {
      const clip = clips.find((row) => row.key === key);
      if (!clip) continue;
      const playCount = Number(draft.playCount);
      const gapSeconds = Number(draft.gapSeconds);
      if (!Number.isInteger(playCount) || !Number.isInteger(gapSeconds)) continue;
      if (playCount === clip.playCount && gapSeconds === clip.gapSeconds) continue;
      handles.push(window.setTimeout(() => {
        void saveRepeatRef.current(key as GateSpeechKey, playCount, gapSeconds, clip);
      }, 500));
    }
    return () => {
      handles.forEach((id) => window.clearTimeout(id));
    };
  }, [repeatDrafts, clips]);

  async function saveText(key: GateSpeechKey) {
    setBusyKey(key);
    setError(null);
    try {
      const res = await fetch(`/api/admin/gate-speech/${key}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: drafts[key] ?? '' }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      replaceClip(data.clip as Clip);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyKey(null);
    }
  }

  async function generate(key: GateSpeechKey) {
    setBusyKey(key);
    setError(null);
    try {
      const res = await fetch(`/api/admin/gate-speech/${key}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: drafts[key] ?? '' }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      replaceClip(data.clip as Clip);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyKey(null);
    }
  }

  async function clearAudio(key: GateSpeechKey) {
    setBusyKey(key);
    setError(null);
    try {
      const res = await fetch(`/api/admin/gate-speech/${key}`, { method: 'DELETE' });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error ?? res.statusText);
      replaceClip(data.clip as Clip);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyKey(null);
    }
  }

  return (
    <div className="mt-6 max-w-3xl">
      <p className="text-sm text-zinc-600 dark:text-zinc-400">
        Each row is one fence alert on the driver phone. Edit the words, then generate the speech.
        The clip is stored in the database and written to <span className="font-mono">public/gate-speech</span>.
        The phone plays it when that alert appears. Deploying those files, or using this same database, is what makes them available on the live server.
      </p>
      {error && (
        <div className="mt-4 rounded bg-red-100 p-3 text-sm text-red-800 dark:bg-red-900/30 dark:text-red-200">{error}</div>
      )}
      <section className="mt-4 rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
        <div className="grid gap-3 sm:grid-cols-3">
          <label className="block text-sm text-zinc-700 dark:text-zinc-200">
            Gender
            <select
              value={genderDraft}
              onChange={(event) => {
                const next = event.target.value === 'female' ? 'female' : 'male';
                setGenderDraft(next);
                if (!voicesForGender(next).some((item) => item.id === voiceDraft)) {
                  setVoiceDraft(defaultVoiceForGender(next));
                }
              }}
              className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            >
              {GATE_SPEECH_GENDERS.map((item) => (
                <option key={item.id} value={item.id}>{item.label}</option>
              ))}
            </select>
          </label>
          <label className="block text-sm text-zinc-700 dark:text-zinc-200">
            Accent country
            <select
              value={accentDraft}
              onChange={(event) => setAccentDraft(event.target.value as GateSpeechAccent)}
              className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            >
              {GATE_SPEECH_ACCENTS.map((item) => (
                <option key={item} value={item}>{item}</option>
              ))}
            </select>
          </label>
          <label className="block text-sm text-zinc-700 dark:text-zinc-200">
            Speaker
            <select
              value={voiceDraft}
              onChange={(event) => setVoiceDraft(event.target.value as GateSpeechVoice)}
              className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
            >
              {voicesForGender(genderDraft).map((item) => (
                <option key={item.id} value={item.id}>{item.label} · {item.heardAs}</option>
              ))}
            </select>
          </label>
        </div>
        <fieldset className="mt-4">
          <legend className="text-sm text-zinc-700 dark:text-zinc-200">Try a chime</legend>
          <p className="mt-1 text-xs text-zinc-500">Hear a sound here. Each alert below keeps its own choice, played before that speech.</p>
          <div className="mt-2 flex flex-col gap-1">
            {GATE_SPEECH_CHIMES.map((item) => (
              <div key={item.id} className="flex items-center gap-2">
                <span className="min-w-0 flex-1 text-sm text-zinc-800 dark:text-zinc-100">{item.label}</span>
                <button
                  type="button"
                  onClick={() => playChime(item.id)}
                  className="rounded border border-zinc-300 px-2 py-1 text-xs dark:border-zinc-600"
                >
                  Test
                </button>
              </div>
            ))}
          </div>
        </fieldset>
        <label className="mt-3 block text-sm text-zinc-700 dark:text-zinc-200">
          Tone prompt
          <textarea
            value={promptDraft}
            rows={3}
            maxLength={1000}
            onChange={(event) => setPromptDraft(event.target.value)}
            className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
          />
        </label>
        <p className="mt-2 text-xs text-zinc-500">
          Gender chooses the speaker. Accent is added to the instructions. These two lines are everything sent to the speech model.
        </p>
        <p className="mt-1 rounded bg-zinc-50 px-2 py-1.5 font-mono text-xs text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300">
          Voice: {voiceDraft}
          <br />
          Instructions: {gateSpeechInstructions(promptDraft, accentDraft)}
        </p>
        <button
          type="button"
          disabled={promptBusy || (promptDraft === prompt && genderDraft === gender && accentDraft === accent && voiceDraft === voice)}
          onClick={() => void savePrompt()}
          className="mt-2 rounded border border-zinc-300 px-3 py-1.5 text-sm disabled:opacity-50 dark:border-zinc-600"
        >
          {promptBusy ? 'Saving…' : 'Save voice settings'}
        </button>
        {(promptDraft !== prompt || genderDraft !== gender || accentDraft !== accent || voiceDraft !== voice) && (
          <p className="mt-2 text-xs text-zinc-500">Voice settings are not saved yet.</p>
        )}
      </section>
      <div className="mt-4 flex flex-col gap-3">
        {clips.map((clip) => {
          const draft = drafts[clip.key] ?? clip.speechText;
          const dirty = draft !== clip.speechText;
          const staleText = clip.hasAudio && clip.spokenText != null && clip.spokenText !== clip.speechText;
          const stalePrompt = clip.hasAudio && (clip.spokenPrompt !== gateSpeechInstructions(prompt, accent) || clip.spokenVoice !== voice);
          const busy = busyKey === clip.key;
          const parts = gateSpeechParts(clip.sound, clip.chime, clip.hasAudio);
          return (
            <section key={clip.key} className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
              <div className="flex flex-wrap items-center gap-2">
                <span
                  className={`rounded px-2 py-0.5 text-xs font-semibold ${
                    clip.alert === 'red'
                      ? 'bg-red-100 text-red-800 dark:bg-red-950/60 dark:text-red-200'
                      : 'bg-amber-100 text-amber-900 dark:bg-amber-950/60 dark:text-amber-100'
                  }`}
                >
                  {clip.label}
                </span>
                <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">{clip.place}</h2>
              </div>
              {clip.note && <p className="mt-1 text-xs text-zinc-500">{clip.note}</p>}
              <label className="mt-3 block text-sm text-zinc-700 dark:text-zinc-200">
                Speech
                <textarea
                  value={draft}
                  rows={2}
                  maxLength={400}
                  onChange={(event) => setDrafts((current) => ({ ...current, [clip.key]: event.target.value }))}
                  className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                />
              </label>
              <label className="mt-3 block text-sm text-zinc-700 dark:text-zinc-200">
                Chime
                <select
                  value={clip.chime}
                  disabled={chimeBusyKey === clip.key}
                  onChange={(event) => void saveChime(clip.key, event.target.value, clip.chime)}
                  className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                >
                  <option value={GATE_SPEECH_CHIME_NONE}>None</option>
                  {GATE_SPEECH_CHIMES.map((item) => (
                    <option key={item.id} value={item.id}>{item.label}</option>
                  ))}
                </select>
              </label>
              <p className="mt-1 text-xs text-zinc-500">Played before this speech. None skips the chime.</p>
              <label className="mt-3 block text-sm text-zinc-700 dark:text-zinc-200">
                Play
                <select
                  value={clip.sound}
                  onChange={(event) => void saveSound(clip.key, event.target.value, clip.sound)}
                  className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                >
                  {GATE_SPEECH_SOUNDS.map((item) => (
                    <option key={item.id} value={item.id}>{item.label}</option>
                  ))}
                </select>
              </label>
              <p className="mt-1 text-xs text-zinc-500">Nothing turns this alert off. The phone stays quiet for this gate.</p>
              <div className="mt-3 grid gap-3 sm:grid-cols-2">
                <label className="block text-sm text-zinc-700 dark:text-zinc-200">
                  Times to play
                  <input
                    type="number"
                    min={GATE_SPEECH_PLAY_MIN}
                    max={GATE_SPEECH_PLAY_MAX}
                    inputMode="numeric"
                    value={repeatDrafts[clip.key]?.playCount ?? String(clip.playCount)}
                    onChange={(event) => setRepeatDrafts((current) => ({
                      ...current,
                      [clip.key]: {
                        playCount: event.target.value,
                        gapSeconds: current[clip.key]?.gapSeconds ?? String(clip.gapSeconds),
                      },
                    }))}
                    onBlur={(event) => void saveRepeat(
                      clip.key,
                      Number(event.currentTarget.value),
                      Number(repeatDrafts[clip.key]?.gapSeconds ?? clip.gapSeconds),
                      clip,
                    )}
                    className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                  />
                </label>
                <label className="block text-sm text-zinc-700 dark:text-zinc-200">
                  Seconds between
                  <input
                    type="number"
                    min={GATE_SPEECH_GAP_MIN}
                    max={GATE_SPEECH_GAP_MAX}
                    inputMode="numeric"
                    value={repeatDrafts[clip.key]?.gapSeconds ?? String(clip.gapSeconds)}
                    onChange={(event) => setRepeatDrafts((current) => ({
                      ...current,
                      [clip.key]: {
                        playCount: current[clip.key]?.playCount ?? String(clip.playCount),
                        gapSeconds: event.target.value,
                      },
                    }))}
                    onBlur={(event) => void saveRepeat(
                      clip.key,
                      Number(repeatDrafts[clip.key]?.playCount ?? clip.playCount),
                      Number(event.currentTarget.value),
                      clip,
                    )}
                    className="mt-1 w-full rounded border border-zinc-300 bg-white px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800"
                  />
                </label>
              </div>
              <p className="mt-1 text-xs text-zinc-500">
                The phone plays this alert this many times, then stops. Seconds between is the pause after each play. One time has no pause.
              </p>
              <div className="mt-2 flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  disabled={busy || !dirty}
                  onClick={() => void saveText(clip.key)}
                  className="rounded border border-zinc-300 px-3 py-1.5 text-sm disabled:opacity-50 dark:border-zinc-600"
                >
                  Save text
                </button>
                <button
                  type="button"
                  disabled={busy || !draft.trim()}
                  onClick={() => void generate(clip.key)}
                  className="rounded bg-zinc-900 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
                >
                  {busy ? 'Working…' : 'Generate speech'}
                </button>
                <button
                  type="button"
                  disabled={!parts.speech && parts.chimeId === 'none'}
                  onClick={() => playClip(clip.key, clip.generatedAt, clip.chime, clip.sound, clip.hasAudio)}
                  className="rounded border border-zinc-300 px-3 py-1.5 text-sm disabled:opacity-50 dark:border-zinc-600"
                >
                  Play
                </button>
                <button
                  type="button"
                  disabled={busy || !clip.hasAudio}
                  onClick={() => void clearAudio(clip.key)}
                  className="rounded border border-red-300 px-3 py-1.5 text-sm text-red-700 disabled:opacity-50 dark:border-red-800 dark:text-red-300"
                >
                  Clear file
                </button>
              </div>
              <p className="mt-2 text-xs text-zinc-500">
                {clip.hasAudio
                  ? `Speech file${clip.generatedAt ? ` ${clip.generatedAt} UTC` : ''} · ${fileSize(clip.bytes)}`
                  : 'No speech file yet.'}
                {staleText ? ' Generate again so the file matches this text.' : ''}
                {stalePrompt ? ' Generate again so the file matches the gender, accent, and prompt.' : ''}
                {dirty ? ' Text is not saved yet.' : ''}
              </p>
            </section>
          );
        })}
      </div>
    </div>
  );
}

export default memo(GateSpeechPanel);

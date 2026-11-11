import fs from 'node:fs/promises';
import path from 'node:path';
import { execute, query } from '@/lib/db';
import { GATE_SPEECH_CHIME_NONE, isGateSpeechChime, type GateSpeechChime } from '@/lib/gate-speech-chimes';
import {
  GATE_SPEECH_ACCENT,
  GATE_SPEECH_GENDER,
  GATE_SPEECH_PROMPT,
  GATE_SPEECH_PROMPT_MAX,
  GATE_SPEECH_GAP_DEFAULT,
  GATE_SPEECH_GAP_MAX,
  GATE_SPEECH_GAP_MIN,
  GATE_SPEECH_PLAY_MAX,
  GATE_SPEECH_PLAY_MIN,
  GATE_SPEECH_SCENARIOS,
  GATE_SPEECH_VOICE,
  GATE_SPEECH_VOICES,
  GATE_SPEECH_SOUND_DEFAULT,
  defaultPlayCount,
  gateSpeechInstructions,
  isGateSpeechSound,
  parseGapSeconds,
  parsePlayCount,
  isGateSpeechAccent,
  isGateSpeechGender,
  isGateSpeechVoice,
  voicesForGender,
  type GateSpeechAccent,
  type GateSpeechGender,
  type GateSpeechKey,
  type GateSpeechScenario,
  type GateSpeechSound,
  type GateSpeechVoice,
} from '@/lib/gate-speech-scenarios';

const SPEECH_MODEL = 'gpt-4o-mini-tts';
const SPEECH_MAX = 400;

export type GateSpeechRecord = {
  key: GateSpeechKey;
  alert: GateSpeechScenario['alert'];
  step: GateSpeechScenario['step'];
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

type SpeechRow = {
  scenario: string;
  speech_text: string | null;
  spoken_text: string | null;
  spoken_prompt: string | null;
  spoken_voice: string | null;
  chime: string | null;
  play_count: number | null;
  gap_seconds: number | null;
  sound: string | null;
  has_audio: boolean;
  bytes: number | null;
  generated_at: string | null;
};

let ensured: Promise<void> | null = null;

function speechFilePath(key: GateSpeechKey): string {
  return path.join(process.cwd(), 'public', 'gate-speech', `${key}.mp3`);
}

function scenarioNote(row: GateSpeechScenario): string | null {
  return 'note' in row ? row.note : null;
}

function cleanSpeech(text: string): string {
  const speech = text.replace(/\s+/g, ' ').trim();
  if (!speech) throw new Error('Enter the words to speak.');
  if (speech.length > SPEECH_MAX) throw new Error(`Keep the speech under ${SPEECH_MAX} characters.`);
  return speech;
}

function cleanPrompt(text: string): string {
  const prompt = text.replace(/\s+/g, ' ').trim();
  if (!prompt) throw new Error('Enter the tone prompt.');
  if (prompt.length > GATE_SPEECH_PROMPT_MAX) {
    throw new Error(`Keep the tone prompt under ${GATE_SPEECH_PROMPT_MAX} characters.`);
  }
  return prompt;
}

function asAudio(value: unknown): Buffer | null {
  if (value == null) return null;
  if (Buffer.isBuffer(value)) return value.length > 0 ? value : null;
  if (value instanceof Uint8Array) {
    const bytes = Buffer.from(value);
    return bytes.length > 0 ? bytes : null;
  }
  if (typeof value === 'string' && value.length > 0) {
    const hex = value.startsWith('\\x') ? value.slice(2) : value;
    const bytes = Buffer.from(hex, 'hex');
    return bytes.length > 0 ? bytes : null;
  }
  return null;
}

async function fileBytes(key: GateSpeechKey): Promise<number> {
  try {
    const stat = await fs.stat(speechFilePath(key));
    return stat.size;
  } catch {
    return 0;
  }
}

export async function ensureGateSpeechSchema(): Promise<void> {
  if (!ensured) {
    ensured = seedGateSpeech().catch((err) => {
      ensured = null;
      throw err;
    });
  }
  return ensured;
}

async function seedGateSpeech(): Promise<void> {
  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_gate_speech (
      scenario     text PRIMARY KEY,
      speech_text  text NOT NULL,
      spoken_text  text,
      audio        bytea,
      generated_at timestamp without time zone
    )`);

  await execute(`ALTER TABLE tbl_gate_speech ADD COLUMN IF NOT EXISTS spoken_prompt text`);
  await execute(`ALTER TABLE tbl_gate_speech ADD COLUMN IF NOT EXISTS spoken_voice text`);
  await execute(`ALTER TABLE tbl_gate_speech ADD COLUMN IF NOT EXISTS chime text`);
  await execute(
    `UPDATE tbl_gate_speech SET chime = $1 WHERE chime IS NULL OR btrim(chime) = ''`,
    [GATE_SPEECH_CHIME_NONE],
  );
  await execute(`ALTER TABLE tbl_gate_speech ADD COLUMN IF NOT EXISTS play_count integer`);
  await execute(`ALTER TABLE tbl_gate_speech ADD COLUMN IF NOT EXISTS gap_seconds integer`);
  await execute(`UPDATE tbl_gate_speech SET play_count = 1 WHERE play_count IS NULL AND scenario LIKE 'amber-%'`);
  await execute(`UPDATE tbl_gate_speech SET play_count = 3 WHERE play_count IS NULL AND scenario LIKE 'red-%'`);
  await execute(`UPDATE tbl_gate_speech SET gap_seconds = $1 WHERE gap_seconds IS NULL`, [GATE_SPEECH_GAP_DEFAULT]);
  await execute(`ALTER TABLE tbl_gate_speech ADD COLUMN IF NOT EXISTS sound text`);
  await execute(
    `UPDATE tbl_gate_speech SET sound = $1 WHERE sound IS NULL OR btrim(sound) = ''`,
    [GATE_SPEECH_SOUND_DEFAULT],
  );

  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_gate_speech_prompt (
      id     smallint PRIMARY KEY,
      prompt text NOT NULL
    )`);
  await execute(`ALTER TABLE tbl_gate_speech_prompt ADD COLUMN IF NOT EXISTS voice text`);
  await execute(`ALTER TABLE tbl_gate_speech_prompt ADD COLUMN IF NOT EXISTS gender text`);
  await execute(`ALTER TABLE tbl_gate_speech_prompt ADD COLUMN IF NOT EXISTS accent text`);
  await execute(`ALTER TABLE tbl_gate_speech_prompt ADD COLUMN IF NOT EXISTS chime text`);
  await execute(
    `UPDATE tbl_gate_speech_prompt SET chime = $1 WHERE chime IS NULL OR btrim(chime) = ''`,
    [GATE_SPEECH_CHIME_NONE],
  );
  await execute(
    `UPDATE tbl_gate_speech_prompt SET voice = $1 WHERE voice IS NULL OR btrim(voice) = ''`,
    [GATE_SPEECH_VOICE],
  );
  await execute(
    `UPDATE tbl_gate_speech_prompt SET gender = $1 WHERE gender IS NULL OR btrim(gender) = ''`,
    [GATE_SPEECH_GENDER],
  );
  await execute(
    `UPDATE tbl_gate_speech_prompt SET accent = $1 WHERE accent IS NULL OR btrim(accent) = ''`,
    [GATE_SPEECH_ACCENT],
  );

  await execute(
    `INSERT INTO tbl_gate_speech_prompt (id, prompt, voice, gender, accent, chime)
     VALUES (1, $1, $2, $3, $4, $5)
     ON CONFLICT (id) DO NOTHING`,
    [GATE_SPEECH_PROMPT, GATE_SPEECH_VOICE, GATE_SPEECH_GENDER, GATE_SPEECH_ACCENT, GATE_SPEECH_CHIME_NONE],
  );

  for (const row of GATE_SPEECH_SCENARIOS) {
    await execute(
      `INSERT INTO tbl_gate_speech (scenario, speech_text)
       VALUES ($1, $2)
       ON CONFLICT (scenario) DO NOTHING`,
      [row.key, row.speech],
    );
  }
}

export type GateSpeechSettings = {
  prompt: string;
  voice: GateSpeechVoice;
  gender: GateSpeechGender;
  accent: GateSpeechAccent;
  instructions: string;
};

function voiceFor(value: string | null | undefined, gender: GateSpeechGender): GateSpeechVoice {
  const voice = value?.trim() ?? '';
  const match = GATE_SPEECH_VOICES.find((item) => item.id === voice && item.gender === gender);
  if (match) return match.id;
  const fallback = voicesForGender(gender)[0];
  return fallback?.id ?? GATE_SPEECH_VOICE;
}

export async function getGateSpeechSettings(): Promise<GateSpeechSettings> {
  await ensureGateSpeechSchema();
  const rows = await query<{ prompt: string | null; voice: string | null; gender: string | null; accent: string | null }>(
    `SELECT prompt, voice, gender, accent FROM tbl_gate_speech_prompt WHERE id = 1`,
  );
  const gender = isGateSpeechGender(rows[0]?.gender?.trim() ?? '') ? (rows[0]?.gender?.trim() as GateSpeechGender) : GATE_SPEECH_GENDER;
  const accent = isGateSpeechAccent(rows[0]?.accent?.trim() ?? '') ? (rows[0]?.accent?.trim() as GateSpeechAccent) : GATE_SPEECH_ACCENT;
  const prompt = rows[0]?.prompt?.trim() || GATE_SPEECH_PROMPT;
  const voice = voiceFor(rows[0]?.voice, gender);
  return { prompt, voice, gender, accent, instructions: gateSpeechInstructions(prompt, accent) };
}

export async function saveGateSpeechSettings(
  text: string,
  voice: string,
  gender: string,
  accent: string,
): Promise<GateSpeechSettings> {
  await ensureGateSpeechSchema();
  const prompt = cleanPrompt(text);
  const speaker = voice.trim();
  const chosenGender = gender.trim();
  const chosenAccent = accent.trim();
  if (!isGateSpeechGender(chosenGender)) throw new Error('Choose male or female.');
  if (!isGateSpeechAccent(chosenAccent)) throw new Error('Choose an accent country.');
  if (!isGateSpeechVoice(speaker) || !voicesForGender(chosenGender).some((item) => item.id === speaker)) {
    throw new Error('Choose a speaker that matches the gender.');
  }
  const updated = await execute(
    `UPDATE tbl_gate_speech_prompt SET prompt = $1, voice = $2, gender = $3, accent = $4 WHERE id = 1`,
    [prompt, speaker, chosenGender, chosenAccent],
  );
  if (updated === 0) {
    await execute(
      `INSERT INTO tbl_gate_speech_prompt (id, prompt, voice, gender, accent, chime) VALUES (1, $1, $2, $3, $4, $5)`,
      [prompt, speaker, chosenGender, chosenAccent, GATE_SPEECH_CHIME_NONE],
    );
  }
  return {
    prompt,
    voice: speaker,
    gender: chosenGender,
    accent: chosenAccent,
    instructions: gateSpeechInstructions(prompt, chosenAccent),
  };
}

export async function listGateSpeech(): Promise<GateSpeechRecord[]> {
  await ensureGateSpeechSchema();
  const rows = await query<SpeechRow>(
    `SELECT scenario,
            speech_text,
            spoken_text,
            spoken_prompt,
            spoken_voice,
            chime,
            play_count,
            gap_seconds,
            sound,
            (audio IS NOT NULL) AS has_audio,
            COALESCE(octet_length(audio), 0)::int AS bytes,
            to_char(generated_at, 'YYYY-MM-DD HH24:MI:SS') AS generated_at
     FROM tbl_gate_speech`,
  );
  const byKey = new Map(rows.map((row) => [row.scenario, row]));
  return Promise.all(GATE_SPEECH_SCENARIOS.map(async (scenario) => {
    const row = byKey.get(scenario.key);
    const onDisk = await fileBytes(scenario.key);
    const inDb = row?.has_audio === true && Number(row.bytes) > 0;
    return {
      key: scenario.key,
      alert: scenario.alert,
      step: scenario.step,
      label: scenario.label,
      place: scenario.place,
      note: scenarioNote(scenario),
      speechText: row?.speech_text?.trim() || scenario.speech,
      spokenText: row?.spoken_text?.trim() || null,
      spokenPrompt: row?.spoken_prompt?.trim() || null,
      spokenVoice: row?.spoken_voice?.trim() || null,
      chime: isGateSpeechChime(row?.chime?.trim() ?? '') ? (row?.chime?.trim() as GateSpeechChime | typeof GATE_SPEECH_CHIME_NONE) : GATE_SPEECH_CHIME_NONE,
      playCount: storedPlayCount(row?.play_count, scenario.alert),
      gapSeconds: storedGapSeconds(row?.gap_seconds),
      sound: storedSound(row?.sound),
      hasAudio: inDb || onDisk > 0,
      bytes: inDb ? Number(row?.bytes ?? 0) : onDisk,
      generatedAt: row?.generated_at ?? null,
    };
  }));
}

function storedPlayCount(value: number | null | undefined, alert: 'amber' | 'red'): number {
  if (value == null) return defaultPlayCount(alert);
  const count = Number(value);
  if (!Number.isInteger(count)) return defaultPlayCount(alert);
  return Math.min(GATE_SPEECH_PLAY_MAX, Math.max(GATE_SPEECH_PLAY_MIN, count));
}

function storedGapSeconds(value: number | null | undefined): number {
  if (value == null) return GATE_SPEECH_GAP_DEFAULT;
  const seconds = Number(value);
  if (!Number.isInteger(seconds)) return GATE_SPEECH_GAP_DEFAULT;
  return Math.min(GATE_SPEECH_GAP_MAX, Math.max(GATE_SPEECH_GAP_MIN, seconds));
}

function storedSound(value: string | null | undefined): GateSpeechSound {
  const sound = value?.trim() ?? '';
  return isGateSpeechSound(sound) ? sound : GATE_SPEECH_SOUND_DEFAULT;
}

function storedChime(value: string): GateSpeechChime | typeof GATE_SPEECH_CHIME_NONE {
  const chosen = value.trim();
  if (!isGateSpeechChime(chosen)) throw new Error('Choose a chime.');
  return chosen;
}

/** Chime and whether a speech file exists, for one GPS alert on the driver phone. */
export async function getGateSpeechPlayback(key: GateSpeechKey): Promise<{
  chime: GateSpeechChime | typeof GATE_SPEECH_CHIME_NONE;
  hasAudio: boolean;
  playCount: number;
  gapSeconds: number;
  sound: GateSpeechSound;
}> {
  await ensureGateSpeechSchema();
  const rows = await query<{ chime: string | null; bytes: number | null; play_count: number | null; gap_seconds: number | null; sound: string | null }>(
    `SELECT chime, play_count, gap_seconds, sound, COALESCE(octet_length(audio), 0)::int AS bytes
     FROM tbl_gate_speech WHERE scenario = $1`,
    [key],
  );
  const stored = rows[0]?.chime?.trim() ?? '';
  const chime = isGateSpeechChime(stored) ? stored : GATE_SPEECH_CHIME_NONE;
  const hasAudio = Number(rows[0]?.bytes ?? 0) > 0 || (await fileBytes(key)) > 0;
  const alert = key.startsWith('red') ? 'red' : 'amber';
  return {
    chime,
    hasAudio,
    playCount: storedPlayCount(rows[0]?.play_count, alert),
    gapSeconds: storedGapSeconds(rows[0]?.gap_seconds),
    sound: storedSound(rows[0]?.sound),
  };
}

export async function saveGateSpeechSound(key: GateSpeechKey, sound: string): Promise<GateSpeechRecord> {
  await ensureGateSpeechSchema();
  const chosen = sound.trim();
  if (!isGateSpeechSound(chosen)) throw new Error('Choose chime, text, chime and text, or nothing.');
  const updated = await execute(
    `UPDATE tbl_gate_speech SET sound = $2 WHERE scenario = $1`,
    [key, chosen],
  );
  if (updated === 0) throw new Error('That alert was not found.');
  const rows = await listGateSpeech();
  const row = rows.find((item) => item.key === key);
  if (!row) throw new Error('That alert was not found.');
  return row;
}

export async function saveGateSpeechRepeat(key: GateSpeechKey, playCount: number, gapSeconds: number): Promise<GateSpeechRecord> {
  await ensureGateSpeechSchema();
  const times = parsePlayCount(playCount);
  const pause = parseGapSeconds(gapSeconds);
  const updated = await execute(
    `UPDATE tbl_gate_speech SET play_count = $2, gap_seconds = $3 WHERE scenario = $1`,
    [key, times, pause],
  );
  if (updated === 0) throw new Error('That alert was not found.');
  const rows = await listGateSpeech();
  const row = rows.find((item) => item.key === key);
  if (!row) throw new Error('That alert was not found.');
  return row;
}

export async function saveGateSpeechChime(key: GateSpeechKey, chime: string): Promise<GateSpeechRecord> {
  await ensureGateSpeechSchema();
  const chosen = storedChime(chime);
  const updated = await execute(
    `UPDATE tbl_gate_speech SET chime = $2 WHERE scenario = $1`,
    [key, chosen],
  );
  if (updated === 0) throw new Error('That alert was not found.');
  const rows = await listGateSpeech();
  const row = rows.find((item) => item.key === key);
  if (!row) throw new Error('That alert was not found.');
  return row;
}

export async function saveGateSpeechText(key: GateSpeechKey, text: string): Promise<GateSpeechRecord> {
  await ensureGateSpeechSchema();
  const speech = cleanSpeech(text);
  const updated = await execute(
    `UPDATE tbl_gate_speech SET speech_text = $2 WHERE scenario = $1`,
    [key, speech],
  );
  if (updated === 0) throw new Error('That alert was not found.');
  const rows = await listGateSpeech();
  const row = rows.find((item) => item.key === key);
  if (!row) throw new Error('That alert was not found.');
  return row;
}

async function speak(text: string, instructions: string, voice: GateSpeechVoice): Promise<Buffer> {
  const apiKey = process.env.OPENAI_API_KEY?.trim() ?? '';
  if (!apiKey) throw new Error('OPENAI_API_KEY is not set on this server.');
  const res = await fetch('https://api.openai.com/v1/audio/speech', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: SPEECH_MODEL,
      voice,
      input: text,
      instructions,
      response_format: 'mp3',
    }),
  });
  if (!res.ok) {
    const detail = (await res.text()).slice(0, 300);
    throw new Error(`Speech generation failed (${res.status}). ${detail}`);
  }
  const bytes = Buffer.from(await res.arrayBuffer());
  if (bytes.length === 0) throw new Error('Speech generation returned an empty file.');
  return bytes;
}

/** Saves the words, stores the mp3 in the database, and writes public/gate-speech so a deploy can carry it. */
export async function generateGateSpeech(key: GateSpeechKey, text: string): Promise<GateSpeechRecord> {
  await ensureGateSpeechSchema();
  const speech = cleanSpeech(text);
  const settings = await getGateSpeechSettings();
  const audio = await speak(speech, settings.instructions, settings.voice);
  const updated = await execute(
    `UPDATE tbl_gate_speech
     SET speech_text = $2, spoken_text = $2, spoken_prompt = $3, spoken_voice = $4, audio = $5, generated_at = (clock_timestamp() AT TIME ZONE 'UTC')
     WHERE scenario = $1`,
    [key, speech, settings.instructions, settings.voice, audio],
  );
  if (updated === 0) throw new Error('That alert was not found.');
  await fs.mkdir(path.dirname(speechFilePath(key)), { recursive: true });
  await fs.writeFile(speechFilePath(key), audio);
  const rows = await listGateSpeech();
  const row = rows.find((item) => item.key === key);
  if (!row) throw new Error('That alert was not found.');
  return row;
}

export async function clearGateSpeechAudio(key: GateSpeechKey): Promise<GateSpeechRecord> {
  await ensureGateSpeechSchema();
  const updated = await execute(
    `UPDATE tbl_gate_speech
     SET audio = NULL, spoken_text = NULL, spoken_prompt = NULL, spoken_voice = NULL, generated_at = NULL
     WHERE scenario = $1`,
    [key],
  );
  if (updated === 0) throw new Error('That alert was not found.');
  await fs.unlink(speechFilePath(key)).catch((err: NodeJS.ErrnoException) => {
    if (err.code !== 'ENOENT') throw err;
  });
  const rows = await listGateSpeech();
  const row = rows.find((item) => item.key === key);
  if (!row) throw new Error('That alert was not found.');
  return row;
}

export async function readGateSpeechAudio(key: GateSpeechKey): Promise<Buffer | null> {
  await ensureGateSpeechSchema();
  const rows = await query<{ audio: unknown }>(
    `SELECT audio FROM tbl_gate_speech WHERE scenario = $1`,
    [key],
  );
  const stored = asAudio(rows[0]?.audio);
  if (stored) return stored;
  try {
    return await fs.readFile(speechFilePath(key));
  } catch {
    return null;
  }
}

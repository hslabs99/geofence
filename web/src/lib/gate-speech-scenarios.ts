export const GATE_SPEECH_GENDERS = [
  { id: 'male', label: 'Male' },
  { id: 'female', label: 'Female' },
] as const;

export type GateSpeechGender = (typeof GATE_SPEECH_GENDERS)[number]['id'];

export const GATE_SPEECH_ACCENTS = [
  'New Zealand',
  'Australia',
  'United Kingdom',
  'Ireland',
  'United States',
  'Canada',
  'South Africa',
] as const;

export type GateSpeechAccent = (typeof GATE_SPEECH_ACCENTS)[number];

/**
 * Built-in OpenAI speakers. Gender is how each voice is commonly heard.
 * Asking for a gender in the tone prompt does not change this speaker.
 */
export const GATE_SPEECH_VOICES = [
  { id: 'echo', label: 'Echo', gender: 'male', heardAs: 'Clear' },
  { id: 'onyx', label: 'Onyx', gender: 'male', heardAs: 'Deep' },
  { id: 'ash', label: 'Ash', gender: 'male', heardAs: 'Conversational' },
  { id: 'ballad', label: 'Ballad', gender: 'male', heardAs: 'Light British' },
  { id: 'verse', label: 'Verse', gender: 'male', heardAs: 'Expressive' },
  { id: 'cedar', label: 'Cedar', gender: 'male', heardAs: 'Warm' },
  { id: 'coral', label: 'Coral', gender: 'female', heardAs: 'Warm' },
  { id: 'nova', label: 'Nova', gender: 'female', heardAs: 'Bright' },
  { id: 'sage', label: 'Sage', gender: 'female', heardAs: 'Even' },
  { id: 'shimmer', label: 'Shimmer', gender: 'female', heardAs: 'Light' },
  { id: 'marin', label: 'Marin', gender: 'female', heardAs: 'Polished' },
] as const;

export type GateSpeechVoice = (typeof GATE_SPEECH_VOICES)[number]['id'];

export const GATE_SPEECH_VOICE: GateSpeechVoice = 'echo';
export const GATE_SPEECH_GENDER: GateSpeechGender = 'male';
export const GATE_SPEECH_ACCENT: GateSpeechAccent = 'New Zealand';

export function isGateSpeechGender(value: string): value is GateSpeechGender {
  return GATE_SPEECH_GENDERS.some((gender) => gender.id === value);
}

export function isGateSpeechAccent(value: string): value is GateSpeechAccent {
  return (GATE_SPEECH_ACCENTS as readonly string[]).includes(value);
}

export function voicesForGender(gender: GateSpeechGender) {
  return GATE_SPEECH_VOICES.filter((voice) => voice.gender === gender);
}

export function defaultVoiceForGender(gender: GateSpeechGender): GateSpeechVoice {
  return gender === 'female' ? 'coral' : 'echo';
}

const ACCENT_PHRASE: Record<GateSpeechAccent, string> = {
  'New Zealand': 'a New Zealand English accent',
  Australia: 'an Australian English accent',
  'United Kingdom': 'a British English accent',
  Ireland: 'an Irish English accent',
  'United States': 'an American English accent',
  Canada: 'a Canadian English accent',
  'South Africa': 'a South African English accent',
};

/** The only instruction text sent with a clip, after the saved tone prompt. */
export function gateSpeechInstructions(prompt: string, accent: string): string {
  const phrase = isGateSpeechAccent(accent) ? ACCENT_PHRASE[accent] : `a ${accent} English accent`;
  return `${prompt.trim()} Speak with ${phrase}.`;
}

export function isGateSpeechVoice(value: string): value is GateSpeechVoice {
  return GATE_SPEECH_VOICES.some((voice) => voice.id === value);
}

/** Tonality sent to the speech model with every clip. */
export const GATE_SPEECH_PROMPT =
  'Speak like an airport arrivals announcement. Bright, clear, and welcoming, with a light lift in the voice. Keep it short. Say only the given words.';

export const GATE_SPEECH_PROMPT_MAX = 1000;

/** Fixed fence alerts that can speak on the driver phone. */

export const GATE_SPEECH_SCENARIOS = [
  {
    key: 'amber-2',
    alert: 'amber',
    step: 2,
    label: 'Amber step 2',
    place: 'Arriving at the vineyard',
    speech: 'Arriving at the vineyard',
  },
  {
    key: 'red-2',
    alert: 'red',
    step: 2,
    label: 'Red step 2',
    place: 'Within the vineyard',
    speech: 'Within the vineyard',
  },
  {
    key: 'red-3',
    alert: 'red',
    step: 3,
    label: 'Red step 3',
    place: 'You have left the vineyard',
    speech: 'You have left the vineyard',
    note: 'Played when the truck has left the vineyard. The reminder to head for the winery stays silent.',
  },
  {
    key: 'amber-4',
    alert: 'amber',
    step: 4,
    label: 'Amber step 4',
    place: 'Arriving at the winery',
    speech: 'Arriving at the winery',
  },
  {
    key: 'red-4',
    alert: 'red',
    step: 4,
    label: 'Red step 4',
    place: 'You are within the winery',
    speech: 'You are within the winery',
  },
] as const;

export type GateSpeechScenario = (typeof GATE_SPEECH_SCENARIOS)[number];
export type GateSpeechKey = GateSpeechScenario['key'];

export function isGateSpeechKey(value: string): value is GateSpeechKey {
  return GATE_SPEECH_SCENARIOS.some((row) => row.key === value);
}

/** What the phone plays for one gate. Text is the spoken file. */
export const GATE_SPEECH_SOUNDS = [
  { id: 'chime', label: 'Chime' },
  { id: 'speech', label: 'Text' },
  { id: 'both', label: 'Chime and text' },
  { id: 'off', label: 'Nothing' },
] as const;

export type GateSpeechSound = (typeof GATE_SPEECH_SOUNDS)[number]['id'];

export const GATE_SPEECH_SOUND_DEFAULT: GateSpeechSound = 'both';

export function isGateSpeechSound(value: string): value is GateSpeechSound {
  return GATE_SPEECH_SOUNDS.some((item) => item.id === value);
}

/** Chime and speech flags for one play, from the gate's sound setting. Nothing plays neither. */
export function gateSpeechParts(sound: string, chimeId: string, hasAudio: boolean): { chimeId: string; speech: boolean } {
  const mode = isGateSpeechSound(sound) ? sound : GATE_SPEECH_SOUND_DEFAULT;
  if (mode === 'off') return { chimeId: 'none', speech: false };
  return {
    chimeId: mode === 'speech' ? 'none' : chimeId,
    speech: mode !== 'chime' && hasAudio,
  };
}

export const GATE_SPEECH_PLAY_MIN = 1;
export const GATE_SPEECH_PLAY_MAX = 10;
export const GATE_SPEECH_GAP_MIN = 5;
export const GATE_SPEECH_GAP_MAX = 300;
export const GATE_SPEECH_GAP_DEFAULT = 30;

/** Arrivals play once. A red alert plays a few times, then stops. */
export function defaultPlayCount(alert: 'amber' | 'red'): number {
  return alert === 'red' ? 3 : 1;
}

export function parsePlayCount(value: number): number {
  if (!Number.isInteger(value) || value < GATE_SPEECH_PLAY_MIN || value > GATE_SPEECH_PLAY_MAX) {
    throw new Error(`Play the alert between ${GATE_SPEECH_PLAY_MIN} and ${GATE_SPEECH_PLAY_MAX} times.`);
  }
  return value;
}

export function parseGapSeconds(value: number): number {
  if (!Number.isInteger(value) || value < GATE_SPEECH_GAP_MIN || value > GATE_SPEECH_GAP_MAX) {
    throw new Error(`Keep the pause between plays from ${GATE_SPEECH_GAP_MIN} to ${GATE_SPEECH_GAP_MAX} seconds.`);
  }
  return value;
}

/** Amber is the soft (yellow) marker. Red step 3 speaks only for a real vineyard exit. */
export function gateSpeechKey(
  step: 2 | 3 | 4 | null | undefined,
  alert: 'yellow' | 'red' | null | undefined,
  remind = false,
): GateSpeechKey | null {
  if (step == null || alert == null || remind) return null;
  const color = alert === 'yellow' ? 'amber' : 'red';
  const found = GATE_SPEECH_SCENARIOS.find((row) => row.step === step && row.alert === color);
  return found?.key ?? null;
}

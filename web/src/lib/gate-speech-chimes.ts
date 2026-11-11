/** Pre-made announcement chimes. Played before the spoken words, or not at all. */

export const GATE_SPEECH_CHIMES = [
  { id: 'ding-dong', label: 'Ding dong' },
  { id: 'three-notes', label: 'Three notes' },
  { id: 'double-ping', label: 'Double ping' },
  { id: 'departure', label: 'Departure' },
  { id: 'arrival', label: 'Arrival' },
  { id: 'soft-bell', label: 'Soft bell' },
] as const;

export type GateSpeechChime = (typeof GATE_SPEECH_CHIMES)[number]['id'];

export const GATE_SPEECH_CHIME_NONE = 'none';

export function isGateSpeechChime(value: string): value is GateSpeechChime | typeof GATE_SPEECH_CHIME_NONE {
  return value === GATE_SPEECH_CHIME_NONE || GATE_SPEECH_CHIMES.some((chime) => chime.id === value);
}

export function chimeFileUrl(id: string): string | null {
  if (!isGateSpeechChime(id) || id === GATE_SPEECH_CHIME_NONE) return null;
  return `/gate-speech/chimes/${id}.wav`;
}

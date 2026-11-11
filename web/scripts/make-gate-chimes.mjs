import fs from 'node:fs';
import path from 'node:path';

const rate = 44100;
const outDir = path.join(process.cwd(), 'public', 'gate-speech', 'chimes');

/** Short tones with a bell-like decay. Times are seconds. */
const chimes = {
  'ding-dong': [
    { t: 0, dur: 0.28, freq: 880, decay: 2.4 },
    { t: 0.3, dur: 0.55, freq: 659, decay: 1.8 },
  ],
  'three-notes': [
    { t: 0, dur: 0.16, freq: 523, decay: 3 },
    { t: 0.18, dur: 0.16, freq: 659, decay: 3 },
    { t: 0.36, dur: 0.38, freq: 784, decay: 2 },
  ],
  'double-ping': [
    { t: 0, dur: 0.14, freq: 1175, decay: 4 },
    { t: 0.18, dur: 0.28, freq: 1568, decay: 3 },
  ],
  departure: [
    { t: 0, dur: 0.22, freq: 698, decay: 2.6 },
    { t: 0.24, dur: 0.48, freq: 880, decay: 1.8 },
  ],
  arrival: [
    { t: 0, dur: 0.22, freq: 880, decay: 2.6 },
    { t: 0.24, dur: 0.5, freq: 659, decay: 1.7 },
  ],
  'soft-bell': [{ t: 0, dur: 0.9, freq: 784, decay: 1.35 }],
};

function render(events) {
  const end = Math.max(...events.map((event) => event.t + event.dur)) + 0.12;
  const n = Math.floor(end * rate);
  const samples = new Float64Array(n);
  for (const event of events) {
    const start = Math.floor(event.t * rate);
    const len = Math.floor(event.dur * rate);
    for (let i = 0; i < len; i++) {
      const idx = start + i;
      if (idx >= n) break;
      const p = i / len;
      const attack = Math.min(1, i / (rate * 0.012));
      const env = attack * Math.exp(-event.decay * p);
      const wave =
        Math.sin((2 * Math.PI * event.freq * i) / rate) +
        0.18 * Math.sin((2 * Math.PI * event.freq * 2.01 * i) / rate) * Math.exp(-4 * p);
      samples[idx] += env * wave;
    }
  }
  let peak = 0;
  for (const sample of samples) peak = Math.max(peak, Math.abs(sample));
  const scale = peak > 0 ? 0.8 / peak : 1;
  const dataSize = n * 2;
  const buf = Buffer.alloc(44 + dataSize);
  buf.write('RIFF', 0);
  buf.writeUInt32LE(36 + dataSize, 4);
  buf.write('WAVE', 8);
  buf.write('fmt ', 12);
  buf.writeUInt32LE(16, 16);
  buf.writeUInt16LE(1, 20);
  buf.writeUInt16LE(1, 22);
  buf.writeUInt32LE(rate, 24);
  buf.writeUInt32LE(rate * 2, 28);
  buf.writeUInt16LE(2, 32);
  buf.writeUInt16LE(16, 34);
  buf.write('data', 36);
  buf.writeUInt32LE(dataSize, 40);
  for (let i = 0; i < n; i++) {
    const value = Math.max(-1, Math.min(1, samples[i] * scale));
    buf.writeInt16LE(Math.round(value * 32767), 44 + i * 2);
  }
  return buf;
}

fs.mkdirSync(outDir, { recursive: true });
for (const [name, events] of Object.entries(chimes)) {
  const file = path.join(outDir, `${name}.wav`);
  fs.writeFileSync(file, render(events));
  console.log(file);
}

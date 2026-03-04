/**
 * Firebase App Hosting expects .next/standalone/.next/routes-manifest.json.
 * Next.js 16 puts output in .next/standalone/web/ when package name is "web".
 * This script flattens standalone/web/* into standalone/.
 */
import fs from 'fs';
import path from 'path';

const rootDir = process.cwd();
const standaloneDir = path.join(rootDir, '.next', 'standalone');
const nestedDir = path.join(standaloneDir, 'web');

if (!fs.existsSync(nestedDir)) {
  console.log('fix-standalone: no standalone/web folder, skipping');
  process.exit(0);
}

const entries = fs.readdirSync(nestedDir, { withFileTypes: true });
for (const ent of entries) {
  const src = path.join(nestedDir, ent.name);
  const dest = path.join(standaloneDir, ent.name);
  if (fs.existsSync(dest)) {
    if (ent.isDirectory()) {
      fs.rmSync(dest, { recursive: true });
    } else {
      fs.unlinkSync(dest);
    }
  }
  fs.renameSync(src, dest);
}
fs.rmdirSync(nestedDir);
console.log('fix-standalone: flattened .next/standalone for App Hosting');

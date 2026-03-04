/**
 * Firebase App Hosting expects .next/standalone/.next/routes-manifest.json.
 * Next.js 16 puts output in .next/standalone/web/ when package name is "web".
 * This script: (1) flattens standalone/web/* into standalone/, (2) ensures Prisma is in standalone node_modules.
 */
import fs from 'fs';
import path from 'path';

const rootDir = process.cwd();
const standaloneDir = path.join(rootDir, '.next', 'standalone');
const nestedDir = path.join(standaloneDir, 'web');

function copyDirRecursive(src, dest) {
  if (!fs.existsSync(src)) return;
  fs.mkdirSync(dest, { recursive: true });
  for (const ent of fs.readdirSync(src, { withFileTypes: true })) {
    const s = path.join(src, ent.name);
    const d = path.join(dest, ent.name);
    if (ent.isDirectory()) {
      copyDirRecursive(s, d);
    } else {
      fs.copyFileSync(s, d);
    }
  }
}

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

// Ensure Prisma client is in standalone node_modules (runtime "Cannot find module @prisma/client" fix)
const srcPrisma = path.join(rootDir, 'node_modules', '.prisma');
const srcAtPrisma = path.join(rootDir, 'node_modules', '@prisma');
const destNm = path.join(standaloneDir, 'node_modules');
if (fs.existsSync(srcPrisma)) {
  const destPrisma = path.join(destNm, '.prisma');
  if (fs.existsSync(destPrisma)) fs.rmSync(destPrisma, { recursive: true });
  copyDirRecursive(srcPrisma, destPrisma);
  console.log('fix-standalone: copied .prisma into standalone');
}
if (fs.existsSync(srcAtPrisma)) {
  const destAtPrisma = path.join(destNm, '@prisma');
  if (fs.existsSync(destAtPrisma)) fs.rmSync(destAtPrisma, { recursive: true });
  copyDirRecursive(srcAtPrisma, destAtPrisma);
  console.log('fix-standalone: copied @prisma into standalone');
}

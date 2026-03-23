#!/usr/bin/env node
/**
 * VERBATIM TIMES — Check that critical paths do not contain timezone-conversion patterns.
 * Run: node scripts/check-verbatim-times.mjs (or npm run check:verbatim)
 * Exits 1 if any forbidden pattern is found in API→position_time code. See web/docs/VERBATIM_TIMES.md
 */

import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, '..');
const src = join(root, 'src');

// In apifeed/import and apifeed/merge we must NEVER use these — position_time must be raw API string only.
const FORBIDDEN_IN_APIFEED = [
  { pattern: /formatDateForDebug/g, name: 'formatDateForDebug — never use for position_time; use positionTimeForStorage(apiString) only' },
  { pattern: /\.toISOString\s*\(\s*\)/g, name: 'toISOString() — never use for position_time' },
  { pattern: /getUTCFullYear|getUTCMonth|getUTCDate|getUTCHours|getUTCMinutes|getUTCSeconds/g, name: 'getUTC*() — never build position_time from Date; store raw string' },
];

// In tracksolid: gpsTime must be passed through as string only (no conversion).
const FORBIDDEN_IN_TRACKSOLID = [
  { pattern: /gpsTime.*toISOString|toISOString.*gpsTime/g, name: 'gpsTime must not be converted via toISOString' },
  { pattern: /gpsTime.*getUTC|getUTC.*gpsTime/g, name: 'gpsTime must not be formatted via getUTC*' },
];

const APIFEED_FILES = ['src/app/api/apifeed/import/route.ts', 'src/app/api/apifeed/merge-tracking/route.ts'];
const TRACKSOLID_FILE = 'src/lib/tracksolid.ts';

function checkFile(filePath, forbiddenList) {
  const fullPath = join(root, filePath);
  let content;
  try {
    content = readFileSync(fullPath, 'utf8');
  } catch (e) {
    console.error(`Cannot read ${filePath}:`, e.message);
    return { filePath, errors: [{ line: 0, msg: 'File not found or unreadable' }] };
  }

  const lines = content.split('\n');
  const errors = [];

  for (const { pattern, name } of forbiddenList) {
    const re = new RegExp(pattern.source, pattern.flags);
    let m;
    while ((m = re.exec(content)) !== null) {
      const lineNum = content.slice(0, m.index).split('\n').length;
      const line = lines[lineNum - 1] || '';
      if (line.trim().startsWith('//') || line.trim().startsWith('*')) continue;
      errors.push({ line: lineNum, msg: name, snippet: line.trim().slice(0, 90) });
    }
  }

  return { filePath, errors };
}

let failed = false;
for (const filePath of APIFEED_FILES) {
  const { filePath: p, errors } = checkFile(filePath, FORBIDDEN_IN_APIFEED);
  if (errors.length > 0) {
    failed = true;
    console.error(`\n${p}: VERBATIM TIMES — forbidden pattern(s):`);
    for (const e of errors) {
      console.error(`  Line ${e.line}: ${e.msg}`);
      if (e.snippet) console.error(`    ${e.snippet}`);
    }
  }
}
const tsResult = checkFile(TRACKSOLID_FILE, FORBIDDEN_IN_TRACKSOLID);
if (tsResult.errors.length > 0) {
  failed = true;
  console.error(`\n${tsResult.filePath}: VERBATIM TIMES — gpsTime must be passed through as-is:`);
  for (const e of tsResult.errors) {
    console.error(`  Line ${e.line}: ${e.msg}`);
    if (e.snippet) console.error(`    ${e.snippet}`);
  }
}

if (failed) {
  console.error('\nSee web/docs/VERBATIM_TIMES.md — we NEVER adjust timezones. position_time must be the raw API string.\n');
  process.exit(1);
}
console.log('check:verbatim — no forbidden timezone patterns in critical paths.');

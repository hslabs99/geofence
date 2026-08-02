import fs from 'node:fs';

function hostOf(file, key) {
  if (!fs.existsSync(file)) return { file, key, exists: false };
  for (const raw of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#') || !line.startsWith(`${key}=`)) continue;
    let v = line.slice(key.length + 1).trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    try {
      const u = new URL(v);
      return {
        file,
        key,
        parse: 'url',
        host: u.hostname,
        user: u.username,
        path: u.pathname,
        proto: u.protocol,
      };
    } catch (e) {
      const m = v.match(/@([^/?#:]+)/);
      return {
        file,
        key,
        parse: 'fail',
        err: e.message,
        atHost: m?.[1] ?? null,
        len: v.length,
        starts: v.slice(0, 32),
      };
    }
  }
  return { file, key, exists: true, found: false };
}

console.log(
  JSON.stringify(
    [
      hostOf('.env.local', 'DATABASE_URL'),
      hostOf('.env.local', 'CELGPS_DATABASE_URL'),
      hostOf('.env.local', 'DATABASE_URL'),
    ],
    null,
    2
  )
);

// Copies the non-TypeScript runtime assets next to the compiled modules and makes the CLI
// executable. Paths mirror src/ so `new URL('./x', import.meta.url)` resolves the same way
// under tsx (src/) and when installed (dist/).
import { chmodSync, copyFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

const assets = ['storage/schema.sql', 'contracts/policy-defaults.json'];
for (const rel of assets) {
  const to = join('dist', rel);
  mkdirSync(dirname(to), { recursive: true });
  copyFileSync(join('src', rel), to);
}

const cli = join('dist', 'cli.js');
const body = readFileSync(cli, 'utf8');
if (!body.startsWith('#!')) writeFileSync(cli, `#!/usr/bin/env node\n${body}`);
chmodSync(cli, 0o755);
console.log(`copied ${assets.length} assets; cli marked executable`);

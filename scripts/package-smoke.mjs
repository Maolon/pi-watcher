// Package smoke test: pack the tarball, check its contents for leaks, install it into a clean
// temp project (with the host-provided peer `typebox`, as Pi supplies it), then load the
// compiled extension with plain Node (no tsx) and check that it registers its tool/command.
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const root = resolve('.');
const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
const work = mkdtempSync(join(tmpdir(), 'pw-pack-'));
const fail = (msg) => { console.error(`package smoke FAILED: ${msg}`); process.exitCode = 1; };

try {
  const packed = JSON.parse(execFileSync('npm', ['pack', '--json', '--pack-destination', work], { cwd: root, encoding: 'utf8' }));
  const files = packed[0].files.map((f) => f.path);
  const tgz = join(work, packed[0].filename);

  // 1) contents: compiled output + docs only
  const bad = files.filter((f) => /^(src|tests|scripts|qualification)\//.test(f) || /\.map$/.test(f) || /(^|\/)\.(env|pi-watcher)/.test(f));
  if (bad.length) fail(`unexpected files in tarball: ${bad.join(', ')}`);
  for (const need of ['dist/pi-extension.js', 'dist/cli.js', 'dist/storage/schema.sql', 'dist/contracts/policy-defaults.json', 'README.md', 'LICENSE']) {
    if (!files.includes(need)) fail(`missing ${need}`);
  }

  // 2) no local paths, personal data or private-repo references in shipped text
  const extract = join(work, 'extract');
  execFileSync('mkdir', ['-p', extract]);
  execFileSync('tar', ['-xzf', tgz, '-C', extract]);
  // Generic markers only; maintainers can add private ones locally via PI_WATCHER_LEAK_MARKERS
  // (comma-separated literals) without committing them.
  const extra = (process.env.PI_WATCHER_LEAK_MARKERS ?? '').split(',').map((m) => m.trim()).filter(Boolean)
    .map((m) => m.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const leak = new RegExp(['\\/Users\\/', '\\/home\\/[a-z]', 'apikey_[0-9a-f]{20,}', 'sk-[A-Za-z0-9]{32,}', ...extra].join('|'));
  const walk = (d) => readdirSync(d, { withFileTypes: true }).flatMap((e) => (e.isDirectory() ? walk(join(d, e.name)) : [join(d, e.name)]));
  for (const f of walk(join(extract, 'package'))) {
    const m = readFileSync(f, 'utf8').match(leak);
    if (m) fail(`leak marker "${m[0]}" in ${f.slice(extract.length + 1)}`);
  }

  // 3) install into a clean project and load the extension with plain Node
  const app = join(work, 'app');
  execFileSync('mkdir', ['-p', app]);
  writeFileSync(join(app, 'package.json'), JSON.stringify({ name: 'smoke', private: true, type: 'module' }));
  execFileSync('npm', ['install', '--no-audit', '--no-fund', tgz, 'typebox'], { cwd: app, stdio: 'inherit' });
  const entry = join(app, 'node_modules', ...pkg.name.split('/'), pkg.pi.extensions[0]);
  const mod = await import(pathToFileURL(entry).href);
  const tools = [];
  const commands = [];
  mod.default({
    registerTool: (t) => tools.push(t.name),
    registerCommand: (name) => commands.push(name),
    on: () => {},
    events: { on: () => () => {}, emit: () => {} }
  }, { relay: false });
  if (!tools.includes('watcher')) fail(`watcher tool not registered (got ${tools})`);
  if (!commands.includes('watcher')) fail(`/watcher command not registered (got ${commands})`);

  // 4) native modules work from the installed tree: open a store (better-sqlite3) under the root lock (fs-ext flock)
  const pkgDir = join(app, 'node_modules', ...pkg.name.split('/'));
  const { WatchStore } = await import(pathToFileURL(join(pkgDir, 'dist', 'storage', 'store.js')).href);
  const store = await WatchStore.open(join(work, 'store-root'), { mode: 'embedded' });
  store.close();

  // 5) the CLI runs from the installed bin
  const out = execFileSync(join(app, 'node_modules', '.bin', 'pi-watcher'), ['doctor', '--root', join(work, 'doctor-root')], { encoding: 'utf8' });
  if (!out.trim()) fail('pi-watcher doctor printed nothing');

  if (!process.exitCode) console.log(`package smoke OK: ${packed[0].filename} (${files.length} files, ${packed[0].size} bytes)`);
} finally {
  rmSync(work, { recursive: true, force: true });
}

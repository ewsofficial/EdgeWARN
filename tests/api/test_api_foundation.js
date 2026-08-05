import { afterEach, describe, expect, it } from '@jest/globals';
import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { createConfig } from '../../src/api/config/index.js';
import { ArtifactRepository, ArtifactError } from '../../src/api/repositories/artifactRepository.js';

describe('unified API configuration', () => {
  it('uses one resolved base directory and accepts deprecated aliases only when equal', () => {
    const config = createConfig({ env: { EDGEWARN_BASE_DIR: '/tmp/edgewarn', BASE_DIR: '/tmp/edgewarn', PORT: '5001' }, argv: [] });
    expect(config.baseDir).toBe('/tmp/edgewarn');
    expect(config.port).toBe(5001);
    expect(() => createConfig({ env: { EDGEWARN_BASE_DIR: '/tmp/a', BASE_DIR: '/tmp/b' }, argv: [] })).toThrow('Conflicting base directory settings');
  });

  it('uses exact origins and rejects unsafe production proxy shorthand', () => {
    const config = createConfig({ env: { ALLOWED_ORIGINS: 'https://one.example,https://two.example' }, argv: [] });
    expect(config.allowedOrigins).toEqual(['https://one.example', 'https://two.example']);
    expect(() => createConfig({ env: { NODE_ENV: 'production', TRUST_PROXY: 'true' }, argv: [] })).toThrow('TRUST_PROXY=true');
  });
});

describe('ArtifactRepository', () => {
  let root;
  afterEach(async () => { if (root) await fs.rm(root, { recursive: true, force: true }); });

  it('reads a bounded regular JSON artifact and rejects symlink escapes', async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'artifact-repository-'));
    const outside = await fs.mkdtemp(path.join(os.tmpdir(), 'artifact-outside-'));
    await fs.writeFile(path.join(root, 'valid.json'), '{"ok":true}');
    await fs.writeFile(path.join(outside, 'secret.json'), '{"secret":true}');
    await fs.symlink(path.join(outside, 'secret.json'), path.join(root, 'escape.json'));
    const repository = new ArtifactRepository({ runtime: root });
    await expect(repository.readJson('runtime', ['valid.json'])).resolves.toEqual({ ok: true });
    await expect(repository.readJson('runtime', ['escape.json'])).rejects.toMatchObject({ code: 'INVALID_PATH' });
    await fs.rm(outside, { recursive: true, force: true });
  });

  it('turns malformed and oversized publishing artifacts into bounded errors', async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'artifact-repository-'));
    await fs.writeFile(path.join(root, 'broken.json'), '{');
    await fs.writeFile(path.join(root, 'large.json'), 'x'.repeat(64));
    const repository = new ArtifactRepository({ runtime: root }, { json: 32 });
    await expect(repository.readJson('runtime', ['broken.json'])).rejects.toBeInstanceOf(ArtifactError);
    await expect(repository.readJson('runtime', ['large.json'])).rejects.toMatchObject({ code: 'INVALID_ARTIFACT' });
  });

  it('caches only a matching artifact identity and observes replacement', async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'artifact-repository-'));
    const target = path.join(root, 'index.json');
    await fs.writeFile(target, '{"generation":1}');
    const repository = new ArtifactRepository({ runtime: root });
    await expect(repository.readJson('runtime', ['index.json'])).resolves.toEqual({ generation: 1 });
    await new Promise((resolve) => setTimeout(resolve, 2));
    await fs.writeFile(target, '{"generation":2}');
    await expect(repository.readJson('runtime', ['index.json'])).resolves.toEqual({ generation: 2 });
  });
});

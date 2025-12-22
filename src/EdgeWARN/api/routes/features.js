import express from 'express';
import fs from 'fs';
import path from 'path';
import apiConfig from '../config.js';

const router = express.Router();

function isSafeFilename(name) {
  if (!name) return false;
  if (name.includes('..') || name.includes('/') || name.includes('\\')) return false;
  return name.toLowerCase().endsWith('.json') && path.basename(name) === name;
}

async function readJsonFile(dir, name) {
  if (!isSafeFilename(name)) {
    const e = new Error('Invalid filename');
    e.code = 'EINVAL';
    throw e;
  }
  const full = path.join(dir, name);
  // Ensure resolved path is inside dir (prevent traversal)
  const resolvedDir = path.resolve(dir);
  const resolvedFull = path.resolve(full);
  if (!resolvedFull.startsWith(resolvedDir + path.sep) && resolvedFull !== resolvedDir) {
    const e = new Error('Path outside allowed directory');
    e.code = 'EACCES';
    throw e;
  }
  if (!fs.existsSync(full)) {
    const e = new Error('Not found');
    e.code = 'ENOENT';
    throw e;
  }
  const txt = await fs.promises.readFile(full, 'utf8');
  return JSON.parse(txt);
}

// Return a single stormcell JSON by filename
router.get('/list/:name', async (req, res) => {
  const name = req.params.name;
  try {
    const content = await readJsonFile(apiConfig.STORMCELL_DIR, name);
    res.json({ name, content });
  } catch (err) {
    if (err.code === 'ENOENT') return res.status(404).json({ error: 'File not found' });
    if (err.code === 'EINVAL' || err.code === 'EACCES') return res.status(400).json({ error: 'Invalid filename' });
    console.error('Error reading stormcell file', name, err);
    res.status(500).json({ error: 'Failed to read stormcell file' });
  }
});

// Return a single cell JSON by filename
router.get('/cells/:name', async (req, res) => {
  const name = req.params.name;
  try {
    const content = await readJsonFile(apiConfig.CELL_DIR, name);
    res.json({ name, content });
  } catch (err) {
    if (err.code === 'ENOENT') return res.status(404).json({ error: 'File not found' });
    if (err.code === 'EINVAL' || err.code === 'EACCES') return res.status(400).json({ error: 'Invalid filename' });
    console.error('Error reading cell file', name, err);
    res.status(500).json({ error: 'Failed to read cell file' });
  }
});

export default router;

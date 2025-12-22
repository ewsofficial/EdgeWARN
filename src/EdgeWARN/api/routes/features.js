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

async function listJsonFiles(dir) {
  try {
    if (!fs.existsSync(dir)) return [];
    const files = await fs.promises.readdir(dir);
    const jsonFiles = files.filter((f) => f.toLowerCase().endsWith('.json')).sort();
    const items = await Promise.all(
      jsonFiles.map(async (f) => {
        const full = path.join(dir, f);
        try {
          const st = await fs.promises.stat(full);
          return {
            name: f,
            size: st.size,
            lastModified: st.mtime.toISOString()
          };
        } catch (err) {
          return { name: f, error: String(err) };
        }
      })
    );
    return items;
  } catch (err) {
    console.error('Error listing directory', dir, err);
    throw err;
  }
}

// List available stormcell filenames. Serves HTML when client accepts HTML, JSON otherwise.
router.get('/list', async (req, res) => {
  try {
    const files = await listJsonFiles(apiConfig.STORMCELL_DIR);
    if (req.accepts('html')) {
      const rows = files
        .map((f) => {
          if (f.error) return `<tr><td>${f.name}</td><td>-</td><td>-</td></tr>`;
          return `<tr><td><a href="/features/list/${encodeURIComponent(f.name)}">${f.name}</a></td><td>${f.size}</td><td>${f.lastModified}</td></tr>`;
        })
        .join('\n');
      const html = `<!doctype html><html><head><meta charset="utf-8"><title>Stormcells</title></head><body><h1>Stormcell files</h1><table border="1" cellpadding="4"><thead><tr><th>name</th><th>size</th><th>lastModified</th></tr></thead><tbody>${rows}</tbody></table></body></html>`;
      res.type('html').send(html);
    } else {
      res.json(files.map((f) => f.name));
    }
  } catch (err) {
    console.error('Error listing stormcell files', err);
    res.status(500).json({ error: 'Failed to list stormcell files' });
  }
});

// List available cell filenames. Serves HTML when client accepts HTML, JSON otherwise.
router.get('/cells', async (req, res) => {
  try {
    const files = await listJsonFiles(apiConfig.CELL_DIR);
    if (req.accepts('html')) {
      const rows = files
        .map((f) => {
          if (f.error) return `<tr><td>${f.name}</td><td>-</td><td>-</td></tr>`;
          return `<tr><td><a href="/features/cells/${encodeURIComponent(f.name)}">${f.name}</a></td><td>${f.size}</td><td>${f.lastModified}</td></tr>`;
        })
        .join('\n');
      const html = `<!doctype html><html><head><meta charset="utf-8"><title>Cells</title></head><body><h1>Cell files</h1><table border="1" cellpadding="4"><thead><tr><th>name</th><th>size</th><th>lastModified</th></tr></thead><tbody>${rows}</tbody></table></body></html>`;
      res.type('html').send(html);
    } else {
      res.json(files.map((f) => f.name));
    }
  } catch (err) {
    console.error('Error listing cell files', err);
    res.status(500).json({ error: 'Failed to list cell files' });
  }
});

// Features root: show both lists (HTML or JSON)
router.get('/', async (req, res) => {
  try {
    const stormFiles = await listJsonFiles(apiConfig.STORMCELL_DIR);
    const cellFiles = await listJsonFiles(apiConfig.CELL_DIR);
    if (req.accepts('html')) {
      const stormRows = stormFiles
        .map((f) => (f.error ? `<tr><td>${f.name}</td><td>-</td><td>-</td></tr>` : `<tr><td><a href="/features/list/${encodeURIComponent(f.name)}">${f.name}</a></td><td>${f.size}</td><td>${f.lastModified}</td></tr>`))
        .join('\n');
      const cellRows = cellFiles
        .map((f) => (f.error ? `<tr><td>${f.name}</td><td>-</td><td>-</td></tr>` : `<tr><td><a href="/features/cells/${encodeURIComponent(f.name)}">${f.name}</a></td><td>${f.size}</td><td>${f.lastModified}</td></tr>`))
        .join('\n');
      const html = `<!doctype html><html><head><meta charset="utf-8"><title>Features</title></head><body><h1>Features</h1><h2>Stormcells</h2><table border="1" cellpadding="4"><thead><tr><th>name</th><th>size</th><th>lastModified</th></tr></thead><tbody>${stormRows}</tbody></table><h2>Cells</h2><table border="1" cellpadding="4"><thead><tr><th>name</th><th>size</th><th>lastModified</th></tr></thead><tbody>${cellRows}</tbody></table></body></html>`;
      res.type('html').send(html);
    } else {
      res.json({ stormcells: stormFiles.map((f) => f.name), cells: cellFiles.map((f) => f.name) });
    }
  } catch (err) {
    console.error('Error building features index', err);
    res.status(500).json({ error: 'Failed to build features index' });
  }
});

export default router;

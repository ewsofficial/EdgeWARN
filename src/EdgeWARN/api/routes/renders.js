import fs from 'fs/promises';
import path from 'path';
import config from '../config.js';
import express from 'express';

const router = express.Router();

// Serve static files from the GUI directory
// This allows accessing images at /renders/Product/file.png
router.use('/', express.static(config.GUI_DIR));

// GET /renders/
// Returns a list of products and their rendered files
// Helper to get files recursively
async function getFilesRecursively(dir) {
  let results = [];
  try {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        const nested = await getFilesRecursively(fullPath);
        results = results.concat(nested);
      } else {
        results.push(fullPath);
      }
    }
  } catch (err) {
    // Ignore access errors or empty dirs during recursion if needed
    // console.error(`Error traversing ${dir}:`, err);
  }
  return results;
}

// GET /renders/
// Returns an HTML list of products (subdirectories) or JSON if requested
router.get('/', async (req, res) => {
  try {
    const guiDir = config.GUI_DIR;

    // Check if GUI directory exists
    try {
      await fs.access(guiDir);
    } catch {
      // Prioritize HTML by listing it first in accepts
      if (req.query.format === 'json' || req.accepts(['html', 'json']) === 'json') {
        return res.json({ products: [] });
      }
      return res.send('<h1>No Renders Found</h1><p>GUI directory does not exist.</p>');
    }

    // Read top-level directories (Products)
    const entries = await fs.readdir(guiDir, { withFileTypes: true });
    
    // Sort directories alphabetically
    const productNames = entries
      .filter(entry => entry.isDirectory())
      .map(entry => entry.name)
      .sort();

    // Check for JSON request
    // req.accepts(['html', 'json']) returns the best match. 
    // If Accept is */*, it returns the first one ('html').
    // If Accept is application/json, it returns 'json'.
    if (req.query.format === 'json' || req.accepts(['html', 'json']) === 'json') {
      return res.json({ products: productNames });
    }

    const html = `
      <!DOCTYPE html>
      <html>
        <head>
          <title>EdgeWARN Renders</title>
          <style>
            body { font-family: sans-serif; padding: 20px; }
            ul { list-style-type: none; padding: 0; }
            li { margin: 10px 0; }
            a { text-decoration: none; color: #007bff; font-size: 1.2em; }
            a:hover { text-decoration: underline; }
          </style>
        </head>
        <body>
          <h1>Available Products</h1>
          <ul>
            ${productNames.map(name => `<li><a href="/renders/${name}">${name}</a></li>`).join('')}
          </ul>
        </body>
      </html>
    `;

    res.send(html);
  } catch (err) {
    console.error('Error listing products:', err);
    if (req.query.format === 'json' || req.accepts(['html', 'json']) === 'json') {
      res.status(500).json({ error: 'Internal Server Error' });
    } else {
      res.status(500).send('<h1>Internal Server Error</h1>');
    }
  }
});

// GET /renders/:product
// Returns an HTML list of files for a specific product or JSON if requested
router.get('/:product', async (req, res) => {
  try {
    const productName = req.params.product;
    const guiDir = config.GUI_DIR;
    const productPath = path.join(guiDir, productName);

    // Security check to prevent directory traversal
    if (!productPath.startsWith(guiDir)) {
      if (req.query.format === 'json' || req.accepts(['html', 'json']) === 'json') {
        return res.status(403).json({ error: 'Access Denied' });
      }
      return res.status(403).send('<h1>Access Denied</h1>');
    }

    try {
      await fs.access(productPath);
    } catch {
      if (req.query.format === 'json' || req.accepts(['html', 'json']) === 'json') {
        return res.status(404).json({ error: `Product "${productName}" not found` });
      }
      return res.status(404).send(`<h1>Product "${productName}" not found</h1>`);
    }

    const allFiles = await getFilesRecursively(productPath);
    
    // Filter for images (can expand extensions if needed)
    const imageFiles = allFiles
      .filter(f => /\.(png|jpg|jpeg|gif|webp)$/i.test(f))
      .map(f => {
        let rel = path.relative(productPath, f);
        if (path.sep === '\\') {
          rel = rel.split(path.sep).join('/');
        }
        return rel;
      })
      .sort();

    // Check for JSON request
    if (req.query.format === 'json' || req.accepts(['html', 'json']) === 'json') {
      return res.json({ 
        product: productName,
        files: imageFiles.map(f => `renders/${productName}/${f}`) 
      });
    }

    const html = `
      <!DOCTYPE html>
      <html>
        <head>
          <title>${productName} - Renders</title>
          <style>
            body { font-family: sans-serif; padding: 20px; }
            h1 { margin-bottom: 20px; }
            .back-link { display: inline-block; margin-bottom: 20px; color: #666; }
            ul { list-style-type: none; padding: 0; }
            li { margin: 5px 0; }
            a { text-decoration: none; color: #007bff; }
            a:hover { text-decoration: underline; }
          </style>
        </head>
        <body>
          <a href="/renders" class="back-link">&larr; Back to Products</a>
          <h1>${productName}</h1>
          <ul>
            ${imageFiles.map(file => {
              // file path relative to product dir
              // Link format: /renders/Product/file.png
              // We need to encode the file path components for URL safety
              const encodedFile = file.split('/').map(encodeURIComponent).join('/');
              return `<li><a href="/renders/${encodeURIComponent(productName)}/${file}" download="${file.split('/').pop()}">${file}</a></li>`;
            }).join('')}
          </ul>
          ${imageFiles.length === 0 ? '<p>No images found.</p>' : ''}
        </body>
      </html>
    `;

    res.send(html);

  } catch (err) {
    console.error(`Error listing files for ${req.params.product}:`, err);
    if (req.query.format === 'json' || req.accepts(['html', 'json']) === 'json') {
      res.status(500).json({ error: 'Internal Server Error' });
    } else {
      res.status(500).send('<h1>Internal Server Error</h1>');
    }
  }
});

export default router;

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
// Returns a list of products and their rendered files (recursively)
router.get('/', async (req, res) => {
  try {
    const guiDir = config.GUI_DIR;
    
    // Check if GUI directory exists
    try {
      await fs.access(guiDir);
    } catch {
      return res.json({ products: [] });
    }

    // Read top-level directories (Products)
    // We only care about directories here
    const productEntries = await fs.readdir(guiDir, { withFileTypes: true });
    
    const products = [];

    for (const entry of productEntries) {
      if (entry.isDirectory()) {
        const productName = entry.name;
        const productPath = path.join(guiDir, productName);
        
        try {
          // Get all files recursively in this product directory
          const allFiles = await getFilesRecursively(productPath);
          
          // Filter for likely image files (ends with .png)
          const imageFiles = allFiles
            .filter(f => f.endsWith('.png'))
            .map(f => {
              // Convert absolute path to relative path from product directory
              // e.g., "C:\...\gui\Product\subdir\img.png" -> "subdir/img.png"
              // Ensure we use forward slashes for URLs if on Windows
              let rel = path.relative(productPath, f);
              if (path.sep === '\\') {
                rel = rel.split(path.sep).join('/');
              }
              // Return path relative to the renders mount point
              // e.g. "renders/Product/subdir/img.png"
              return `renders/${productName}/${rel}`;
            });
          
          if (imageFiles.length > 0) {
            products.push({
              name: productName,
              files: imageFiles
            });
          }
        } catch (err) {
          console.error(`Error reading directory ${productName}:`, err);
          // Skip this directory if unreadable
        }
      }
    }

    res.json({ products });
  } catch (err) {
    console.error('Error listing renders:', err);
    res.status(500).json({ error: 'Failed to list renders' });
  }
});

export default router;

import fs from 'fs/promises';
import path from 'path';
import config from '../config.js';
import express from 'express';

const router = express.Router();

// GET /renders/
// Returns a list of products and their rendered files
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
    const productEntries = await fs.readdir(guiDir, { withFileTypes: true });
    
    const products = [];

    for (const entry of productEntries) {
      if (entry.isDirectory()) {
        const productName = entry.name;
        const productPath = path.join(guiDir, productName);
        
        // Read files in product directory
        try {
          const files = await fs.readdir(productPath);
          // Filter for likely image files if needed, or just return all
          const imageFiles = files.filter(f => f.endsWith('.png'));
          
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

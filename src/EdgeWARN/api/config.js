import path from 'path';
import fs from 'fs';

// Determine base directory similar to Python util/file.py
let BASE_DIR;
if (process.platform === 'win32') {
  BASE_DIR = path.resolve('C:\\EdgeWARN_input');
} else {
  // Try /home/EdgeWARN_input first, then /workspaces/EdgeWARN_input, then fallback
  if (fs.existsSync('/home/EdgeWARN_input')) {
    BASE_DIR = path.resolve('/home/EdgeWARN_input');
  } else if (fs.existsSync('/workspaces/EdgeWARN_input')) {
    BASE_DIR = path.resolve('/workspaces/EdgeWARN_input');
  } else {
    BASE_DIR = path.resolve('EdgeWARN_input');
  }
}

const config = {
  BASE_DIR,

  // Filenames used by GUI
  STORMCELL_JSON: 'src/stormcell_test.json'
};

export default config;

import path from 'path';
import fs from 'fs';

// Parse CLI arguments for --base-dir
function parseBaseDir() {
  const args = process.argv.slice(2);
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--base-dir' && args[i + 1]) {
      return path.resolve(args[i + 1]);
    }
    if (args[i].startsWith('--base-dir=')) {
      return path.resolve(args[i].split('=')[1]);
    }
  }
  return null;
}

// Determine base directory: CLI arg > environment > auto-detect
let BASE_DIR = parseBaseDir();

if (!BASE_DIR) {
  // Check environment variable
  if (process.env.EDGEWARN_BASE_DIR) {
    BASE_DIR = path.resolve(process.env.EDGEWARN_BASE_DIR);
  } else if (process.platform === 'win32') {
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
}

const DATA_DIR = path.join(BASE_DIR, 'data');

const config = {
  BASE_DIR,
  DATA_DIR,
  MRMS_RALA_DIR: path.join(DATA_DIR, 'RALA'),
  MRMS_CGFLASH_DIR: path.join(DATA_DIR, 'NLDN'),
  MRMS_NLDN_DIR: path.join(DATA_DIR, 'NLDN_Density'),
  MRMS_ECHOTOP18_DIR: path.join(DATA_DIR, 'EchoTop18'),
  MRMS_ECHOTOP30_DIR: path.join(DATA_DIR, 'EchoTop30'),
  MRMS_QPE_DIR: path.join(DATA_DIR, 'QPE_01H'),
  MRMS_RAIN_DIR: path.join(DATA_DIR, 'WarmRainProbability'),
  MRMS_PRECIPRATE_DIR: path.join(DATA_DIR, 'PrecipRate'),
  MRMS_PROBSEVERE_DIR: path.join(DATA_DIR, 'ProbSevere'),
  MRMS_FLASH_DIR: path.join(DATA_DIR, 'FLASH'),
  MRMS_VIL_DIR: path.join(DATA_DIR, 'VILDensity'),
  MRMS_VII_DIR: path.join(DATA_DIR, 'VII'),
  MRMS_ROTATIONT_DIR: path.join(DATA_DIR, 'RotationTrack30min'),
  MRMS_COMPOSITE_DIR: path.join(DATA_DIR, 'CompRefQC'),
  MRMS_RHOHV_DIR: path.join(DATA_DIR, 'RhoHV'),
  MRMS_PRECIPTYP_DIR: path.join(DATA_DIR, 'PrecipFlag'),
  MRMS_MESH_DIR: path.join(DATA_DIR, 'MESH'),
  GOES_GLM_DIR: path.join(DATA_DIR, 'GLM'),
  ABI_CLOUDPRES_DIR: path.join(DATA_DIR, 'ABI-CloudPressure'),
  STORMCELL_DIR: path.join(DATA_DIR, 'stormcells'),
  CELL_DIR: path.join(DATA_DIR, 'cells'),
  METAR_DIR: path.join(DATA_DIR, 'METAR'),
  NWS_DIR: path.join(DATA_DIR, 'NWS'),

  // Filenames used by GUI
  STORMCELL_JSON: 'src/stormcell_test.json'
};

// Log which base directory is being used
console.log(`[Config] Using BASE_DIR: ${BASE_DIR}`);

export default config;

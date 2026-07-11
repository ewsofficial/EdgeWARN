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

// Parse CLI arguments for --debug_server
function isDebugServer() {
  return process.argv.includes('--debug_server');
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
    // Try user home directory first, then common locations
    const homeDir = process.env.HOME || '/home';
    const userEdgewarnPath = path.join(homeDir, 'EdgeWARN_input');

    if (fs.existsSync(userEdgewarnPath)) {
      BASE_DIR = path.resolve(userEdgewarnPath);
    } else if (fs.existsSync('/home/EdgeWARN_input')) {
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
  MRMS_PRECIPRATE_DIR: path.join(DATA_DIR, 'PrecipRate'),
  MRMS_PROBSEVERE_DIR: path.join(DATA_DIR, 'ProbSevere'),
  MRMS_FLASH_CREST_MAXUNIT_DIR: path.join(DATA_DIR, 'FLASH_CREST_MAXUNIT'),
  MRMS_FLASH_ARIMAX_DIR: path.join(DATA_DIR, 'FLASH_ARIMAX'),
  MRMS_FLASH_ARI30M_DIR: path.join(DATA_DIR, 'FLASH_ARI30M'),
  MRMS_FLASH_ARI01H_DIR: path.join(DATA_DIR, 'FLASH_ARI01H'),
  MRMS_FLASH_HP_MAXUNIT_DIR: path.join(DATA_DIR, 'FLASH_HP_MAXUNIT'),
  MRMS_FLASH_SAC_MAXSOIL_DIR: path.join(DATA_DIR, 'FLASH_SAC_MAXSOIL'),
  MRMS_FLASH_FFGMAX_DIR: path.join(DATA_DIR, 'FLASH_FFGMAX'),
  MRMS_DVIL_DIR: path.join(DATA_DIR, 'VILDensity'),
  MRMS_VIL_DIR: path.join(DATA_DIR, 'VIL'),
  MRMS_VII_DIR: path.join(DATA_DIR, 'VII'),
  MRMS_ROTATIONT_DIR: path.join(DATA_DIR, 'RotationTrack30min'),
  MRMS_COMPOSITE_DIR: path.join(DATA_DIR, 'CompRefQC'),
  MRMS_RHOHV_DIR: path.join(DATA_DIR, 'RhoHV'),
  MRMS_PRECIPTYP_DIR: path.join(DATA_DIR, 'PrecipFlag'),
  MRMS_MESH_DIR: path.join(DATA_DIR, 'MESH'),
  GOES_GLM_DIR: path.join(DATA_DIR, 'GLM'),
  STORMCELL_DIR: path.join(DATA_DIR, 'stormcells'),
  CELL_DIR: path.join(DATA_DIR, 'cells'),
  METAR_DIR: path.join(DATA_DIR, 'METAR'),
  NWS_DIR: path.join(DATA_DIR, 'NWS'),
  SURFACE_DIR: path.join(DATA_DIR, 'surface_features'),
  ALERTS_DIR: path.join(DATA_DIR, 'Alerts'),
  EDGEWARN_ALERTS_DIR: path.join(DATA_DIR, 'Alerts', 'EdgeWARN'),
  EDGEWARN_ALERTS_IDS_DIR: path.join(DATA_DIR, 'Alerts', 'EdgeWARN', 'ids'),
  EDGEWARN_ALERTS_TS_DIR: path.join(DATA_DIR, 'Alerts', 'EdgeWARN', 'timestamps'),
  OFFICIAL_ALERTS_DIR: path.join(DATA_DIR, 'Alerts', 'official'),
  OFFICIAL_ALERTS_IDS_DIR: path.join(DATA_DIR, 'Alerts', 'official', 'ids'),
  OFFICIAL_ALERTS_TS_DIR: path.join(DATA_DIR, 'Alerts', 'official', 'timestamps'),

  // Filenames used by GUI
  STORMCELL_JSON: 'src/stormcell_test.json',

  // Debug server mode
  DEBUG_SERVER: isDebugServer(),
  DEBUG_PORT: 3001,
  DEFAULT_PORT: 5000
};

// Validate directories exist
const requiredDirs = [
  config.MRMS_RALA_DIR,
  config.MRMS_CGFLASH_DIR,
  config.MRMS_NLDN_DIR,
  config.MRMS_ECHOTOP18_DIR,
  config.MRMS_ECHOTOP30_DIR,
  config.MRMS_QPE_DIR,
  config.MRMS_PRECIPRATE_DIR,
  config.MRMS_PROBSEVERE_DIR,
  config.MRMS_FLASH_CREST_MAXUNIT_DIR,
  config.MRMS_FLASH_ARIMAX_DIR,
  config.MRMS_FLASH_ARI30M_DIR,
  config.MRMS_FLASH_ARI01H_DIR,
  config.MRMS_FLASH_HP_MAXUNIT_DIR,
  config.MRMS_FLASH_SAC_MAXSOIL_DIR,
  config.MRMS_FLASH_FFGMAX_DIR,
  config.MRMS_DVIL_DIR,
  config.MRMS_VIL_DIR,
  config.MRMS_VII_DIR,
  config.MRMS_ROTATIONT_DIR,
  config.MRMS_COMPOSITE_DIR,
  config.MRMS_RHOHV_DIR,
  config.MRMS_PRECIPTYP_DIR,
  config.MRMS_MESH_DIR,
  config.GOES_GLM_DIR,
  config.STORMCELL_DIR,
  config.CELL_DIR,
  config.METAR_DIR,
  config.NWS_DIR,
  config.SURFACE_DIR,
  config.ALERTS_DIR,
  config.EDGEWARN_ALERTS_DIR,
  config.EDGEWARN_ALERTS_IDS_DIR,
  config.EDGEWARN_ALERTS_TS_DIR,
  config.OFFICIAL_ALERTS_DIR,
  config.OFFICIAL_ALERTS_IDS_DIR,
  config.OFFICIAL_ALERTS_TS_DIR
];

for (const dir of requiredDirs) {
  if (!fs.existsSync(dir)) {
    console.warn(`[Config] Directory not found. Creating: ${dir}`);
    fs.mkdirSync(dir, { recursive: true });
  }
}

// Log which base directory is being used
console.log(`[Config] Using BASE_DIR: ${BASE_DIR}`);

export default config;

import path from 'path';

// Determine base directory similar to Python util/file.py
let BASE_DIR;
if (process.platform === 'win32') {
  BASE_DIR = path.resolve('C:\\EdgeWARN_input');
} else {
  try {
    BASE_DIR = path.resolve('/home/EdgeWARN_input');
  } catch (e) {
    BASE_DIR = path.resolve('EdgeWARN_input');
  }
}

const GUI_DIR = path.join(BASE_DIR, 'gui');

const config = {
  BASE_DIR,
  GUI_DIR,
  GUI_RALA_DIR: path.join(GUI_DIR, 'RALA'),
  GUI_NLDN_DIR: path.join(GUI_DIR, 'NLDN'),
  GUI_ECHOTOP18_DIR: path.join(GUI_DIR, 'EchoTop18'),
  GUI_ECHOTOP30_DIR: path.join(GUI_DIR, 'EchoTop30'),
  GUI_QPE_DIR: path.join(GUI_DIR, 'QPE_01H'),
  GUI_PRECIPRATE_DIR: path.join(GUI_DIR, 'PrecipRate'),
  GUI_PROBSEVERE_DIR: path.join(GUI_DIR, 'ProbSevere'),
  GUI_FLASH_DIR: path.join(GUI_DIR, 'FLASH'),
  GUI_VIL_DIR: path.join(GUI_DIR, 'VILDensity'),
  GUI_VII_DIR: path.join(GUI_DIR, 'VII'),
  GUI_ROTATIONT_DIR: path.join(GUI_DIR, 'RotationTrack30min'),
  GUI_COMPOSITE_DIR: path.join(GUI_DIR, 'CompRefQC'),
  GUI_RHOHV_DIR: path.join(GUI_DIR, 'RhoHV'),
  GUI_PRECIPTYP_DIR: path.join(GUI_DIR, 'PrecipFlag'),
  GUI_MAP_DIR: path.join(GUI_DIR, 'maps'),

  // Filenames used by GUI
  GUI_MANIFEST_JSON: 'src/overlay_manifest.json',
  GUI_COLORMAP_JSON: 'src/colormaps.json',
  STORMCELL_JSON: 'src/stormcell_test.json'
};

export default config;

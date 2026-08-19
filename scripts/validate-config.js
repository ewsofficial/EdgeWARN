import { CONFIG_NAMES, ConfigError, validateAllConfigs } from '../src/config/loader.js';

function main() {
  try {
    validateAllConfigs();
  } catch (error) {
    if (error instanceof ConfigError) {
      console.log(`FAIL -> ${error.message}`);
    process.exitCode = 1;
    return;
    }
    throw error;
  }
  console.log(`All ${CONFIG_NAMES.length} config files passed validation`);
}

main();

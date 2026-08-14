import { CONFIG_NAMES, ConfigError, loadConfig } from '../src/config/loader.js';

function main() {
  let failures = 0;
  for (const name of CONFIG_NAMES) {
    try {
      loadConfig(name);
      console.log(`OK   ${name}`);
    } catch (error) {
      if (error instanceof ConfigError) {
        console.log(`FAIL ${name} -> ${error.message}`);
        failures += 1;
      } else {
        throw error;
      }
    }
  }
  if (failures > 0) {
    console.log(`${failures}/${CONFIG_NAMES.length} config file(s) failed validation`);
    process.exitCode = 1;
    return;
  }
  console.log(`All ${CONFIG_NAMES.length} config files passed validation`);
}

main();

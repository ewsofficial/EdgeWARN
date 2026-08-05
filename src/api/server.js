import { pathToFileURL } from 'url';
import { createApp } from './app.js';

export async function startServer(options = {}) {
  const { app, config } = await createApp(options);
  const debug = (options.argv || process.argv.slice(2)).includes('--debug-server');
  const port = options.port || (debug ? 3001 : config.port);
  const server = app.listen(port, options.host || '0.0.0.0', () => console.log(`Unified EdgeWARN API listening on port ${port}`));
  return { app, server, port, config };
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  if (process.argv.includes('--compat=edgewarn') || process.argv.includes('--compat=ewmrs')) console.warn('[Deprecation] Use npm run api; compatibility start commands now launch the unified service.');
  startServer().catch((error) => { console.error(error); process.exitCode = 1; });
}

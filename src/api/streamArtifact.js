import { pipeline } from 'stream/promises';

/**
 * Send an already-validated artifact and deterministically release its handle.
 *
 * The caller owns no stream events: this function waits for the HTTP pipeline
 * to settle and then waits for FileHandle.close().  Keeping those awaits in
 * the request lifetime avoids Node 26 treating a deferred close as a leaked
 * handle during garbage collection.
 */
export async function streamArtifact(req, res, opened, type, headers = {}, cacheHeaders = {}) {
  try {
    res.set(opened.headers || {}).set(headers).set(cacheHeaders).type(type);
    if (req.fresh) return res.status(304).end();
    res.set('Content-Length', String(opened.size));
    if (req.method === 'HEAD') return res.end();

    await pipeline(opened.handle.createReadStream({ autoClose: false }), res);
  } catch (error) {
    // A disconnected client has no response left to repair.  The finally block
    // still closes the descriptor before this request handler returns.
    if (!req.destroyed && !res.destroyed) throw error;
  } finally {
    await opened.handle.close();
  }
}

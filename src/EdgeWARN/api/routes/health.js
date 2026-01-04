import express from 'express';
import os from 'os';

const router = express.Router();

// Initialize previous CPU times
let lastCpus = os.cpus();

/**
 * GET /health
 * Returns server health status, total CPU usage (100% = 1 core), and system memory usage in MB
 */
router.get('/', (req, res) => {
  const currentCpus = os.cpus();
  let totalUsage = 0;

  for (let i = 0; i < currentCpus.length; i++) {
    const prev = lastCpus[i];
    const curr = currentCpus[i];

    let prevTotal = 0;
    let currTotal = 0;

    for (const type in prev.times) prevTotal += prev.times[type];
    for (const type in curr.times) currTotal += curr.times[type];

    const totalDiff = currTotal - prevTotal;
    const idleDiff = curr.times.idle - prev.times.idle;

    if (totalDiff > 0) {
      totalUsage += 1 - idleDiff / totalDiff;
    }
  }

  // Update lastCpus for the next request
  lastCpus = currentCpus;

  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMemMB = Math.round((totalMem - freeMem) / (1024 * 1024));

  res.json({
    status: 'OK',
    cpuUsage: Number((totalUsage * 100).toFixed(2)),
    systemMemoryUsageMB: usedMemMB
  });
});

export default router;

import express from 'express';
import os from 'os';

const router = express.Router();

/**
 * GET /health
 * Returns server health status, uptime, CPU, and memory usage
 */
router.get('/', (req, res) => {
  const uptimeSeconds = process.uptime();

  // Memory usage (bytes) and percent of system memory
  const mem = process.memoryUsage();
  const totalSystemMem = os.totalmem();
  const memory = {
    rss: mem.rss,
    heapTotal: mem.heapTotal,
    heapUsed: mem.heapUsed,
    external: mem.external,
    systemTotal: totalSystemMem,
    rssPercentOfSystem: Number(((mem.rss / totalSystemMem) * 100).toFixed(2))
  };

  // CPU usage: compute average busy percentage across cores (since boot)
  const cpus = os.cpus();
  let totalIdle = 0;
  let totalTick = 0;
  cpus.forEach((c) => {
    for (const t in c.times) {
      totalTick += c.times[t];
    }
    totalIdle += c.times.idle;
  });
  const avgIdle = totalIdle / cpus.length;
  const avgTotal = totalTick / cpus.length;
  const cpuUsagePercent = Number((100 * (1 - avgIdle / avgTotal)).toFixed(2));

  const cpu = {
    cores: cpus.length,
    usagePercent: cpuUsagePercent,
    loadAverage: os.loadavg()
  };

  res.json({ 
    status: 'OK', 
    uptimeSeconds: Math.round(uptimeSeconds), 
    cpu, 
    memory 
  });
});

export default router;

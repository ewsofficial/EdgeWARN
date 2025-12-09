import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import rendersRouter from './routes/renders.js';
import path from 'path';
import os from 'os';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// Routes
app.get('/', (req, res) => {
  res.json({ message: 'EdgeWARN Backend API' });
});

app.get('/health', (req, res) => {
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

  res.json({ status: 'OK', uptimeSeconds: Math.round(uptimeSeconds), cpu, memory });
});

// Renders subtab (placeholder)
app.use('/renders', rendersRouter);

// Serve robots.txt
app.get('/robots.txt', (req, res) => {
  const robotsPath = path.resolve(process.cwd(), 'src/EdgeWARN/backend/robots.txt');
  res.sendFile(robotsPath, (err) => {
    if (err) {
      console.error('Error sending robots.txt:', err);
      res.status(404).end();
    }
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});

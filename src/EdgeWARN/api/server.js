import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import path from 'path';
import featuresRouter from './routes/features/index.js';
import healthRouter from './routes/health.js';
import rateLimit from 'express-rate-limit';
import compression from 'compression';
import cluster from 'cluster';
import os from 'os';

dotenv.config();

const PORT = process.env.PORT || 5000;
const numCPUs = os.cpus().length;

if (cluster.isPrimary) {
  console.log(`Primary ${process.pid} is running`);

  // Fork workers.
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} died`);
    // Restart worker
    cluster.fork();
  });
} else {
  const app = express();

  // Middleware
  app.use(compression());
  app.use(cors());
  app.use(express.json());

  // Rate Limiting
  const limiter = rateLimit({
    windowMs: 1000, // 1 second
    max: 10, // Limit each IP to 10 requests per `windowMs`
    standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
    legacyHeaders: false, // Disable the `X-RateLimit-*` headers
  });

  // Apply the rate limiting middleware to all requests
  app.use(limiter);

  // Routes
  app.get('/', (req, res) => {
    res.json({ message: 'EdgeWARN Backend API' });
  });

  // Mount feature routes
  app.use('/features', featuresRouter);

  // Mount health route
  app.use('/health', healthRouter);

  // Serve robots.txt
  app.get('/robots.txt', (req, res) => {
    const robotsPath = path.resolve(process.cwd(), 'src/EdgeWARN/api/robots.txt');
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
  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Worker ${process.pid} started on http://localhost:${PORT}`);
  });
}

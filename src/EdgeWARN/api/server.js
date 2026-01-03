import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import path from 'path';
import featuresRouter from './routes/features/index.js';
import healthRouter from './routes/health.js';
import rateLimit from 'express-rate-limit';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;


// Middleware
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
  console.log(`Server running on http://localhost:${PORT}`);
});

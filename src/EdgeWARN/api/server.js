import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import path from 'path';
import featuresRouter from './routes/features/index.js';
import healthRouter from './routes/health.js';

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
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});

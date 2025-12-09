import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import rendersRouter from './routes/renders.js';
import path from 'path';

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
  res.json({ status: 'OK' });
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

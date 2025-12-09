import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import rendersRouter from './routes/renders.js';

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

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});

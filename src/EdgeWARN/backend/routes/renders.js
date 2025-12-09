import express from 'express';

const router = express.Router();

// GET /renders/
router.get('/', (req, res) => {
  res.json({ message: 'Renders subtab placeholder' });
});

export default router;

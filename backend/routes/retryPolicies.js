const express = require('express');
const router = express.Router();
const { v4: uuidv4 } = require('uuid');
const db = require('../db');
const { authenticateToken, requireRole } = require('../middleware/auth');

// 1. Get all retry policies for a project
router.get('/', authenticateToken, (req, res) => {
  const { project_id } = req.query;
  if (!project_id) {
    return res.status(400).json({ error: 'project_id is required' });
  }

  try {
    const policies = db.prepare('SELECT * FROM retry_policies WHERE project_id = ?').all(project_id);
    return res.json(policies);
  } catch (error) {
    console.error('Fetch retry policies error:', error);
    return res.status(500).json({ error: 'Failed to fetch retry policies' });
  }
});

// 2. Create a new retry policy (Owner only)
router.post('/', authenticateToken, requireRole(['owner']), (req, res) => {
  const { project_id, name, strategy, delay_ms, max_retries } = req.body;

  if (!project_id || !name || !strategy || delay_ms === undefined || max_retries === undefined) {
    return res.status(400).json({ error: 'project_id, name, strategy, delay_ms, and max_retries are required' });
  }

  if (!['fixed', 'linear', 'exponential'].includes(strategy)) {
    return res.status(400).json({ error: 'Invalid strategy. Must be fixed, linear, or exponential' });
  }

  try {
    const id = uuidv4();
    db.prepare(`
      INSERT INTO retry_policies (id, project_id, name, strategy, delay_ms, max_retries, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(id, project_id, name, strategy, Number(delay_ms), Number(max_retries), Date.now(), Date.now());

    const policy = db.prepare('SELECT * FROM retry_policies WHERE id = ?').get(id);
    return res.status(201).json(policy);
  } catch (error) {
    console.error('Create retry policy error:', error);
    return res.status(500).json({ error: 'Failed to create retry policy' });
  }
});

// 3. Update a retry policy (Owner only)
router.patch('/:id', authenticateToken, requireRole(['owner']), (req, res) => {
  const { id } = req.params;
  const { name, strategy, delay_ms, max_retries } = req.body;

  try {
    const current = db.prepare('SELECT * FROM retry_policies WHERE id = ?').get(id);
    if (!current) {
      return res.status(404).json({ error: 'Retry policy not found' });
    }

    const newName = name !== undefined ? name : current.name;
    const newStrategy = strategy !== undefined ? strategy : current.strategy;
    const newDelay = delay_ms !== undefined ? Number(delay_ms) : current.delay_ms;
    const newMaxRetries = max_retries !== undefined ? Number(max_retries) : current.max_retries;

    if (!['fixed', 'linear', 'exponential'].includes(newStrategy)) {
      return res.status(400).json({ error: 'Invalid strategy. Must be fixed, linear, or exponential' });
    }

    db.prepare(`
      UPDATE retry_policies
      SET name = ?, strategy = ?, delay_ms = ?, max_retries = ?, updated_at = ?
      WHERE id = ?
    `).run(newName, newStrategy, newDelay, newMaxRetries, Date.now(), id);

    const updated = db.prepare('SELECT * FROM retry_policies WHERE id = ?').get(id);
    return res.json(updated);
  } catch (error) {
    console.error('Update retry policy error:', error);
    return res.status(500).json({ error: 'Failed to update retry policy' });
  }
});

// 4. Delete a retry policy (Owner only)
router.delete('/:id', authenticateToken, requireRole(['owner']), (req, res) => {
  const { id } = req.params;

  try {
    const current = db.prepare('SELECT * FROM retry_policies WHERE id = ?').get(id);
    if (!current) {
      return res.status(404).json({ error: 'Retry policy not found' });
    }

    db.prepare('DELETE FROM retry_policies WHERE id = ?').run(id);
    return res.json({ message: 'Retry policy deleted successfully' });
  } catch (error) {
    console.error('Delete retry policy error:', error);
    return res.status(500).json({ error: 'Failed to delete retry policy' });
  }
});

module.exports = router;

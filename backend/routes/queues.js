const express = require('express');
const router = express.Router();
const { v4: uuidv4 } = require('uuid');
const db = require('../db');
const { authenticateToken } = require('../middleware/auth');

// Get all queues for a project (with job count summaries)
router.get('/', authenticateToken, (req, res) => {
  const { project_id } = req.query;
  if (!project_id) {
    return res.status(400).json({ error: 'project_id is required' });
  }

  try {
    // Verify membership in the organization of the project
    const authCheck = db.prepare(`
      SELECT om.role
      FROM projects p
      JOIN organizations o ON p.organization_id = o.id
      JOIN organization_members om ON o.id = om.organization_id
      WHERE p.id = ? AND om.user_id = ?
    `).get(project_id, req.user.id);

    if (!authCheck) {
      return res.status(403).json({ error: 'Access denied' });
    }

    const queues = db.prepare(`
      SELECT q.*, rp.name as retry_policy_name
      FROM queues q
      LEFT JOIN retry_policies rp ON q.retry_policy_id = rp.id
      WHERE q.project_id = ?
    `).all(project_id);

    // Attach current counts
    const enrichedQueues = queues.map(q => {
      const counts = db.prepare(`
        SELECT status, COUNT(*) as count 
        FROM jobs 
        WHERE queue_id = ? 
        GROUP BY status
      `).all(q.id);

      const dlqCount = db.prepare(`
        SELECT COUNT(*) as count
        FROM dead_letter_queue
        WHERE queue_id = ?
      `).get(q.id).count;

      const summary = {
        queued: 0,
        scheduled: 0,
        claimed: 0,
        running: 0,
        completed: 0,
        failed: 0,
        cancelled: 0,
        dlq: dlqCount
      };

      counts.forEach(row => {
        summary[row.status] = row.count;
      });

      return {
        ...q,
        stats: summary
      };
    });

    return res.json(enrichedQueues);
  } catch (error) {
    console.error('Fetch queues error:', error);
    return res.status(500).json({ error: 'Failed to fetch queues' });
  }
});

// Create a new queue
router.post('/', authenticateToken, (req, res) => {
  const { project_id, name, priority, concurrency_limit, retry_policy_id } = req.body;
  if (!project_id || !name) {
    return res.status(400).json({ error: 'project_id and name are required' });
  }

  try {
    // Auth Check
    const authCheck = db.prepare(`
      SELECT om.role
      FROM projects p
      JOIN organizations o ON p.organization_id = o.id
      JOIN organization_members om ON o.id = om.organization_id
      WHERE p.id = ? AND om.user_id = ?
    `).get(project_id, req.user.id);

    if (!authCheck) {
      return res.status(403).json({ error: 'Access denied' });
    }

    const queueId = uuidv4();
    db.prepare(`
      INSERT INTO queues (id, project_id, name, priority, concurrency_limit, retry_policy_id, is_paused, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
    `).run(
      queueId,
      project_id,
      name,
      priority !== undefined ? Number(priority) : 10,
      concurrency_limit !== undefined ? Number(concurrency_limit) : 5,
      retry_policy_id || null,
      Date.now(),
      Date.now()
    );

    const queue = db.prepare('SELECT * FROM queues WHERE id = ?').get(queueId);
    return res.status(201).json(queue);
  } catch (error) {
    if (error.message.includes('UNIQUE constraint failed')) {
      return res.status(400).json({ error: 'A queue with this name already exists in the project' });
    }
    console.error('Create queue error:', error);
    return res.status(500).json({ error: 'Failed to create queue' });
  }
});

// Update queue configuration (priority, concurrency_limit, pause/resume)
router.patch('/:id', authenticateToken, (req, res) => {
  const { id } = req.params;
  const { priority, concurrency_limit, is_paused, retry_policy_id } = req.body;

  try {
    // Auth Check
    const authCheck = db.prepare(`
      SELECT om.role
      FROM queues q
      JOIN projects p ON q.project_id = p.id
      JOIN organizations o ON p.organization_id = o.id
      JOIN organization_members om ON o.id = om.organization_id
      WHERE q.id = ? AND om.user_id = ?
    `).get(id, req.user.id);

    if (!authCheck) {
      return res.status(403).json({ error: 'Access denied' });
    }

    const current = db.prepare('SELECT * FROM queues WHERE id = ?').get(id);
    if (!current) {
      return res.status(404).json({ error: 'Queue not found' });
    }

    const newPriority = priority !== undefined ? Number(priority) : current.priority;
    const newLimit = concurrency_limit !== undefined ? Number(concurrency_limit) : current.concurrency_limit;
    const newPaused = is_paused !== undefined ? (is_paused ? 1 : 0) : current.is_paused;
    const newPolicyId = retry_policy_id !== undefined ? retry_policy_id : current.retry_policy_id;

    db.prepare(`
      UPDATE queues
      SET priority = ?, concurrency_limit = ?, is_paused = ?, retry_policy_id = ?, updated_at = ?
      WHERE id = ?
    `).run(newPriority, newLimit, newPaused, newPolicyId, Date.now(), id);

    const updated = db.prepare('SELECT * FROM queues WHERE id = ?').get(id);
    return res.json(updated);
  } catch (error) {
    console.error('Update queue error:', error);
    return res.status(500).json({ error: 'Failed to update queue' });
  }
});

module.exports = router;

const express = require('express');
const router = express.Router();
const db = require('../db');
const { authenticateToken } = require('../middleware/auth');

router.get('/', authenticateToken, (req, res) => {
  const { project_id } = req.query;
  if (!project_id) {
    return res.status(400).json({ error: 'project_id is required' });
  }

  try {
    // 1. Get Queues under project
    const queues = db.prepare('SELECT id FROM queues WHERE project_id = ?').all(project_id);
    const queueIds = queues.map(q => q.id);

    if (queueIds.length === 0) {
      return res.json({
        summary: { queued: 0, scheduled: 0, claimed: 0, running: 0, completed: 0, failed: 0, cancelled: 0, dlq: 0 },
        workers: [],
        throughput: [],
        queues: []
      });
    }

    const placeholders = queueIds.map(() => '?').join(',');

    // 2. Count jobs in each status for this project's queues
    const statusCounts = db.prepare(`
      SELECT status, COUNT(*) as count
      FROM jobs
      WHERE queue_id IN (${placeholders})
      GROUP BY status
    `).all(...queueIds);

    const dlqCount = db.prepare(`
      SELECT COUNT(*) as count
      FROM dead_letter_queue
      WHERE queue_id IN (${placeholders})
    `).get(...queueIds).count;

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

    statusCounts.forEach(row => {
      summary[row.status] = row.count;
    });

    // 3. Workers currently active (heartbeat within 15 seconds)
    const activeWorkers = db.prepare(`
      SELECT * FROM workers
      ORDER BY last_heartbeat DESC
    `).all();

    // 4. Job throughput: jobs completed per minute over the last 15 minutes
    const fifteenMinutesAgo = Date.now() - 15 * 60 * 1000;
    const throughput = db.prepare(`
      SELECT 
        (finished_at / 60000) * 60000 as minute_bucket,
        COUNT(*) as count
      FROM job_executions
      WHERE finished_at >= ?
        AND status = 'completed'
      GROUP BY minute_bucket
      ORDER BY minute_bucket ASC
    `).all(fifteenMinutesAgo);

    // Format throughput data points
    const formattedThroughput = throughput.map(t => ({
      time: new Date(t.minute_bucket).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      count: t.count
    }));

    return res.json({
      summary,
      workers: activeWorkers,
      throughput: formattedThroughput
    });
  } catch (error) {
    console.error('Fetch metrics error:', error);
    return res.status(500).json({ error: 'Failed to fetch system metrics' });
  }
});

module.exports = router;

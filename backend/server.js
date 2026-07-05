const express = require('express');
const cors = require('cors');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const db = require('./db');

const authRoutes = require('./routes/auth');
const projectRoutes = require('./routes/projects');
const queueRoutes = require('./routes/queues');
const jobRoutes = require('./routes/jobs');
const metricsRoutes = require('./routes/metrics');
const retryPoliciesRoutes = require('./routes/retryPolicies');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

// Set up SSE clients tracking
let sseClients = new Set();

// SSE Endpoint for Live Updates
app.get('/api/live', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  // Send initial connected ping
  res.write(`data: ${JSON.stringify({ type: 'ping', time: Date.now() })}\n\n`);

  sseClients.add(res);

  req.on('close', () => {
    sseClients.delete(res);
  });
});

// Broadcast helper
function broadcastEvent(type, data) {
  const payload = JSON.stringify({ type, data, timestamp: Date.now() });
  for (const client of sseClients) {
    client.write(`data: ${payload}\n\n`);
  }
}

// Mount Routes
app.use('/api/auth', authRoutes);
app.use('/api/projects', projectRoutes);
app.use('/api/queues', queueRoutes);
app.use('/api/jobs', jobRoutes);
app.use('/api/metrics', metricsRoutes);
app.use('/api/retry-policies', retryPoliciesRoutes);

// Global Error Handler
app.use((err, req, res, next) => {
  console.error('API Error:', err);
  res.status(500).json({ error: err.message || 'Internal Server Error' });
});

// Stale Worker & Orphaned Jobs Recovery Scavenger
setInterval(() => {
  try {
    const staleThreshold = Date.now() - 15000; // 15 seconds limit
    
    // Find active workers that haven't updated heartbeat
    const staleWorkers = db.prepare(`
      SELECT id, hostname FROM workers 
      WHERE status = 'active' AND last_heartbeat < ?
    `).all(staleThreshold);

    if (staleWorkers.length > 0) {
      db.transaction(() => {
        for (const w of staleWorkers) {
          console.log(`[Watchdog] Worker ${w.hostname} (${w.id}) is offline. Reclaiming running jobs.`);
          
          // Mark worker as offline
          db.prepare("UPDATE workers SET status = 'offline' WHERE id = ?").run(w.id);

          // Find jobs claimed/running by this worker
          const orphanedJobs = db.prepare(`
            SELECT * FROM jobs 
            WHERE worker_id = ? AND status IN ('claimed', 'running')
          `).all(w.id);

          for (const job of orphanedJobs) {
            const nextAttempt = job.attempt_number + 1;
            
            if (nextAttempt >= job.max_retries) {
              // Exceeded max retries, move to DLQ
              db.prepare(`
                INSERT INTO dead_letter_queue (
                  id, job_id, queue_id, name, payload, error_message, failed_at, original_created_at, attempt_number
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
              `).run(
                uuidv4(),
                job.id,
                job.queue_id,
                job.name,
                job.payload,
                `Worker lost mid-run. Exhausted ${job.max_retries} attempts.`,
                Date.now(),
                job.created_at,
                nextAttempt
              );

              // Remove from jobs
              db.prepare('DELETE FROM jobs WHERE id = ?').run(job.id);
            } else {
              // Reschedule back to queued
              db.prepare(`
                UPDATE jobs 
                SET status = 'queued', worker_id = NULL, attempt_number = ?, error_message = ?, updated_at = ?
                WHERE id = ?
              `).run(nextAttempt, 'Worker heartbeat lost (rescheduled)', Date.now(), job.id);

              db.prepare(`
                INSERT INTO job_logs (id, job_id, level, message, timestamp)
                VALUES (?, ?, 'warn', 'Worker heartbeat lost. Rescheduling job attempt.', ?)
              `).run(uuidv4(), job.id, Date.now());
            }
          }
        }
      })();

      // Notify clients of membership shifts
      broadcastEvent('worker_update', { workers: staleWorkers.length });
    }
  } catch (err) {
    console.error('[Watchdog] Error cleaning up stale workers:', err);
  }
}, 5000);

// Broadcast statistics tick every 3 seconds to active SSE streams
setInterval(() => {
  if (sseClients.size > 0) {
    try {
      // Just emit a simple tick so clients know they can refresh metrics or pull summary data
      broadcastEvent('tick', { time: Date.now() });
    } catch (err) {
      console.error('[SSE] Broadcast error:', err);
    }
  }
}, 3000);

app.listen(PORT, () => {
  console.log(`[API Server] Running on http://localhost:${PORT}`);
});

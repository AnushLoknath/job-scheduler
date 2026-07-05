const os = require('os');
const { v4: uuidv4 } = require('uuid');
const parser = require('cron-parser');
const db = require('../backend/db');

const WORKER_ID = `worker-${uuidv4().substring(0, 8)}`;
const HOSTNAME = os.hostname();
const CONCURRENCY_LIMIT = 5; // Worker thread limit
const HEARTBEAT_INTERVAL_MS = 5000;
const POLL_INTERVAL_MS = 1000;

let activeJobsCount = 0;
let isShuttingDown = false;
const activeExecutions = new Map(); // jobId -> timeout/interval or active promise

console.log(`[Worker] Starting worker ${WORKER_ID} on host ${HOSTNAME} (Concurrency: ${CONCURRENCY_LIMIT})...`);

// 1. Worker Heartbeat Loop
function registerAndHeartbeat() {
  try {
    const now = Date.now();
    // Insert or update worker state
    db.prepare(`
      INSERT INTO workers (id, hostname, status, concurrency_limit, last_heartbeat, started_at)
      VALUES (?, ?, 'active', ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET 
        last_heartbeat = excluded.last_heartbeat,
        status = 'active'
    `).run(WORKER_ID, HOSTNAME, CONCURRENCY_LIMIT, now, now);

    // Also write heartbeat history record
    db.prepare(`
      INSERT INTO worker_heartbeats (id, worker_id, timestamp, active_jobs, concurrency_limit)
      VALUES (?, ?, ?, ?, ?)
    `).run(uuidv4(), WORKER_ID, now, activeJobsCount, CONCURRENCY_LIMIT);

    // Prune heartbeat logs older than 24 hours
    db.prepare(`
      DELETE FROM worker_heartbeats WHERE timestamp < ?
    `).run(now - 24 * 60 * 60 * 1000);
  } catch (err) {
    console.error('[Worker] Heartbeat error:', err);
  }
}

// Register on boot
registerAndHeartbeat();
const heartbeatInterval = setInterval(registerAndHeartbeat, HEARTBEAT_INTERVAL_MS);

// 2. Helper sleep function
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// 3. Calculate Retry Delay
function calculateNextRunTime(job) {
  const attempt = job.attempt_number; // current attempt count just finished
  const strategy = job.retry_strategy;
  const baseDelay = job.retry_delay || 1000;

  let delay = baseDelay;
  if (strategy === 'linear') {
    delay = baseDelay * attempt;
  } else if (strategy === 'exponential') {
    delay = baseDelay * Math.pow(2, attempt - 1);
  }
  return Date.now() + delay;
}

// Helper to update batch statistics
function updateBatchProgress(batchId, status) {
  if (!batchId) return;
  try {
    db.transaction(() => {
      const batch = db.prepare('SELECT * FROM batches WHERE id = ?').get(batchId);
      if (!batch) return;

      let completedInc = status === 'completed' ? 1 : 0;
      let failedInc = status === 'failed' ? 1 : 0;

      db.prepare(`
        UPDATE batches
        SET completed_jobs = completed_jobs + ?,
            failed_jobs = failed_jobs + ?,
            updated_at = ?
        WHERE id = ?
      `).run(completedInc, failedInc, Date.now(), batchId);
    })();
  } catch (err) {
    console.error('[Worker] Error updating batch progress:', err);
  }
}

// 4. Atomic SQLite Claim Transaction
const claimNextJob = db.transaction((workerId) => {
  if (activeJobsCount >= CONCURRENCY_LIMIT) {
    return null;
  }

  // Fetch the highest priority job from active (non-paused) queues,
  // respecting individual queue concurrency limits and job dependencies.
  const job = db.prepare(`
    SELECT j.*, q.concurrency_limit as queue_limit, q.priority as queue_priority
    FROM jobs j
    JOIN queues q ON j.queue_id = q.id
    WHERE q.is_paused = 0
      AND (j.status = 'queued' OR (j.status = 'scheduled' AND j.run_at <= ?))
      -- Check queue concurrency limits
      AND (
        SELECT COUNT(*)
        FROM jobs active_j
        WHERE active_j.queue_id = q.id
          AND active_j.status IN ('claimed', 'running')
      ) < q.concurrency_limit
      -- Check that all parent dependencies are completed
      AND NOT EXISTS (
        SELECT 1
        FROM job_dependencies jd
        JOIN jobs parent ON jd.parent_job_id = parent.id
        WHERE jd.child_job_id = j.id
          AND parent.status != 'completed'
      )
    ORDER BY q.priority DESC, j.run_at ASC, j.created_at ASC
    LIMIT 1
  `).get(Date.now());

  if (!job) {
    return null;
  }

  // Atomically claim the job
  db.prepare(`
    UPDATE jobs
    SET status = 'claimed', worker_id = ?, claimed_at = ?, updated_at = ?
    WHERE id = ?
  `).run(workerId, Date.now(), Date.now(), job.id);

  // Delete from scheduled_jobs table
  db.prepare('DELETE FROM scheduled_jobs WHERE job_id = ?').run(job.id);

  return job;
});

// 5. Job Execution Runner
async function executeJob(job) {
  activeJobsCount++;
  const startTime = Date.now();
  const attempt = job.attempt_number + 1;

  // Track executing promise for graceful shutdown
  let resolveExecution;
  const executionPromise = new Promise((r) => { resolveExecution = r; });
  activeExecutions.set(job.id, executionPromise);

  try {
    // 1. Transition job to 'running'
    db.prepare(`
      UPDATE jobs
      SET status = 'running', attempt_number = ?, started_at = ?, updated_at = ?
      WHERE id = ?
    `).run(attempt, startTime, Date.now(), job.id);

    db.prepare(`
      INSERT INTO job_logs (id, job_id, level, message, timestamp)
      VALUES (?, ?, 'info', 'Job execution started (Attempt #${attempt})', ?)
    `).run(uuidv4(), job.id, startTime);

    console.log(`[Worker] Running job ${job.name} (ID: ${job.id}) - Attempt #${attempt}`);

    // Parse payload
    let payload = {};
    try {
      payload = JSON.parse(job.payload);
    } catch (e) {
      payload = {};
    }

    // 2. Perform Job Logic based on name/type
    // Simulate runtime and possible errors based on job configs
    const executionDuration = payload.duration_ms || 2000;
    const shouldFail = payload.should_fail || false;
    const failRate = payload.fail_rate || 0; // 0 to 1

    await sleep(executionDuration);

    if (shouldFail || (failRate > 0 && Math.random() < failRate)) {
      throw new Error(payload.error_message || 'Simulated execution failure');
    }

    // Success Execution Flow
    const duration = Date.now() - startTime;

    db.prepare(`
      INSERT INTO job_executions (id, job_id, worker_id, attempt_number, status, started_at, finished_at, duration_ms)
      VALUES (?, ?, ?, ?, 'completed', ?, ?, ?)
    `).run(uuidv4(), job.id, WORKER_ID, attempt, startTime, Date.now(), duration);

    db.prepare(`
      INSERT INTO job_logs (id, job_id, level, message, timestamp)
      VALUES (?, ?, 'info', 'Job completed successfully in ${duration}ms', ?)
    `).run(uuidv4(), job.id, Date.now());

    // Check if it's a recurring cron job
    if (job.cron_expression) {
      try {
        const nextRun = parser.parseExpression(job.cron_expression).next().getTime();
        db.prepare(`
          UPDATE jobs
          SET status = 'scheduled', run_at = ?, worker_id = NULL, claimed_at = NULL, started_at = NULL, error_message = NULL, updated_at = ?
          WHERE id = ?
        `).run(nextRun, Date.now(), job.id);

        // Sync with scheduled_jobs
        db.prepare(`
          INSERT INTO scheduled_jobs (id, job_id, run_at, cron_expression, created_at)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(job_id) DO UPDATE SET run_at = excluded.run_at
        `).run(uuidv4(), job.id, nextRun, job.cron_expression, Date.now());

        db.prepare(`
          INSERT INTO job_logs (id, job_id, level, message, timestamp)
          VALUES (?, ?, 'info', 'Recurring cron rescheduled for next run at: ' || ?, ?)
        `).run(uuidv4(), job.id, new Date(nextRun).toISOString(), Date.now());

        console.log(`[Worker] Cron job ${job.name} completed. Rescheduled next run for ${new Date(nextRun).toISOString()}`);
      } catch (cronErr) {
        // Fallback: if cron expression fails parsing, complete the job normally
        db.prepare(`
          UPDATE jobs
          SET status = 'completed', completed_at = ?, updated_at = ?
          WHERE id = ?
        `).run(Date.now(), Date.now(), job.id);
      }
    } else {
      // Normal non-recurring completion
      db.prepare(`
        UPDATE jobs
        SET status = 'completed', completed_at = ?, updated_at = ?
        WHERE id = ?
      `).run(Date.now(), Date.now(), job.id);
    }

    updateBatchProgress(job.batch_id, 'completed');

  } catch (error) {
    // Failure Execution Flow
    const duration = Date.now() - startTime;
    console.error(`[Worker] Job ${job.name} failed: ${error.message}`);

    db.prepare(`
      INSERT INTO job_executions (id, job_id, worker_id, attempt_number, status, error_message, started_at, finished_at, duration_ms)
      VALUES (?, ?, ?, ?, 'failed', ?, ?, ?, ?)
    `).run(uuidv4(), job.id, WORKER_ID, attempt, error.message, startTime, Date.now(), duration);

    db.prepare(`
      INSERT INTO job_logs (id, job_id, level, message, timestamp)
      VALUES (?, ?, 'error', 'Job execution failed: ' || ?, ?)
    `).run(uuidv4(), job.id, error.message, Date.now());

    // Handle retries
    if (attempt < job.max_retries) {
      const nextRun = calculateNextRunTime({ ...job, attempt_number: attempt });
      
      db.prepare(`
        UPDATE jobs
        SET status = 'scheduled', attempt_number = ?, run_at = ?, error_message = ?, worker_id = NULL, claimed_at = NULL, started_at = NULL, updated_at = ?
        WHERE id = ?
      `).run(attempt, nextRun, error.message, Date.now(), job.id);

      // Sync with scheduled_jobs
      db.prepare(`
        INSERT INTO scheduled_jobs (id, job_id, run_at, cron_expression, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET run_at = excluded.run_at
      `).run(uuidv4(), job.id, nextRun, job.cron_expression || null, Date.now());

      db.prepare(`
        INSERT INTO job_logs (id, job_id, level, message, timestamp)
        VALUES (?, ?, 'warn', 'Job rescheduled for retry in ' || ? || 'ms', ?)
      `).run(uuidv4(), job.id, nextRun - Date.now(), Date.now());

      console.log(`[Worker] Job ${job.name} will be retried at ${new Date(nextRun).toISOString()}`);
    } else {
      // Move to DLQ (Dead Letter Queue)
      try {
        db.transaction(() => {
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
            `Failed after ${attempt} attempts. Last error: ${error.message}`,
            Date.now(),
            job.created_at,
            attempt
          );

          // Delete from regular jobs table
          db.prepare('DELETE FROM jobs WHERE id = ?').run(job.id);
          // Delete from scheduled_jobs
          db.prepare('DELETE FROM scheduled_jobs WHERE job_id = ?').run(job.id);
        })();

        console.log(`[Worker] Job ${job.name} failed permanently. Moved to Dead Letter Queue.`);
      } catch (dlqErr) {
        console.error('[Worker] Fatal: failed moving job to DLQ:', dlqErr);
      }

      updateBatchProgress(job.batch_id, 'failed');
    }
  } finally {
    activeJobsCount--;
    activeExecutions.delete(job.id);
    resolveExecution();
  }
}

// 6. Polling loop
async function pollingLoop() {
  while (!isShuttingDown) {
    try {
      if (activeJobsCount < CONCURRENCY_LIMIT) {
        const job = claimNextJob(WORKER_ID);
        if (job) {
          // Execute asynchronously (non-blocking the poll cycle)
          executeJob(job);
          // Immediately check for more capacity without sleeping
          continue;
        }
      }
    } catch (err) {
      console.error('[Worker] Polling loop error:', err);
    }
    // Sleep before next poll
    await sleep(POLL_INTERVAL_MS);
  }
}

// Start polling
pollingLoop();

// 7. Graceful Shutdown
async function handleShutdown(signal) {
  if (isShuttingDown) return;
  console.log(`[Worker] Received ${signal}. Starting graceful shutdown...`);
  isShuttingDown = true;

  // Clear intervals
  clearInterval(heartbeatInterval);

  if (activeExecutions.size > 0) {
    console.log(`[Worker] Waiting for ${activeExecutions.size} active jobs to complete... (Max wait: 10s)`);
    
    // Create a timeout promise
    const timeoutPromise = sleep(10000).then(() => {
      console.log('[Worker] Graceful shutdown timeout reached. Terminating remaining jobs.');
    });

    // Wait for all executing jobs or the shutdown timeout
    await Promise.race([
      Promise.all(activeExecutions.values()),
      timeoutPromise
    ]);
  }

  // Mark worker as offline
  try {
    db.prepare("UPDATE workers SET status = 'offline' WHERE id = ?").run(WORKER_ID);
    console.log('[Worker] Unregistered from coordinator.');
  } catch (err) {
    console.error('[Worker] Unregister error:', err);
  }

  console.log('[Worker] Shutdown complete. Exiting.');
  process.exit(0);
}

process.on('SIGINT', () => handleShutdown('SIGINT'));
process.on('SIGTERM', () => handleShutdown('SIGTERM'));

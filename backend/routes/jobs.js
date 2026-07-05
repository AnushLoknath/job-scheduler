const express = require('express');
const router = express.Router();
const { v4: uuidv4 } = require('uuid');
const parser = require('cron-parser');
const db = require('../db');
const { authenticateToken } = require('../middleware/auth');
const rateLimiter = require('../middleware/rateLimit');

// Helper to calculate next run time for cron
function getNextCronTime(cronExpression) {
  try {
    const interval = parser.parseExpression(cronExpression);
    return interval.next().getTime();
  } catch (err) {
    throw new Error('Invalid cron expression: ' + err.message);
  }
}

// 1. Get List of Jobs (Paginated & Filtered)
router.get('/', authenticateToken, (req, res) => {
  const { queue_id, status, batch_id, search, page = 1, limit = 10 } = req.query;
  const offset = (Number(page) - 1) * Number(limit);

  try {
    let query = 'SELECT j.*, q.name as queue_name FROM jobs j JOIN queues q ON j.queue_id = q.id WHERE 1=1';
    const params = [];

    if (queue_id) {
      query += ' AND j.queue_id = ?';
      params.push(queue_id);
    }
    if (status) {
      query += ' AND j.status = ?';
      params.push(status);
    }
    if (batch_id) {
      query += ' AND j.batch_id = ?';
      params.push(batch_id);
    }
    if (search) {
      query += ' AND (j.name LIKE ? OR j.id LIKE ? OR j.error_message LIKE ?)';
      params.push(`%${search}%`, `%${search}%`, `%${search}%`);
    }

    // Get total count for pagination headers
    let countQuery = query.replace('SELECT j.*, q.name as queue_name', 'SELECT COUNT(*) as count');
    const totalCount = db.prepare(countQuery).get(...params).count;

    // Apply Sorting and Pagination
    query += ' ORDER BY j.created_at DESC LIMIT ? OFFSET ?';
    params.push(Number(limit), offset);

    const jobs = db.prepare(query).all(...params);

    return res.json({
      jobs,
      pagination: {
        total: totalCount,
        page: Number(page),
        limit: Number(limit),
        pages: Math.ceil(totalCount / Number(limit))
      }
    });
  } catch (error) {
    console.error('Fetch jobs error:', error);
    return res.status(500).json({ error: 'Failed to fetch jobs' });
  }
});

// 2. Get Single Job Details (with Executions and Logs)
router.get('/:id', authenticateToken, (req, res) => {
  const { id } = req.params;

  try {
    const job = db.prepare(`
      SELECT j.*, q.name as queue_name 
      FROM jobs j 
      JOIN queues q ON j.queue_id = q.id 
      WHERE j.id = ?
    `).get(id);

    if (!job) {
      // Check Dead Letter Queue as well!
      const dlqJob = db.prepare(`
        SELECT d.*, q.name as queue_name
        FROM dead_letter_queue d
        JOIN queues q ON d.queue_id = q.id
        WHERE d.job_id = ?
      `).get(id);

      if (dlqJob) {
        // Mock a dead job representation
        return res.json({
          job: {
            id: dlqJob.job_id,
            queue_id: dlqJob.queue_id,
            queue_name: dlqJob.queue_name,
            name: dlqJob.name,
            payload: dlqJob.payload,
            status: 'failed',
            attempt_number: dlqJob.attempt_number,
            error_message: dlqJob.error_message,
            created_at: dlqJob.original_created_at,
            updated_at: dlqJob.failed_at,
            is_dlq: true
          },
          executions: [],
          logs: [
            { id: 'dlq-log', message: `Job permanently failed and entered Dead Letter Queue: ${dlqJob.error_message}`, level: 'error', timestamp: dlqJob.failed_at }
          ]
        });
      }

      return res.status(404).json({ error: 'Job not found' });
    }

    const executions = db.prepare('SELECT * FROM job_executions WHERE job_id = ? ORDER BY started_at DESC').all(id);
    const logs = db.prepare('SELECT * FROM job_logs WHERE job_id = ? ORDER BY timestamp ASC').all(id);
    
    // Check for dependencies
    const dependencies = db.prepare(`
      SELECT jd.parent_job_id, j.name, j.status
      FROM job_dependencies jd
      JOIN jobs j ON jd.parent_job_id = j.id
      WHERE jd.child_job_id = ?
    `).all(id);

    return res.json({ job, executions, logs, dependencies });
  } catch (error) {
    console.error('Fetch job details error:', error);
    return res.status(500).json({ error: 'Failed to fetch job' });
  }
});

// 3. Create a Single Job (Immediate, Delayed, Cron, Dependencies)
router.post('/', authenticateToken, rateLimiter(30, 60000), (req, res) => {
  const { 
    queue_id, 
    name, 
    payload = {}, 
    delay_ms = 0, 
    cron_expression, 
    max_retries, 
    retry_strategy, 
    retry_delay,
    retry_policy_id,
    dependencies = [] // list of parent job IDs
  } = req.body;

  if (!queue_id || !name) {
    return res.status(400).json({ error: 'queue_id and name are required' });
  }

  try {
    const queue = db.prepare('SELECT id, retry_policy_id FROM queues WHERE id = ?').get(queue_id);
    if (!queue) {
      return res.status(404).json({ error: 'Queue not found' });
    }

    // Resolve retry policy (body overrides queue-level policy)
    let finalMaxRetries = max_retries !== undefined ? Number(max_retries) : null;
    let finalStrategy = retry_strategy;
    let finalDelay = retry_delay !== undefined ? Number(retry_delay) : null;
    let resolvedPolicyId = retry_policy_id || null;

    if (resolvedPolicyId) {
      const policy = db.prepare('SELECT * FROM retry_policies WHERE id = ?').get(resolvedPolicyId);
      if (policy) {
        if (finalMaxRetries === null) finalMaxRetries = policy.max_retries;
        if (!finalStrategy) finalStrategy = policy.strategy;
        if (finalDelay === null) finalDelay = policy.delay_ms;
      }
    } else if (queue.retry_policy_id) {
      resolvedPolicyId = queue.retry_policy_id;
      const policy = db.prepare('SELECT * FROM retry_policies WHERE id = ?').get(resolvedPolicyId);
      if (policy) {
        if (finalMaxRetries === null) finalMaxRetries = policy.max_retries;
        if (!finalStrategy) finalStrategy = policy.strategy;
        if (finalDelay === null) finalDelay = policy.delay_ms;
      }
    }

    // Standard fallbacks if no policy is associated
    if (finalMaxRetries === null) finalMaxRetries = 3;
    if (!finalStrategy) finalStrategy = 'exponential';
    if (finalDelay === null) finalDelay = 1000;

    let runAt = Date.now() + Number(delay_ms);
    let initialStatus = 'queued';

    if (Number(delay_ms) > 0) {
      initialStatus = 'scheduled';
    }

    if (cron_expression) {
      runAt = getNextCronTime(cron_expression);
      initialStatus = 'scheduled';
    }

    const jobId = uuidv4();
    const payloadStr = typeof payload === 'string' ? payload : JSON.stringify(payload);

    const transaction = db.transaction(() => {
      // Create Job
      db.prepare(`
        INSERT INTO jobs (
          id, queue_id, name, status, payload, run_at, max_retries, 
          retry_strategy, retry_delay, retry_policy_id, attempt_number, cron_expression, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
      `).run(
        jobId,
        queue_id,
        name,
        initialStatus,
        payloadStr,
        runAt,
        finalMaxRetries,
        finalStrategy,
        finalDelay,
        resolvedPolicyId,
        cron_expression || null,
        Date.now(),
        Date.now()
      );

      // Create scheduled_job entry if delayed or cron
      if (initialStatus === 'scheduled') {
        db.prepare(`
          INSERT INTO scheduled_jobs (id, job_id, run_at, cron_expression, created_at)
          VALUES (?, ?, ?, ?, ?)
        `).run(uuidv4(), jobId, runAt, cron_expression || null, Date.now());
      }

      // Create log
      db.prepare(`
        INSERT INTO job_logs (id, job_id, level, message, timestamp)
        VALUES (?, ?, 'info', 'Job created successfully', ?)
      `).run(uuidv4(), jobId, Date.now());

      // Create dependencies
      if (dependencies && dependencies.length > 0) {
        const insertDep = db.prepare(`
          INSERT INTO job_dependencies (id, parent_job_id, child_job_id, created_at)
          VALUES (?, ?, ?, ?)
        `);
        for (const parentId of dependencies) {
          // Verify parent job exists
          const parentExists = db.prepare('SELECT id FROM jobs WHERE id = ?').get(parentId);
          if (parentExists) {
            insertDep.run(uuidv4(), parentId, jobId, Date.now());
          }
        }
      }
    });

    transaction();

    const createdJob = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
    return res.status(201).json(createdJob);
  } catch (error) {
    console.error('Create job error:', error);
    return res.status(400).json({ error: error.message || 'Failed to create job' });
  }
});

// 4. Create a Batch of Jobs
router.post('/batch', authenticateToken, rateLimiter(15, 60000), (req, res) => {
  const { project_id, name, queue_id, jobs = [] } = req.body;
  if (!project_id || !name || !queue_id || jobs.length === 0) {
    return res.status(400).json({ error: 'project_id, name, queue_id, and a non-empty array of jobs are required' });
  }

  try {
    const queue = db.prepare('SELECT id, retry_policy_id FROM queues WHERE id = ? AND project_id = ?').get(queue_id, project_id);
    if (!queue) {
      return res.status(404).json({ error: 'Queue not found in this project' });
    }

    // Resolve queue's retry policy for batch-level fallback
    let defaultMaxRetries = 3;
    let defaultStrategy = 'exponential';
    let defaultDelay = 1000;
    let resolvedPolicyId = queue.retry_policy_id || null;

    if (resolvedPolicyId) {
      const policy = db.prepare('SELECT * FROM retry_policies WHERE id = ?').get(resolvedPolicyId);
      if (policy) {
        defaultMaxRetries = policy.max_retries;
        defaultStrategy = policy.strategy;
        defaultDelay = policy.delay_ms;
      }
    }

    const batchId = uuidv4();
    const createdJobs = [];

    const batchTransaction = db.transaction(() => {
      // 1. Create Batch Entry
      db.prepare(`
        INSERT INTO batches (id, project_id, name, total_jobs, completed_jobs, failed_jobs, created_at, updated_at)
        VALUES (?, ?, ?, ?, 0, 0, ?, ?)
      `).run(batchId, project_id, name, jobs.length, Date.now(), Date.now());

      // 2. Insert individual jobs
      const insertJob = db.prepare(`
        INSERT INTO jobs (
          id, queue_id, batch_id, retry_policy_id, name, status, payload, run_at, max_retries, 
          retry_strategy, retry_delay, attempt_number, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, 0, ?, ?)
      `);

      const insertLog = db.prepare(`
        INSERT INTO job_logs (id, job_id, level, message, timestamp)
        VALUES (?, ?, 'info', ?, ?)
      `);

      for (const item of jobs) {
        const jobId = uuidv4();
        const payloadStr = typeof item.payload === 'string' ? item.payload : JSON.stringify(item.payload || {});
        
        const maxR = item.max_retries !== undefined ? Number(item.max_retries) : defaultMaxRetries;
        const strat = item.retry_strategy || defaultStrategy;
        const delay = item.retry_delay !== undefined ? Number(item.retry_delay) : defaultDelay;

        insertJob.run(
          jobId,
          queue_id,
          batchId,
          resolvedPolicyId,
          item.name || `${name} Task`,
          payloadStr,
          Date.now(),
          maxR,
          strat,
          delay,
          Date.now(),
          Date.now()
        );

        insertLog.run(uuidv4(), jobId, `Job created as part of batch '${name}'`, Date.now());
        createdJobs.push({ id: jobId, name: item.name });
      }
    });

    batchTransaction();

    return res.status(201).json({
      batch_id: batchId,
      name,
      total_jobs: jobs.length,
      jobs: createdJobs
    });
  } catch (error) {
    console.error('Create batch error:', error);
    return res.status(500).json({ error: 'Failed to create batch' });
  }
});

// 5. Retry a failed or DLQ Job (Manual Override)
router.post('/:id/retry', authenticateToken, (req, res) => {
  const { id } = req.params;

  try {
    // 1. Try to find in DLQ first
    const dlqEntry = db.prepare('SELECT * FROM dead_letter_queue WHERE job_id = ?').get(id);

    if (dlqEntry) {
      const transaction = db.transaction(() => {
        // Move back to jobs table
        db.prepare(`
          INSERT INTO jobs (
            id, queue_id, name, status, payload, run_at, max_retries, 
            retry_strategy, retry_delay, attempt_number, created_at, updated_at
          ) VALUES (?, ?, ?, 'queued', ?, ?, ?, 'exponential', 1000, 0, ?, ?)
        `).run(
          dlqEntry.job_id,
          dlqEntry.queue_id,
          dlqEntry.name,
          dlqEntry.payload,
          Date.now(), // Run immediately
          3, // reset retries to 3
          Date.now(),
          Date.now()
        );

        // Delete from DLQ
        db.prepare('DELETE FROM dead_letter_queue WHERE job_id = ?').run(id);

        // Add log
        db.prepare(`
          INSERT INTO job_logs (id, job_id, level, message, timestamp)
          VALUES (?, ?, 'info', 'Job recovered from Dead Letter Queue and queued for retry', ?)
        `).run(uuidv4(), dlqEntry.job_id, Date.now());
      });

      transaction();
      return res.json({ message: 'Job recovered from DLQ and requeued successfully' });
    }

    // 2. Otherwise find in regular jobs table
    const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(id);
    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    if (job.status !== 'failed' && job.status !== 'cancelled') {
      return res.status(400).json({ error: `Only failed or cancelled jobs can be retried. Current status is ${job.status}` });
    }

    db.prepare(`
      UPDATE jobs
      SET status = 'queued', attempt_number = 0, run_at = ?, error_message = NULL, updated_at = ?
      WHERE id = ?
    `).run(Date.now(), Date.now(), id);

    db.prepare(`
      INSERT INTO job_logs (id, job_id, level, message, timestamp)
      VALUES (?, ?, 'info', 'Job manual retry triggered', ?)
    `).run(uuidv4(), id, Date.now());

    return res.json({ message: 'Job requeued for retry' });
  } catch (error) {
    console.error('Retry job error:', error);
    return res.status(500).json({ error: 'Failed to retry job' });
  }
});

// 6. Cancel a Job
router.post('/:id/cancel', authenticateToken, (req, res) => {
  const { id } = req.params;

  try {
    const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(id);
    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    if (['completed', 'failed', 'cancelled'].includes(job.status)) {
      return res.status(400).json({ error: `Cannot cancel job in status: ${job.status}` });
    }

    db.prepare(`
      UPDATE jobs
      SET status = 'cancelled', updated_at = ?
      WHERE id = ?
    `).run(Date.now(), id);

    db.prepare(`
      INSERT INTO job_logs (id, job_id, level, message, timestamp)
      VALUES (?, ?, 'warn', 'Job cancelled by user', ?)
    `).run(uuidv4(), id, Date.now());

    return res.json({ message: 'Job successfully cancelled' });
  } catch (error) {
    console.error('Cancel job error:', error);
    return res.status(500).json({ error: 'Failed to cancel job' });
  }
});

// 7. Get AI Failure Summary (Bonus Feature)
router.get('/:id/ai-summary', authenticateToken, (req, res) => {
  const { id } = req.params;

  try {
    let errorMsg = null;
    let jobName = '';

    // First try standard jobs table
    const job = db.prepare('SELECT name, error_message FROM jobs WHERE id = ?').get(id);
    if (job) {
      errorMsg = job.error_message;
      jobName = job.name;
    } else {
      // Then check DLQ
      const dlqJob = db.prepare('SELECT name, error_message FROM dead_letter_queue WHERE job_id = ?').get(id);
      if (dlqJob) {
        errorMsg = dlqJob.error_message;
        jobName = dlqJob.name;
      }
    }

    if (!job && !errorMsg) {
      return res.status(404).json({ error: 'Job not found or has no failure record' });
    }

    if (!errorMsg) {
      return res.json({
        summary: 'Job is not in a failed state.',
        suggestions: 'No diagnostic action required.'
      });
    }

    // Rule-based diagnostic simulator (acting as localized AI parser)
    let summary = 'Execution encountered an unhandled exception.';
    let suggestions = 'Verify target code reference parameters and variables, check for syntax errors, or review backend logs.';

    const lowerErr = errorMsg.toLowerCase();
    if (lowerErr.includes('timeout') || lowerErr.includes('timed out')) {
      summary = `The execution of task '${jobName}' timed out before receiving a response from the designated service endpoint.`;
      suggestions = 'Check payment provider or network gateway status. Review the "duration_ms" payload configuration or increase endpoint request timeouts.';
    } else if (lowerErr.includes('unique constraint') || lowerErr.includes('duplicate')) {
      summary = `The database transaction failed due to a unique key constraint violation on task '${jobName}'.`;
      suggestions = 'Validate that your payload contains unique identifiers. Avoid resubmitting the identical job run key twice (idempotency check).';
    } else if (lowerErr.includes('heartbeat lost') || lowerErr.includes('worker lost')) {
      summary = `The worker node executing '${jobName}' disconnected or crashed mid-run (lost socket communication/heartbeat).`;
      suggestions = 'Check if the worker process terminated due to Out-Of-Memory (OOM) error. Review worker log files or adjust heartbeat stale threshold in backend config.';
    } else if (lowerErr.includes('simulated')) {
      summary = `An intentional testing failure was triggered for task '${jobName}' based on payload instruction settings.`;
      suggestions = 'Verify the job payload parameter settings: set "should_fail" to false or decrease the "fail_rate" variable to zero.';
    }

    return res.json({
      summary,
      suggestions,
      rawError: errorMsg
    });
  } catch (error) {
    console.error('AI summary diagnostic error:', error);
    return res.status(500).json({ error: 'Failed to generate failure diagnostic summary' });
  }
});

module.exports = router;

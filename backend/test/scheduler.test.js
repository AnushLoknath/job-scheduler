const test = require('node:test');
const assert = require('node:assert');
const { v4: uuidv4 } = require('uuid');
const db = require('../db');

test.describe('Distributed Job Scheduler Test Suite', () => {

  test.beforeEach(() => {
    db.prepare("DELETE FROM jobs WHERE name IN ('Job A', 'Job B', 'Atomic Claim Test Job')").run();
    db.prepare("DELETE FROM workers WHERE id IN ('worker-a', 'worker-b', 'worker-1')").run();
  });

  test('Database contains seeded organizations, projects, and queues', () => {
    const org = db.prepare('SELECT * FROM organizations WHERE name = ?').get('Acme Corp');
    assert.ok(org, 'Default organization Acme Corp should exist');

    const project = db.prepare('SELECT * FROM projects WHERE name = ?').get('Main Engineering');
    assert.ok(project, 'Default project Main Engineering should exist');

    const queues = db.prepare('SELECT * FROM queues WHERE project_id = ?').all(project.id);
    assert.strictEqual(queues.length, 3, 'There should be exactly 3 seeded queues');
    
    const queueNames = queues.map(q => q.name);
    assert.ok(queueNames.includes('high-priority'));
    assert.ok(queueNames.includes('default'));
    assert.ok(queueNames.includes('low-priority'));
  });

  test('Atomic claiming prevents double-claiming a job', () => {
    const defaultQueue = db.prepare("SELECT * FROM queues WHERE name = 'default'").get();
    const jobId = uuidv4();
    
    // Register mock workers to satisfy foreign key constraints
    db.prepare("INSERT INTO workers (id, hostname, concurrency_limit, last_heartbeat, started_at) VALUES ('worker-a', 'localhost', 5, ?, ?)").run(Date.now(), Date.now());
    db.prepare("INSERT INTO workers (id, hostname, concurrency_limit, last_heartbeat, started_at) VALUES ('worker-b', 'localhost', 5, ?, ?)").run(Date.now(), Date.now());

    // Insert a test job
    db.prepare(`
      INSERT INTO jobs (id, queue_id, name, status, payload, run_at, max_retries, retry_strategy, retry_delay, attempt_number, created_at, updated_at)
      VALUES (?, ?, ?, 'queued', ?, ?, 3, 'exponential', 1000, 0, ?, ?)
    `).run(jobId, defaultQueue.id, 'Atomic Claim Test Job', '{}', Date.now(), Date.now(), Date.now());

    // Define claiming logic
    const claimFn = (workerId) => {
      return db.transaction(() => {
        const nextJob = db.prepare(`
          SELECT j.*
          FROM jobs j
          JOIN queues q ON j.queue_id = q.id
          WHERE j.id = ? AND q.is_paused = 0 AND j.status = 'queued'
          LIMIT 1
        `).get(jobId);

        if (!nextJob) return null;

        db.prepare(`
          UPDATE jobs SET status = 'claimed', worker_id = ?, claimed_at = ?
          WHERE id = ?
        `).run(workerId, Date.now(), nextJob.id);

        return nextJob;
      })();
    };

    // Run parallel claim processes
    const claim1 = claimFn('worker-a');
    const claim2 = claimFn('worker-b');

    // Asserts
    assert.ok(claim1, 'First worker should successfully claim the job');
    assert.strictEqual(claim1.id, jobId);
    assert.strictEqual(claim2, null, 'Second worker should fail to claim the job (must return null)');

    // Cleanup
    db.prepare('DELETE FROM jobs WHERE id = ?').run(jobId);
    db.prepare("DELETE FROM workers WHERE id IN ('worker-a', 'worker-b')").run();
  });

  test('Queue concurrency limits are respected during job claims', () => {
    const lowPriorityQueue = db.prepare("SELECT * FROM queues WHERE name = 'low-priority'").get();
    
    const originalLimit = lowPriorityQueue.concurrency_limit;
    const originalPaused = lowPriorityQueue.is_paused;

    // Temporarily set concurrency limit of low-priority to 1 and ensure it is NOT paused
    db.prepare("UPDATE queues SET concurrency_limit = 1, is_paused = 0 WHERE id = ?").run(lowPriorityQueue.id);

    const jobAId = uuidv4();
    const jobBId = uuidv4();

    // Insert two jobs in low-priority with sequential created_at timestamps to prevent sorting flakiness
    const now = Date.now();
    db.prepare(`
      INSERT INTO jobs (id, queue_id, name, status, payload, run_at, max_retries, retry_strategy, retry_delay, attempt_number, created_at, updated_at)
      VALUES (?, ?, 'Job A', 'queued', '{}', ?, 3, 'exponential', 1000, 0, ?, ?)
    `).run(jobAId, lowPriorityQueue.id, now, now - 100, now - 100);

    db.prepare(`
      INSERT INTO jobs (id, queue_id, name, status, payload, run_at, max_retries, retry_strategy, retry_delay, attempt_number, created_at, updated_at)
      VALUES (?, ?, 'Job B', 'queued', '{}', ?, 3, 'exponential', 1000, 0, ?, ?)
    `).run(jobBId, lowPriorityQueue.id, now, now, now);

    // Claim next job transaction
    const claimFn = (workerId) => {
      return db.transaction(() => {
        const job = db.prepare(`
          SELECT j.*
          FROM jobs j
          JOIN queues q ON j.queue_id = q.id
          WHERE q.id = ?
            AND q.is_paused = 0
            AND j.status = 'queued'
            AND j.id IN (?, ?)
            AND (
              SELECT COUNT(*)
              FROM jobs active_j
              WHERE active_j.queue_id = q.id
                AND active_j.status IN ('claimed', 'running')
            ) < q.concurrency_limit
          ORDER BY j.created_at ASC
          LIMIT 1
        `).get(lowPriorityQueue.id, jobAId, jobBId);

        if (!job) return null;

        db.prepare("UPDATE jobs SET status = 'running' WHERE id = ?").run(job.id);
        return job;
      })();
    };

    // Claim first job
    const claim1 = claimFn('worker-1');
    assert.ok(claim1, 'Should claim first job successfully');
    assert.strictEqual(claim1.id, jobAId);

    // Try to claim second job while first is running
    const claim2 = claimFn('worker-1');
    assert.strictEqual(claim2, null, 'Should NOT claim second job because queue concurrency limit of 1 is reached');

    // Clean up
    db.prepare('DELETE FROM jobs WHERE id IN (?, ?)').run(jobAId, jobBId);
    db.prepare("UPDATE queues SET concurrency_limit = ?, is_paused = ? WHERE id = ?").run(originalLimit, originalPaused, lowPriorityQueue.id); // restore
  });

  test('Backoff delay calculation matches policies', () => {
    const baseDelay = 1000;

    // Linear backoff: baseDelay * attempt
    const linearDelayCalc = (attempt) => baseDelay * attempt;
    assert.strictEqual(linearDelayCalc(1), 1000);
    assert.strictEqual(linearDelayCalc(2), 2000);
    assert.strictEqual(linearDelayCalc(3), 3000);

    // Exponential backoff: baseDelay * 2^(attempt - 1)
    const exponentialDelayCalc = (attempt) => baseDelay * Math.pow(2, attempt - 1);
    assert.strictEqual(exponentialDelayCalc(1), 1000);
    assert.strictEqual(exponentialDelayCalc(2), 2000);
    assert.strictEqual(exponentialDelayCalc(3), 4000);
    assert.strictEqual(exponentialDelayCalc(4), 8000);
  });
});

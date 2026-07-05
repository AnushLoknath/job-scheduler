const Database = require('better-sqlite3');
const path = require('path');
const bcrypt = require('bcryptjs');
const { v4: uuidv4 } = require('uuid');

const dbPath = path.join(__dirname, '../database.sqlite');
const db = new Database(dbPath);

// Enable Foreign Keys and WAL journal mode for concurrency
db.pragma('foreign_keys = ON');
db.pragma('journal_mode = WAL');

// Create Schema
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS organizations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS organization_members (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(organization_id) REFERENCES organizations(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(organization_id) REFERENCES organizations(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS retry_policies (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    strategy TEXT NOT NULL, -- 'fixed', 'linear', 'exponential'
    delay_ms INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS queues (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 10,
    concurrency_limit INTEGER NOT NULL DEFAULT 5,
    retry_policy_id TEXT,
    is_paused INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY(retry_policy_id) REFERENCES retry_policies(id) ON DELETE SET NULL,
    UNIQUE(project_id, name)
  );

  CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    total_jobs INTEGER NOT NULL DEFAULT 0,
    completed_jobs INTEGER NOT NULL DEFAULT 0,
    failed_jobs INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active', -- 'active', 'offline'
    concurrency_limit INTEGER NOT NULL,
    last_heartbeat INTEGER NOT NULL,
    started_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    queue_id TEXT NOT NULL,
    batch_id TEXT,
    retry_policy_id TEXT,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued', -- 'queued', 'scheduled', 'claimed', 'running', 'completed', 'failed', 'cancelled'
    payload TEXT NOT NULL, -- JSON string
    run_at INTEGER NOT NULL, -- Unix timestamp in ms
    max_retries INTEGER NOT NULL DEFAULT 3,
    retry_strategy TEXT NOT NULL DEFAULT 'exponential', -- 'fixed', 'linear', 'exponential'
    retry_delay INTEGER NOT NULL DEFAULT 1000, -- in ms
    attempt_number INTEGER NOT NULL DEFAULT 0,
    cron_expression TEXT,
    worker_id TEXT,
    claimed_at INTEGER,
    started_at INTEGER,
    completed_at INTEGER,
    error_message TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE CASCADE,
    FOREIGN KEY(batch_id) REFERENCES batches(id) ON DELETE SET NULL,
    FOREIGN KEY(worker_id) REFERENCES workers(id) ON DELETE SET NULL,
    FOREIGN KEY(retry_policy_id) REFERENCES retry_policies(id) ON DELETE SET NULL
  );

  CREATE TABLE IF NOT EXISTS job_executions (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL, -- 'completed', 'failed'
    error_message TEXT,
    started_at INTEGER NOT NULL,
    finished_at INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS job_dependencies (
    id TEXT PRIMARY KEY,
    parent_job_id TEXT NOT NULL,
    child_job_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(parent_job_id) REFERENCES jobs(id) ON DELETE CASCADE,
    FOREIGN KEY(child_job_id) REFERENCES jobs(id) ON DELETE CASCADE,
    UNIQUE(parent_job_id, child_job_id)
  );

  CREATE TABLE IF NOT EXISTS job_logs (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    level TEXT NOT NULL, -- 'info', 'warn', 'error'
    message TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id TEXT PRIMARY KEY,
    job_id TEXT UNIQUE NOT NULL,
    queue_id TEXT NOT NULL,
    name TEXT NOT NULL,
    payload TEXT NOT NULL,
    error_message TEXT,
    failed_at INTEGER NOT NULL,
    original_created_at INTEGER NOT NULL,
    attempt_number INTEGER NOT NULL,
    FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS worker_heartbeats (
    id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    active_jobs INTEGER NOT NULL,
    concurrency_limit INTEGER NOT NULL,
    FOREIGN KEY(worker_id) REFERENCES workers(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id TEXT PRIMARY KEY,
    job_id TEXT UNIQUE NOT NULL,
    run_at INTEGER NOT NULL,
    cron_expression TEXT,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS distributed_locks (
    key TEXT PRIMARY KEY,
    holder TEXT NOT NULL,
    expires_at INTEGER NOT NULL
  );
`);

// Create Optimization Indexes
db.exec(`
  CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at ON jobs(status, run_at);
  CREATE INDEX IF NOT EXISTS idx_jobs_queue_id ON jobs(queue_id);
  CREATE INDEX IF NOT EXISTS idx_jobs_batch_id ON jobs(batch_id);
  CREATE INDEX IF NOT EXISTS idx_job_executions_job_id ON job_executions(job_id);
  CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);
  CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status, last_heartbeat);
  CREATE INDEX IF NOT EXISTS idx_job_dependencies_child ON job_dependencies(child_job_id);
  CREATE INDEX IF NOT EXISTS idx_job_dependencies_parent ON job_dependencies(parent_job_id);
  CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_run_at ON scheduled_jobs(run_at);
  CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_timestamp ON worker_heartbeats(timestamp);
`);

// Database Migrations (safely alter tables to add columns if they do not exist in existing sqlite files)
try {
  db.exec("ALTER TABLE queues ADD COLUMN retry_policy_id TEXT REFERENCES retry_policies(id) ON DELETE SET NULL;");
} catch (e) {
  // Column already exists or table doesn't exist
}

try {
  db.exec("ALTER TABLE jobs ADD COLUMN retry_policy_id TEXT REFERENCES retry_policies(id) ON DELETE SET NULL;");
} catch (e) {
  // Column already exists or table doesn't exist
}

// Seed default data
function seed() {
  const userCount = db.prepare('SELECT COUNT(*) as count FROM users').get().count;
  if (userCount === 0) {
    console.log('Seeding default database records...');
    
    // Default User: admin / admin123
    const userId = uuidv4();
    const passwordHash = bcrypt.hashSync('admin123', 10);
    db.prepare(`
      INSERT INTO users (id, username, email, password_hash, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(userId, 'admin', 'admin@codity.io', passwordHash, Date.now(), Date.now());

    // Default Org
    const orgId = uuidv4();
    db.prepare(`
      INSERT INTO organizations (id, name, created_at, updated_at)
      VALUES (?, ?, ?, ?)
    `).run(orgId, 'Acme Corp', Date.now(), Date.now());

    // Link user to org as owner
    db.prepare(`
      INSERT INTO organization_members (id, user_id, organization_id, role, created_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(uuidv4(), userId, orgId, 'owner', Date.now());

    // Default Project
    const projectId = uuidv4();
    db.prepare(`
      INSERT INTO projects (id, organization_id, name, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(projectId, orgId, 'Main Engineering', Date.now(), Date.now());

    // Seed default retry policies
    const policies = [
      { id: uuidv4(), name: 'Standard Exponential Backoff', strategy: 'exponential', delay_ms: 1000, max_retries: 3 },
      { id: uuidv4(), name: 'Aggressive Linear Backoff', strategy: 'linear', delay_ms: 2000, max_retries: 5 },
      { id: uuidv4(), name: 'Quick Fixed Backoff', strategy: 'fixed', delay_ms: 500, max_retries: 4 }
    ];

    const insertPolicy = db.prepare(`
      INSERT INTO retry_policies (id, project_id, name, strategy, delay_ms, max_retries, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    for (const p of policies) {
      insertPolicy.run(p.id, projectId, p.name, p.strategy, p.delay_ms, p.max_retries, Date.now(), Date.now());
    }

    // Default Queues (link default queue to standard policy)
    const queues = [
      { id: uuidv4(), name: 'high-priority', priority: 50, concurrency_limit: 10, retry_policy_id: null },
      { id: uuidv4(), name: 'default', priority: 10, concurrency_limit: 5, retry_policy_id: policies[0].id },
      { id: uuidv4(), name: 'low-priority', priority: 1, concurrency_limit: 2, retry_policy_id: null }
    ];

    const insertQueue = db.prepare(`
      INSERT INTO queues (id, project_id, name, priority, concurrency_limit, retry_policy_id, is_paused, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
    `);

    for (const q of queues) {
      insertQueue.run(q.id, projectId, q.name, q.priority, q.concurrency_limit, q.retry_policy_id, Date.now(), Date.now());
    }

    console.log('Database seeded successfully.');
  }
}

seed();

module.exports = db;

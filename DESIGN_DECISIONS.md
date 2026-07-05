# Technical Design Decisions

This document details the engineering trade-offs, concurrency models, and reliability design choices implemented in the distributed job scheduling platform.

---

## 1. Relational SQLite in WAL (Write-Ahead Log) Mode

While distributed schedulers often use Redis (for speed) or PostgreSQL (for transaction strength), we chose **SQLite in WAL Mode** to satisfy server-side storage requirements while keeping the deployment environment zero-dependency and serverless.

### Why WAL Mode?
- **Concurrent Readers and Writers**: By default, SQLite locks the entire database file during writes, blocking readers. In WAL mode, writes are appended to a separate `.log-wal` file. Readers can query the database concurrently while a write transaction is in progress, drastically increasing system throughput.
- **Immediate Write Transactions**: We configure the driver with `busy_timeout = 5000` to automatically retry queries under contention. We start coordination locks with `BEGIN IMMEDIATE` transactions to prevent write collisions and lock-acquisition deadlocks.

---

## 2. Atomic Claiming Engine

Preventing double-claiming of jobs by parallel workers is a core requirement of any distributed scheduler. We implement this through an **Atomic claiming transaction block** inside SQLite.

### Execution steps:
1. **Transaction Hook**: The worker begins an `IMMEDIATE` transaction, gaining an exclusive write lock.
2. **Find Eligible Job**: The database finds the highest priority job matching the criteria:
   - Queue is active (`is_paused = 0`).
   - Active jobs on the queue are below its `concurrency_limit`.
   - Job is ready to execute (`status = 'queued'` or `status = 'scheduled'` and `run_at <= now`).
   - Parent dependencies are fully completed.
3. **Atomic Claim**: If found, the job is atomically updated:
   - Status transitions from `queued`/`scheduled` to `claimed`.
   - Worker ID, claimed timestamp, and updated timestamp are recorded.
   - The job is deleted from `scheduled_jobs` to clean up the schedule registry.
4. **Commit**: The transaction commits, releasing the write lock. Because of the exclusive transaction lock, parallel workers polling the same query will never see or claim the same job.

---

## 3. Distributed Locking

To coordinate tasks and prevent duplicate runs on external resources (such as sending duplicate payment requests or email dispatches for the same resource key), we exposed a lease-based **Distributed Locking interface** (`backend/lock.js`).

### Coordination properties:
- **Atomic Lease Acquisition**: Implemented using an SQLite transaction check. If a lock key is present and has not expired (`expires_at > now`), acquisition fails. If it is expired or missing, it inserts/overwrites the lock lease.
- **Automatic Lease Expiry (TTL)**: Unlike infinite locks, every lock has a lease duration (`ttlMs`). If a worker crashes while holding a lock, the lock automatically expires after its TTL, allowing other nodes to safely acquire it.
- **Owned Release Check**: A lock can only be deleted if the worker ID of the releaser matches the holder stored in the database. This prevents a slow worker from accidentally releasing a lock that has already been re-acquired by another node.

---

## 4. Graceful Shutdown Protocol

To avoid orphaned jobs and database pollution when a worker node terminates (e.g., due to container deployment or autoscaling), workers intercept termination signals (`SIGINT`, `SIGTERM`):

1. **Stop Polling**: The worker immediately stops querying the database for new jobs.
2. **Await In-Flight Executions**: The worker waits for active jobs to complete or reach a safe threshold (up to a timeout grace period).
3. **Graceful Status Reversion**: Any claimed or running jobs that cannot complete within the grace period are reverted in the database back to `queued`/`scheduled` status so they can be claimed by active nodes immediately.
4. **Offline Heartbeat**: The worker sets its status to `offline` in the worker registry and exits cleanly.

---

## 5. Performance Indexes & Database Tuning

To maintain sub-millisecond query performance as database logs grow, we added optimization indexes:

- `idx_jobs_status_run_at`: Accelerates worker claim scans.
- `idx_scheduled_jobs_run_at`: Accelerates scheduler triggers.
- `idx_worker_heartbeats_timestamp`: Accelerates telemetry database pruning.
- Foreign Key indexes on `queue_id`, `batch_id`, and `worker_id` prevent table scans during cascade deletes.

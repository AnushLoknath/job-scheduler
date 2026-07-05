# DISTRIBUTED JOB SCHEDULER
## Technical Design & Architecture Documentation

**Author**: ANUSH LOKNATH  
**Registration No**: RA2311056010185  
**Email**: [al0861@srmist.edu.in](mailto:al0861@srmist.edu.in)  
**Repository**: [github.com/AnushLoknath/job-scheduler](https://github.com/AnushLoknath/job-scheduler)  
**Stack**: Node.js · Express · SQLite (WAL) · React · SSE  
**Date**: July 2026  

---

## 1. Project Overview

The **Distributed Job Scheduler** is a production-grade, multi-tenant backend platform designed for scheduling, queuing, executing, and observing background jobs at scale. Built with a lightweight yet high-concurrency architecture, it enables organizations to submit immediate, delayed, and recurring (cron-based) jobs, distributing that workload dynamically across a cluster of self-registering worker processes. The status of queues, heartbeats, and worker loads is observable in real-time through an interactive front-end dashboard.

Rather than relying on resource-intensive message broker daemons, the system is engineered around a **Single-Source-of-Truth Database Architecture**. It utilizes **SQLite in Write-Ahead Log (WAL) mode** to maintain high-performance transactional isolation, coordinating task claiming and locking operations across multiple concurrent background worker nodes natively.

### 1.1 Core Capabilities
- **Multi-Tenant Hierarchy**: Structures workflows logically under Organizations and Projects, protected by a comprehensive Role-Based Access Control (RBAC) layer supporting Owner, Admin, and Member permissions.
- **Queue Management**: Offers fine-grained controls with configurable execution priority weights, queue-specific concurrency limits, and dynamic pause/resume toggles.
- **Robust Job Lifecycle**: Explicit tracking of job states: `queued`, `scheduled`, `claimed`, `running`, `completed`, `failed`, and `cancelled`.
- **Flexible Scheduling Profiles**: Accommodates immediate triggers, epoch-timestamped delayed executions, and cron-expression calendars.
- **Decoupled Retry Policies**: Reusable retry strategies (fixed, linear, and exponential backoff configurations) linked directly to queues or overridden at the individual job level.
- **Dead-Letter Queue (DLQ)**: Automatically isolates jobs that exhaust their maximum attempts, capturing failure reasons and payloads to facilitate manual analysis or replay operations.
- **Distributed Worker Pool**: Autonomous background threads that periodically send heartbeat indicators (active job count, concurrency capacity) and atomically claim jobs to guarantee at-most-once/at-least-once task distribution.
- **Real-Time Observation Stream**: Implements Server-Sent Events (SSE) to broadcast system state transitions and worker heartbeat statistics to the React dashboard instantly.

---

## 2. System Architecture

The system employs a decoupled, three-tier architecture: the **Presentation Layer** (Vite React), the **API & Processing Layer** (Express Server and Worker Pool), and the **Storage Layer** (SQLite WAL).

```mermaid
graph TD
    subgraph Frontend Layer
        Dash[Vite React Dashboard]
    end

    subgraph API & Runtime Layer
        Server[Express REST Server]
        SSE[SSE Live Events Stream]
    end

    subgraph Background Execution Layer
        Worker1[Worker Node A]
        Worker2[Worker Node B]
    end

    subgraph Storage Layer
        DB[(SQLite WAL Database)]
    end

    Dash -->|REST APIs| Server
    Dash <==|Real-time Telemetry| SSE
    Server -->|Read / Write State| DB
    Worker1 -->|Atomic Claims / Telemetry| DB
    Worker2 -->|Atomic Claims / Telemetry| DB
    Server -.->|Broadcast Status| SSE
```

### 2.1 Components
- **Vite React Dashboard**: A responsive UI rendering job metrics (active, queued, failed, completed counts), live worker heartbeats, and paginated logs. Connects to the Express SSE endpoint for immediate reactive updates without polling.
- **Express API Server**: Serving client API requests. Validates parameters, executes database state mutations (submitting jobs, updating projects/queues/policies), manages authentication tokens via JWT, and exposes an HTTP SSE connection to push updates down.
- **Background Worker Pool**: Multi-process instance nodes running continuous claim-execute loops. Nodes execute jobs inside thread pools limiting local thread consumption while periodically posting diagnostic data (concurrency, load) back into the central database.

### 2.2 Data & Dispatch Flow
When a client posts a job, the Express API stores it in the SQLite database in a `queued` state. Concurrently, active background worker threads query the database at regular intervals. Utilizing SQL Transactions, a worker atomically queries for eligible pending jobs, verifies queue-level concurrency limits, ensures no sibling or parent dependencies are blocked, and marks the selected job as `claimed`. Once claimed, the worker changes the status to `running`, executes the target business logic, logs attempts, and transitions the state to either `completed` or `failed`. In case of failure, retry offsets are calculated based on the active Retry Policy, and the job is rescheduled.

---

## 3. Database Design — Schema Layout

The system relies on a fully normalized SQLite schema structure. Concurrency and consistency are guaranteed through foreign keys and WAL mode.

### Core Database Tables

| Table Name | Primary Key | Foreign Keys | Core Fields & Schema Purpose |
| :--- | :--- | :--- | :--- |
| `users` | `id` | - | Stores user details: `username`, `email`, and bcrypt `password_hash`. |
| `organizations` | `id` | - | Represents the highest level tenant bucket. |
| `organization_members` | `id` | `user_id, organization_id` | Maps users to organizations with a security `role` (Owner, Member). |
| `projects` | `id` | `organization_id` | Namespaced isolation boundary within an organization. |
| `retry_policies` | `id` | `project_id` | Custom configs for backoffs: `strategy` (fixed, linear, exponential), `delay_ms`, `max_retries`. |
| `queues` | `id` | `project_id, retry_policy_id` | Holds queue limits: `priority`, `concurrency_limit`, and pause flag `is_paused`. |
| `batches` | `id` | `project_id` | Aggregates multiple jobs: track `total_jobs`, `completed_jobs`, `failed_jobs`. |
| `workers` | `id` | - | Tracks worker status: `hostname`, `status` (active, offline), `concurrency_limit`, `last_heartbeat`. |
| `jobs` | `id` | `queue_id, batch_id, retry_policy_id, worker_id` | The core task record: `status`, `payload` (JSON text), execution timing `run_at`, cron expressions. |
| `job_executions` | `id` | `job_id` | Maintains execution run history: `attempt_number`, `status`, `error_message`, `duration_ms`. |
| `job_dependencies` | `id` | `parent_job_id, child_job_id` | Models Directed Acyclic Graphs (DAG) for chaining parent and child tasks. |
| `job_logs` | `id` | `job_id` | Detailed logging strings: `level` (info, warn, error), `message`, and `timestamp`. |
| `dead_letter_queue` | `id` | `queue_id` | Isolates failed jobs exceeding maximum retries: records original payloads and errors. |
| `worker_heartbeats` | `id` | `worker_id` | Saves time-series history of active jobs and loads for dashboard graph generation. |

---

## 4. API Documentation

All endpoints are exposed under the base path `/api`. Authenticated endpoints require a Bearer token in the `Authorization` header (`Authorization: Bearer <JWT_TOKEN>`).

### 4.1 Authentication Endpoints
- **`POST /api/auth/register`**: Registers a new user and sets up baseline user schemas.
- **`POST /api/auth/login`**: Authenticates credentials; returns a JSON Web Token (JWT).
- **`GET /api/auth/me`**: Fetches the profile details of the current authenticated user.

### 4.2 Projects & Tenant Configuration
- **`GET /api/projects`**: Lists all projects in the user's organization scope.
- **`POST /api/projects`**: Creates a new project namespace inside the organization.

### 4.3 Queue Management
- **`GET /api/queues`**: Lists all active task queues.
- **`POST /api/queues`**: Defines a new queue with explicit concurrency limits and priorities.
- **`PATCH /api/queues/:id`**: Updates queue configuration attributes (e.g., toggles `is_paused`).

### 4.4 Jobs & Scheduling
- **`GET /api/jobs`**: Queries list of jobs with filters (queue, status, project).
- **`GET /api/jobs/:id`**: Returns details, historical executions, and log entries of a job.
- **`POST /api/jobs`** *(Rate Limited)*: Enqueues a new job immediately or schedules it for the future.
- **`POST /api/jobs/batch`** *(Rate Limited)*: Enqueues a collection of jobs grouped under a single batch ID.
- **`POST /api/jobs/:id/retry`**: Manually forces execution retry for a failed or DLQ job.
- **`POST /api/jobs/:id/cancel`**: Cancels a pending queued or scheduled recurring execution.
- **`GET /api/jobs/:id/ai-summary`**: Returns an LLM summary of job failure logs and debugging info.

### 4.5 Retry Policy & Real-Time SSE Streams
- **`GET /api/retry-policies`**: Retrieves defined retry policies for the project.
- **`POST /api/retry-policies`**: Creates a new retry configuration (linear, fixed, exponential).
- **`GET /api/metrics`**: Calculates real-time throughput metrics, success percentages, etc.
- **`GET /api/live`**: **SSE stream**: Establishes persistent live event push connection.

---

## 5. Design Decisions & Trade-offs

### 5.1 SQLite WAL Mode vs. Dedicated Message Brokers
- **Decision**: Using SQLite in Write-Ahead Log (WAL) mode as the database engine instead of running PostgreSQL + Redis.
- **Rationale & Trade-off**: Setting up a heavy Redis/PostgreSQL ecosystem increases operations complexity, deployment overhead, and infrastructure costs. By enabling `journal_mode = WAL` on SQLite via the `better-sqlite3` package, the system unlocks concurrent readers alongside a writer, executing thousands of transactions per second on single-file storage. This guarantees relational foreign-key integrity natively. The trade-off is that SQLite runs in-process, meaning the database must reside on a shared file system (like an SSD volume) if multiple server/worker nodes run on different virtual machines. For low-to-medium deployments, it provides exceptional simplicity with zero setup complexity.

### 5.2 Server-Sent Events (SSE) vs. WebSockets
- **Decision**: Using HTTP-based Server-Sent Events (SSE) for the live dashboard event stream instead of bidirectionally-open WebSockets.
- **Rationale & Trade-off**: WebSockets require full-duplex communication and specialized server-side connection handling, bypassing normal HTTP headers, proxies, and security controls. Because the dashboard client only needs to receive server updates (unidirectional stream) rather than pushing messages back, SSE is a much simpler, robust, and native browser choice. It uses standard HTTP connections, supports automatic reconnection out-of-the-box, and traverses load balancers and firewalls easily.

### 5.3 Atomic Database-Level Claims vs. Central Dispatcher
- **Decision**: Implementing a pull-based worker polling loop backed by an atomic SQLite transaction instead of a push-based central scheduling thread.
- **Rationale**: In a central push model, if the main dispatcher server goes down, the entire execution pipeline halts. By implementing a pull-based claim query utilizing SQLite transactions, we ensure that multiple worker processes can query the database concurrently. SQLite locks the file during the write step, preventing race conditions (no job can ever be claimed by two workers). If a worker crashes mid-execution, other workers detect the lack of heartbeats and recover the job, making the system highly resilient and stateless.

### 5.4 Decoupled Retry Policies
- **Decision**: Designing retry logic as individual entities (`retry_policies` table) referenced by queues, rather than hardcoding attempt counters inside the jobs code.
- **Rationale**: Decoupling retry behaviors allows operators to modify retry configurations globally. For example, updating the delay of the "Linear Backoff" policy instantly changes the behavior of all queues referencing it, without requiring the modification of individual job payloads. The trade-off is one additional table join operation during the job polling step, which has been optimized through indexed lookups.

### 5.5 Directed Acyclic Graph (DAG) Sibling Dependencies
- **Decision**: Designing a `job_dependencies` structure to enforce parent-child relations natively.
- **Rationale**: Many batch-processing tasks require steps to run in sequence (e.g., step B starts only after step A completes). By validating dependencies inside the claiming query, we enforce job dependency logic cleanly at the database level. Child jobs remain in the queue but will not be picked up by any worker node until all parent jobs have transitioned to a `completed` status.

### 5.6 Dead-Letter Queue (DLQ) vs. Silent Failure
- **Decision**: Shifting failing jobs to a `dead_letter_queue` table rather than discarding them or attempting retries indefinitely.
- **Rationale**: When a worker script fails repeatedly (due to code syntax, network dropouts, or external API timeouts), retrying indefinitely consumes worker threads and starves other queues. Discarding them silently leads to untraceable data loss. Moving them to the DLQ isolates the failure, records the exact error details, and lets developers requeue the job manually once the underlying code is fixed.

---

## 6. Testing Strategy & Source Code Setup

### 6.1 Testing Strategy
- **Unit Testing**: Focuses on backoff arithmetic calculations (fixed, linear, and exponential offsets), parsing cron strings, and verifying database connection settings.
- **Integration Testing**: Walks jobs through the entire lifecycle (from `POST /api/jobs` -> worker claim -> status update -> logging details).
- **Concurrency Validation**: Simulates multiple worker clients querying the database at the exact same millisecond, verifying that SQLite's lock transaction prevents duplicate claiming.
- **Dependency Validation**: Asserts that child jobs are blocked until all parents complete, and immediately schedules the child once parent requirements are satisfied.

### 7.2 Clone and Configure
```bash
git clone https://github.com/AnushLoknath/job-scheduler.git
cd job-scheduler
```

### 7.3 Dependencies Installation
To compile the project dependencies recursively, execute the root package.json command:
```bash
npm run install-all
```

### 7.4 Running the Services
Start all services together concurrently:
```bash
npm run dev
```

- **Express Server API**: [http://localhost:3001](http://localhost:3001)
- **React Live Dashboard**: [http://localhost:5173](http://localhost:5173)

### 7.5 Executing the Test Suite
```bash
npm run test
```

---

## 8. Conclusion

The **Distributed Job Scheduler** delivers a highly optimized, light-footprint scheduling platform. By moving key execution validation (concurrency checks, dependency constraints, and atomic claiming) directly into SQLite SQL transactions, the project achieves a high degree of horizontal worker scalability with zero external broker setup overhead. It coordinates data states reliably and observably across the entire distributed system.

### Architecture Performance Characteristics

| Metric Dimension | Architectural Guarantee | Implementation Strategy |
| :--- | :--- | :--- |
| **Reliability** | At-least-once execution and retry guarantees. | SQLite transaction checks, decoupled retry backoffs, and Dead-Letter Queue isolation. |
| **Scalability** | Horizontally scalable background worker nodes. | Stateless worker structure. Autoregistration and heartbeats written to database. |
| **Observability** | Real-time tracking of queue depth and logs. | Server-Sent Events (SSE) broadcasting states dynamically to React. |
| **Safety** | Rate limits & concurrency protections. | Token-bucket Express middleware and queue concurrency limits. |

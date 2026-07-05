# Codity.io Distributed Job Scheduler

A robust, production-grade distributed job scheduling platform featuring a real-time monitoring dashboard, atomic worker claims, customizable retry policies, and multi-tenant isolation. 

The platform leverages a **Single-Source-of-Truth Database Architecture** using SQLite in WAL (Write-Ahead Log) mode to manage states reliably across concurrent worker and API processes.

---

## 🚀 Key Features

- **Distributed Worker Engine**: Atomic database-level claims ensure that multiple background worker daemons can run concurrently without executing any job twice.
- **Flexible Scheduling**: Supports immediate, delayed, recurring (scheduled), and batch job executions.
- **Custom Retry Policies**: Define custom retry backoff strategies (`fixed`, `linear`, `exponential`) with backoff delays and maximum retry limits.
- **Real-Time Telemetry Dashboard**: Built with Vite and React, displaying live-updating charts, active worker node metrics, and queue capacity logs using Server-Sent Events (SSE).
- **Concurrency & Rate Limiting**: Built-in rate limiting (token-bucket rate limiter) on the Express API server and queue-level concurrency limits.
- **Multi-Tenant Architecture**: Organizes queues, retry policies, and jobs under projects, owned by organizations, with Role-Based Access Control (RBAC).

---

## 🛠 Tech Stack

- **Backend API Layer**: Node.js, Express, Token-Bucket Rate Limiter
- **Background Execution Layer**: Background Worker Daemon (infinite claim-execute loop)
- **Frontend Layer**: React, Vite, Server-Sent Events (SSE)
- **Database / Storage**: SQLite (via `better-sqlite3` driver in WAL mode)
- **Testing Suite**: Node.js Native Test Runner

---

## 📐 Architecture Overview

```mermaid
graph TD
    subgraph Frontend Layer
        Dash[Vite React Dashboard]
    end

    subgraph Backend API Layer
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

For more deep-dive details on the normalized entity-relationship layout and distributed locking mechanism, see the [ARCHITECTURE.md](file:///c:/Users/anush/OneDrive/Desktop/codity/ARCHITECTURE.md) guide.

---

## ⚙️ Quick Start

For a detailed walkthrough of pre-requisites and individual service execution, see the [SETUP.md](file:///c:/Users/anush/OneDrive/Desktop/codity/SETUP.md) guide.

### 1. Install Dependencies
Install all parent, backend, worker, and frontend dependencies recursively:
```bash
npm run install-all
```

### 2. Run the Platform Concurrently
Start the Express API server, the background worker node, and the React dashboard simultaneously:
```bash
npm run dev
```

- **Vite React Dashboard**: [http://localhost:5173](http://localhost:5173)
- **Express REST Server**: [http://localhost:3001](http://localhost:3001)

### 3. Run the Test Suite
Run the automated test suite verifying backoff math, rate limiters, distributed lease locks, and concurrency limits:
```bash
npm run test
```

---

## 📁 Repository Structure

```text
├── backend/            # Express REST API & Database Models
│   ├── routes/         # Auth, jobs, metrics, queues, retryPolicies endpoints
│   ├── middleware/     # Rate limiting, Auth middleware
│   └── test/           # Unit and integration test suite
├── worker/             # Background Worker Daemon polling loop
├── frontend/           # Vite React Dashboard SPA
├── ARCHITECTURE.md     # In-depth architectural & ER diagrams
├── SETUP.md            # Detailed installation and configuration guide
└── README.md           # This file
```

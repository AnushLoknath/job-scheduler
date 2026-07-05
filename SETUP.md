# Codity.io Distributed Job Scheduler Setup Guide

This guide details the steps required to install dependencies, run tests, and spin up the complete distributed scheduling workspace (Express REST Server, background Worker threads, and Vite React Dashboard).

---

## 1. Prerequisites

- **Node.js** (v18.x or higher recommended)
- **npm** (v9.x or higher)
- **SQLite3** (No installation needed; driver and database engine compile automatically in-process via `better-sqlite3`).

---

## 2. Dependencies Installation

To install all parent, backend, worker, and frontend package dependencies in a single command, execute:

```bash
npm run install-all
```

This script triggers recursive dependency compilation for:
- Root workspace controller
- Express API server (`backend/`)
- Background worker execution daemon (`worker/`)
- Vite React live telemetry dashboard (`frontend/`)

---

## 3. Database Initialization & Seeding

The SQLite database file `database.sqlite` is automatically initialized in the project root directory on the first backend server or worker daemon startup.

- The system runs migrations dynamically to ensure all required tables exist.
- Standard baseline records (an admin developer user, Acme Corp organization, Main Engineering project, default queues, and Exponential/Linear/Fixed retry policies) are seeded automatically if the database is empty.

---

## 4. Launching the Services

You can spin up individual components or run the entire ecosystem concurrently.

### A. Run Concurrently (Recommended)

To run the REST server, background worker process, and Vite client together in one terminal, run:

```bash
npm run dev
```

The system output displays console logs from all three components in parallel:
- **Express API Server**: [http://localhost:3001](http://localhost:3001)
- **React Dashboard**: [http://localhost:5173](http://localhost:5173)
- **Background Worker**: Active polling thread claiming jobs

### B. Run Services Separately

If you prefer to run services in dedicated terminal tabs:

1. **REST API Server**:
   ```bash
   npm run dev:backend
   ```
2. **Background Worker Node**:
   ```bash
   npm run dev:worker
   ```
3. **Vite React Dashboard Client**:
   ```bash
   npm run dev:frontend
   ```

---

## 5. Automated Tests Suite

To execute the full testing suite verifying queue claiming logic, concurrency limits, backoff math, rate limit middleware, and distributed lease locking, run:

```bash
npm run test
```

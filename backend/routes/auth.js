const express = require('express');
const router = express.Router();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const db = require('../db');
const { JWT_SECRET, authenticateToken } = require('../middleware/auth');

// Register Endpoint
router.post('/register', (req, res) => {
  const { username, email, password } = req.body;
  if (!username || !email || !password) {
    return res.status(400).json({ error: 'Username, email, and password are required' });
  }

  try {
    const passwordHash = bcrypt.hashSync(password, 10);
    const userId = uuidv4();
    const orgId = uuidv4();
    const projectId = uuidv4();

    // Use a database transaction to ensure registration is atomic
    const registerTransaction = db.transaction(() => {
      // 1. Insert User
      db.prepare(`
        INSERT INTO users (id, username, email, password_hash, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
      `).run(userId, username, email, passwordHash, Date.now(), Date.now());

      // 2. Insert Organization
      db.prepare(`
        INSERT INTO organizations (id, name, created_at, updated_at)
        VALUES (?, ?, ?, ?)
      `).run(orgId, `${username}'s Org`, Date.now(), Date.now());

      // 3. Link User to Org
      db.prepare(`
        INSERT INTO organization_members (id, user_id, organization_id, role, created_at)
        VALUES (?, ?, ?, ?, ?)
      `).run(uuidv4(), userId, orgId, 'owner', Date.now());

      // 4. Create Project
      db.prepare(`
        INSERT INTO projects (id, organization_id, name, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
      `).run(projectId, orgId, 'Primary Project', Date.now(), Date.now());

      // 5. Seed default queues for new project
      const queues = [
        { id: uuidv4(), name: 'high-priority', priority: 50, concurrency_limit: 10 },
        { id: uuidv4(), name: 'default', priority: 10, concurrency_limit: 5 },
        { id: uuidv4(), name: 'low-priority', priority: 1, concurrency_limit: 2 }
      ];

      const insertQueue = db.prepare(`
        INSERT INTO queues (id, project_id, name, priority, concurrency_limit, is_paused, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 0, ?, ?)
      `);

      for (const q of queues) {
        insertQueue.run(q.id, projectId, q.name, q.priority, q.concurrency_limit, Date.now(), Date.now());
      }
    });

    registerTransaction();

    const token = jwt.sign({ id: userId, username, email }, JWT_SECRET, { expiresIn: '24h' });
    return res.status(201).json({ token, user: { id: userId, username, email } });
  } catch (error) {
    if (error.message.includes('UNIQUE constraint failed')) {
      return res.status(400).json({ error: 'Username or email already exists' });
    }
    console.error('Registration error:', error);
    return res.status(500).json({ error: 'Failed to register user' });
  }
});

// Login Endpoint
router.post('/login', (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ error: 'Username and password are required' });
  }

  try {
    const user = db.prepare('SELECT * FROM users WHERE username = ? OR email = ?').get(username, username);
    if (!user || !bcrypt.compareSync(password, user.password_hash)) {
      return res.status(401).json({ error: 'Invalid username or password' });
    }

    const token = jwt.sign({ id: user.id, username: user.username, email: user.email }, JWT_SECRET, { expiresIn: '24h' });
    return res.json({ token, user: { id: user.id, username: user.username, email: user.email } });
  } catch (error) {
    console.error('Login error:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Get current user profile & tenant structure
router.get('/me', authenticateToken, (req, res) => {
  try {
    const user = db.prepare('SELECT id, username, email FROM users WHERE id = ?').get(req.user.id);
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Find organizations and projects this user belongs to
    const orgs = db.prepare(`
      SELECT o.id, o.name, om.role
      FROM organizations o
      JOIN organization_members om ON o.id = om.organization_id
      WHERE om.user_id = ?
    `).all(req.user.id);

    const projects = [];
    for (const org of orgs) {
      const orgProjects = db.prepare('SELECT id, name FROM projects WHERE organization_id = ?').all(org.id);
      projects.push(...orgProjects.map(p => ({ ...p, organization_id: org.id, org_name: org.name })));
    }

    return res.json({
      user,
      organizations: orgs,
      projects
    });
  } catch (error) {
    console.error('Fetch profile error:', error);
    return res.status(500).json({ error: 'Failed to fetch profile' });
  }
});

module.exports = router;

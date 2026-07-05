const express = require('express');
const router = express.Router();
const { v4: uuidv4 } = require('uuid');
const db = require('../db');
const { authenticateToken } = require('../middleware/auth');

// Get all projects the user is authorized to see
router.get('/', authenticateToken, (req, res) => {
  try {
    const projects = db.prepare(`
      SELECT p.*, o.name as organization_name
      FROM projects p
      JOIN organizations o ON p.organization_id = o.id
      JOIN organization_members om ON o.id = om.organization_id
      WHERE om.user_id = ?
    `).all(req.user.id);
    
    return res.json(projects);
  } catch (error) {
    console.error('Fetch projects error:', error);
    return res.status(500).json({ error: 'Failed to fetch projects' });
  }
});

// Create a project in a specified organization
router.post('/', authenticateToken, (req, res) => {
  const { name, organization_id } = req.body;
  if (!name || !organization_id) {
    return res.status(400).json({ error: 'Name and organization_id are required' });
  }

  try {
    // Verify membership in the organization
    const member = db.prepare(`
      SELECT role FROM organization_members
      WHERE user_id = ? AND organization_id = ?
    `).get(req.user.id, organization_id);

    if (!member) {
      return res.status(403).json({ error: 'You do not have access to this organization' });
    }

    const projectId = uuidv4();
    db.prepare(`
      INSERT INTO projects (id, organization_id, name, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(projectId, organization_id, name, Date.now(), Date.now());

    // Create default queues automatically for this project
    const defaultQueues = [
      { id: uuidv4(), name: 'high-priority', priority: 50, concurrency_limit: 10 },
      { id: uuidv4(), name: 'default', priority: 10, concurrency_limit: 5 },
      { id: uuidv4(), name: 'low-priority', priority: 1, concurrency_limit: 2 }
    ];

    const insertQueue = db.prepare(`
      INSERT INTO queues (id, project_id, name, priority, concurrency_limit, is_paused, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, 0, ?, ?)
    `);

    for (const q of defaultQueues) {
      insertQueue.run(q.id, projectId, q.name, q.priority, q.concurrency_limit, Date.now(), Date.now());
    }

    const project = db.prepare('SELECT * FROM projects WHERE id = ?').get(projectId);
    return res.status(201).json(project);
  } catch (error) {
    console.error('Create project error:', error);
    return res.status(500).json({ error: 'Failed to create project' });
  }
});

module.exports = router;

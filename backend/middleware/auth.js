const jwt = require('jsonwebtoken');
const db = require('../db');

const JWT_SECRET = process.env.JWT_SECRET || 'codity-super-secret-key-123456';

function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1]; // Bearer <token>

  if (!token) {
    return res.status(401).json({ error: 'Access token required' });
  }

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid or expired token' });
    }
    req.user = user;
    next();
  });
}

function requireRole(allowedRoles = ['owner']) {
  return (req, res, next) => {
    const projectId = req.body.project_id || req.query.project_id || req.params.project_id;
    const orgId = req.body.organization_id || req.query.organization_id || req.params.organization_id;
    const queueId = req.params.id; // for queue routes /api/queues/:id
    
    let resolvedOrgId = orgId;
    
    try {
      if (!resolvedOrgId && projectId) {
        const project = db.prepare('SELECT organization_id FROM projects WHERE id = ?').get(projectId);
        if (project) resolvedOrgId = project.organization_id;
      }
      
      if (!resolvedOrgId && queueId) {
        // Check if queueId corresponds to a queue
        const queue = db.prepare(`
          SELECT p.organization_id 
          FROM queues q
          JOIN projects p ON q.project_id = p.id
          WHERE q.id = ?
        `).get(queueId);
        if (queue) resolvedOrgId = queue.organization_id;
      }
      
      if (!resolvedOrgId) {
        // Fallback: If we cannot determine org from parameters, proceed to authorization
        return next();
      }
      
      const member = db.prepare(`
        SELECT role FROM organization_members
        WHERE user_id = ? AND organization_id = ?
      `).get(req.user.id, resolvedOrgId);
      
      if (!member) {
        return res.status(403).json({ error: 'Access denied: not an organization member' });
      }
      
      if (!allowedRoles.includes(member.role)) {
        return res.status(403).json({ error: 'Access denied: insufficient privileges' });
      }
      
      next();
    } catch (err) {
      console.error('RBAC validation error:', err);
      return res.status(500).json({ error: 'RBAC verification failure' });
    }
  };
}

module.exports = {
  authenticateToken,
  requireRole,
  JWT_SECRET
};

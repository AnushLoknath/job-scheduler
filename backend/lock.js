const db = require('./db');

/**
 * Atomically attempt to acquire a lease-based distributed lock.
 * @param {string} key Unique identifier for the lock key.
 * @param {string} holder The identifier of the worker attempting to lock.
 * @param {number} ttlMs Time-to-live duration in milliseconds.
 * @returns {boolean} True if lock was successfully acquired, false otherwise.
 */
function acquireLock(key, holder, ttlMs = 10000) {
  const now = Date.now();
  const expiresAt = now + ttlMs;

  return db.transaction(() => {
    const existing = db.prepare('SELECT * FROM distributed_locks WHERE key = ?').get(key);
    if (existing && existing.expires_at > now) {
      // Lock is active and held by another process
      return false;
    }
    // Lock is either unassigned or expired: acquire lease
    db.prepare(`
      INSERT OR REPLACE INTO distributed_locks (key, holder, expires_at)
      VALUES (?, ?, ?)
    `).run(key, holder, expiresAt);
    return true;
  })();
}

/**
 * Release a held distributed lock.
 * @param {string} key Unique identifier of the lock.
 * @param {string} holder The identifier of the worker releasing the lock.
 * @returns {boolean} True if released, false if lock wasn't owned by holder or wasn't present.
 */
function releaseLock(key, holder) {
  const result = db.prepare(`
    DELETE FROM distributed_locks WHERE key = ? AND holder = ?
  `).run(key, holder);
  return result.changes > 0;
}

module.exports = {
  acquireLock,
  releaseLock
};

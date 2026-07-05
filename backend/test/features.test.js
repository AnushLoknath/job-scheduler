const test = require('node:test');
const assert = require('node:assert');
const rateLimiter = require('../middleware/rateLimit');
const { acquireLock, releaseLock } = require('../lock');
const db = require('../db');

test.describe('Distributed Job Scheduler Extended Features', () => {

  test('Rate Limiter restricts excess requests within a sliding window', () => {
    const limit = 2;
    const windowMs = 1000;
    const limiter = rateLimiter(limit, windowMs);

    const mockReq = { ip: '1.2.3.4' };
    let statusCalled = null;
    let jsonCalled = null;
    let nextCalledCount = 0;

    const mockRes = {
      status(code) {
        statusCalled = code;
        return this;
      },
      json(obj) {
        jsonCalled = obj;
        return this;
      },
      on() {}
    };

    const mockNext = () => {
      nextCalledCount++;
    };

    // 1st request - allow
    limiter(mockReq, mockRes, mockNext);
    // 2nd request - allow
    limiter(mockReq, mockRes, mockNext);
    // 3rd request - deny (429)
    limiter(mockReq, mockRes, mockNext);

    assert.strictEqual(nextCalledCount, 2, 'Next should have been called exactly twice');
    assert.strictEqual(statusCalled, 429, 'Status code should be 429 on third call');
    assert.ok(jsonCalled.error.includes('Too many requests'), 'Response should suggest retry');
  });

  test('Distributed Locking prevents duplicate acquisition and allows release', () => {
    const lockKey = 'resource:123';
    const holderA = 'worker-a';
    const holderB = 'worker-b';

    // Cleanup first
    db.prepare('DELETE FROM distributed_locks WHERE key = ?').run(lockKey);

    // Acquire lock for holderA - should succeed
    const firstAcquire = acquireLock(lockKey, holderA, 5000);
    assert.strictEqual(firstAcquire, true, 'Worker A should acquire the lock');

    // Try to acquire lock for holderB while Worker A holds it - should fail
    const secondAcquire = acquireLock(lockKey, holderB, 5000);
    assert.strictEqual(secondAcquire, false, 'Worker B should fail to acquire lock');

    // Worker B releases (should fail as B does not hold the lock)
    const releaseFailed = releaseLock(lockKey, holderB);
    assert.strictEqual(releaseFailed, false, 'Worker B cannot release Worker A\'s lock');

    // Worker A releases lock - should succeed
    const releaseSuccess = releaseLock(lockKey, holderA);
    assert.strictEqual(releaseSuccess, true, 'Worker A should release the lock successfully');

    // Worker B should now be able to acquire lock
    const thirdAcquire = acquireLock(lockKey, holderB, 5000);
    assert.strictEqual(thirdAcquire, true, 'Worker B should successfully acquire the released lock');

    // Cleanup lock
    db.prepare('DELETE FROM distributed_locks WHERE key = ?').run(lockKey);
  });

  test('Distributed Locking lease expires correctly', () => {
    const lockKey = 'resource:expiry-test';
    const holderA = 'worker-a';
    const holderB = 'worker-b';

    // Cleanup
    db.prepare('DELETE FROM distributed_locks WHERE key = ?').run(lockKey);

    // Acquire lock with short expiration
    acquireLock(lockKey, holderA, 5000);
    // Simulate lock expiry manually in the database
    db.prepare('UPDATE distributed_locks SET expires_at = ? WHERE key = ?').run(Date.now() - 1000, lockKey);

    // Now holder B attempts to acquire it. Should succeed since lock expired!
    const acquireExpired = acquireLock(lockKey, holderB, 5000);
    assert.strictEqual(acquireExpired, true, 'Worker B should acquire lock since Worker A\'s lease has expired');

    // Cleanup
    db.prepare('DELETE FROM distributed_locks WHERE key = ?').run(lockKey);
  });
});

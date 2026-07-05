const rateLimitMap = new Map();

/**
 * Custom sliding-window rate limiting middleware.
 * @param {number} limit Max number of requests allowed in the window.
 * @param {number} windowMs Window duration in milliseconds.
 */
function rateLimiter(limit = 60, windowMs = 60000) {
  return (req, res, next) => {
    // Determine client identifier (IP)
    const ip = req.ip || req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
    const now = Date.now();
    const windowStart = now - windowMs;

    if (!rateLimitMap.has(ip)) {
      rateLimitMap.set(ip, []);
    }

    // Retain only timestamps within the current sliding window
    let timestamps = rateLimitMap.get(ip).filter(t => t > windowStart);
    
    if (timestamps.length >= limit) {
      return res.status(429).json({ 
        error: 'Too many requests, please try again later.',
        retryAfterMs: Math.max(0, timestamps[0] + windowMs - now)
      });
    }

    timestamps.push(now);
    rateLimitMap.set(ip, timestamps);

    // Occasional map garbage collection on finished requests to prevent memory growth
    res.on('finish', () => {
      if (rateLimitMap.size > 1000) {
        for (const [key, val] of rateLimitMap.entries()) {
          const fresh = val.filter(t => t > Date.now() - windowMs);
          if (fresh.length === 0) {
            rateLimitMap.delete(key);
          } else {
            rateLimitMap.set(key, fresh);
          }
        }
      }
    });

    next();
  };
}

module.exports = rateLimiter;

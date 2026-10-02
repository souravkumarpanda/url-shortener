const { redis } = require('../config/redis');

// Atomic fixed-window counter: INCR + set expiry on first hit, return count and TTL.
const SCRIPT = `
local c = redis.call('INCR', KEYS[1])
if c == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
local ttl = redis.call('PTTL', KEYS[1])
return {c, ttl}
`;

function rateLimit({ keyPrefix, limit, windowSeconds }) {
  return async (req, res, next) => {
    if (!redis.isReady) return next(); // fail open
    try {
      const key = `rl:${keyPrefix}:${req.ip}`;
      const [count, ttlMs] = await redis.eval(SCRIPT, {
        keys: [key],
        arguments: [String(windowSeconds * 1000)],
      });

      res.set('X-RateLimit-Limit', String(limit));
      res.set('X-RateLimit-Remaining', String(Math.max(0, limit - count)));

      if (count > limit) {
        res.set('Retry-After', String(Math.ceil(ttlMs / 1000)));
        return res.status(429).json({ error: 'Too many requests, slow down.' });
      }
      next();
    } catch (err) {
      console.error('[rateLimit] failed:', err.message);
      next();
    }
  };
}

module.exports = { rateLimit };

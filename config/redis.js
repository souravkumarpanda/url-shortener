const { createClient } = require('redis');

const client = createClient({
  url: process.env.REDIS_URL || 'redis://localhost:6379',
  socket: {
    // Keep retrying with backoff (max 3s) instead of crashing the app
    reconnectStrategy: (retries) => Math.min(retries * 200, 3000),
  },
});

let lastErrorLog = 0;
client.on('error', (err) => {
  // Throttle: a down Redis would otherwise spam the console
  const now = Date.now();
  if (now - lastErrorLog > 5000) {
    console.error('[redis] error:', err.message);
    lastErrorLog = now;
  }
});
client.on('ready', () => console.log('[redis] ready'));
client.on('end', () => console.log('[redis] connection closed'));

async function connectToRedis() {
  try {
    await client.connect();
  } catch (err) {
    // App still works without Redis (falls back to MongoDB)
    console.error('[redis] initial connect failed, running without cache:', err.message);
  }
}

module.exports = { redis: client, connectToRedis };

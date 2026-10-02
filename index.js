const os = require('os');
const path = require('path');
const express = require('express');
const mongoose = require('mongoose');
const urlRoutes = require('./routes/url');
const { handleRedirect } = require('./controllers/url');
const { connectToDatabase } = require('./connect');
const { redis, connectToRedis } = require('./config/redis');
const { startClickProducer, stopClickProducer, isProducerReady } = require('./services/clickProducer');

const app = express();
const port = process.env.PORT || 8001;
const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/short-url';
const instanceId = process.env.INSTANCE_ID || os.hostname();

// Behind a load balancer, the TCP peer is the balancer, not the user. Trusting one proxy hop
// makes req.ip the real client address (from X-Forwarded-For), so the Redis rate limiter
// limits each client instead of lumping everyone together. Set TRUST_PROXY=0 when running
// without a balancer so clients can't spoof the header.
const trustProxy = process.env.TRUST_PROXY ?? '0';
app.set('trust proxy', /^\d+$/.test(trustProxy) ? Number(trustProxy) : trustProxy);

connectToDatabase(mongoUrl).then(() => {
  console.log(`[${instanceId}] Connected to MongoDB`);
}).catch(err => {
  console.error('Failed to connect to MongoDB', err);
});

connectToRedis();
startClickProducer();

let shuttingDown = false;

// Tells you which instance answered (handy for seeing the load balancer work)
app.use((req, res, next) => {
  res.set('X-Served-By', instanceId);
  next();
});

app.use(express.json());

// Health checks. Defined before '/:shorten' so they aren't treated as short codes.
// Liveness: the process is up.
app.get('/healthz', (req, res) => res.json({ status: 'ok', instance: instanceId }));
// Readiness: safe to receive traffic. Only MongoDB is required; Redis and Kafka are
// optional because the app degrades gracefully without them.
app.get('/readyz', (req, res) => {
  const mongo = mongoose.connection.readyState === 1;
  const body = {
    instance: instanceId,
    mongo,
    redis: redis.isReady,
    kafka: isProducerReady(),
  };
  if (shuttingDown || !mongo) {
    return res.status(503).json({ status: shuttingDown ? 'shutting_down' : 'not_ready', ...body });
  }
  res.json({ status: 'ready', ...body });
});

// Landing page at '/', defined before '/:shorten'
app.use(express.static(path.join(__dirname, 'public')));

app.use('/url', urlRoutes);

app.get('/:shorten', handleRedirect);

const server = app.listen(port, () => {
  console.log(`[${instanceId}] Server is running on http://localhost:${port}`);
});

// Graceful shutdown: stop taking new connections, let in-flight requests finish,
// then close backing services. Important when containers are replaced during deploys.
async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  const force = setTimeout(() => process.exit(1), 10000);
  force.unref();

  await new Promise((resolve) => {
    server.close(resolve);
    server.closeIdleConnections();
  });
  await stopClickProducer();
  if (redis.isOpen) await redis.quit();
  await mongoose.disconnect();
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

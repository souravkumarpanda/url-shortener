// Separate process: node worker.js
// Consumes click events from Kafka and writes them to MongoDB.
const mongoose = require('mongoose');
const { connectToDatabase } = require('./connect');
const { startClickConsumer } = require('./workers/clickConsumer');

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/short-url';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Kafka takes a while to boot in Docker, so retry instead of dying on the first attempt.
async function startWithRetry() {
  for (let attempt = 1; ; attempt++) {
    try {
      return await startClickConsumer();
    } catch (err) {
      if (attempt >= 20) throw err;
      console.error(`[worker] start failed (attempt ${attempt}/20), retrying in 3s: ${err.message}`);
      await sleep(3000);
    }
  }
}

(async () => {
  await connectToDatabase(mongoUrl);
  console.log('[worker] connected to MongoDB');
  const stop = await startWithRetry();

  async function shutdown() {
    await stop();
    await mongoose.disconnect();
    process.exit(0);
  }
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
})().catch((err) => {
  console.error('[worker] fatal:', err);
  process.exit(1);
});

const { kafka, TOPICS, ensureTopics } = require('../config/kafka');
const Click = require('../models/click');

let producer = null;
let ready = false;
let stopped = false;
let retryTimer = null;

async function connectWithRetry() {
  try {
    await producer.connect();
    await ensureTopics();
    ready = true;
    console.log('[kafka] producer ready');
  } catch (err) {
    ready = false;
    if (stopped) return;
    console.error('[kafka] producer connect failed, retrying in 5s:', err.message);
    retryTimer = setTimeout(connectWithRetry, 5000);
    retryTimer.unref();
  }
}

// Non-blocking: the app starts (and serves redirects) even if Kafka is down.
function startClickProducer() {
  producer = kafka.producer({ allowAutoTopicCreation: false });
  producer.on(producer.events.DISCONNECT, () => {
    ready = false;
    if (!stopped) connectWithRetry();
  });
  connectWithRetry();
}

async function stopClickProducer() {
  stopped = true;
  clearTimeout(retryTimer);
  ready = false;
  if (producer) await producer.disconnect().catch(() => {});
}

async function publishClick(event) {
  if (!ready) return false;
  try {
    await producer.send({
      topic: TOPICS.CLICKS,
      acks: -1, // wait for all in-sync replicas: the "durable" part
      timeout: 3000,
      // Same key -> same partition -> clicks for one code stay ordered
      messages: [{ key: event.shortUrl, value: JSON.stringify(event) }],
    });
    return true;
  } catch (err) {
    console.error('[kafka] publish failed:', err.message);
    return false;
  }
}

// Same shape as the consumer's write, so a click is never counted twice
// even if both paths end up running for it.
function toUpsert(event) {
  return {
    updateOne: {
      filter: { eventId: event.eventId },
      update: { $setOnInsert: event },
      upsert: true,
    },
  };
}

// Used by the redirect handler. Never throws; the redirect must not fail on analytics.
// If Kafka is unavailable we fall back to writing straight to MongoDB.
async function recordClick(event) {
  const published = await publishClick(event);
  if (published) return;
  try {
    await Click.bulkWrite([toUpsert(event)]);
  } catch (err) {
    console.error('[click] fallback write failed, click lost:', err.message);
  }
}

const isProducerReady = () => ready;

module.exports = { isProducerReady, startClickProducer, stopClickProducer, publishClick, recordClick, toUpsert };

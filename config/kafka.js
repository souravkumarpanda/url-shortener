const { Kafka, logLevel } = require('kafkajs');

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

const kafka = new Kafka({
  clientId: 'short-url',
  brokers,
  logLevel: logLevel.NOTHING,
  retry: { initialRetryTime: 300, retries: 5 },
});

const TOPICS = {
  CLICKS: process.env.KAFKA_CLICKS_TOPIC || 'url-clicks',
  DLQ: process.env.KAFKA_DLQ_TOPIC || 'url-clicks.dlq',
};

// Idempotent: createTopics returns false (not an error) if a topic already exists.
async function ensureTopics() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        { topic: TOPICS.CLICKS, numPartitions: 3 },
        { topic: TOPICS.DLQ, numPartitions: 1 },
      ],
    });
  } finally {
    await admin.disconnect();
  }
}

module.exports = { kafka, TOPICS, ensureTopics };

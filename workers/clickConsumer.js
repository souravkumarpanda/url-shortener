const { kafka, TOPICS, ensureTopics } = require('../config/kafka');
const Click = require('../models/click');
const { toUpsert } = require('../services/clickProducer');

const GROUP_ID = process.env.KAFKA_GROUP_ID || 'click-writers';

function parse(message) {
  try {
    const e = JSON.parse(message.value.toString());
    if (!e.eventId || !e.shortUrl || typeof e.timeStamp !== 'number') return null;
    return {
      eventId: e.eventId,
      shortUrl: e.shortUrl,
      timeStamp: e.timeStamp,
      userAgent: e.userAgent,
      referer: e.referer,
    };
  } catch {
    return null;
  }
}

async function startClickConsumer() {
  await ensureTopics();

  const consumer = kafka.consumer({ groupId: GROUP_ID });
  const dlqProducer = kafka.producer();
  await Promise.all([consumer.connect(), dlqProducer.connect()]);
  await consumer.subscribe({ topic: TOPICS.CLICKS, fromBeginning: false });

  await consumer.run({
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
      const ops = [];
      const poison = [];

      for (const message of batch.messages) {
        const event = parse(message);
        if (event) ops.push(toUpsert(event));
        else poison.push({ key: message.key, value: message.value });
      }

      // Unparseable messages go to a dead-letter topic instead of blocking the partition
      if (poison.length) {
        await dlqProducer.send({ topic: TOPICS.DLQ, acks: -1, messages: poison });
      }

      if (ops.length) {
        try {
          // Upserts keyed on eventId: redelivered events are no-ops
          await Click.bulkWrite(ops, { ordered: false });
        } catch (err) {
          // A duplicate-key race between two upserts is harmless; anything else
          // (e.g. MongoDB down) is rethrown so Kafka retries the batch. Offsets
          // are only committed after success, so nothing is lost.
          const onlyDupes =
            err.code === 11000 ||
            (Array.isArray(err.writeErrors) && err.writeErrors.every((w) => w.code === 11000));
          if (!onlyDupes) throw err;
        }
      }

      const last = batch.messages[batch.messages.length - 1];
      if (last) {
        resolveOffset(last.offset);
        await commitOffsetsIfNecessary();
      }
      await heartbeat();
      console.log(`[consumer] partition ${batch.partition}: ${ops.length} clicks, ${poison.length} to DLQ`);
    },
  });

  console.log(`[consumer] running in group "${GROUP_ID}"`);

  return async function stop() {
    await consumer.disconnect().catch(() => {});
    await dlqProducer.disconnect().catch(() => {});
  };
}

module.exports = { startClickConsumer };

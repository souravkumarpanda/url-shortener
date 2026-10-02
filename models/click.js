const mongoose = require('mongoose');

// One document per click. Replaces the unbounded visitHistory array on Url,
// which would eventually hit MongoDB's 16MB document limit.
const clickSchema = new mongoose.Schema({
  // Unique id generated at redirect time; makes the consumer idempotent
  // (Kafka delivers at-least-once, so the same event can arrive twice).
  eventId: { type: String, required: true, unique: true },
  shortUrl: { type: String, required: true },
  timeStamp: { type: Number, required: true },
  userAgent: { type: String },
  referer: { type: String },
});

clickSchema.index({ shortUrl: 1, timeStamp: -1 });

module.exports = mongoose.model('Click', clickSchema);

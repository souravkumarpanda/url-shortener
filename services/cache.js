const { redis } = require('../config/redis');

const URL_TTL_SECONDS = Number(process.env.CACHE_TTL_SECONDS) || 60 * 60 * 24; // 24h
const NOT_FOUND_TTL_SECONDS = 60; // short-lived negative cache
const NOT_FOUND = '__NOT_FOUND__';

const urlKey = (code) => `url:${code}`;

// Every helper fails open: if Redis is down we just behave as a cache miss.
async function getCachedUrl(code) {
  if (!redis.isReady) return null;
  try {
    return await redis.get(urlKey(code));
  } catch (err) {
    console.error('[cache] get failed:', err.message);
    return null;
  }
}

async function setCachedUrl(code, originalUrl) {
  if (!redis.isReady) return;
  try {
    await redis.set(urlKey(code), originalUrl, { EX: URL_TTL_SECONDS });
  } catch (err) {
    console.error('[cache] set failed:', err.message);
  }
}

async function setCachedNotFound(code) {
  if (!redis.isReady) return;
  try {
    await redis.set(urlKey(code), NOT_FOUND, { EX: NOT_FOUND_TTL_SECONDS });
  } catch (err) {
    console.error('[cache] negative set failed:', err.message);
  }
}

async function invalidate(code) {
  if (!redis.isReady) return;
  try {
    await redis.del(urlKey(code));
  } catch (err) {
    console.error('[cache] del failed:', err.message);
  }
}

module.exports = {
  NOT_FOUND,
  getCachedUrl,
  setCachedUrl,
  setCachedNotFound,
  invalidate,
};

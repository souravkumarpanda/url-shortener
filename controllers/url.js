const shortid = require("shortid");
const { randomUUID } = require("crypto");
const URL = require("../models/url");
const Click = require("../models/click");
const { recordClick } = require("../services/clickProducer");

// shortid defaults to worker id 0 in every process, so two app instances could mint the
// same code in the same instant. A random worker id per process makes that unlikely, and
// the retry below (on the unique index) makes it harmless.
shortid.worker(Math.floor(Math.random() * 16));
const {
  NOT_FOUND,
  getCachedUrl,
  setCachedUrl,
  setCachedNotFound,
} = require("../services/cache");

function isValidHttpUrl(value) {
    try {
        const u = new globalThis.URL(value);
        return u.protocol === "http:" || u.protocol === "https:";
    } catch {
        return false;
    }
}

async function handleGenerateNewShortUrl(req, res) {
    const body = req.body;
    if (!body.originalUrl) {
        return res.status(400).json({ error: "originalUrl is required" });
    }
    if (!isValidHttpUrl(body.originalUrl)) {
        return res.status(400).json({ error: "originalUrl must be a valid http(s) URL" });
    }
    let shortId;
    for (let attempt = 1; ; attempt++) {
        shortId = shortid();
        try {
            await URL.create({
                originalUrl: body.originalUrl,
                shortUrl: shortId,
                visitHistory: []
            });
            break;
        } catch (err) {
            // 11000 = duplicate key: another instance (or an earlier call) already used this code
            if (err.code === 11000 && attempt < 5) continue;
            throw err;
        }
    }

    // Warm the cache so the very first click is already a hit
    await setCachedUrl(shortId, body.originalUrl);

    // BASE_URL pins the public address (e.g. https://sho.rt). Otherwise derive it from the
    // request, which works behind nginx and tunnels because Host / X-Forwarded-Proto are forwarded.
    const base = (process.env.BASE_URL || `${req.protocol}://${req.get("host")}`).replace(/\/$/, "");
    return res.json({ shortUrl: shortId, shortLink: `${base}/${shortId}` });
}

async function handleRedirect(req, res) {
    const shortId = req.params.shorten;

    // 1. Try Redis
    const cached = await getCachedUrl(shortId);
    if (cached === NOT_FOUND) {
        return res.status(404).send("Short URL not found");
    }

    let target = cached;
    let source = "cache";

    // 2. Miss -> MongoDB, then populate cache
    if (!target) {
        const entry = await URL.findOne({ shortUrl: shortId }).select("originalUrl").lean();
        if (!entry) {
            await setCachedNotFound(shortId);
            return res.status(404).send("Short URL not found");
        }
        target = entry.originalUrl;
        source = "db";
        await setCachedUrl(shortId, target);
    }

    // 3. Emit a click event to Kafka (async; a consumer writes it to MongoDB).
    //    Not awaited: the redirect never waits on analytics. recordClick never throws
    //    and falls back to a direct DB write if Kafka is unavailable.
    recordClick({
        eventId: randomUUID(),
        shortUrl: shortId,
        timeStamp: Date.now(),
        userAgent: req.get("user-agent"),
        referer: req.get("referer"),
    });

    res.set("X-Cache", source === "cache" ? "HIT" : "MISS");
    return res.redirect(target);
}

async function handleGetAnalytics(req, res) {
    const shortId = req.params.shorten;
    const entry = await URL.findOne({ shortUrl: shortId }).select("visitHistory").lean();
    if (!entry) {
        return res.status(404).json({ error: "Short URL not found" });
    }
    const limit = Math.min(Number(req.query.limit) || 100, 1000);

    // New clicks live in the Click collection; clicks recorded before the Kafka
    // change are still in visitHistory, so both are counted.
    const legacy = entry.visitHistory || [];
    const [newCount, recent] = await Promise.all([
        Click.countDocuments({ shortUrl: shortId }),
        Click.find({ shortUrl: shortId }).sort({ timeStamp: -1 }).limit(limit).select("timeStamp -_id").lean(),
    ]);

    return res.json({
        totalClicks: legacy.length + newCount,
        analytics: recent, // latest `limit` clicks (default 100, max 1000)
    });
}

module.exports = {
    handleGenerateNewShortUrl,
    handleRedirect,
    handleGetAnalytics
};

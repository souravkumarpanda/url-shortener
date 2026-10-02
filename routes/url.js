const express = require('express');
const {handleGenerateNewShortUrl,handleGetAnalytics} = require('../controllers/url');
const { rateLimit } = require('../middlewares/rateLimit');
const router = express.Router();

// 10 short URLs per IP per minute
const createLimiter = rateLimit({ keyPrefix: 'create', limit: 10, windowSeconds: 60 });

router.post('/', createLimiter, handleGenerateNewShortUrl);
router.get('/analytics/:shorten', handleGetAnalytics);
module.exports = router;

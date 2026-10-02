const mongoose = require('mongoose');

async function connectToDatabase(url) {
    return mongoose.connect(url);
}

module.exports = {connectToDatabase};
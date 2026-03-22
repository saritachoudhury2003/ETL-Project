const { app } = require('@azure/functions');
const { BlobServiceClient } = require('@azure/storage-blob');
const axios = require('axios');
app.setup({
    enableHttpStream: true,
});

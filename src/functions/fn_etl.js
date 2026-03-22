const { app } = require('@azure/functions');
const { BlobServiceClient } = require('@azure/storage-blob');
const axios = require('axios');

// ── Config ────────────────────────────────────────────────────────────────────
const NEWS_API_KEY        = process.env.NEWS_API_KEY;           // newsapi.org
const WORLD_NEWS_API_KEY  = process.env.WORLD_NEWS_API_KEY;     // worldnewsapi.com
const AZURE_STORAGE_CONN  = process.env.AZURE_STORAGE_CONNECTION_STRING;
const CONTAINER_NAME      = process.env.BLOB_CONTAINER_NAME || 'news-etl';

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Fetch top headlines from newsapi.org */
async function fetchNewsAPI(query = 'technology', pageSize = 20) {
    const url = 'https://newsapi.org/v2/top-headlines';
    const { data } = await axios.get(url, {
        params: { q: query, pageSize, language: 'en', apiKey: NEWS_API_KEY },
        timeout: 10_000,
    });

    if (data.status !== 'ok') throw new Error(`NewsAPI error: ${data.message}`);

    return (data.articles || []).map(a => ({
        source:      { id: a.source?.id || null, name: a.source?.name || null },
        author:      a.author      || null,
        title:       a.title       || null,
        description: a.description || null,
        url:         a.url         || null,
        urlToImage:  a.urlToImage  || null,
        publishedAt: a.publishedAt || null,
        content:     a.content     || null,
    }));
}

/** Fetch articles from worldnewsapi.com — normalized to NewsAPI shape */
async function fetchWorldNewsAPI(query = 'technology', number = 20) {
    const url = 'https://api.worldnewsapi.com/search-news';
    const { data } = await axios.get(url, {
        params: {
            text:                   query,
            number,
            language:               'en',
            'earliest-publish-date': new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)
                                        .toISOString()
                                        .slice(0, 10),   // last 7 days
        },
        headers: {
            'x-api-key': WORLD_NEWS_API_KEY,
        },
        timeout: 10_000,
    });

    // Normalize to the same shape as NewsAPI articles
    return (data.news || []).map(a => ({
        source:      { id: null, name: a.source_country || 'worldnewsapi.com' },
        author:      a.author  || null,
        title:       a.title   || null,
        description: a.summary || null,
        url:         a.url     || null,
        urlToImage:  a.image   || null,
        publishedAt: a.publish_date || null,
        content:     a.text    || null,
    }));
}

/** Upload a JSON payload to Azure Blob Storage and return the blob URL */
async function uploadToBlob(blobName, payload) {
    const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONN);
    const containerClient   = blobServiceClient.getContainerClient(CONTAINER_NAME);

    // Create container if it doesn't exist
    await containerClient.createIfNotExists({ access: 'container' });

    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    const content         = JSON.stringify(payload, null, 2);

    await blockBlobClient.upload(content, Buffer.byteLength(content), {
        blobHTTPHeaders: { blobContentType: 'application/json' },
    });

    return blockBlobClient.url;
}

// ── Handler ───────────────────────────────────────────────────────────────────
app.http('fn_etl', {
    methods: ['GET', 'POST'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        context.log(`ETL triggered — url: ${request.url}`);

        // Accept query from GET (?query=...) or POST body { "query": "..." }
        let query = request.query.get('query');
        if (!query && request.method === 'POST') {
            try {
                const body = await request.json();
                query = body?.query;
            } catch { /* ignore parse errors */ }
        }
        query = query || 'technology';

        // ── Validate env vars ──────────────────────────────────────────────
        const missing = [];
        if (!NEWS_API_KEY)       missing.push('NEWS_API_KEY');
        if (!WORLD_NEWS_API_KEY) missing.push('WORLD_NEWS_API_KEY');
        if (!AZURE_STORAGE_CONN) missing.push('AZURE_STORAGE_CONNECTION_STRING');
        if (missing.length) {
            return {
                status:  500,
                headers: { 'Content-Type': 'application/json' },
                body:    JSON.stringify({ error: `Missing env vars: ${missing.join(', ')}` }),
            };
        }

        // ── Fetch from both APIs (parallel) ───────────────────────────────
        const results = await Promise.allSettled([
            fetchNewsAPI(query),
            fetchWorldNewsAPI(query),
        ]);

        const newsApiArticles   = results[0].status === 'fulfilled' ? results[0].value : [];
        const worldNewsArticles = results[1].status === 'fulfilled' ? results[1].value : [];

        if (results[0].status === 'rejected')
            context.log(`NewsAPI fetch failed: ${results[0].reason}`);
        if (results[1].status === 'rejected')
            context.log(`WorldNewsAPI fetch failed: ${results[1].reason}`);

        // ── Tag each article with its origin, then deduplicate by URL ────
        newsApiArticles.forEach(a   => (a._origin = 'newsapi.org'));
        worldNewsArticles.forEach(a => (a._origin = 'worldnewsapi.com'));

        const seen     = new Set();
        const articles = [...newsApiArticles, ...worldNewsArticles].filter(a => {
            if (!a.url || seen.has(a.url)) return false;
            seen.add(a.url);
            return true;
        });

        // ── Build payload ─────────────────────────────────────────────────
        const timestamp = new Date().toISOString();
        const payload = {
            metadata: {
                fetchedAt:     timestamp,
                query,
                totalArticles: articles.length,
                sources: {
                    'newsapi.org':    newsApiArticles.length,
                    'worldnewsapi.com': worldNewsArticles.length,
                },
            },
            articles,
        };

        // ── Upload to Blob ────────────────────────────────────────────────
        // e.g. news-data/2025-06-10T12-00-00/technology.json
        const datePart  = timestamp.slice(0, 19).replace(/:/g, '-');
        const blobName  = `etl/${datePart}/${query}.json`;
        const blobUrl   = await uploadToBlob(blobName, payload);

        context.log(`Blob uploaded: ${blobUrl}`);

        return {
            status:  200,
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({
                message:  'ETL completed successfully',
                blobUrl,
                blobName,
                metadata: payload.metadata,
            }),
        };
    },
});
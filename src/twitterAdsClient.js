// src/twitterAdsClient.js
const axios = require('axios');
const crypto = require('crypto');

const BASE = process.env.TW_ADS_BASE || 'https://ads-api.twitter.com/11';

const BEARER = process.env.TW_BEARER_TOKEN || process.env.TWITTER_BEARER_TOKEN;
const ACC = process.env.TW_ACCOUNT_ID || process.env.TWITTER_ACCOUNT_ID;

// OAuth1 creds (optional fallback)
const CK = process.env.TW_API_KEY || process.env.TWITTER_API_KEY;           // API Key
const CS = process.env.TW_API_SECRET || process.env.TWITTER_API_SECRET;     // API Key Secret
const AT = process.env.TW_ACCESS_TOKEN || process.env.TWITTER_ACCESS_TOKEN; // Access Token
const AS = process.env.TW_ACCESS_SECRET || process.env.TWITTER_ACCESS_SECRET; // Access Token Secret

function ensureAccount() {
  if (!ACC) {
    const err = new Error('Twitter Ads: Account ID missing. Set TW_ACCOUNT_ID (or TWITTER_ACCOUNT_ID) in .env');
    err.code = 'TW_NO_ACCOUNT';
    throw err;
  }
}

function oauth1Header(method, url, params) {
  const nonce = crypto.randomBytes(16).toString('hex');
  const timestamp = Math.floor(Date.now() / 1000).toString();

  const baseParams = {
    oauth_consumer_key: CK,
    oauth_nonce: nonce,
    oauth_signature_method: 'HMAC-SHA1',
    oauth_timestamp: timestamp,
    oauth_token: AT,
    oauth_version: '1.0',
    ...params,
  };

  const enc = s => encodeURIComponent(s).replace(/[!*()']/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
  const norm = Object.keys(baseParams).sort().map(k => `${enc(k)}=${enc(baseParams[k])}`).join('&');
  const baseString = [method.toUpperCase(), enc(url), enc(norm)].join('&');
  const signingKey = `${enc(CS)}&${enc(AS)}`;
  const sig = crypto.createHmac('sha1', signingKey).update(baseString).digest('base64');

  const authParams = {
    oauth_consumer_key: CK,
    oauth_nonce: nonce,
    oauth_signature: sig,
    oauth_signature_method: 'HMAC-SHA1',
    oauth_timestamp: timestamp,
    oauth_token: AT,
    oauth_version: '1.0',
  };
  const header = 'OAuth ' + Object.keys(authParams)
    .sort()
    .map(k => `${enc(k)}="${enc(authParams[k])}"`)
    .join(', ');
  return header;
}

async function adsGet(path, query = {}) {
  ensureAccount();
  const url = `${BASE}${path}`;

  // Prefer bearer if present
  if (BEARER) {
    const r = await axios.get(url, {
      params: query,
      headers: { Authorization: `Bearer ${BEARER}`, Accept: 'application/json' },
      validateStatus: () => true,
    });
    if (r.status === 200) return r.data;
    // Fallback to OAuth1 if we have creds and bearer failed auth
    if ((r.status === 401 || r.status === 403) && CK && CS && AT && AS) {
      // fall through to OAuth1 path below
    } else {
      const err = new Error(`Twitter Ads GET failed (${r.status})`);
      err.response = r;
      throw err;
    }
  }

  if (!CK || !CS || !AT || !AS) {
    const err = new Error('Twitter Ads OAuth1 creds missing. Set TW_API_KEY, TW_API_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET.');
    err.code = 'TW_NO_OAUTH1';
    throw err;
  }

  // OAuth1
  const method = 'GET';
  const auth = oauth1Header(method, url, query);
  const r2 = await axios.get(url, {
    params: query,
    headers: { Authorization: auth, Accept: 'application/json' },
    validateStatus: () => true
  });
  if (r2.status !== 200) {
    const err = new Error(`Twitter Ads GET (oauth1) failed (${r2.status})`);
    err.response = r2;
    throw err;
  }
  return r2.data;
}

// Convenience helpers
function accountPath(suffix) { ensureAccount(); return `/accounts/${ACC}${suffix}`; }

module.exports = {
  adsGet,
  accountPath,
  ACC,
};

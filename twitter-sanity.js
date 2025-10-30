// twitter-sanity.js
require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');

const ACC = process.env.TW_ACCOUNT_ID || process.env.TWITTER_ACCOUNT_ID;
const BEARER = process.env.TW_BEARER_TOKEN || process.env.TWITTER_BEARER_TOKEN;

const CK = process.env.TW_API_KEY || process.env.TWITTER_API_KEY;
const CS = process.env.TW_API_SECRET || process.env.TWITTER_API_SECRET;
const AT = process.env.TW_ACCESS_TOKEN || process.env.TWITTER_ACCESS_TOKEN;
const AS = process.env.TW_ACCESS_SECRET || process.env.TWITTER_ACCESS_SECRET;

const BASE = 'https://ads-api.twitter.com/11';

async function tryBearer() {
  if (!BEARER) return { ok: false, why: 'No bearer in env' };
  try {
    const r = await axios.get(`${BASE}/accounts/${ACC}`, {
      headers: { Authorization: `Bearer ${BEARER}` },
      validateStatus: () => true
    });
    return { ok: r.status === 200, status: r.status, data: r.data };
  } catch (e) {
    return { ok: false, err: e.message };
  }
}

// very small OAuth 1.0a signer
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

async function tryOAuth1() {
  if (!CK || !CS || !AT || !AS) return { ok: false, why: 'Missing one of TW_API_KEY, TW_API_SECRET, TW_ACCESS_TOKEN, TW_ACCESS_SECRET' };
  const url = `${BASE}/accounts/${ACC}`;
  const auth = oauth1Header('GET', url, {});
  try {
    const r = await axios.get(url, {
      headers: { Authorization: auth },
      validateStatus: () => true
    });
    return { ok: r.status === 200, status: r.status, data: r.data };
  } catch (e) {
    return { ok: false, err: e.message };
  }
}

(async()=>{
  console.log('Sanity on accounts/', ACC);
  const bearer = await tryBearer();
  console.log('Bearer result:', bearer);
  const oauth1 = await tryOAuth1();
  console.log('OAuth1 result:', oauth1);
})();

// File: src/twitterAdsReport.js
'use strict';
require('dotenv').config();
const axios = require('axios');

/**
 * ENV supported (either naming works):
 *  Preferred:
 *    - TW_BEARER_TOKEN
 *    - TW_ACCOUNT_ID                (e.g. "18ce54mzwyl")
 *  Also accepted:
 *    - TWITTER_BEARER_TOKEN
 *    - TWITTER_ACCOUNT_ID
 *
 * Optional:
 *    - TW_TIMEOUT_MS
 *
 * Notes:
 *  - Uses Ads API v11 stats endpoint (granularity=DAY).
 *  - Returns per-campaign rows with impressions, clicks, conversions, and spend (major units).
 */

function pickEnv(...keys) {
  for (const k of keys) {
    const v = process.env[k];
    if (v && String(v).trim() !== '') return v;
  }
  return undefined;
}

const TW_BEARER_TOKEN = pickEnv('TW_BEARER_TOKEN', 'TWITTER_BEARER_TOKEN');
const TW_ACCOUNT_ID   = pickEnv('TW_ACCOUNT_ID', 'TWITTER_ACCOUNT_ID');
const TW_TIMEOUT_MS   = pickEnv('TW_TIMEOUT_MS');

const TIMEOUT = Number(TW_TIMEOUT_MS || 45000);

function authHeaders() {
  if (!TW_BEARER_TOKEN) {
    throw new Error(
      'Twitter ads auth missing: set TW_BEARER_TOKEN (or TWITTER_BEARER_TOKEN) in .env'
    );
  }
  return { Authorization: `Bearer ${TW_BEARER_TOKEN}` };
}

/**
 * Convert micro currency to major units.
 * Twitter Ads typically returns spend in micro units under billed_charge_local_micro.
 * 1,000,000 micro = 1.00 major
 */
function microToMajor(micro) {
  const n = Number(micro || 0);
  // To avoid floating issues, do a two-step division.
  return Math.round(n / 10000) / 100; // == n / 1_000_000 with 2dp rounding
}

/**
 * Fetch per-campaign metrics for a single day (UTC).
 */
async function fetchDay(ymd) {
  if (!TW_ACCOUNT_ID) {
    throw new Error(
      'Twitter ads account missing: set TW_ACCOUNT_ID (or TWITTER_ACCOUNT_ID) in .env'
    );
  }
  const start = `${ymd}T00:00:00Z`;
  const end   = `${ymd}T23:59:59Z`;

  const url = `https://ads-api.twitter.com/11/stats/accounts/${encodeURIComponent(TW_ACCOUNT_ID)}`;
  const params = {
    entity: 'CAMPAIGN',
    start_time: start,
    end_time: end,
    granularity: 'DAY',
    placement: 'ALL_ON_TWITTER',
    metric_groups: 'ENGAGEMENT,BILLING,WEB_CONVERSIONS',
  };

  const r = await axios.get(url, {
    headers: authHeaders(),
    params,
    timeout: TIMEOUT,
    validateStatus: () => true
  });

  if (r.status !== 200) {
    throw new Error(`Twitter stats failed: ${r.status} ${r.statusText} ${JSON.stringify(r.data)}`);
  }
  return r.data; // { data: [...], ... }
}

function pick(metric, fallback = 0) {
  if (!Array.isArray(metric) || metric.length === 0) return fallback;
  const v = Number(metric[0]) || 0;
  return v;
}

/**
 * Normalise response -> array of rows:
 * { date, campaignId, campaignName, impressions, clicks, conversions, spend }
 */
function normaliseCampaignRows(ymd, payload) {
  const rows = [];
  const list = payload?.data || [];
  for (const item of list) {
    const name = item?.id_data?.[0]?.name || item?.id || '';
    const metrics = item?.id_data?.[0]?.metrics || {};

    const impressions = pick(metrics.impressions);
    const clicks = pick(metrics.clicks);
    // conservative conversion pick: website_conversions first, otherwise fallback to 0
    const conversions = pick(metrics.website_conversions, 0);

    // spend often in micro currency under “billed_charge_local_micro”
    let spendMajor = 0;
    const micro = metrics.billed_charge_local_micro;
    if (Array.isArray(micro) && micro.length) {
      spendMajor = microToMajor(micro[0]);
    }

    rows.push({
      date: ymd,
      campaignId: item?.id || '',
      campaignName: name,
      impressions,
      clicks,
      conversions,
      spend: spendMajor,
    });
  }
  return rows;
}

async function getDailyCampaignRows(isoDate) {
  const data = await fetchDay(isoDate);
  return normaliseCampaignRows(isoDate, data);
}

module.exports = { getDailyCampaignRows };

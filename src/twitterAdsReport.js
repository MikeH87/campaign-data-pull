// File: src/twitterAdsReport.js
'use strict';
require('dotenv').config();
const axios = require('axios');

/**
 * ENV required:
 *  - TW_BEARER_TOKEN           (OAuth2 Bearer for Ads API v11)
 *  - TW_ACCOUNT_ID             (e.g. "18ce54mzwyl")
 *  - TW_TIMEOUT_MS             (optional)
 *
 * Notes:
 *  - This uses the v11 stats endpoint for campaigns with DAY granularity.
 *  - We fetch metrics: impressions, clicks, conversions (link_click or website_conversions depending on data),
 *    and spend (in MAJOR units).
 */

const {
  TW_BEARER_TOKEN,
  TW_ACCOUNT_ID,
  TW_TIMEOUT_MS,
} = process.env;

const TIMEOUT = Number(TW_TIMEOUT_MS || 45000);
if (!TW_BEARER_TOKEN || !TW_ACCOUNT_ID) {
  // Don’t throw at import-time (so bing-only runs still work),
  // but we will throw when called.
}

function authHeaders() {
  if (!TW_BEARER_TOKEN) throw new Error('TW_BEARER_TOKEN missing');
  return { Authorization: `Bearer ${TW_BEARER_TOKEN}` };
}

/**
 * Convert micro currency (if returned) to major units.
 * Twitter Ads usually returns spend in micro currency.
 */
function microToMajor(micro) {
  const n = Number(micro || 0);
  return Math.round(n / 10000) / 100; // 1,000,000 micro = 1.00 major
}

function toISO(dateYMD) {
  // just “YYYY-MM-DD”
  return dateYMD;
}

/**
 * Fetch per-campaign metrics for a single day.
 * We request granularity=DAY and set start_time/end_time inclusive of the day (UTC).
 */
async function fetchDay(ymd) {
  if (!TW_ACCOUNT_ID) throw new Error('TW_ACCOUNT_ID missing');
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

  const r = await axios.get(url, { headers: authHeaders(), params, timeout: TIMEOUT, validateStatus: () => true });
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

function normaliseCampaignRows(ymd, payload) {
  const rows = [];
  const list = payload?.data || [];
  for (const item of list) {
    const name = item?.id_data?.[0]?.name || item?.id || '';
    const metrics = item?.id_data?.[0]?.metrics || {};

    const impressions = pick(metrics.impressions);
    const clicks = pick(metrics.clicks);
    // “conversions” can be varied; take website_conversions or follows with best-effort fallback
    const conversions = pick(metrics.website_conversions, pick(metrics.qualified_impressions, 0));

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

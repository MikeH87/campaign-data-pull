// File: src/hubspotClient.js
require('dotenv').config();
const axios = require('axios');

const HUBSPOT_BASE = 'https://api.hubapi.com';
const HS_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;

// Your env property names (provided by you)
const HSPROP_TOTAL_SPEND       = process.env.HSPROP_TOTAL_SPEND       || 'hs_spend_items_sum_amount'; // READ-ONLY (we never write)
const HSPROP_TOTAL_CLICKS      = process.env.HSPROP_TOTAL_CLICKS      || 'total_clicks';
const HSPROP_TOTAL_IMPRESSIONS = process.env.HSPROP_TOTAL_IMPRESSIONS || 'total_impressions';
const HSPROP_TOTAL_CONVERSIONS = process.env.HSPROP_TOTAL_CONVERSIONS || 'total_conversions';
const HSPROP_LAST_AVG_CPC      = process.env.HSPROP_LAST_AVG_CPC      || 'avg_cpc_last';
const HSPROP_LAST_CPL          = process.env.HSPROP_LAST_CPL          || 'cpl_last';
const HSPROP_LAST_STATUS       = process.env.HSPROP_LAST_STATUS       || 'bing_last_status';
const HSPROP_LAST_BING_DATE    = process.env.HSPROP_LAST_BING_DATE    || 'bing_last_processed';

function authHeaders() {
  return {
    Authorization: `Bearer ${HS_TOKEN}`,
    'Content-Type': 'application/json',
  };
}
function toEpochMillis(isoYmd) {
  const d = new Date(isoYmd.length === 10 ? `${isoYmd}T00:00:00Z` : isoYmd);
  return d.getTime();
}

// ------ Campaigns (v3) ------
async function listCampaignsPage(limit = 100, after = undefined) {
  const url = new URL(`${HUBSPOT_BASE}/marketing/v3/campaigns`);
  url.searchParams.set('limit', String(Math.max(1, Math.min(100, limit))));
  if (after != null) url.searchParams.set('after', String(after));
  const r = await axios.get(url.toString(), { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 200) throw new Error(`List campaigns failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}
async function findCampaignByName(name) {
  let after;
  while (true) {
    const data = await listCampaignsPage(100, after);
    const found = (data.results || []).find(c => {
      const n = c?.properties?.hs_name || c?.properties?.name || '';
      return n === name;
    });
    if (found) return found;
    after = data?.paging?.next?.after;
    if (!after) return null;
  }
}
async function getCampaignById(id) {
  const url = `${HUBSPOT_BASE}/marketing/v3/campaigns/${id}`;
  const r = await axios.get(url, { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 200) throw new Error(`Get campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}
async function createCampaign(name) {
  const body = { properties: { hs_name: name, [HSPROP_LAST_STATUS]: 'CREATED' } };
  const url = `${HUBSPOT_BASE}/marketing/v3/campaigns`;
  const r = await axios.post(url, body, { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 201) throw new Error(`Create campaign failed for "${name}" (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}
async function ensureCampaign(name) {
  const existing = await findCampaignByName(name);
  if (existing) return existing;
  return createCampaign(name);
}

// ------ Spend Items ------
// amountMajor is a decimal number (e.g., 23.15)
async function createSpendItem(campaignId, { isoDate, amountMajor, source }) {
  const order = Number(new Date(isoDate).toISOString().slice(0,10).replace(/-/g,'')); // 20221105
  const name = `${source || 'Ads'} ${isoDate}`;
  const body = {
    name,
    amount: Number(amountMajor),     // major units
    order,
    date: toEpochMillis(isoDate),    // epoch ms
  };
  const url = `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}/spend`;
  const r = await axios.post(url, body, { headers: authHeaders(), validateStatus: () => true });
  if (r.status === 201) return r.data;
  if (r.status === 409) return r.data; // duplicate order — treat as idempotent
  throw new Error(`Create spend item failed (${r.status}) ${JSON.stringify(r.data)}`);
}

// ------ Totals (custom props) ------

async function getTotalsAndLastDate(campaignId) {
  const data = await getCampaignById(campaignId);
  const p = data?.properties || {};
  const clicks = Number(p[HSPROP_TOTAL_CLICKS]      || 0);
  const imps   = Number(p[HSPROP_TOTAL_IMPRESSIONS] || 0);
  const convs  = Number(p[HSPROP_TOTAL_CONVERSIONS] || 0);
  const lastMs = p[HSPROP_LAST_BING_DATE] != null ? Number(p[HSPROP_LAST_BING_DATE]) : 0;
  return { clicks, imps, convs, lastMs };
}

async function patchCampaignProperties(campaignId, props) {
  // Basic primitives only. LAST_BING_DATE must be a number (epoch ms).
  const normalized = {};
  for (const [k, v] of Object.entries(props)) {
    if (k === HSPROP_LAST_BING_DATE) {
      normalized[k] = typeof v === 'number' ? v : Number(v);
    } else {
      // HubSpot accepts strings for number fields; send strings consistently.
      normalized[k] = `${Number(v || 0)}`;
    }
  }
  const body = { properties: normalized };
  const url = `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}`;
  const r = await axios.patch(url, body, { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 200) throw new Error(`PATCH ${campaignId} failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}

/**
 * Idempotent, monotonic daily add:
 * - reads current totals + last processed date
 * - if dateISO <= lastProcessed, SKIP (prevents “going backwards”)
 * - otherwise, add the day’s deltas and set lastProcessed = dateISO
 */
async function addDailyTotalsMonotonic(campaignId, { clicks, impressions, conversions, dateISO }) {
  const { clicks: c0, imps: i0, convs: v0, lastMs } = await getTotalsAndLastDate(campaignId);
  const dayMs = toEpochMillis(dateISO);
  if (lastMs && dayMs <= lastMs) return false; // already processed >= this day

  const next = {
    [HSPROP_TOTAL_CLICKS]:      c0 + Number(clicks || 0),
    [HSPROP_TOTAL_IMPRESSIONS]: i0 + Number(impressions || 0),
    [HSPROP_TOTAL_CONVERSIONS]: v0 + Number(conversions || 0),
    [HSPROP_LAST_STATUS]:       'OK',
    [HSPROP_LAST_BING_DATE]:    dayMs,
  };
  await patchCampaignProperties(campaignId, next);
  return true;
}

function createHubspotClient() {
  return {
    ensureCampaign,
    findCampaignByName,
    createCampaign,
    createSpendItem,
    addDailyTotalsMonotonic,
    patchCampaignProperties,
    getCampaignById,
  };
}
function getHubspotClient() { return createHubspotClient(); }

module.exports = {
  createHubspotClient,
  getHubspotClient,
  // export names (optional)
  HSPROP_TOTAL_SPEND,
  HSPROP_TOTAL_CLICKS,
  HSPROP_TOTAL_IMPRESSIONS,
  HSPROP_TOTAL_CONVERSIONS,
  HSPROP_LAST_AVG_CPC,
  HSPROP_LAST_CPL,
  HSPROP_LAST_STATUS,
  HSPROP_LAST_BING_DATE,
};

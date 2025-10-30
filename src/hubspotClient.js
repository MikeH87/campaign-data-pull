// File: src/hubspotClient.js
require('dotenv').config();
const axios = require('axios');

const HUBSPOT_BASE = 'https://api.hubapi.com';
const HS_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;

// ---- property names (from .env, with safe defaults) ----
const HSPROP_TOTAL_SPEND       = process.env.HSPROP_TOTAL_SPEND       || 'hs_spend_items_sum_amount';
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

function toNum(v) {
  if (v === null || v === undefined || v === '') return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

// ---- log which props are in play (runs once on require)
console.log('[HS] Totals props:',
  { clicks: HSPROP_TOTAL_CLICKS, imps: HSPROP_TOTAL_IMPRESSIONS, convs: HSPROP_TOTAL_CONVERSIONS });
console.log('[HS] Last props:',
  { status: HSPROP_LAST_STATUS, lastDate: HSPROP_LAST_BING_DATE, lastAvgCpc: HSPROP_LAST_AVG_CPC, lastCpl: HSPROP_LAST_CPL });

/* ------------------ Campaign helpers ------------------ */

async function listCampaignsPage(limit = 100, after) {
  const url = new URL(`${HUBSPOT_BASE}/marketing/v3/campaigns`);
  url.searchParams.set('limit', String(limit));
  if (after) url.searchParams.set('after', String(after));
  const r = await axios.get(url.toString(), { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 200) throw new Error(`List campaigns failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}

async function findCampaignByName(name) {
  let after;
  while (true) {
    const data = await listCampaignsPage(100, after);
    const found = (data.results || []).find(c => (c.properties?.hs_name || '') === name);
    if (found) return found;
    after = data?.paging?.next?.after;
    if (!after) return null;
  }
}

async function getCampaignById(id) {
  const r = await axios.get(`${HUBSPOT_BASE}/marketing/v3/campaigns/${id}`, {
    headers: authHeaders(),
    validateStatus: () => true,
  });
  if (r.status !== 200) throw new Error(`Get campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}

async function createCampaign(name) {
  const body = { properties: { hs_name: name, [HSPROP_LAST_STATUS]: 'CREATED' } };
  const r = await axios.post(`${HUBSPOT_BASE}/marketing/v3/campaigns`, body, {
    headers: authHeaders(),
    validateStatus: () => true,
  });
  if (r.status !== 201) throw new Error(`Create campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}

async function ensureCampaign(name) {
  const existing = await findCampaignByName(name);
  if (existing) return existing;
  return createCampaign(name);
}

/* ------------------ Spend items ------------------ */

async function createSpendItem(campaignId, { isoDate, amountMajor, source }) {
  // 'order' must be stable per-day so duplicates are prevented
  const order = Number(new Date(isoDate).toISOString().slice(0, 10).replace(/-/g, ''));
  const body = {
    name: `${source || 'Ads'} ${isoDate}`,
    amount: Number(amountMajor), // in major units (e.g. GBP)
    order,
    date: toEpochMillis(isoDate),
  };
  const r = await axios.post(
    `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}/spend`,
    body,
    { headers: authHeaders(), validateStatus: () => true }
  );
  if (r.status === 201 || r.status === 409) return r.data; // 409 = that order already exists
  throw new Error(`Create spend item failed (${r.status}) ${JSON.stringify(r.data)}`);
}

/* ------------------ Totals (additive) ------------------ */

async function patchCampaignProperties(campaignId, props) {
  const body = { properties: props };
  const r = await axios.patch(
    `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}`,
    body,
    { headers: authHeaders(), validateStatus: () => true }
  );
  if (r.status !== 200) throw new Error(`PATCH ${campaignId} failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}

async function getTotals(campaignId) {
  const data = await getCampaignById(campaignId);
  const p = data?.properties || {};
  const current = {
    clicks: toNum(p[HSPROP_TOTAL_CLICKS]),
    imps:   toNum(p[HSPROP_TOTAL_IMPRESSIONS]),
    convs:  toNum(p[HSPROP_TOTAL_CONVERSIONS]),
  };
  return current;
}

/**
 * Add daily totals additively (never overwrite).
 * Reads the current HS totals, then adds the day’s numbers and PATCHes.
 */
async function addDailyTotalsAccumulative(campaignId, { clicks, impressions, conversions, dateISO }) {
  const cur = await getTotals(campaignId);
  const addClicks = toNum(clicks);
  const addImps   = toNum(impressions);
  const addConvs  = toNum(conversions);

  const next = {
    [HSPROP_TOTAL_CLICKS]:      cur.clicks + addClicks,
    [HSPROP_TOTAL_IMPRESSIONS]: cur.imps   + addImps,
    [HSPROP_TOTAL_CONVERSIONS]: cur.convs  + addConvs,
  };

  // “Last seen” / housekeeping (optional)
  if (HSPROP_LAST_STATUS)    next[HSPROP_LAST_STATUS] = 'OK';
  if (HSPROP_LAST_BING_DATE) next[HSPROP_LAST_BING_DATE] = toEpochMillis(dateISO);

  // Log what we'll write (helps catch property name mismatches)
  console.log('[HS] PATCH totals', {
    id: campaignId,
    write: {
      [HSPROP_TOTAL_CLICKS]: next[HSPROP_TOTAL_CLICKS],
      [HSPROP_TOTAL_IMPRESSIONS]: next[HSPROP_TOTAL_IMPRESSIONS],
      [HSPROP_TOTAL_CONVERSIONS]: next[HSPROP_TOTAL_CONVERSIONS],
    }
  });

  await patchCampaignProperties(campaignId, next);
  return true;
}

function getHubspotClient() {
  return {
    ensureCampaign,
    createSpendItem,
    addDailyTotalsAccumulative,
    findCampaignByName,
    getCampaignById,
  };
}

module.exports = { getHubspotClient };

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
console.log('[HS] Totals props:', {
  clicks: HSPROP_TOTAL_CLICKS,
  imps:   HSPROP_TOTAL_IMPRESSIONS,
  convs:  HSPROP_TOTAL_CONVERSIONS
});
console.log('[HS] Last props:', {
  status:   HSPROP_LAST_STATUS,
  lastDate: HSPROP_LAST_BING_DATE,
  lastAvgCpc: HSPROP_LAST_AVG_CPC,
  lastCpl:    HSPROP_LAST_CPL
});

/* ------------------ Campaign helpers ------------------ */

async function listCampaignsPage(limit = 100, after, archived = false) {
  // limit must be 1..100
  const lim = Math.max(1, Math.min(100, Number(limit) || 100));
  const url = new URL(`${HUBSPOT_BASE}/marketing/v3/campaigns`);
  url.searchParams.set('limit', String(lim));
  if (after) url.searchParams.set('after', String(after));
  // Many HS v3 list endpoints support ?archived=true to include archived records.
  url.searchParams.set('archived', archived ? 'true' : 'false');

  const r = await axios.get(url.toString(), { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 200) throw new Error(`List campaigns failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}

async function findCampaignByName(name, { includeArchived = false } = {}) {
  // First pass: active only
  let after;
  while (true) {
    const data = await listCampaignsPage(100, after, false);
    const found = (data.results || []).find(c => (c.properties?.hs_name || '') === name);
    if (found) return found;
    after = data?.paging?.next?.after;
    if (!after) break;
  }

  if (!includeArchived) return null;

  // Second pass: archived=true
  after = undefined;
  while (true) {
    const data = await listCampaignsPage(100, after, true);
    const found = (data.results || []).find(c => (c.properties?.hs_name || '') === name);
    if (found) return found;
    after = data?.paging?.next?.after;
    if (!after) break;
  }

  return null;
}

async function getCampaignById(id) {
  const url = new URL(`${HUBSPOT_BASE}/marketing/v3/campaigns/${id}`);
  // Ask HubSpot to return archived if needed
  url.searchParams.set('archived', 'true');
  const r = await axios.get(url.toString(), {
    headers: authHeaders(),
    validateStatus: () => true,
  });
  if (r.status !== 200) throw new Error(`Get campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}

async function createCampaign(name) {
  const body = { properties: { hs_name: name, ...(HSPROP_LAST_STATUS ? { [HSPROP_LAST_STATUS]: 'CREATED' } : {}) } };
  const r = await axios.post(`${HUBSPOT_BASE}/marketing/v3/campaigns`, body, {
    headers: authHeaders(),
    validateStatus: () => true,
  });

  if (r.status === 201) return r.data;

  // Name conflict (existing record). The API often returns 409 with *no id*,
  // especially if there’s an archived one. We’ll resolve by searching (active, then archived).
  if (r.status === 409) {
    const found = await findCampaignByName(name, { includeArchived: true });
    if (found) return found;

    // Try a short backoff + another search (eventual consistency)
    await new Promise(res => setTimeout(res, 800));
    const found2 = await findCampaignByName(name, { includeArchived: true });
    if (found2) return found2;

    throw new Error(`Create campaign 409 but could not find "${name}" via list after retries`);
  }

  throw new Error(`Create campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
}

async function ensureCampaign(name) {
  // Fast path: check by name (active, then archived)
  const existing = await findCampaignByName(name, { includeArchived: true });
  if (existing) return existing;
  return createCampaign(name);
}

/* ------------------ Spend items ------------------ */

async function createSpendItem(campaignId, { isoDate, amountMajor, source }) {
  // HubSpot requires "order" to dedupe; use YYYYMMDD as a stable int
  const order = Number(new Date(isoDate).toISOString().slice(0, 10).replace(/-/g, ''));
  const body = {
    name: `${source || 'Ads'} ${isoDate}`,
    amount: Number(amountMajor), // major units e.g. GBP
    order,
    date: toEpochMillis(isoDate),
  };
  const r = await axios.post(
    `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}/spend`,
    body,
    { headers: authHeaders(), validateStatus: () => true }
  );
  if (r.status === 201 || r.status === 409) return r.data; // 409 = already exists for that order
  throw new Error(`Create spend item failed (${r.status}) ${JSON.stringify(r.data)}`);
}

/* ------------------ Totals helpers ------------------ */

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
  return {
    clicks: toNum(p[HSPROP_TOTAL_CLICKS]),
    imps:   toNum(p[HSPROP_TOTAL_IMPRESSIONS]),
    convs:  toNum(p[HSPROP_TOTAL_CONVERSIONS]),
  };
}

/**
 * ADD today’s metrics onto current totals in HubSpot.
 * (This is the additive path to avoid overwriting totals.)
 */
async function addToTotals(campaignId, { clicks = 0, impressions = 0, conversions = 0 }, dateISO) {
  const current = await getTotals(campaignId);
  const next = {
    [HSPROP_TOTAL_CLICKS]:      toNum(current.clicks) + toNum(clicks),
    [HSPROP_TOTAL_IMPRESSIONS]: toNum(current.imps)   + toNum(impressions),
    [HSPROP_TOTAL_CONVERSIONS]: toNum(current.convs)  + toNum(conversions),
  };
  if (HSPROP_LAST_STATUS)    next[HSPROP_LAST_STATUS] = 'OK';
  if (HSPROP_LAST_BING_DATE) next[HSPROP_LAST_BING_DATE] = toEpochMillis(dateISO);

  console.log('[HS] PATCH totals (add)', {
    id: campaignId,
    add:   { clicks, impressions, conversions },
    write: {
      clicks: next[HSPROP_TOTAL_CLICKS],
      imps:   next[HSPROP_TOTAL_IMPRESSIONS],
      convs:  next[HSPROP_TOTAL_CONVERSIONS],
    }
  });

  await patchCampaignProperties(campaignId, next);
  return true;
}

function getHubspotClient() {
  return {
    ensureCampaign,
    findCampaignByName,
    getCampaignById,
    createSpendItem,
    getTotals,
    addToTotals,
  };
}

module.exports = { getHubspotClient };

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

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ---- log which props are in play (runs once on require)
console.log('[HS] Totals props:', {
  clicks: HSPROP_TOTAL_CLICKS,
  imps:   HSPROP_TOTAL_IMPRESSIONS,
  convs:  HSPROP_TOTAL_CONVERSIONS,
});
console.log('[HS] Last props:', {
  status:    HSPROP_LAST_STATUS,
  lastDate:  HSPROP_LAST_BING_DATE,
  lastAvgCpc:HSPROP_LAST_AVG_CPC,
  lastCpl:   HSPROP_LAST_CPL,
});

/* ------------------ Campaign helpers ------------------ */

async function listCampaignsPage(limit = 100, after, archived = false) {
  const url = new URL(`${HUBSPOT_BASE}/marketing/v3/campaigns`);
  url.searchParams.set('limit', String(Math.min(Math.max(limit,1),100)));
  if (after) url.searchParams.set('after', String(after));
  // archived default is false; we will search both false and true when needed
  if (archived) url.searchParams.set('archived', 'true');

  const r = await axios.get(url.toString(), { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 200) {
    throw new Error(`List campaigns failed (${r.status}) ${JSON.stringify(r.data)}`);
  }
  return r.data;
}

async function listAllCampaigns(archived = false) {
  const results = [];
  let after;
  let pages = 0;
  while (true) {
    pages++;
    const data = await listCampaignsPage(100, after, archived);
    (data.results || []).forEach(c => results.push(c));
    after = data?.paging?.next?.after;
    if (!after) break;
    if (pages > 100) break; // hard safety cap
  }
  return results;
}

function sameName(a, b) {
  return (String(a||'').trim().toLowerCase() === String(b||'').trim().toLowerCase());
}

async function findCampaignByName(name) {
  // First pass: live (archived=false)
  let all = await listAllCampaigns(false);
  let found = all.find(c => sameName(c?.properties?.hs_name, name));
  if (found) return found;

  // Second pass: archived=true (in case it’s archived)
  all = await listAllCampaigns(true);
  found = all.find(c => sameName(c?.properties?.hs_name, name));
  return found || null;
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
  const body = { properties: { hs_name: name, ...(HSPROP_LAST_STATUS ? {[HSPROP_LAST_STATUS]:'CREATED'} : {}) } };
  const r = await axios.post(`${HUBSPOT_BASE}/marketing/v3/campaigns`, body, {
    headers: authHeaders(),
    validateStatus: () => true,
  });
  if (r.status === 201) return r.data;
  if (r.status === 409) {
    throw Object.assign(new Error('NAME_CONFLICT'), { code: 409, data: r.data });
  }
  throw new Error(`Create campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
}

/**
 * Robust ensure:
 * 1) Try to find by name (both archived & live lists).
 * 2) If not found, attempt create.
 * 3) If create returns 409, retry find by name with backoff; if still not found after retries, fail clearly.
 */
async function ensureCampaign(name) {
  const before = await findCampaignByName(name);
  if (before) return before;

  try {
    const created = await createCampaign(name);
    return created;
  } catch (e) {
    if (e && e.code === 409) {
      console.warn(`[HS] 409 on create; will retry list for "${name}"`);
      // Retry finding up to 6 times with progressive backoff
      for (let i = 0; i < 6; i++) {
        await sleep(1000 * (i + 1));
        const again = await findCampaignByName(name);
        if (again) return again;
      }
      throw new Error(`Create campaign 409 but could not find "${name}" via list after retries`);
    }
    throw e;
  }
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
 * Add deltas to current totals (authoritative read-add-write).
 * Also updates status & last processed date.
 */
async function addTotals(campaignId, { addClicks=0, addImps=0, addConvs=0 }, dateISO) {
  const curr = await getTotals(campaignId);
  const nextClicks = Math.max(0, toNum(curr.clicks) + toNum(addClicks));
  const nextImps   = Math.max(0, toNum(curr.imps)   + toNum(addImps));
  const nextConvs  = Math.max(0, toNum(curr.convs)  + toNum(addConvs));

  const props = {
    [HSPROP_TOTAL_CLICKS]:      nextClicks,
    [HSPROP_TOTAL_IMPRESSIONS]: nextImps,
    [HSPROP_TOTAL_CONVERSIONS]: nextConvs,
  };
  if (HSPROP_LAST_STATUS)    props[HSPROP_LAST_STATUS] = 'OK';
  if (HSPROP_LAST_BING_DATE) props[HSPROP_LAST_BING_DATE] = toEpochMillis(dateISO);

  console.log('[HS] ADD totals', { id: campaignId, addClicks, addImps, addConvs, nextClicks, nextImps, nextConvs });
  await patchCampaignProperties(campaignId, props);
  return true;
}

function getHubspotClient() {
  return {
    ensureCampaign,
    createSpendItem,
    getTotals,
    addTotals,
    findCampaignByName,
    getCampaignById,
  };
}

module.exports = { getHubspotClient };

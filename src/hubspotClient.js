// File: src/hubspotClient.js
require('dotenv').config();
const axios = require('axios');

const HUBSPOT_BASE = 'https://api.hubapi.com';
const HS_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;

// ----- Custom property names (MUST be writable custom props on HubSpot Campaign) -----
const HSPROP_TOTAL_SPEND       = envOr('HSPROP_TOTAL_SPEND',       'hs_spend_items_sum_amount'); // HS built-in sum of spend items (read-only in UI, but OK to read)
const HSPROP_TOTAL_CLICKS      = envOr('HSPROP_TOTAL_CLICKS',      'total_clicks');              // <-- your custom field (e.g. bing_click_total)
const HSPROP_TOTAL_IMPRESSIONS = envOr('HSPROP_TOTAL_IMPRESSIONS', 'total_impressions');         // <-- your custom field (e.g. bing_impression_total)
const HSPROP_TOTAL_CONVERSIONS = envOr('HSPROP_TOTAL_CONVERSIONS', 'total_conversions');         // <-- your custom field (e.g. bing_conversion_total)
const HSPROP_LAST_AVG_CPC      = envOr('HSPROP_LAST_AVG_CPC',      'avg_cpc_last');              // optional
const HSPROP_LAST_CPL          = envOr('HSPROP_LAST_CPL',          'cpl_last');                  // optional
const HSPROP_LAST_STATUS       = envOr('HSPROP_LAST_STATUS',       'bing_last_status');          // optional
const HSPROP_LAST_BING_DATE    = envOr('HSPROP_LAST_BING_DATE',    'bing_last_processed');       // optional

function envOr(name, def) {
  const v = process.env[name];
  return (v && v.trim().length) ? v.trim() : def;
}

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

// Log which props we’ll use (helps catch env mismatches)
console.log('[HS] Totals props:', {
  clicks: HSPROP_TOTAL_CLICKS,
  imps:   HSPROP_TOTAL_IMPRESSIONS,
  convs:  HSPROP_TOTAL_CONVERSIONS,
});
console.log('[HS] Last props:', {
  status:     HSPROP_LAST_STATUS,
  lastDate:   HSPROP_LAST_BING_DATE,
  lastAvgCpc: HSPROP_LAST_AVG_CPC,
  lastCpl:    HSPROP_LAST_CPL,
});

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
  // HubSpot requires unique "order" per spend item; use yyyymmdd as integer
  const order = Number(new Date(isoDate).toISOString().slice(0, 10).replace(/-/g, ''));
  const body = {
    name: `${source || 'Ads'} ${isoDate}`,
    amount: Number(amountMajor),           // major units (e.g., 23.15)
    order,
    date: toEpochMillis(isoDate),          // epoch ms for the date
  };
  const r = await axios.post(
    `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}/spend`,
    body,
    { headers: authHeaders(), validateStatus: () => true }
  );
  if (r.status === 201 || r.status === 409) return r.data; // 409 means already exists for that order
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
  return {
    clicks: toNum(p[HSPROP_TOTAL_CLICKS]),
    imps:   toNum(p[HSPROP_TOTAL_IMPRESSIONS]),
    convs:  toNum(p[HSPROP_TOTAL_CONVERSIONS]),
  };
}

/**
 * Add daily totals additively (never overwrite).
 * Reads the current HS totals, then adds the day’s numbers and PATCHes.
 */
async function addDailyTotalsAccumulative(campaignId, { clicks, impressions, conversions, dateISO }) {
  // read existing
  const cur = await getTotals(campaignId);

  // coerce increments
  const addClicks = toNum(clicks);
  const addImps   = toNum(impressions);
  const addConvs  = toNum(conversions);

  // compute new totals
  const nextTotals = {
    [HSPROP_TOTAL_CLICKS]:      cur.clicks + addClicks,
    [HSPROP_TOTAL_IMPRESSIONS]: cur.imps   + addImps,
    [HSPROP_TOTAL_CONVERSIONS]: cur.convs  + addConvs,
  };

  // housekeeping (optional)
  const housekeeping = {};
  if (HSPROP_LAST_STATUS)    housekeeping[HSPROP_LAST_STATUS]    = 'OK';
  if (HSPROP_LAST_BING_DATE) housekeeping[HSPROP_LAST_BING_DATE] = toEpochMillis(dateISO);

  const writeProps = { ...nextTotals, ...housekeeping };

  // log what we’ll write (for debugging mismatches)
  console.log('[HS] PATCH totals', {
    id: campaignId,
    write: {
      [HSPROP_TOTAL_CLICKS]: writeProps[HSPROP_TOTAL_CLICKS],
      [HSPROP_TOTAL_IMPRESSIONS]: writeProps[HSPROP_TOTAL_IMPRESSIONS],
      [HSPROP_TOTAL_CONVERSIONS]: writeProps[HSPROP_TOTAL_CONVERSIONS],
    }
  });

  await patchCampaignProperties(campaignId, writeProps);
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

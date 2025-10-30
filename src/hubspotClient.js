// File: src/hubspotClient.js
require('dotenv').config();
const axios = require('axios');

const HUBSPOT_BASE = 'https://api.hubapi.com';
const HS_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;

// Use YOUR env var names exactly as provided
const HSPROP_TOTAL_CLICKS      = process.env.HSPROP_TOTAL_CLICKS      || 'total_clicks';
const HSPROP_TOTAL_IMPRESSIONS = process.env.HSPROP_TOTAL_IMPRESSIONS || 'total_impressions';
const HSPROP_TOTAL_CONVERSIONS = process.env.HSPROP_TOTAL_CONVERSIONS || 'total_conversions';
const HSPROP_LAST_STATUS       = process.env.HSPROP_LAST_STATUS       || 'bing_last_status';
const HSPROP_LAST_BING_DATE    = process.env.HSPROP_LAST_BING_DATE    || 'bing_last_processed';

// We DO NOT write to total spend directly (HubSpot calculates hs_spend_items_sum_amount from spend items)

function authHeaders() {
  return {
    Authorization: `Bearer ${HS_TOKEN}`,
    'Content-Type': 'application/json',
  };
}

function toEpochMillis(isoYmd) {
  // Accepts "YYYY-MM-DD" (or full ISO). Returns epoch ms (number).
  const d = new Date(isoYmd.length === 10 ? `${isoYmd}T00:00:00Z` : isoYmd);
  return d.getTime();
}

// -------------------- Marketing Campaigns (v3) --------------------

async function listCampaignsPage(limit = 100, after = undefined) {
  const url = new URL(`${HUBSPOT_BASE}/marketing/v3/campaigns`);
  url.searchParams.set('limit', String(Math.max(1, Math.min(100, limit))));
  if (after != null) url.searchParams.set('after', String(after));
  const r = await axios.get(url.toString(), {
    headers: authHeaders(),
    validateStatus: () => true,
  });
  if (r.status !== 200) {
    throw new Error(`List campaigns failed (${r.status}) ${JSON.stringify(r.data)}`);
  }
  return r.data; // { results: [...], paging?: { next: { after } } }
}

async function findCampaignByName(name) {
  // Paginate client-side and match by hs_name exactly.
  let after = undefined;
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
  if (r.status !== 200) {
    throw new Error(`Get campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
  }
  return r.data; // has .properties
}

async function createCampaign(name) {
  // Minimal payload; include your custom status property for traceability
  const body = {
    properties: {
      hs_name: name,
      [HSPROP_LAST_STATUS]: 'CREATED',
    },
  };
  const url = `${HUBSPOT_BASE}/marketing/v3/campaigns`;
  const r = await axios.post(url, body, { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 201) {
    throw new Error(`Create campaign failed for "${name}" (${r.status}) ${JSON.stringify(r.data)}`);
  }
  return r.data; // includes id
}

async function ensureCampaign(name) {
  const existing = await findCampaignByName(name);
  if (existing) return existing;
  const created = await createCampaign(name);
  return created;
}

// -------------------- Spend Items --------------------

async function createSpendItem(campaignId, { isoDate, amountMinorUnits, source }) {
  // HubSpot "spend item" requires: name, amount, order, date (epoch)
  // Deterministic "order" from date for idempotency.
  const order = Number(new Date(isoDate).toISOString().slice(0,10).replace(/-/g,'')); // e.g. 20221105
  const name = `${source || 'Bing Ads'} ${isoDate}`;

  const body = {
    name,                   // string
    amount: amountMinorUnits, // integer in minor units (pence/cents)
    order,                  // integer
    date: toEpochMillis(isoDate), // epoch ms
  };

  const url = `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}/spend`;
  const r = await axios.post(url, body, { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 201) {
    throw new Error(`Create spend item failed (${r.status}) ${JSON.stringify(r.data)}`);
  }
  return r.data;
}

// -------------------- Totals Patch (custom props) --------------------

async function getTotals(campaignId) {
  const data = await getCampaignById(campaignId);
  const p = data?.properties || {};
  const clicks = Number(p[HSPROP_TOTAL_CLICKS]      || 0);
  const imps   = Number(p[HSPROP_TOTAL_IMPRESSIONS] || 0);
  const convs  = Number(p[HSPROP_TOTAL_CONVERSIONS] || 0);
  return { clicks, imps, convs };
}

async function patchCampaignProperties(campaignId, props) {
  // Props must be primitives. LAST_BING_DATE must be a numeric long (epoch ms).
  const normalized = {};
  for (const [k, v] of Object.entries(props)) {
    if (k === HSPROP_LAST_BING_DATE) {
      normalized[k] = typeof v === 'number' ? v : Number(v); // numeric long
    } else {
      normalized[k] = typeof v === 'number' ? String(v) : String(v);
    }
  }

  const body = { properties: normalized };
  const url = `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}`;
  const r = await axios.patch(url, body, { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 200) {
    throw new Error(`PATCH ${campaignId} failed (${r.status}) ${JSON.stringify(r.data)}`);
  }
  return r.data;
}

async function addDailyTotals(campaignId, { clicks, impressions, conversions, dateISO }) {
  const current = await getTotals(campaignId);
  const next = {
    [HSPROP_TOTAL_CLICKS]:      current.clicks + Number(clicks || 0),
    [HSPROP_TOTAL_IMPRESSIONS]: current.imps   + Number(impressions || 0),
    [HSPROP_TOTAL_CONVERSIONS]: current.convs  + Number(conversions || 0),
    [HSPROP_LAST_STATUS]:       'OK',
    [HSPROP_LAST_BING_DATE]:    toEpochMillis(dateISO),
  };
  return patchCampaignProperties(campaignId, next);
}

// -------------------- Export --------------------

function createHubspotClient() {
  return {
    ensureCampaign,
    findCampaignByName,
    createCampaign,
    createSpendItem,
    addDailyTotals,
    patchCampaignProperties,
    getCampaignById,
  };
}

// Legacy helper for consumers using getHubspotClient()
function getHubspotClient() {
  return createHubspotClient();
}

module.exports = {
  createHubspotClient,
  getHubspotClient,
  // export property names for logging if needed
  HSPROP_TOTAL_CLICKS,
  HSPROP_TOTAL_IMPRESSIONS,
  HSPROP_TOTAL_CONVERSIONS,
  HSPROP_LAST_STATUS,
  HSPROP_LAST_BING_DATE,
};

// File: src/hubspotClient.js
require('dotenv').config();
const axios = require('axios');

const HUBSPOT_BASE = 'https://api.hubapi.com';
const HS_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;

// ---- property names (from .env) ----
const HSPROP_TOTAL_SPEND       = process.env.HSPROP_TOTAL_SPEND       || 'hs_spend_items_sum_amount';
const HSPROP_TOTAL_CLICKS      = process.env.HSPROP_TOTAL_CLICKS      || 'bing_click_total';
const HSPROP_TOTAL_IMPRESSIONS = process.env.HSPROP_TOTAL_IMPRESSIONS || 'bing_impression_total';
const HSPROP_TOTAL_CONVERSIONS = process.env.HSPROP_TOTAL_CONVERSIONS || 'bing_conversion_total';

const HSPROP_LAST_AVG_CPC      = process.env.HSPROP_LAST_AVG_CPC      || 'avg_cpc_last';
const HSPROP_LAST_CPL          = process.env.HSPROP_LAST_CPL          || 'cpl_last';
const HSPROP_LAST_STATUS       = process.env.HSPROP_LAST_STATUS       || 'bing_last_status';
const HSPROP_LAST_BING_DATE    = process.env.HSPROP_LAST_BING_DATE    || 'bing_last_processed';

// ---- helpers ----
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
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

// ---- log which props are active ----
console.log('[HS] Totals props:', {
  clicks: HSPROP_TOTAL_CLICKS,
  imps:   HSPROP_TOTAL_IMPRESSIONS,
  convs:  HSPROP_TOTAL_CONVERSIONS,
});
console.log('[HS] Last props:', {
  status:   HSPROP_LAST_STATUS,
  lastDate: HSPROP_LAST_BING_DATE,
  lastAvgCpc: HSPROP_LAST_AVG_CPC,
  lastCpl:    HSPROP_LAST_CPL,
});

/* ------------------ Campaign helpers ------------------ */

async function listCampaignsPage(limit = 100, after, { archived = false } = {}) {
  // Clamp limit to HubSpot’s allowed range 1..100
  const lim = Math.max(1, Math.min(100, Number(limit) || 100));
  const url = new URL(`${HUBSPOT_BASE}/marketing/v3/campaigns`);
  url.searchParams.set('limit', String(lim));
  if (after) url.searchParams.set('after', String(after));
  // Some HubSpot resources support archived param; include it and try both ways.
  url.searchParams.set('archived', archived ? 'true' : 'false');

  const r = await axios.get(url.toString(), {
    headers: authHeaders(),
    validateStatus: () => true,
  });
  if (r.status !== 200) {
    throw new Error(`List campaigns failed (${r.status}) ${JSON.stringify(r.data)}`);
  }
  return r.data;
}

async function findCampaignByNameOnce(name, { archived = false, maxPages = 50 } = {}) {
  let after;
  let pages = 0;
  const needle = String(name || '').trim().toLowerCase();

  while (pages < maxPages) {
    const data = await listCampaignsPage(100, after, { archived });
    const found = (data.results || []).find(c => {
      const hsName = (c?.properties?.hs_name || '').trim().toLowerCase();
      return hsName === needle;
    });
    if (found) return found;
    after = data?.paging?.next?.after;
    pages += 1;
    if (!after) break;
  }
  return null;
}

async function findCampaignByName(name, { maxSweeps = 1, maxPages = 50 } = {}) {
  // Try non-archived first, then archived. Repeat up to maxSweeps.
  for (let sweep = 0; sweep < maxSweeps; sweep++) {
    const live = await findCampaignByNameOnce(name, { archived: false, maxPages });
    if (live) return live;
    const archived = await findCampaignByNameOnce(name, { archived: true, maxPages });
    if (archived) return archived;
  }
  return null;
}

async function getCampaignById(id) {
  const r = await axios.get(`${HUBSPOT_BASE}/marketing/v3/campaigns/${id}`, {
    headers: authHeaders(),
    validateStatus: () => true,
  });
  if (r.status !== 200) {
    throw new Error(`Get campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
  }
  return r.data;
}

async function createCampaign(name) {
  const props = { hs_name: name };
  if (HSPROP_LAST_STATUS) props[HSPROP_LAST_STATUS] = 'CREATED';

  const r = await axios.post(`${HUBSPOT_BASE}/marketing/v3/campaigns`, { properties: props }, {
    headers: authHeaders(),
    validateStatus: () => true,
  });
  if (r.status === 201) return r.data;

  const err = new Error(`Create campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
  err.status = r.status;
  throw err;
}

async function ensureCampaign(name) {
  // 1) Try to find first (cheap)
  const pre = await findCampaignByName(name, { maxSweeps: 1, maxPages: 50 });
  if (pre) return pre;

  // 2) Try to create
  try {
    const created = await createCampaign(name);
    return created;
  } catch (e) {
    // 3) If 409 (already exists), retry find with multiple sweeps & increasing delays
    if (e && e.status === 409) {
      const maxRetries = 15;            // ~45-60s total
      for (let i = 0; i < maxRetries; i++) {
        const delay = 1000 + i * 250;   // backoff
        await sleep(delay);
        const again = await findCampaignByName(name, { maxSweeps: 2, maxPages: 60 });
        if (again) return again;
      }
      throw new Error(`Create campaign 409 but could not find "${name}" via list after retries`);
    }
    throw e;
  }
}

/* ------------------ Spend items ------------------ */

async function createSpendItem(campaignId, { isoDate, amountMajor, source }) {
  // 'order' stable per-day to dedupe
  const order = Number(new Date(isoDate).toISOString().slice(0, 10).replace(/-/g, ''));
  const body = {
    name: `${source || 'Ads'} ${isoDate}`,
    amount: Number(Number(amountMajor).toFixed(2)), // major units, 2dp
    order,
    date: toEpochMillis(isoDate),
  };
  const r = await axios.post(
    `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}/spend`,
    body,
    { headers: authHeaders(), validateStatus: () => true }
  );
  if (r.status === 201 || r.status === 409) return r.data; // 409 = already exists
  throw new Error(`Create spend item failed (${r.status}) ${JSON.stringify(r.data)}`);
}

/* ------------------ Totals helpers ------------------ */

async function patchCampaignProperties(campaignId, props) {
  const r = await axios.patch(
    `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}`,
    { properties: props },
    { headers: authHeaders(), validateStatus: () => true }
  );
  if (r.status !== 200) {
    throw new Error(`PATCH ${campaignId} failed (${r.status}) ${JSON.stringify(r.data)}`);
  }
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

async function addTotals(campaignId, deltas, dateISO) {
  const current = await getTotals(campaignId);
  const next = {
    clicks: (current.clicks || 0) + toNum(deltas.clicks),
    imps:   (current.imps   || 0) + toNum(deltas.impressions),
    convs:  (current.convs  || 0) + toNum(deltas.conversions),
  };

  const props = {
    [HSPROP_TOTAL_CLICKS]:      next.clicks,
    [HSPROP_TOTAL_IMPRESSIONS]: next.imps,
    [HSPROP_TOTAL_CONVERSIONS]: next.convs,
  };
  if (HSPROP_LAST_STATUS)    props[HSPROP_LAST_STATUS] = 'OK';
  if (HSPROP_LAST_BING_DATE) props[HSPROP_LAST_BING_DATE] = toEpochMillis(dateISO);

  console.log('[HS] ADD totals', {
    id: campaignId,
    delta: {
      clicks: toNum(deltas.clicks),
      imps:   toNum(deltas.impressions),
      convs:  toNum(deltas.conversions),
    },
    write: {
      clicks: props[HSPROP_TOTAL_CLICKS],
      imps:   props[HSPROP_TOTAL_IMPRESSIONS],
      convs:  props[HSPROP_TOTAL_CONVERSIONS],
    }
  });

  await patchCampaignProperties(campaignId, props);
  return next;
}

async function setTotalsDirect(campaignId, totals, dateISO) {
  const props = {
    [HSPROP_TOTAL_CLICKS]:      Number(totals.clicks || 0),
    [HSPROP_TOTAL_IMPRESSIONS]: Number(totals.impressions || 0),
    [HSPROP_TOTAL_CONVERSIONS]: Number(totals.conversions || 0),
  };
  if (HSPROP_LAST_STATUS)    props[HSPROP_LAST_STATUS] = 'OK';
  if (HSPROP_LAST_BING_DATE) props[HSPROP_LAST_BING_DATE] = toEpochMillis(dateISO);

  console.log('[HS] SET totals (direct)', { id: campaignId, write: props });
  await patchCampaignProperties(campaignId, props);
  return true;
}

function getHubspotClient() {
  return {
    // campaign
    ensureCampaign,
    findCampaignByName,
    getCampaignById,

    // spend
    createSpendItem,

    // totals
    getTotals,
    addTotals,
    setTotalsDirect,
  };
}

module.exports = { getHubspotClient };

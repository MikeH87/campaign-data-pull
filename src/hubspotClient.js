// File: src/hubspotClient.js
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const HUBSPOT_BASE = 'https://api.hubapi.com';
const HS_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;

// ---- property names (from .env) ----
// IMPORTANT: these must match your custom props in HubSpot
const HSPROP_TOTAL_SPEND       = process.env.HSPROP_TOTAL_SPEND       || 'hs_spend_items_sum_amount';
const HSPROP_TOTAL_CLICKS      = process.env.HSPROP_TOTAL_CLICKS      || 'bing_click_total';
const HSPROP_TOTAL_IMPRESSIONS = process.env.HSPROP_TOTAL_IMPRESSIONS || 'bing_impression_total';
const HSPROP_TOTAL_CONVERSIONS = process.env.HSPROP_TOTAL_CONVERSIONS || 'bing_conversion_total';
const HSPROP_LAST_AVG_CPC      = process.env.HSPROP_LAST_AVG_CPC      || 'avg_cpc_last';     // optional
const HSPROP_LAST_CPL          = process.env.HSPROP_LAST_CPL          || 'cpl_last';         // optional
const HSPROP_LAST_STATUS       = process.env.HSPROP_LAST_STATUS       || 'bing_last_status';
const HSPROP_LAST_BING_DATE    = process.env.HSPROP_LAST_BING_DATE    || 'bing_last_processed';

console.log('[HS] Totals props:', {
  clicks: HSPROP_TOTAL_CLICKS,
  imps:   HSPROP_TOTAL_IMPRESSIONS,
  convs:  HSPROP_TOTAL_CONVERSIONS
});
console.log('[HS] Last props:', {
  status:   HSPROP_LAST_STATUS,
  lastDate: HSPROP_LAST_BING_DATE,
  lastAvgCpc: HSPROP_LAST_AVG_CPC,
  lastCpl:    HSPROP_LAST_CPL,
});

function authHeaders() {
  return {
    Authorization: `Bearer ${HS_TOKEN}`,
    'Content-Type': 'application/json',
  };
}

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

function toEpochMillis(isoYmd) {
  // isoYmd "YYYY-MM-DD" or full ISO
  const d = new Date(isoYmd.length === 10 ? `${isoYmd}T00:00:00Z` : isoYmd);
  return d.getTime();
}

function toNum(v) {
  if (v === null || v === undefined || v === '') return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

/* ------------------ local campaign map cache ------------------ */
const MAP_PATH = path.resolve(process.cwd(), 'campaignMap.json');

function readMap() {
  try {
    const raw = fs.readFileSync(MAP_PATH, 'utf8');
    return JSON.parse(raw);
  } catch {
    return { byName: {} };
  }
}

function writeMap(map) {
  try {
    fs.writeFileSync(MAP_PATH, JSON.stringify(map, null, 2), 'utf8');
  } catch {}
}

function mapGetId(name) {
  const map = readMap();
  return map.byName?.[name] || null;
}

function mapSetId(name, id) {
  const map = readMap();
  if (!map.byName) map.byName = {};
  map.byName[name] = id;
  writeMap(map);
}

/* ------------------ Campaign helpers (HubSpot) ------------------ */

async function listCampaignsPage(limit = 100, after) {
  const url = new URL(`${HUBSPOT_BASE}/marketing/v3/campaigns`);
  url.searchParams.set('limit', String(limit));
  if (after) url.searchParams.set('after', String(after));
  const r = await axios.get(url.toString(), { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 200) throw new Error(`List campaigns failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}

async function findCampaignByName(name, maxPages = 15) {
  // First, check the local cache (fast path)
  const cached = mapGetId(name);
  if (cached) {
    try {
      const obj = await getCampaignById(cached);
      const hsName = obj?.properties?.hs_name || '';
      if (hsName === name) return obj; // valid cache hit
    } catch (_) {
      // cache stale → fall through to listing
    }
  }

  let after;
  let pages = 0;
  while (true) {
    pages++;
    const data = await listCampaignsPage(100, after);
    const found = (data.results || []).find(c => (c.properties?.hs_name || '') === name);
    if (found) {
      mapSetId(name, found.id);
      return found;
    }
    after = data?.paging?.next?.after;
    if (!after || pages >= maxPages) return null;
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
  const body = { properties: { hs_name: name, ...(HSPROP_LAST_STATUS ? {[HSPROP_LAST_STATUS]:'CREATED'} : {}) } };
  const r = await axios.post(`${HUBSPOT_BASE}/marketing/v3/campaigns`, body, {
    headers: authHeaders(),
    validateStatus: () => true,
  });
  if (r.status === 201) return r.data;
  if (r.status === 409) {
    // exists → let caller handle follow-up listing
    const err = new Error(`Create campaign 409 for "${name}"`);
    err.status = 409;
    err.data = r.data;
    throw err;
  }
  throw new Error(`Create campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
}

/**
 * Resolve a campaign ID for a given name, creating if needed.
 * Very defensive against 409/list races.
 */
async function ensureCampaignIdForName(name) {
  // 1) fast path via cache
  const cached = mapGetId(name);
  if (cached) {
    try {
      const obj = await getCampaignById(cached);
      if ((obj?.properties?.hs_name || '') === name) return obj.id;
    } catch {}
  }

  // 2) try listing first (avoid unnecessary create)
  const foundPre = await findCampaignByName(name);
  if (foundPre?.id) {
    mapSetId(name, foundPre.id);
    return foundPre.id;
  }

  // 3) attempt create
  try {
    const created = await createCampaign(name);
    mapSetId(name, created.id);
    return created.id;
  } catch (e) {
    if (e && e.status === 409) {
      // 4) list a few times with backoff — the campaign exists already
      for (let i = 0; i < 6; i++) {
        await sleep(1000 + i * 500);
        const found = await findCampaignByName(name, 25);
        if (found?.id) {
          mapSetId(name, found.id);
          return found.id;
        }
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
    amount: Number(amountMajor), // major units (e.g. GBP)
    order,
    date: toEpochMillis(isoDate),
  };
  const r = await axios.post(
    `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}/spend`,
    body,
    { headers: authHeaders(), validateStatus: () => true }
  );
  if (r.status === 201 || r.status === 409) return r.data; // 409 = already exists (same order)
  throw new Error(`Create spend item failed (${r.status}) ${JSON.stringify(r.data)}`);
}

/* ------------------ Totals helpers (ADD, not overwrite) ------------------ */

async function patchCampaignProperties(campaignId, props) {
  const r = await axios.patch(
    `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}`,
    { properties: props },
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
 * Add deltas to existing totals (robust additive update).
 */
async function addTotalsDelta(campaignId, { clicks, impressions, conversions }, dateISO) {
  const current = await getTotals(campaignId);
  const next = {
    [HSPROP_TOTAL_CLICKS]:      toNum(current.clicks) + toNum(clicks),
    [HSPROP_TOTAL_IMPRESSIONS]: toNum(current.imps)   + toNum(impressions),
    [HSPROP_TOTAL_CONVERSIONS]: toNum(current.convs)  + toNum(conversions),
  };
  if (HSPROP_LAST_STATUS)       next[HSPROP_LAST_STATUS]    = 'OK';
  if (HSPROP_LAST_BING_DATE)    next[HSPROP_LAST_BING_DATE] = toEpochMillis(dateISO);

  // optional “last” metrics if you want to keep them
  if (HSPROP_LAST_AVG_CPC && toNum(clicks) > 0) {
    const spend = 0; // leave if you don’t track last-day spend here
    // You could compute and set avg CPC last if needed later.
  }
  if (HSPROP_LAST_CPL && toNum(conversions) > 0) {
    // Similarly for CPL.
  }

  // Log to prove ADD, not overwrite
  console.log('[HS] ADD totals', {
    id: campaignId,
    add: { clicks: toNum(clicks), imps: toNum(impressions), convs: toNum(conversions) },
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
    ensureCampaignIdForName,
    createSpendItem,
    addTotalsDelta,
    getTotals,
  };
}

module.exports = { getHubspotClient };

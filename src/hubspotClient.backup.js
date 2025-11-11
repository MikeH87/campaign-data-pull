// File: src/hubspotClient.js
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const HUBSPOT_BASE = 'https://api.hubapi.com';
const HS_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;

// ---- property names (from .env, with your known defaults) ----
const HSPROP_TOTAL_SPEND       = process.env.HSPROP_TOTAL_SPEND       || 'hs_spend_items_sum_amount';
const HSPROP_TOTAL_CLICKS      = process.env.HSPROP_TOTAL_CLICKS      || 'bing_click_total';
const HSPROP_TOTAL_IMPRESSIONS = process.env.HSPROP_TOTAL_IMPRESSIONS || 'bing_impression_total';
const HSPROP_TOTAL_CONVERSIONS = process.env.HSPROP_TOTAL_CONVERSIONS || 'bing_conversion_total';

const HSPROP_LAST_AVG_CPC      = process.env.HSPROP_LAST_AVG_CPC      || 'avg_cpc_last';
const HSPROP_LAST_CPL          = process.env.HSPROP_LAST_CPL          || 'cpl_last';
const HSPROP_LAST_STATUS       = process.env.HSPROP_LAST_STATUS       || 'bing_last_status';
const HSPROP_LAST_BING_DATE    = process.env.HSPROP_LAST_BING_DATE    || 'bing_last_processed';

// on-disk name->id cache (keeps retries short and stabilises 409 races)
const MAP_FILE = path.resolve(process.cwd(), 'campaign-map.json');

function authHeaders() {
  return {
    Authorization: `Bearer ${HS_TOKEN}`,
    'Content-Type': 'application/json',
    Accept: 'application/json',
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

// ---- log which props we’re using (visible at startup)
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

// ------------ tiny local cache helpers ------------
function loadMap() {
  try {
    const raw = fs.readFileSync(MAP_FILE, 'utf8');
    return JSON.parse(raw);
  } catch {
    return {};
  }
}
function saveMap(map) {
  fs.writeFileSync(MAP_FILE, JSON.stringify(map, null, 2));
}
function getMappedId(name) {
  const m = loadMap();
  return m[name] || null;
}
function setMapping(name, id) {
  const m = loadMap();
  m[name] = id;
  saveMap(m);
}
function deleteMapping(name) {
  const m = loadMap();
  if (m[name]) {
    delete m[name];
    saveMap(m);
  }
}

// ------------------ Campaign helpers ------------------
async function listCampaignsPage(limit = 100, after) {
  const url = new URL(`${HUBSPOT_BASE}/marketing/v3/campaigns`);
  url.searchParams.set('limit', String(Math.max(1, Math.min(100, limit))));
  if (after) url.searchParams.set('after', String(after));
  const r = await axios.get(url.toString(), { headers: authHeaders(), validateStatus: () => true });
  if (r.status !== 200) {
    throw new Error(`List campaigns failed (${r.status}) ${JSON.stringify(r.data)}`);
  }
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
  if (r.status === 404) throw Object.assign(new Error('Campaign 404'), { code: 404 });
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
  if (r.status === 409) {
    // Name exists — not an error; we’ll resolve actual id via list & map
    const err = new Error('409 name conflict');
    err.code = 409;
    err.payload = r.data;
    throw err;
  }
  throw new Error(`Create campaign failed (${r.status}) ${JSON.stringify(r.data)}`);
}

/**
 * Robust ensure:
 * 1) use local map if present
 * 2) list by name
 * 3) create; on 201 -> map id
 * 4) if 409 -> retry list a few times (eventual consistency) & then map id
 */
async function ensureCampaign(name) {
  // 1) map
  const mapped = getMappedId(name);
  if (mapped) {
    try {
      const c = await getCampaignById(mapped);
      return c;
    } catch (e) {
      if (e && e.code === 404) {
        deleteMapping(name);
      } else {
        throw e;
      }
    }
  }

  // 2) list by name
  const existed = await findCampaignByName(name);
  if (existed?.id) {
    setMapping(name, existed.id);
    return existed;
  }

  // 3) create
  try {
    const created = await createCampaign(name);
    if (created?.id) {
      setMapping(name, created.id);
    }
    return created;
  } catch (e) {
    if (e && e.code === 409) {
      // 4) name conflict: someone (or earlier run) created it; resolve by retry-list
      for (let i = 0; i < 6; i++) {
        await sleep(1500 + i * 500);
        const again = await findCampaignByName(name);
        if (again?.id) {
          setMapping(name, again.id);
          return again;
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
 * ADD to totals — this is the safe path to avoid overwriting:
 * - reads current values
 * - adds deltas
 * - writes back
 * - updates status + last date
 */
async function addToTotals(campaignId, { addClicks = 0, addImps = 0, addConvs = 0 }, dateISO) {
  const cur = await getTotals(campaignId);
  const next = {
    [HSPROP_TOTAL_CLICKS]:      toNum(cur.clicks) + toNum(addClicks),
    [HSPROP_TOTAL_IMPRESSIONS]: toNum(cur.imps)   + toNum(addImps),
    [HSPROP_TOTAL_CONVERSIONS]: toNum(cur.convs)  + toNum(addConvs),
  };
  if (HSPROP_LAST_STATUS)    next[HSPROP_LAST_STATUS] = 'OK';
  if (HSPROP_LAST_BING_DATE) next[HSPROP_LAST_BING_DATE] = toEpochMillis(dateISO);

  // (Optional) keep lastAvgCpc/lastCpl up-to-date if you want — left untouched here

  // Debug line so we can see precisely what we write
  console.log('[HS] PATCH totals (additive)', {
    id: campaignId,
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
    createSpendItem,
    getTotals,
    addToTotals,         // IMPORTANT: use this for accumulating
    findCampaignByName,
    getCampaignById,
  };
}

module.exports = { getHubspotClient };

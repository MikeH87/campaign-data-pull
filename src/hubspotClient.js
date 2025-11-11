require('dotenv').config();
const axios = require('axios');

const HUBSPOT_BASE = 'https://api.hubapi.com';
const HS_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;

// ---- property names from .env (use your existing names) ----
const HSPROP_TOTAL_SPEND       = process.env.HSPROP_TOTAL_SPEND       || 'hs_spend_items_sum_amount';
const HSPROP_TOTAL_CLICKS      = process.env.HSPROP_TOTAL_CLICKS      || 'bing_click_total';
const HSPROP_TOTAL_IMPRESSIONS = process.env.HSPROP_TOTAL_IMPRESSIONS || 'bing_impression_total';
const HSPROP_TOTAL_CONVERSIONS = process.env.HSPROP_TOTAL_CONVERSIONS || 'bing_conversion_total';
const HSPROP_LAST_AVG_CPC      = process.env.HSPROP_LAST_AVG_CPC      || 'avg_cpc_last';
const HSPROP_LAST_CPL          = process.env.HSPROP_LAST_CPL          || 'cpl_last';
const HSPROP_LAST_STATUS       = process.env.HSPROP_LAST_STATUS       || 'bing_last_status';
const HSPROP_LAST_BING_DATE    = process.env.HSPROP_LAST_BING_DATE    || 'bing_last_processed';

// log which props are active (useful for debugging)
console.log('[HS] Totals props:', {
  clicks: HSPROP_TOTAL_CLICKS,
  imps:   HSPROP_TOTAL_IMPRESSIONS,
  convs:  HSPROP_TOTAL_CONVERSIONS,
});
console.log('[HS] Last props:', {
  status:   HSPROP_LAST_STATUS,
  lastDate: HSPROP_LAST_BING_DATE,
  lastAvgCpc: HSPROP_LAST_AVG_CPC,
  lastCpl:  HSPROP_LAST_CPL,
});

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
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/* ------------------ Campaign helpers ------------------ */

async function listCampaignsPage(limit = 100, after) {
  const url = new URL(`${HUBSPOT_BASE}/marketing/v3/campaigns`);
  url.searchParams.set('limit', String(Math.min(Math.max(1, limit), 100)));
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
  const body = { properties: { hs_name: name, ...(HSPROP_LAST_STATUS ? { [HSPROP_LAST_STATUS]: 'CREATED' } : {}) } };
  const r = await axios.post(`${HUBSPOT_BASE}/marketing/v3/campaigns`, body, {
    headers: authHeaders(),
    validateStatus: () => true,
  });
  return r;
}

/**
 * Ensure campaign by name (create if missing).
 * Handles 409 then re-read by name with a few retries to tolerate eventual consistency.
 */
async function ensureCampaign(name) {
  const existing = await findCampaignByName(name);
  if (existing) return existing;

  const resp = await createCampaign(name);
  if (resp.status === 201) return resp.data;

  if (resp.status === 409) {
    for (let i = 0; i < 5; i++) {
      await sleep(750 + i * 250);
      const again = await findCampaignByName(name);
      if (again) return again;
    }
    throw new Error(`Create campaign 409 but could not find "${name}" via list after retries`);
  }

  throw new Error(`Create campaign failed (${resp.status}) ${JSON.stringify(resp.data)}`);
}

/* ------------------ Spend items ------------------ */

async function createSpendItem(campaignId, { isoDate, amountMajor, source }) {
  // Use date (yyyymmdd) as a stable order key so day is idempotent.
  const order = Number(new Date(isoDate).toISOString().slice(0, 10).replace(/-/g, ''));
  const amt = Math.round(Number(amountMajor) * 100) / 100; // major units

  const body = {
    name: `${source || 'Ads'} ${isoDate}`,
    amount: amt,         // <-- major units (e.g. 23.15)
    order,
    date: toEpochMillis(isoDate),
  };

  const r = await axios.post(
    `${HUBSPOT_BASE}/marketing/v3/campaigns/${campaignId}/spend`,
    body,
    { headers: authHeaders(), validateStatus: () => true }
  );
  if (r.status === 201 || r.status === 409) return r.data; // 409 = spend item with same order exists
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

async function getProperties(campaignId) {
  const data = await getCampaignById(campaignId);
  return data?.properties || {};
}

/**
 * Idempotent: adds today's numbers ONCE per date.
 * If HSPROP_LAST_BING_DATE >= isoDate, it skips (already processed).
 */
async function addDailyTotalsIfNew(campaignId, { clicks = 0, impressions = 0, conversions = 0 }, isoDate) {
  const props = await getProperties(campaignId);

  const lastProcessedMillis = toNum(props[HSPROP_LAST_BING_DATE]);
  const todayMillis = toEpochMillis(isoDate);

  if (lastProcessedMillis >= todayMillis) {
    console.log(`[HS] Skip totals (already processed): id=${campaignId} date=${isoDate}`);
    return { skipped: true };
  }

  const prevClicks = toNum(props[HSPROP_TOTAL_CLICKS]);
  const prevImps   = toNum(props[HSPROP_TOTAL_IMPRESSIONS]);
  const prevConvs  = toNum(props[HSPROP_TOTAL_CONVERSIONS]);

  const nextClicks = prevClicks + toNum(clicks);
  const nextImps   = prevImps   + toNum(impressions);
  const nextConvs  = prevConvs  + toNum(conversions);

  const write = {
    [HSPROP_TOTAL_CLICKS]:      nextClicks,
    [HSPROP_TOTAL_IMPRESSIONS]: nextImps,
    [HSPROP_TOTAL_CONVERSIONS]: nextConvs,
  };
  if (HSPROP_LAST_STATUS)    write[HSPROP_LAST_STATUS] = 'OK';
  if (HSPROP_LAST_BING_DATE) write[HSPROP_LAST_BING_DATE] = todayMillis;

  console.log('[HS] PATCH totals (add mode)', {
    id: campaignId,
    prev: { clicks: prevClicks, imps: prevImps, convs: prevConvs },
    add:  { clicks, impressions, conversions },
    next: { clicks: nextClicks, imps: nextImps, convs: nextConvs },
  });

  await patchCampaignProperties(campaignId, write);
  return { updated: true };
}

function getHubspotClient() {
  return {
    ensureCampaign,
    createSpendItem,
    addDailyTotalsIfNew,
    getCampaignById,
    findCampaignByName,
  };
}

module.exports = { getHubspotClient };

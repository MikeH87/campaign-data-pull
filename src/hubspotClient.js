require('dotenv').config();
const fs = require('fs');
const path = require('path');
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

function authHeaders() {
  return {
    Authorization: 'Bearer ' + HS_TOKEN,
    'Content-Type': 'application/json',
    Accept: 'application/json',
  };
}

function toEpochMillis(isoYmd) {
  const d = new Date(isoYmd.length === 10 ? isoYmd + 'T00:00:00Z' : isoYmd);
  return d.getTime();
}

function toNum(v) {
  if (v === null || v === undefined || v === '') return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

/* ------------------ name -> marketing ID map ------------------ */

const MAP_PATH = path.resolve(process.cwd(), 'campaign-map.json');

function loadNameToIdMap() {
  const raw = fs.readFileSync(MAP_PATH, 'utf8');
  return JSON.parse(raw) || {};
}

function ensureCampaignIdForName(name) {
  const map = loadNameToIdMap();
  const id = map[name];
  if (!id) {
    throw new Error('Campaign "' + name + '" not found in campaign-map.json');
  }
  return id;
}

/* ------------------ Marketing campaign helpers ------------------ */

function buildCampaignUrlWithProps(id) {
  const props = [
    HSPROP_TOTAL_CLICKS,
    HSPROP_TOTAL_IMPRESSIONS,
    HSPROP_TOTAL_CONVERSIONS,
    HSPROP_LAST_STATUS,
    HSPROP_LAST_BING_DATE
  ].filter(Boolean);
  const qs = props.length
    ? '?properties=' + props.map(encodeURIComponent).join(',')
    : '';
  return HUBSPOT_BASE + '/marketing/v3/campaigns/' + encodeURIComponent(id) + qs;
}

async function getCampaignById(id) {
  const url = buildCampaignUrlWithProps(id);
  const r = await axios.get(url, {
    headers: authHeaders(),
    validateStatus: () => true
  });
  if (r.status !== 200) {
    throw new Error('Get campaign failed (' + r.status + ') ' + JSON.stringify(r.data));
  }
  return r.data;
}

async function patchCampaignProperties(campaignId, props) {
  const r = await axios.patch(
    HUBSPOT_BASE + '/marketing/v3/campaigns/' + encodeURIComponent(campaignId),
    { properties: props },
    { headers: authHeaders(), validateStatus: () => true }
  );
  if (r.status !== 200) {
    throw new Error('PATCH ' + campaignId + ' failed (' + r.status + ') ' + JSON.stringify(r.data));
  }
  return r.data;
}

async function getTotals(campaignId) {
  const data = await getCampaignById(campaignId);
  const p = (data && data.properties) || {};
  return {
    clicks: toNum(p[HSPROP_TOTAL_CLICKS]),
    imps:   toNum(p[HSPROP_TOTAL_IMPRESSIONS]),
    convs:  toNum(p[HSPROP_TOTAL_CONVERSIONS]),
  };
}

/* ------------------ Spend items ------------------ */

async function createSpendItem(campaignId, opts) {
  const isoDate = opts.isoDate;
  const amountMajor = opts.amountMajor;
  const source = opts.source || 'Bing';

  const order = Number(new Date(isoDate).toISOString().slice(0, 10).replace(/-/g, ''));

  const body = {
    name: source + ' ' + isoDate,
    amount: Number(amountMajor),
    order: order,
    date: toEpochMillis(isoDate),
  };

  const r = await axios.post(
    HUBSPOT_BASE + '/marketing/v3/campaigns/' + encodeURIComponent(campaignId) + '/spend',
    body,
    { headers: authHeaders(), validateStatus: () => true }
  );

  if (r.status === 201 || r.status === 409) {
    return r.data; // 201 created, 409 already existed
  }

  throw new Error('Create spend item failed (' + r.status + ') ' + JSON.stringify(r.data));
}

/* ------------------ Totals (additive) ------------------ */

async function addTotalsDelta(campaignId, metrics, dateISO) {
  const clicks = metrics.clicks || 0;
  const impressions = metrics.impressions || 0;
  const conversions = metrics.conversions || 0;

  const current = await getTotals(campaignId);

  const next = {};
  next[HSPROP_TOTAL_CLICKS]      = toNum(current.clicks) + toNum(clicks);
  next[HSPROP_TOTAL_IMPRESSIONS] = toNum(current.imps)   + toNum(impressions);
  next[HSPROP_TOTAL_CONVERSIONS] = toNum(current.convs)  + toNum(conversions);

  if (HSPROP_LAST_STATUS)    next[HSPROP_LAST_STATUS]    = 'OK';
  if (HSPROP_LAST_BING_DATE) next[HSPROP_LAST_BING_DATE] = toEpochMillis(dateISO);

  console.log('[HS] ADD totals (marketing)', {
    id: campaignId,
    prev: { clicks: current.clicks, imps: current.imps, convs: current.convs },
    add:  { clicks: toNum(clicks),   imps: toNum(impressions), convs: toNum(conversions) },
    write: {
      clicks: next[HSPROP_TOTAL_CLICKS],
      imps:   next[HSPROP_TOTAL_IMPRESSIONS],
      convs:  next[HSPROP_TOTAL_CONVERSIONS],
    }
  });

  await patchCampaignProperties(campaignId, next);
  return true;
}

/* ------------------ Factory ------------------ */

function getHubspotClient() {
  return {
    ensureCampaignIdForName,
    createSpendItem,
    addTotalsDelta,
    getTotals,
  };
}

module.exports = { getHubspotClient };
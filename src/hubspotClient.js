// src/hubspotClient.js
require('dotenv').config();
const axios = require('axios');

const TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!TOKEN) throw new Error('Missing HUBSPOT_PRIVATE_APP_TOKEN');

const h = axios.create({
  baseURL: 'https://api.hubapi.com',
  headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
  validateStatus: () => true
});

async function getHubspotClient() { return h; }

async function listCampaignsByName(name) {
  // Search campaigns by name via marketing/v3/campaigns?name__contains
  const res = await h.get('/marketing/v3/campaigns', { params: { q: name, limit: 50 } });
  if (res.status !== 200) throw new Error(`List campaigns failed (${res.status}) ${JSON.stringify(res.data)}`);
  return (res.data?.results || []).filter(c => c.properties?.hs_name === name || c.properties?.name === name);
}

async function createCampaign(name) {
  const body = { properties: { hs_name: name } };
  const res = await h.post('/marketing/v3/campaigns', body);
  if (res.status !== 201) throw new Error(`Create campaign failed (${res.status}) ${JSON.stringify(res.data)}`);
  return res.data.id;
}

async function ensureCampaignByName(name, { dryRun } = {}) {
  const found = await listCampaignsByName(name);
  if (found.length > 0) return found[0].id;
  if (dryRun) {
    console.log(`[DRY] would create HubSpot campaign "${name}"`);
    return 'dry-run-id';
  }
  const id = await createCampaign(name);
  console.log(`Created campaign "${name}" -> ${id}`);
  return id;
}

async function addSpendItemMajor(campaignId, name, amountMajor, dateYMD) {
  // Create spend item: POST /marketing/v3/campaigns/{id}/spend
  // amount is major units
  const body = {
    name,
    amount: Number(amountMajor.toFixed(2)),
    occurredAt: `${dateYMD}T00:00:00Z`,
    order: 'CHANNEL',
  };
  const res = await h.post(`/marketing/v3/campaigns/${campaignId}/spend`, body);
  if (res.status !== 201) throw new Error(`Create spend item failed (${res.status}) ${JSON.stringify(res.data)}`);
  return res.data;
}

async function patchCampaign(id, properties) {
  const res = await h.patch(`/marketing/v3/campaigns/${id}`, { properties });
  if (res.status !== 200) throw new Error(`PATCH ${id} failed (${res.status}) ${JSON.stringify(res.data)}`);
  return res.data;
}

async function incrementTotals(id, propMap, deltas) {
  // Read current values, add, write back to *channel-specific* props
  // We will fetch the object first:
  const get = await h.get(`/marketing/v3/campaigns/${id}`);
  if (get.status !== 200) throw new Error(`GET ${id} failed (${get.status}) ${JSON.stringify(get.data)}`);
  const props = get.data?.properties || {};

  const curClicks = Number(props[propMap.totals.clicks] || 0);
  const curImps   = Number(props[propMap.totals.impressions] || 0);
  const curConvs  = Number(props[propMap.totals.conversions] || 0);

  const next = {};
  next[propMap.totals.clicks] = curClicks + (deltas.clicks || 0);
  next[propMap.totals.impressions] = curImps + (deltas.impressions || 0);
  next[propMap.totals.conversions] = curConvs + (deltas.conversions || 0);

  return patchCampaign(id, next);
}

async function setLastProps(id, propMap, last) {
  const body = {};
  if (last.avg_cpc != null) body[propMap.last.avg_cpc] = Number(last.avg_cpc.toFixed(2));
  if (last.cpl != null) body[propMap.last.cpl] = Number(last.cpl.toFixed(2));
  if (last.status) body[propMap.last.status] = String(last.status);
  if (last.processedYMD) {
    // HubSpot date properties expect millis since epoch; we send yyyy-mm-dd as millis midnight UTC
    const ms = Date.parse(`${last.processedYMD}T00:00:00Z`);
    body[propMap.last.processed] = ms;
  }
  return patchCampaign(id, body);
}

module.exports = {
  getHubspotClient,
  ensureCampaignByName,
  addSpendItemMajor,
  incrementTotals,
  setLastProps,
};

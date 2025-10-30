// src/hubspotMarketing.js
// Minimal helper for HubSpot Marketing Campaigns v3 (additive totals)

const axios = require('axios');

const HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!HUBSPOT_TOKEN) {
  throw new Error('Missing HUBSPOT_PRIVATE_APP_TOKEN in environment');
}

const BASE = 'https://api.hubapi.com/marketing/v3/campaigns';

function hsHeaders() {
  return {
    Authorization: `Bearer ${HUBSPOT_TOKEN}`,
    'Content-Type': 'application/json',
  };
}

// GET a campaign to read current property values
async function getCampaignById(id) {
  const url = `${BASE}/${id}`;
  const r = await axios.get(url, { headers: hsHeaders() });
  return r.data; // { id, properties, createdAt, updatedAt, ... }
}

// PATCH a campaign with properties
async function patchCampaign(id, properties) {
  const url = `${BASE}/${id}`;
  const body = { properties };
  const r = await axios.patch(url, body, { headers: hsHeaders() });
  return r.data;
}

// Safe add: parse existing numeric property, add delta, return string
function addNumProp(existing, delta) {
  const cur = Number(existing || 0);
  const add = Number(delta || 0);
  const sum = cur + add;
  // HubSpot numeric props usually accept numbers; sending as number is fine.
  // If your property is "number" type this is correct.
  return sum;
}

/**
 * addToCampaignTotals
 * - Reads current totals
 * - Adds today's deltas (clicks, impressions, conversions, spend)
 * - PATCHes the updated totals
 *
 * @param {Object} args
 * @param {string} args.id - HubSpot campaign ID
 * @param {Object} args.deltas - { clicks, impressions, conversions, spend, avgCpc, cpl, status, date }
 * @param {Object} args.props - mapping of env property names: { spend, clicks, imps, conv, avgCpc, cpl, lastStatus, lastDate }
 */
async function addToCampaignTotals({ id, deltas, props }) {
  const {
    clicks = 0,
    impressions = 0,
    conversions = 0,
    spend = 0,
    avgCpc, // optional
    cpl,    // optional
    status, // optional
    date,   // optional (ISO)
  } = deltas || {};

  // 1) Read current values
  const cur = await getCampaignById(id);
  const p = (cur && cur.properties) || {};

  // 2) Build additive payload
  const out = {};
  if (props.clicks)      out[props.clicks] = addNumProp(p[props.clicks], clicks);
  if (props.imps)        out[props.imps]   = addNumProp(p[props.imps], impressions);
  if (props.conv)        out[props.conv]   = addNumProp(p[props.conv], conversions);
  if (props.spend)       out[props.spend]  = addNumProp(p[props.spend], spend);

  // Optional “last” metrics/markers (overwrite with latest snapshot)
  if (props.avgCpc && typeof avgCpc !== 'undefined') out[props.avgCpc] = Number(avgCpc);
  if (props.cpl && typeof cpl !== 'undefined')       out[props.cpl]    = Number(cpl);
  if (props.lastStatus && status)                    out[props.lastStatus] = String(status);
  if (props.lastDate && date)                        out[props.lastDate]   = String(date);

  // 3) PATCH
  return await patchCampaign(id, out);
}

module.exports = {
  getCampaignById,
  patchCampaign,
  addToCampaignTotals,
};

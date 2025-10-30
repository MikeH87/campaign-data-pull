// File: ensure-campaign-props.js
require('dotenv').config();
const axios = require('axios');

const HS_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!HS_TOKEN) {
  console.error('HUBSPOT_PRIVATE_APP_TOKEN missing in .env');
  process.exit(1);
}

const BASE = 'https://api.hubapi.com';
const OBJ = '0-35'; // Campaign object

const GROUP_NAME = 'bing_campaign_data';
const GROUP_LABEL = 'Bing Campaign Data';

// Property env names (fall back to our expected internal names)
const HSPROP_TOTAL_CLICKS      = process.env.HSPROP_TOTAL_CLICKS      || 'bing_click_total';
const HSPROP_TOTAL_IMPRESSIONS = process.env.HSPROP_TOTAL_IMPRESSIONS || 'bing_impression_total';
const HSPROP_TOTAL_CONVERSIONS = process.env.HSPROP_TOTAL_CONVERSIONS || 'bing_conversion_total';
const HSPROP_LAST_BING_DATE    = process.env.HSPROP_LAST_BING_DATE    || 'bing_last_processed';
const HSPROP_LAST_STATUS       = process.env.HSPROP_LAST_STATUS       || 'bing_last_status';

function headers() {
  return { Authorization: `Bearer ${HS_TOKEN}`, 'Content-Type': 'application/json' };
}

async function ensureGroup() {
  // Check existing groups
  const listUrl = `${BASE}/crm/v3/properties/${OBJ}/groups`;
  const list = await axios.get(listUrl, { headers: headers(), validateStatus: () => true });
  if (list.status !== 200) {
    throw new Error(`Failed to list groups (${list.status}): ${JSON.stringify(list.data)}`);
  }
  const found = (list.data?.results || []).find(g => g.name === GROUP_NAME);
  if (found) return found;

  const createUrl = `${BASE}/crm/v3/properties/${OBJ}/groups`;
  const body = {
    name: GROUP_NAME,
    label: GROUP_LABEL,
    displayOrder: 1,
  };
  const r = await axios.post(createUrl, body, { headers: headers(), validateStatus: () => true });
  if (r.status !== 201) {
    throw new Error(`Failed to create group (${r.status}): ${JSON.stringify(r.data)}`);
  }
  return r.data;
}

async function listProps() {
  const url = `${BASE}/crm/v3/properties/${OBJ}`;
  const r = await axios.get(url, { headers: headers(), validateStatus: () => true });
  if (r.status !== 200) throw new Error(`Failed to list properties (${r.status}): ${JSON.stringify(r.data)}`);
  return r.data?.results || [];
}

async function ensurePropNumber(name, label) {
  const exists = (await listProps()).find(p => p.name === name);
  if (exists) return exists;
  const url = `${BASE}/crm/v3/properties/${OBJ}`;
  const body = {
    name,
    label,
    groupName: GROUP_NAME,
    type: 'number',
    fieldType: 'number',
    // writable custom number; no calc, no readOnly
  };
  const r = await axios.post(url, body, { headers: headers(), validateStatus: () => true });
  if (r.status !== 201) throw new Error(`Create prop ${name} failed (${r.status}): ${JSON.stringify(r.data)}`);
  return r.data;
}

async function ensurePropDate(name, label) {
  const exists = (await listProps()).find(p => p.name === name);
  if (exists) return exists;
  const url = `${BASE}/crm/v3/properties/${OBJ}`;
  const body = {
    name,
    label,
    groupName: GROUP_NAME,
    type: 'datetime',
    fieldType: 'date',
  };
  const r = await axios.post(url, body, { headers: headers(), validateStatus: () => true });
  if (r.status !== 201) throw new Error(`Create prop ${name} failed (${r.status}): ${JSON.stringify(r.data)}`);
  return r.data;
}

async function ensurePropText(name, label) {
  const exists = (await listProps()).find(p => p.name === name);
  if (exists) return exists;
  const url = `${BASE}/crm/v3/properties/${OBJ}`;
  const body = {
    name,
    label,
    groupName: GROUP_NAME,
    type: 'string',
    fieldType: 'text',
  };
  const r = await axios.post(url, body, { headers: headers(), validateStatus: () => true });
  if (r.status !== 201) throw new Error(`Create prop ${name} failed (${r.status}): ${JSON.stringify(r.data)}`);
  return r.data;
}

async function main() {
  console.log('Ensuring HubSpot campaign custom properties exist…');
  await ensureGroup();
  await ensurePropNumber(HSPROP_TOTAL_CLICKS, 'Bing Total Clicks');
  await ensurePropNumber(HSPROP_TOTAL_IMPRESSIONS, 'Bing Total Impressions');
  await ensurePropNumber(HSPROP_TOTAL_CONVERSIONS, 'Bing Total Conversions');
  await ensurePropDate(HSPROP_LAST_BING_DATE, 'Bing Last Processed');
  await ensurePropText(HSPROP_LAST_STATUS, 'Bing Last Status');
  console.log('Done.');
}

main().catch(e => {
  console.error(e.message || e);
  process.exit(1);
});

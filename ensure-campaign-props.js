// ensure-campaign-props.js
// Ensure HubSpot Campaign custom properties exist (Bing + Twitter channel-specific)
//
// Requires:
//   HUBSPOT_PRIVATE_APP_TOKEN=...
//
// NOTE: HubSpot blocks writing some read-only/built-in props. We only create writable custom props.
//
// Creates (if missing):
//   bing_last_status           (text)
//   bing_last_processed        (date)
//   avg_cpc_last               (number)
//   cpl_last                   (number)
//   total_clicks               (number)
//   total_impressions          (number)
//   total_conversions          (number)
//
// For Twitter (parallel set):
//   twitter_last_status        (text)
//   twitter_last_processed     (date)
//   twitter_avg_cpc_last       (number)
//   twitter_cpl_last           (number)
//   twitter_click_total        (number)
//   twitter_impression_total   (number)
//   twitter_conversion_total   (number)

require('dotenv').config();
const axios = require('axios');

const BASE = 'https://api.hubapi.com';
const TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;

if (!TOKEN) {
  console.error('Missing HUBSPOT_PRIVATE_APP_TOKEN');
  process.exit(1);
}

const h = axios.create({
  baseURL: BASE,
  headers: { Authorization: `Bearer ${TOKEN}` },
  validateStatus: () => true
});

async function getExistingProperties() {
  const url = '/crm/v3/properties/marketing_events'; // Using a schema that accepts property ops is unreliable across objects;
  // For Campaigns API, properties are managed via schemas for objectType "0-35" (marketing-campaigns object).
  // We will call the dedicated endpoint:
  const res = await h.get('/crm/v3/properties/0-35');
  if (res.status !== 200) {
    throw new Error(`Failed to list properties (${res.status}): ${JSON.stringify(res.data)}`);
  }
  const props = new Map();
  for (const p of res.data?.results || []) props.set(p.name, p);
  return props;
}

async function ensureProp(name, options) {
  const props = await getExistingProperties();
  if (props.has(name)) {
    console.log(`✔ exists: ${name}`);
    return;
  }
  const body = {
    name,
    label: options.label || name,
    type: options.type,     // string|number|date
    fieldType: options.fieldType || (options.type === 'number' ? 'number' : 'text'),
    groupName: options.groupName || 'campaign_information',
    description: options.description || '',
    hasUniqueValue: false,
    hidden: false,
    displayOrder: options.displayOrder || -1
  };
  const res = await h.post('/crm/v3/properties/0-35', body);
  if (res.status !== 201) {
    throw new Error(`Create property failed for ${name} (${res.status}) ${JSON.stringify(res.data)}`);
  }
  console.log(`+ created: ${name}`);
}

(async function main() {
  console.log('Ensuring HubSpot campaign custom properties exist…');

  const list = [
    // Bing (some already present in your env/project)
    { name: 'bing_last_status',        type: 'string', label: 'Bing – Last Status' },
    { name: 'bing_last_processed',     type: 'date',   label: 'Bing – Last Processed (yyyy-mm-dd)' },
    { name: 'avg_cpc_last',            type: 'number', label: 'Bing – Last Avg CPC' },
    { name: 'cpl_last',                type: 'number', label: 'Bing – Last CPL' },
    { name: 'total_clicks',            type: 'number', label: 'Bing – Total Clicks' },
    { name: 'total_impressions',       type: 'number', label: 'Bing – Total Impressions' },
    { name: 'total_conversions',       type: 'number', label: 'Bing – Total Conversions' },

    // Twitter parallel set (channel specific)
    { name: 'twitter_last_status',      type: 'string', label: 'Twitter – Last Status' },
    { name: 'twitter_last_processed',   type: 'date',   label: 'Twitter – Last Processed (yyyy-mm-dd)' },
    { name: 'twitter_avg_cpc_last',     type: 'number', label: 'Twitter – Last Avg CPC' },
    { name: 'twitter_cpl_last',         type: 'number', label: 'Twitter – Last CPL' },
    { name: 'twitter_click_total',      type: 'number', label: 'Twitter – Total Clicks' },
    { name: 'twitter_impression_total', type: 'number', label: 'Twitter – Total Impressions' },
    { name: 'twitter_conversion_total', type: 'number', label: 'Twitter – Total Conversions' },
  ];

  for (const p of list) {
    try { // don’t bomb the whole run if one fails
      await ensureProp(p.name, p);
    } catch (e) {
      console.error(`✖ property ${p.name} failed: ${e.message}`);
    }
  }
  console.log('Done.');
})().catch(e => {
  console.error(e);
  process.exit(1);
});

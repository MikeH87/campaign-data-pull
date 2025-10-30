// ensure-campaign-props.js
require('dotenv').config();
const axios = require('axios');

const HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!HUBSPOT_TOKEN) {
  console.error('HUBSPOT_PRIVATE_APP_TOKEN missing from .env');
  process.exit(1);
}

const api = axios.create({
  baseURL: 'https://api.hubapi.com',
  headers: {
    Authorization: `Bearer ${HUBSPOT_TOKEN}`,
    'Content-Type': 'application/json',
  },
  validateStatus: () => true,
});

const OBJECT_TYPE = 'marketing-campaigns';

// These names must match what your backfill job PATCHes (seen in the error)
const PROPS = [
  {
    name: 'bing_click_total',
    label: 'Bing Click Total',
    description: 'Cumulative Bing Ads clicks (set by backfill job)',
  },
  {
    name: 'bing_impression_total',
    label: 'Bing Impression Total',
    description: 'Cumulative Bing Ads impressions (set by backfill job)',
  },
  {
    name: 'bing_conversion_total',
    label: 'Bing Conversion Total',
    description: 'Cumulative Bing Ads conversions (set by backfill job)',
  },
];

async function getExistingProperties() {
  const res = await api.get(`/crm/v3/properties/${OBJECT_TYPE}`);
  if (res.status !== 200) {
    throw new Error(
      `Failed to list properties (${res.status}): ${JSON.stringify(res.data)}`
    );
  }
  const existing = new Set((res.data?.results || []).map(p => p.name));
  return existing;
}

async function ensureProperty({ name, label, description }) {
  // Create as a writable number in a sensible group
  const body = {
    name,
    label,
    type: 'number',
    fieldType: 'number',
    description,
    groupName: 'campaigninformation', // default HubSpot group for campaigns
    formField: false,
  };

  const res = await api.post(`/crm/v3/properties/${OBJECT_TYPE}`, body);
  if (res.status === 201) {
    console.log(`✅ Created property: ${name}`);
    return true;
  }
  if (res.status === 409) {
    console.log(`ℹ️ Property already exists: ${name}`);
    return false;
  }
  throw new Error(
    `Failed to create property ${name} (${res.status}): ${JSON.stringify(
      res.data
    )}`
  );
}

async function main() {
  console.log('Ensuring HubSpot campaign custom properties exist…');
  const existing = await getExistingProperties();

  let created = 0;
  for (const p of PROPS) {
    if (existing.has(p.name)) {
      console.log(`✔︎ Found: ${p.name}`);
      continue;
    }
    await ensureProperty(p);
    created++;
  }
  console.log(
    created === 0
      ? 'Done. No new properties were needed.'
      : `Done. Created ${created} propert${created === 1 ? 'y' : 'ies'}.`
  );
}

main().catch(err => {
  console.error(err?.stack || err);
  process.exit(1);
});

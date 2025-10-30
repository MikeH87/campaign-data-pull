#!/usr/bin/env node
/* eslint-disable no-console */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');

// --- config from .env ---
const HSPROP_TOTAL_SPEND       = process.env.HSPROP_TOTAL_SPEND       || 'hs_spend_items_sum_amount'; // read-only in HS – we do NOT write it here
const HSPROP_TOTAL_CLICKS      = process.env.HSPROP_TOTAL_CLICKS      || 'total_clicks';
const HSPROP_TOTAL_IMPRESSIONS = process.env.HSPROP_TOTAL_IMPRESSIONS || 'total_impressions';
const HSPROP_TOTAL_CONVERSIONS = process.env.HSPROP_TOTAL_CONVERSIONS || 'total_conversions';
const HSPROP_LAST_AVG_CPC      = process.env.HSPROP_LAST_AVG_CPC      || 'avg_cpc_last';
const HSPROP_LAST_CPL          = process.env.HSPROP_LAST_CPL          || 'cpl_last';
const HSPROP_LAST_STATUS       = process.env.HSPROP_LAST_STATUS       || 'bing_last_status';
const HSPROP_LAST_BING_DATE    = process.env.HSPROP_LAST_BING_DATE    || 'bing_last_processed';

const HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!HUBSPOT_TOKEN) {
  console.error('Missing HUBSPOT_PRIVATE_APP_TOKEN in .env');
  process.exit(1);
}

const { getDailyCampaignRows } = require('./src/msadsReport');

// ---- helpers ----
const headers = {
  Authorization: `Bearer ${HUBSPOT_TOKEN}`,
  'Content-Type': 'application/json'
};

function readCampaignMap() {
  const p = path.join(process.cwd(), 'campaign-map.json');
  if (!fs.existsSync(p)) return {};
  try {
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch (e) {
    console.error('Failed to read campaign-map.json:', e.message);
    return {};
  }
}

function toNum(v) {
  if (v === null || v === undefined || v === '') return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

// Convert 'YYYY-MM-DD' -> epoch milliseconds at 00:00:00 UTC
function ymdToEpochMs(ymd) {
  const [y, m, d] = ymd.split('-').map(Number);
  // Date.UTC month is 0-based
  return Date.UTC(y, (m || 1) - 1, d || 1, 0, 0, 0, 0);
}

async function hsGetCampaignById(id) {
  const url = `https://api.hubapi.com/marketing/v3/campaigns/${id}`;
  const r = await axios.get(url, { headers, validateStatus: () => true });
  if (r.status !== 200) {
    const msg = `GET ${id} failed (${r.status}) ${JSON.stringify(r.data)}`;
    throw new Error(msg);
  }
  return r.data; // { id, properties, ... }
}

async function hsPatchCampaignById(id, properties) {
  const url = `https://api.hubapi.com/marketing/v3/campaigns/${id}`;
  const body = { properties };
  const r = await axios.patch(url, body, { headers, validateStatus: () => true });
  if (r.status !== 200) {
    const msg = `PATCH ${id} failed (${r.status}) ${JSON.stringify(r.data)}`;
    throw new Error(msg);
  }
  return r.data;
}

// ---- main runner ----
async function run() {
  const argv = require('yargs/yargs')(process.argv.slice(2))
    .option('date', { type: 'string', demandOption: true, describe: 'ISO date (yyyy-mm-dd)' })
    .help(false)
    .version(false)
    .argv;

  const date = argv.date;
  console.log(`Add-to-totals for ${date}`);

  // 1) Pull daily Bing rows
  const rows = await getDailyCampaignRows(date);
  console.log(`[MSADS] Daily rows: ${rows.length}`);

  // 2) Load HS campaign map (name -> id)
  const cmap = readCampaignMap();

  let updated = 0, skippedZero = 0, missingMap = 0, failed = 0;

  for (const row of rows) {
    const name = row.name || row.campaignName || '';
    if (!name) continue;

    const id = cmap[name];
    if (!id) {
      console.error(`⚠️  No HubSpot mapping for "${name}". Run ensure-campaign-map first.`);
      missingMap++;
      continue;
    }

    const dayClicks = toNum(row.clicks);
    const dayImps = toNum(row.impressions);
    const dayConv = toNum(row.conversions);

    if (dayClicks === 0 && dayImps === 0 && dayConv === 0) {
      skippedZero++;
      continue;
    }

    try {
      // 3) Read current HS totals
      const cur = await hsGetCampaignById(id);
      const props = (cur && cur.properties) || {};

      const curClicks = toNum(props[HSPROP_TOTAL_CLICKS]);
      const curImps   = toNum(props[HSPROP_TOTAL_IMPRESSIONS]);
      const curConv   = toNum(props[HSPROP_TOTAL_CONVERSIONS]);

      // 4) Additive totals
      const nextProps = {};
      nextProps[HSPROP_TOTAL_CLICKS]      = curClicks + dayClicks;
      nextProps[HSPROP_TOTAL_IMPRESSIONS] = curImps + dayImps;
      nextProps[HSPROP_TOTAL_CONVERSIONS] = curConv + dayConv;

      // 5) “Last seen” fields (all optional)
      if (HSPROP_LAST_AVG_CPC)   nextProps[HSPROP_LAST_AVG_CPC]   = (row.average_cpc ?? '') + '';
      if (HSPROP_LAST_CPL)       nextProps[HSPROP_LAST_CPL]       = (row.all_cost_per_conversion ?? '') + '';
      if (HSPROP_LAST_STATUS)    nextProps[HSPROP_LAST_STATUS]    = (row.campaign_status ?? '') + '';
      if (HSPROP_LAST_BING_DATE) nextProps[HSPROP_LAST_BING_DATE] = ymdToEpochMs(date); // <-- epoch ms required

      await hsPatchCampaignById(id, nextProps);
      console.log(`✅ Updated totals: ${name} clicks+${dayClicks} imps+${dayImps} conv+${dayConv}`);
      updated++;
    } catch (e) {
      console.error(`❌ Failed to update ${name} (${id}):\n${e.message}`);
      failed++;
    }
  }

  console.log(`Done. Updated=${updated}  SkippedZero=${skippedZero}  MissingMap=${missingMap}  Failed=${failed}`);
}

if (require.main === module) {
  run().catch(e => {
    console.error(e?.stack || e);
    process.exit(1);
  });
}

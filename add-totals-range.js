#!/usr/bin/env node
/* eslint-disable no-console */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!HUBSPOT_TOKEN) {
  console.error('Missing HUBSPOT_PRIVATE_APP_TOKEN in .env');
  process.exit(1);
}

// HubSpot property names from .env (same as daily script)
const HSPROP_TOTAL_CLICKS      = process.env.HSPROP_TOTAL_CLICKS      || 'total_clicks';
const HSPROP_TOTAL_IMPRESSIONS = process.env.HSPROP_TOTAL_IMPRESSIONS || 'total_impressions';
const HSPROP_TOTAL_CONVERSIONS = process.env.HSPROP_TOTAL_CONVERSIONS || 'total_conversions';
const HSPROP_LAST_AVG_CPC      = process.env.HSPROP_LAST_AVG_CPC      || 'avg_cpc_last';
const HSPROP_LAST_CPL          = process.env.HSPROP_LAST_CPL          || 'cpl_last';
const HSPROP_LAST_STATUS       = process.env.HSPROP_LAST_STATUS       || 'bing_last_status';
const HSPROP_LAST_BING_DATE    = process.env.HSPROP_LAST_BING_DATE    || 'bing_last_processed';

const headers = {
  Authorization: `Bearer ${HUBSPOT_TOKEN}`,
  'Content-Type': 'application/json'
};

const { getDailyCampaignRows } = require('./src/msadsReport');

function readCampaignMap() {
  const p = path.join(process.cwd(), 'campaign-map.json');
  if (!fs.existsSync(p)) return {};
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); }
  catch (e) {
    console.error('Failed to read campaign-map.json:', e.message);
    return {};
  }
}

function toNum(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; }

function ymdToEpochMs(ymd) {
  const [y,m,d] = ymd.split('-').map(Number);
  return Date.UTC(y, (m||1)-1, d||1, 0,0,0,0);
}

async function hsGetCampaignById(id) {
  const url = `https://api.hubapi.com/marketing/v3/campaigns/${id}`;
  const r = await axios.get(url, { headers, validateStatus: () => true });
  if (r.status !== 200) throw new Error(`GET ${id} failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}

async function hsPatchCampaignById(id, properties) {
  const url = `https://api.hubapi.com/marketing/v3/campaigns/${id}`;
  const r = await axios.patch(url, { properties }, { headers, validateStatus: () => true });
  if (r.status !== 200) throw new Error(`PATCH ${id} failed (${r.status}) ${JSON.stringify(r.data)}`);
  return r.data;
}

function addDays(ymd, n){
  const [y,m,d] = ymd.split('-').map(Number);
  const dt = new Date(Date.UTC(y,(m-1),d,0,0,0,0));
  dt.setUTCDate(dt.getUTCDate()+n);
  return dt.toISOString().slice(0,10);
}

async function addOneDay(date, cmap){
  const rows = await getDailyCampaignRows(date);
  let updated=0, skipped=0, missing=0, failed=0;

  for (const row of rows){
    const name = row.name || row.campaignName || '';
    if (!name) continue;

    const id = cmap[name];
    if (!id){ missing++; continue; }

    const dayClicks = toNum(row.clicks);
    const dayImps   = toNum(row.impressions);
    const dayConv   = toNum(row.conversions);
    if (dayClicks===0 && dayImps===0 && dayConv===0){ skipped++; continue; }

    try{
      const cur = await hsGetCampaignById(id);
      const props = (cur && cur.properties) || {};

      const curClicks = toNum(props[HSPROP_TOTAL_CLICKS]);
      const curImps   = toNum(props[HSPROP_TOTAL_IMPRESSIONS]);
      const curConv   = toNum(props[HSPROP_TOTAL_CONVERSIONS]);

      const nextProps = {};
      nextProps[HSPROP_TOTAL_CLICKS]      = curClicks + dayClicks;
      nextProps[HSPROP_TOTAL_IMPRESSIONS] = curImps   + dayImps;
      nextProps[HSPROP_TOTAL_CONVERSIONS] = curConv   + dayConv;

      if (HSPROP_LAST_AVG_CPC)   nextProps[HSPROP_LAST_AVG_CPC]   = (row.average_cpc ?? '') + '';
      if (HSPROP_LAST_CPL)       nextProps[HSPROP_LAST_CPL]       = (row.all_cost_per_conversion ?? '') + '';
      if (HSPROP_LAST_STATUS)    nextProps[HSPROP_LAST_STATUS]    = (row.campaign_status ?? '') + '';
      if (HSPROP_LAST_BING_DATE) nextProps[HSPROP_LAST_BING_DATE] = ymdToEpochMs(date);

      await hsPatchCampaignById(id, nextProps);
      updated++;
    }catch(e){
      console.error(`❌ ${name} (${id}) ${e.message}`);
      failed++;
    }
  }
  return { updated, skipped, missing, failed, rows: rows.length };
}

async function run(){
  const argv = require('yargs/yargs')(process.argv.slice(2))
    .option('from', { type: 'string', demandOption: true })
    .option('to',   { type: 'string', demandOption: true })
    .help(false).version(false).argv;

  let cur = argv.from, end = argv.to;
  console.log(`Additive backfill from ${cur} to ${end}`);

  const cmap = readCampaignMap();
  let total={updated:0, skipped:0, missing:0, failed:0, days:0, rows:0};

  while (cur <= end){
    try{
      const r = await addOneDay(cur, cmap);
      total.updated += r.updated;
      total.skipped += r.skipped;
      total.missing += r.missing;
      total.failed  += r.failed;
      total.rows    += r.rows;
      total.days++;
      console.log(`• ${cur}  rows=${r.rows}  upd=${r.updated}  skip=${r.skipped}  miss=${r.missing}  fail=${r.failed}`);
    }catch(e){
      console.error(`✖ ${cur} ${e.message}`);
    }
    cur = addDays(cur, 1);
  }

  console.log(`\nDone. days=${total.days} rows=${total.rows} updated=${total.updated} skipped=${total.skipped} missingMap=${total.missing} failed=${total.failed}`);
}

if (require.main === module){
  run().catch(e => { console.error(e?.stack || e); process.exit(1); });
}

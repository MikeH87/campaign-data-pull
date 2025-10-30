// ensure-campaign-map.js
// Creates (or repairs) HubSpot Campaigns for any Bing campaign names with non-zero spend.
// - Only writes hs_name on create (avoids HubSpot validation on hs_campaign_status)
// - Then updates your custom status field (HSPROP_LAST_STATUS, default "bing_last_status").
// - Repairs stale IDs in campaign-map.json by verifying each ID actually exists.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { getDailyCampaignRows } = require('./msadsDailyRows');

const HS_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!HS_TOKEN) throw new Error('Missing HUBSPOT_PRIVATE_APP_TOKEN in .env');

const MAP_PATH = path.join(process.cwd(), 'campaign-map.json');
const CUSTOM_STATUS_PROP = process.env.HSPROP_LAST_STATUS || 'bing_last_status';

function parseArgs() {
  const args = Object.fromEntries(
    process.argv.slice(2).map(p => {
      const [k, v] = p.replace(/^--/, '').split('=');
      return [k, v ?? true];
    })
  );
  return args;
}

function uniq(a) { return Array.from(new Set(a)); }

function datesBetween(from, to) {
  const out = [];
  const d0 = new Date(from + 'T00:00:00Z');
  const d1 = new Date(to + 'T00:00:00Z');
  if (Number.isNaN(d0.getTime()) || Number.isNaN(d1.getTime()) || d0 > d1) {
    throw new Error(`Bad date range. from=${from} to=${to}`);
  }
  for (let d = new Date(d0); d <= d1; d.setUTCDate(d.getUTCDate() + 1)) {
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

async function hsGetCampaign(id) {
  const url = `https://api.hubapi.com/marketing/v3/campaigns/${id}`;
  const headers = { Authorization: `Bearer ${HS_TOKEN}` };
  const r = await axios.get(url, { headers, validateStatus: () => true });
  return r; // caller checks status
}

async function hsCreateCampaignByName(name) {
  const url = 'https://api.hubapi.com/marketing/v3/campaigns';
  const headers = {
    Authorization: `Bearer ${HS_TOKEN}`,
    'Content-Type': 'application/json',
  };
  // IMPORTANT: Only set hs_name. DO NOT set hs_campaign_status (it rejects writes).
  const body = { properties: { hs_name: name } };
  const r = await axios.post(url, body, { headers, validateStatus: () => true });
  if (r.status === 201 || r.status === 200) return r.data?.id;

  if (r.status === 409) {
    // Name conflict → find by name
    const id = await findCampaignIdByName(name);
    if (id) return id;
  }
  throw new Error(`Create failed for "${name}" (HTTP ${r.status}) Body: ${JSON.stringify(r.data)}`);
}

async function hsListCampaignPage(after) {
  const url = new URL('https://api.hubapi.com/marketing/v3/campaigns');
  if (after) url.searchParams.set('after', String(after));
  url.searchParams.set('limit', '100');
  const headers = { Authorization: `Bearer ${HS_TOKEN}` };
  const r = await axios.get(url.toString(), { headers, validateStatus: () => true });
  if (r.status !== 200) throw new Error(`List campaigns failed (HTTP ${r.status}) Body: ${JSON.stringify(r.data)}`);
  return r.data;
}

async function hsGetCampaignProperties(id) {
  const url = `https://api.hubapi.com/marketing/v3/campaigns/${id}`;
  const headers = { Authorization: `Bearer ${HS_TOKEN}` };
  const r = await axios.get(url, { headers, validateStatus: () => true });
  if (r.status !== 200) throw new Error(`GET campaign ${id} failed (HTTP ${r.status}) Body: ${JSON.stringify(r.data)}`);
  return r.data?.properties || {};
}

async function hsUpdateCampaignProperties(id, props) {
  const url = `https://api.hubapi.com/marketing/v3/campaigns/${id}`;
  const headers = {
    Authorization: `Bearer ${HS_TOKEN}`,
    'Content-Type': 'application/json',
  };
  // Only send custom (writable) properties; NEVER include forbidden ones like hs_spend_items_sum_amount or hs_campaign_status.
  const body = { properties: { ...props } };
  const r = await axios.patch(url, body, { headers, validateStatus: () => true });
  if (r.status === 200) return true;
  throw new Error(`PATCH ${id} failed (HTTP ${r.status}) Body: ${JSON.stringify(r.data)}`);
}

async function findCampaignIdByName(name) {
  let after = undefined;
  while (true) {
    const page = await hsListCampaignPage(after);
    const items = Array.isArray(page.results) ? page.results : [];
    for (const item of items) {
      if (!item || !item.id) continue;
      try {
        const props = await hsGetCampaignProperties(item.id);
        if ((props.hs_name || props['hs_name']) === name) return item.id;
      } catch {
        // ignore and continue
      }
    }
    after = page.paging?.next?.after;
    if (!after) break;
  }
  return null;
}

async function ensureOne(name, map, { force = false, statusValue = null } = {}) {
  let id = map[name];

  // If mapped & not force, verify exists
  if (id && !force) {
    const resp = await hsGetCampaign(id);
    if (resp.status === 200) {
      // Optionally update custom status
      if (statusValue && CUSTOM_STATUS_PROP) {
        try { await hsUpdateCampaignProperties(id, { [CUSTOM_STATUS_PROP]: String(statusValue) }); } catch {}
      }
      return { id, created: false, repaired: false };
    }
    console.warn(`Mapping for "${name}" points to missing campaign (${id}), recreating…`);
  }

  // Create (only hs_name)
  id = await hsCreateCampaignByName(name);
  map[name] = id;

  // After create, write custom status into your custom field (if provided)
  if (statusValue && CUSTOM_STATUS_PROP) {
    try {
      await hsUpdateCampaignProperties(id, { [CUSTOM_STATUS_PROP]: String(statusValue) });
    } catch (e) {
      console.warn(`Created ${id} but failed to set ${CUSTOM_STATUS_PROP}="${statusValue}": ${e.message}`);
    }
  }

  return { id, created: true, repaired: !force && !!map[name] };
}

(async function run() {
  const args = parseArgs();
  const { date, from, to, force } = args;

  if (!date && !(from && to)) {
    console.error('Usage:\n  node ensure-campaign-map.js --date=YYYY-MM-DD [--force]\n  node ensure-campaign-map.js --from=YYYY-MM-DD --to=YYYY-MM-DD [--force]');
    process.exit(1);
  }

  // Load or init map
  let map = {};
  if (fs.existsSync(MAP_PATH)) {
    try { map = JSON.parse(fs.readFileSync(MAP_PATH, 'utf8')); }
    catch { console.warn('Warning: bad campaign-map.json, starting fresh.'); map = {}; }
  }

  const dates = date ? [date] : datesBetween(from, to);
  console.log(`Ensuring HubSpot campaigns exist for ${dates.length} day(s)…`);

  // Collect campaign names with spend > 0 and their latest observed status
  const nameToStatus = new Map(); // name -> last seen status
  for (const d of dates) {
    const rows = await getDailyCampaignRows(d);
    for (const r of rows) {
      const nm = (r.name || r.campaignName || '').trim();
      if (!nm) continue;
      if (Number(r.spend) > 0) {
        // keep latest status we see
        if (r.campaign_status) nameToStatus.set(nm, r.campaign_status);
        else if (!nameToStatus.has(nm)) nameToStatus.set(nm, null);
      }
    }
  }

  const names = uniq(Array.from(nameToStatus.keys())).sort();
  console.log(`Found ${names.length} campaign(s) with non-zero spend.`);

  let created = 0, repaired = 0, verified = 0, failed = 0;

  for (const name of names) {
    const statusValue = nameToStatus.get(name) || null;
    try {
      const res = await ensureOne(name, map, { force: !!force, statusValue });
      if (res.created) {
        created++;
        if (res.repaired) repaired++;
        console.log(`✔ Created: ${name} (${res.id})`);
      } else {
        verified++;
        console.log(`✓ Verified: ${name} (${map[name]})`);
      }
    } catch (e) {
      failed++;
      console.error(`❌ Ensure failed for "${name}": ${e.message}`);
    }
  }

  fs.writeFileSync(MAP_PATH, JSON.stringify(map, null, 2), 'utf8');
  console.log(`\nDone. Created=${created}  RepairedMissing=${repaired}  VerifiedExisting=${verified}  Failed=${failed}  TotalInMap=${Object.keys(map).length}`);
})();

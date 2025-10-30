// File: backfill-all.js
require('dotenv').config();

const fs = require('fs');
const path = require('path');

// Sources
const { getDailyCampaignRows: getBingRows } = require('./src/msadsReport');
const { getDailyCampaignRows: getTwitterRows } = require('./src/twitterAdsReport');

// HubSpot
const { getHubspotClient } = require('./src/hubspotClient');

// --- Settings & helpers ---
const MAP_PATH = process.env.CAMPAIGN_MAP_FILE || path.resolve(__dirname, 'campaign-map.json');

function readJsonSafe(filePath) {
  try {
    if (!fs.existsSync(filePath)) return {};
    const raw = fs.readFileSync(filePath, 'utf8');
    if (!raw.trim()) return {};
    return JSON.parse(raw);
  } catch {
    return {};
  }
}
function writeJsonSafe(filePath, obj) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), 'utf8');
}
function getCampaignMap() { return readJsonSafe(MAP_PATH); }
function saveCampaignMap(map) { writeJsonSafe(MAP_PATH, map); }

function* eachDate(fromYmd, toYmd) {
  const d = new Date(`${fromYmd}T00:00:00Z`);
  const end = new Date(`${toYmd}T00:00:00Z`);
  for (let t = d; t <= end; t = new Date(t.getTime() + 86400000)) {
    yield t.toISOString().slice(0, 10);
  }
}
function toMajorUnits(amountFloat) {
  const n = Number(amountFloat || 0);
  return Math.round(n * 100) / 100; // 2dp number
}

// idempotent, persistent ensure by name
async function ensureCampaignIdForName(hs, name, { dryRun }) {
  const map = getCampaignMap();
  const mappedId = map[name];

  if (dryRun) {
    if (mappedId) return mappedId;
    console.log(`[DRY] would create HubSpot campaign "${name}"`);
    return null;
  }

  if (mappedId) {
    try {
      await hs.getCampaignById(mappedId);
      return mappedId;
    } catch { /* stale id, fall through */ }
  }

  const found = await hs.findCampaignByName(name).catch(() => null);
  if (found?.id) {
    map[name] = found.id;
    saveCampaignMap(map);
    return found.id;
  }

  const created = await hs.createCampaign(name);
  const id = created?.id;
  if (!id) throw new Error(`Create campaign did not return id for "${name}"`);
  console.log(`Created campaign "${name}" -> ${id}`);
  map[name] = id;
  saveCampaignMap(map);
  return id;
}

function pickSourceFn(source) {
  const s = (source || '').toLowerCase();
  if (s === 'bing')     return { fn: getBingRows, label: 'Bing Ads' };
  if (s === 'twitter')  return { fn: getTwitterRows, label: 'Twitter Ads' };
  if (s === 'both' || !s) return { fn: null, label: 'Both' };
  throw new Error(`Unknown --source=${source}`);
}

async function processDayForRows(hs, ymd, rows, { dryRun, spendSourceLabel }) {
  if (!rows || rows.length === 0) {
    console.log(`- ${ymd}: no data (skipped)`);
    return { totalsAdded: 0, spendItems: 0, failures: 0 };
  }

  let totalsAdded = 0;
  let spendItems = 0;
  let failures = 0;

  for (const row of rows) {
    const {
      campaignName,
      impressions,
      clicks,
      conversions,
      spend,
    } = row;

    const name = campaignName || row.name;
    if (!name) {
      console.log(`❌ Day ${ymd} row missing campaign name, skipping row.`);
      failures++;
      continue;
    }

    // Ensure HS id
    let campaignId = null;
    try {
      campaignId = await ensureCampaignIdForName(hs, name, { dryRun });
    } catch (e) {
      console.log(`❌ Ensure campaign failed for "${name}": ${e.message || e}`);
      failures++;
      continue;
    }

    // Spend (major)
    const amountMajor = toMajorUnits(spend);
    if (amountMajor > 0) {
      if (dryRun) {
        console.log(`[DRY] spend ${name} ${ymd} £${amountMajor.toFixed(2)}`);
      } else {
        try {
          await hs.createSpendItem(campaignId, {
            isoDate: ymd,
            amountMajor,
            source: spendSourceLabel,
          });
          console.log(`💷 spend: ${name} ${ymd} £${amountMajor.toFixed(2)} (created)`);
          spendItems++;
        } catch (e) {
          console.log(`❌ Spend item failed for "${name}": ${e.message || e}`);
          failures++;
        }
      }
    }

    // Totals (strictly monotonic & date-guarded)
    const addClicks = Number(clicks || 0);
    const addImps = Number(impressions || 0);
    const addConvs = Number(conversions || 0);

    if (addClicks || addImps || addConvs) {
      if (dryRun) {
        console.log(`[DRY] totals ${name} +clicks ${addClicks} +imps ${addImps} +conv ${addConvs}`);
      } else {
        try {
          const ok = await hs.addDailyTotalsMonotonic(campaignId, {
            clicks: addClicks, impressions: addImps, conversions: addConvs, dateISO: ymd,
          });
          if (ok) {
            console.log(`✅ totals: ${name} clicks+${addClicks} imps+${addImps} conv+${addConvs}`);
            totalsAdded++;
          } else {
            console.log(`↩︎ totals skipped (already processed newer date): ${name} ${ymd}`);
          }
        } catch (e) {
          if (e?.response?.data) {
            console.log(`❌ Totals failed for "${name}": ${JSON.stringify(e.response.data, null, 2)}`);
          } else {
            console.log(`❌ Totals failed for "${name}": ${e.message || e}`);
          }
          failures++;
        }
      }
    }
  }

  return { totalsAdded, spendItems, failures };
}

async function processDay(hs, ymd, source, { dryRun }) {
  if (source === 'bing') {
    const rows = await getBingRows(ymd);
    return processDayForRows(hs, ymd, rows, { dryRun, spendSourceLabel: 'Bing Ads' });
  }
  if (source === 'twitter') {
    const rows = await getTwitterRows(ymd);
    return processDayForRows(hs, ymd, rows, { dryRun, spendSourceLabel: 'Twitter Ads' });
  }

  // both
  const bing = await getBingRows(ymd);
  const res1 = await processDayForRows(hs, ymd, bing, { dryRun, spendSourceLabel: 'Bing Ads' });

  const twitter = await getTwitterRows(ymd);
  const res2 = await processDayForRows(hs, ymd, twitter, { dryRun, spendSourceLabel: 'Twitter Ads' });

  return {
    totalsAdded: res1.totalsAdded + res2.totalsAdded,
    spendItems: res1.spendItems + res2.spendItems,
    failures: res1.failures + res2.failures,
  };
}

async function main() {
  const args = process.argv.slice(2);
  // --from=YYYY-MM-DD --to=YYYY-MM-DD [--source=bing|twitter|both] [--dryRun]
  const fromArg = args.find(a => a.startsWith('--from=')) || '';
  const toArg = args.find(a => a.startsWith('--to=')) || '';
  const dryRun = args.some(a => a === '--dryRun');
  const sourceArg = (args.find(a => a.startsWith('--source=')) || '').split('=')[1] || 'both';

  const from = fromArg.split('=')[1];
  const to = toArg.split('=')[1];

  if (!from || !to) {
    console.error('Usage: node backfill-all.js --from=YYYY-MM-DD --to=YYYY-MM-DD [--source=bing|twitter|both] [--dryRun]');
    process.exit(1);
  }

  const hs = getHubspotClient();

  console.log(`Backfill ALL (${sourceArg}) ${from} → ${to}${dryRun ? ' [DRY RUN]' : ''}`);

  let days = 0, totalsAdded = 0, spendItems = 0, failures = 0;

  for (const ymd of eachDate(from, to)) {
    try {
      const res = await processDay(hs, ymd, sourceArg, { dryRun });
      days++;
      totalsAdded += res.totalsAdded;
      spendItems += res.spendItems;
      failures += res.failures;
    } catch (e) {
      console.log(`❌ Day ${ymd} failed: ${e.stack || e.message || e}`);
      failures++;
    }
  }

  if (dryRun) {
    console.log(`Done. Days=${days} TotalsAdded=${totalsAdded} SpendItems=${spendItems} Skipped=0 (DRY)`);
  } else {
    console.log(`Done. Days=${days} TotalsAdded=${totalsAdded} SpendItems=${spendItems} Skipped=0 Failures=${failures}`);
  }
}

if (require.main === module) {
  main().catch(e => {
    console.error(e?.stack || e);
    process.exit(1);
  });
}

// File: backfill-all.js
require('dotenv').config();
const fs = require('fs');
const path = require('path');

// Sources
const { getDailyCampaignRows: getBingRows } = require('./src/msadsReport');
const { getDailyCampaignRows: getTwitterRows } = require('./src/twitterAdsReport'); // safe even if you don't call it

// HubSpot
const { getHubspotClient } = require('./src/hubspotClient');

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
  return Math.round(n * 100) / 100;
}

// ---- campaign ensure (cached locally) ----
async function ensureCampaignIdForName(hs, name, { dryRun }) {
  const map = getCampaignMap();
  const mappedId = map[name];
  if (dryRun) return mappedId || null;

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

  const created = await hs.ensureCampaign(name);
  const id = created?.id;
  if (!id) throw new Error(`Create campaign did not return id for "${name}"`);
  map[name] = id;
  saveCampaignMap(map);
  return id;
}

/**
 * Totals accumulator so we don't rely on read-after-write consistency from HubSpot between days.
 * Structure: totalsCache[campaignId] = { clicks: number, imps: number, convs: number, initialised: boolean }
 */
const totalsCache = Object.create(null);

async function addTotalsWithCache(hs, campaignId, ymd, adds) {
  if (!totalsCache[campaignId]) {
    // Initialise once from HubSpot to get the baseline
    const cur = await hs.getTotals(campaignId);
    totalsCache[campaignId] = {
      clicks: Number(cur.clicks || 0),
      imps:   Number(cur.imps || 0),
      convs:  Number(cur.convs || 0),
      initialised: true,
    };
  }

  const cache = totalsCache[campaignId];
  cache.clicks += Number(adds.clicks || 0);
  cache.imps   += Number(adds.impressions || 0);
  cache.convs  += Number(adds.conversions || 0);

  // Write the accumulated numbers directly (single PATCH per day per campaign).
  await hs.setTotalsDirect(campaignId, {
    clicks: cache.clicks,
    impressions: cache.imps,
    conversions: cache.convs,
  }, ymd);
}

async function processDayForRows(hs, ymd, rows, { dryRun, spendSourceLabel }) {
  if (!rows || rows.length === 0) return { totalsAdded: 0, spendItems: 0, failures: 0 };
  let totalsAdded = 0, spendItems = 0, failures = 0;

  for (const row of rows) {
    const { campaignName, impressions, clicks, conversions, spend } = row;
    const name = campaignName || row.name;
    if (!name) continue;

    let campaignId;
    try {
      campaignId = await ensureCampaignIdForName(hs, name, { dryRun });
    } catch (e) {
      console.log(`❌ Ensure campaign failed for "${name}": ${e.message}`);
      failures++; continue;
    }

    // Spend
    const amountMajor = toMajorUnits(spend);
    if (amountMajor > 0) {
      if (dryRun) {
        console.log(`[DRY] spend ${name} ${ymd} £${amountMajor.toFixed(2)}`);
      } else {
        try {
          await hs.createSpendItem(campaignId, { isoDate: ymd, amountMajor, source: spendSourceLabel });
          console.log(`💷 spend: ${name} ${ymd} £${amountMajor.toFixed(2)} (created)`);
          spendItems++;
        } catch (e) {
          console.log(`❌ Spend item failed for "${name}": ${e.message}`);
          failures++;
        }
      }
    }

    // Totals (additive via local accumulator)
    const addClicks = Number(clicks || 0);
    const addImps   = Number(impressions || 0);
    const addConvs  = Number(conversions || 0);

    if (addClicks || addImps || addConvs) {
      if (dryRun) {
        console.log(`[DRY] totals ${name} +clicks ${addClicks} +imps ${addImps} +conv ${addConvs}`);
      } else {
        try {
          await addTotalsWithCache(hs, campaignId, ymd, {
            clicks: addClicks, impressions: addImps, conversions: addConvs
          });
          console.log(`✅ totals: ${name} clicks+${addClicks} imps+${addImps} conv+${addConvs}`);
          totalsAdded++;
        } catch (e) {
          console.log(`❌ Totals failed for "${name}": ${e.message}`);
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
  throw new Error(`Unknown source=${source}`);
}

async function main() {
  const args = process.argv.slice(2);
  const from = (args.find(a => a.startsWith('--from=')) || '').split('=')[1];
  const to = (args.find(a => a.startsWith('--to=')) || '').split('=')[1];
  const dryRun = args.includes('--dryRun');
  const sourceArg = (args.find(a => a.startsWith('--source=')) || '').split('=')[1] || 'bing';

  if (!from || !to) {
    console.error('Usage: node backfill-all.js --from=YYYY-MM-DD --to=YYYY-MM-DD [--source=bing|twitter] [--dryRun]');
    process.exit(1);
  }

  const hs = getHubspotClient();
  console.log(`Backfill ALL (${sourceArg}) ${from} → ${to}${dryRun ? ' [DRY RUN]' : ''}`);

  let days = 0, totalsAdded = 0, spendItems = 0, failures = 0;
  for (const ymd of eachDate(from, to)) {
    try {
      const res = await processDay(hs, ymd, sourceArg, { dryRun });
      totalsAdded += res.totalsAdded;
      spendItems += res.spendItems;
      failures += res.failures;
      days++;
    } catch (e) {
      console.log(`❌ Day ${ymd} failed: ${e.message}`);
      failures++;
    }
  }

  console.log(`Done. Days=${days} TotalsAdded=${totalsAdded} SpendItems=${spendItems} Failures=${failures}`);
}

if (require.main === module) main().catch(e => { console.error(e); process.exit(1); });

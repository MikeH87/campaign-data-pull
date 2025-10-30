// File: backfill-all.js
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { getDailyCampaignRows } = require('./src/msadsReport');
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

function getCampaignMap() {
  return readJsonSafe(MAP_PATH);
}

function saveCampaignMap(map) {
  writeJsonSafe(MAP_PATH, map);
}

/**
 * Ensure we have a HubSpot campaign ID for a given name.
 * - dryRun: don’t create anything; just log what would happen.
 * - real run: create campaign if missing and persist to campaign-map.json
 */
async function ensureCampaignIdForName(hs, name, { dryRun }) {
  const map = getCampaignMap();
  const mappedId = map[name];

  if (dryRun) {
    if (mappedId) {
      // In DRY mode, don’t validate the ID; just log.
      return mappedId;
    }
    console.log(`[DRY] would create HubSpot campaign "${name}"`);
    // Return null; caller will still log spend/totals as DRY without needing an ID.
    return null;
  }

  // Real mode: if mapped ID exists, sanity check it; otherwise create new.
  if (mappedId) {
    try {
      await hs.getCampaignById(mappedId);
      return mappedId; // still valid
    } catch {
      // mapped ID is stale — we’ll create new below
    }
  }

  // Try find by name (avoids duplicates when map was deleted)
  const found = await hs.findCampaignByName(name).catch(() => null);
  if (found?.id) {
    map[name] = found.id;
    saveCampaignMap(map);
    return found.id;
  }

  // Create fresh
  const created = await hs.createCampaign(name);
  const id = created?.id;
  if (!id) {
    throw new Error(`Create campaign did not return id for "${name}"`);
  }
  console.log(`Created campaign "${name}" -> ${id}`);
  map[name] = id;
  saveCampaignMap(map);
  return id;
}

function* eachDate(fromYmd, toYmd) {
  const d = new Date(`${fromYmd}T00:00:00Z`);
  const end = new Date(`${toYmd}T00:00:00Z`);
  for (let t = d; t <= end; t = new Date(t.getTime() + 86400000)) {
    yield t.toISOString().slice(0, 10);
  }
}

function toMajorUnits(amountFloat) {
  // Ensure a Number with 2 decimal places, not a string.
  const n = Number(amountFloat || 0);
  return Math.round(n * 100) / 100;
}

async function processDay(hs, ymd, { dryRun }) {
  const rows = await getDailyCampaignRows(ymd);
  if (!rows || rows.length === 0) {
    console.log(`- ${ymd}: no data (skipped)`);
    return { totalsAdded: 0, spendItems: 0, already: 0, failures: 0 };
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

    // Ensure HS campaign id (or log DRY)
    let campaignId = null;
    try {
      campaignId = await ensureCampaignIdForName(hs, name, { dryRun });
    } catch (e) {
      console.log(`❌ Ensure campaign failed for "${name}": ${e.message || e}`);
      failures++;
      continue;
    }

    // ----- Spend item (MAJOR units) -----
    const amountMajor = toMajorUnits(spend);

    if (amountMajor > 0) {
      if (dryRun) {
        console.log(`[DRY] spend ${name} ${ymd} £${amountMajor.toFixed(2)}`);
      } else {
        try {
          await hs.createSpendItem(campaignId, {
            isoDate: ymd,
            amountMajor,  // <<<<<< send decimal currency amount
            source: 'Bing Ads',
          });
          console.log(`💷 spend: ${name} ${ymd} £${amountMajor.toFixed(2)} (created)`);
          spendItems++;
        } catch (e) {
          console.log(`❌ Spend item failed for "${name}": ${e.message || e}`);
          failures++;
        }
      }
    }

    // ----- Totals -----
    const addClicks = Number(clicks || 0);
    const addImps = Number(impressions || 0);
    const addConvs = Number(conversions || 0);

    if (addClicks || addImps || addConvs) {
      if (dryRun) {
        console.log(`[DRY] totals ${name} +clicks ${addClicks} +imps ${addImps} +conv ${addConvs}`);
      } else {
        try {
          await hs.addDailyTotals(campaignId, {
            clicks: addClicks,
            impressions: addImps,
            conversions: addConvs,
            dateISO: ymd,
          });
          console.log(`✅ totals: ${name} clicks+${addClicks} imps+${addImps} conv+${addConvs}`);
          totalsAdded++;
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

  return { totalsAdded, spendItems, already: 0, failures };
}

async function main() {
  const args = process.argv.slice(2);
  // simple flags: --from=YYYY-MM-DD --to=YYYY-MM-DD [--dryRun]
  const fromArg = args.find(a => a.startsWith('--from=')) || '';
  const toArg = args.find(a => a.startsWith('--to=')) || '';
  const dryRun = args.some(a => a === '--dryRun');

  const from = fromArg.split('=')[1];
  const to = toArg.split('=')[1];

  if (!from || !to) {
    console.error('Usage: node backfill-all.js --from=YYYY-MM-DD --to=YYYY-MM-DD [--dryRun]');
    process.exit(1);
  }

  const hs = getHubspotClient();

  console.log(`Backfill ALL (spend items + ADD totals) ${from} → ${to}${dryRun ? ' [DRY RUN]' : ''}`);

  let days = 0, totalsAdded = 0, spendItems = 0, already = 0, failures = 0;

  for (const ymd of eachDate(from, to)) {
    try {
      const res = await processDay(hs, ymd, { dryRun });
      days++;
      totalsAdded += res.totalsAdded;
      spendItems += res.spendItems;
      already += res.already || 0;
      failures += res.failures;
    } catch (e) {
      console.log(`❌ Day ${ymd} failed: ${e.stack || e.message || e}`);
      failures++;
    }
  }

  if (dryRun) {
    console.log(`Done. Days=${days} TotalsAdded=${totalsAdded} SpendItems=${spendItems} Skipped=0 (DRY)`);
  } else {
    console.log(`Done. Days=${days} TotalsAdded=${totalsAdded} SpendItems=${spendItems} Skipped=0 Already=${already} Failures=${failures}`);
  }
}

if (require.main === module) {
  main().catch(e => {
    console.error(e?.stack || e);
    process.exit(1);
  });
}

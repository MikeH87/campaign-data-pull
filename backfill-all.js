// File: backfill-all.js
require('dotenv').config();
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const { getDailyCampaignRows } = require('./src/msadsReport');
const { getHubspotClient } = require('./src/hubspotClient');

function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
function isYMD(s){ return /^\d{4}-\d{2}-\d{2}$/.test(s||''); }

const argv = yargs(hideBin(process.argv))
  .option('source', { type: 'string', default: 'bing' })
  .option('from',   { type: 'string', demandOption: true })
  .option('to',     { type: 'string', demandOption: true })
  .option('dryRun', { type: 'boolean', default: false })
  .parse();

const DRY = !!argv.dryRun;
const SRC = (argv.source || 'bing').toLowerCase();

if (!isYMD(argv.from) || !isYMD(argv.to)) {
  console.error('Use ISO dates: --from=YYYY-MM-DD --to=YYYY-MM-DD');
  process.exit(1);
}

function* dayRange(fromYMD, toYMD) {
  const d0 = new Date(`${fromYMD}T00:00:00Z`);
  const d1 = new Date(`${toYMD}T00:00:00Z`);
  for (let d = new Date(d0); d <= d1; d.setUTCDate(d.getUTCDate()+1)) {
    yield d.toISOString().slice(0,10);
  }
}

async function ensureCampaignId(hs, name) {
  // small retry on ensure, because hubspot can be eventually consistent
  for (let i=0;i<4;i++){
    try {
      const c = await hs.ensureCampaign(name);
      return c?.id;
    } catch (e) {
      const msg = (e && e.message) || String(e);
      console.warn(`Ensure "${name}" attempt #${i+1} failed: ${msg}`);
      await sleep(1000*(i+1));
    }
  }
  throw new Error(`Ensure campaign failed for "${name}"`);
}

async function processDay(hs, isoDate) {
  if (SRC !== 'bing') {
    console.log(`Skipping ${isoDate} because source=${SRC}`);
    return;
  }

  const rows = await getDailyCampaignRows(isoDate); // [{campaignName, spend, clicks, impressions, conversions}, ...]
  console.log(`[MSADS] Rows parsed { isoDate: '${isoDate}', count: ${rows.length} }`);

  for (const row of rows) {
    const name  = String(row.campaignName || '').trim();
    const spend = Number(row.spend || 0);
    const clicks= Number(row.clicks || 0);
    const imps  = Number(row.impressions || 0);
    const convs = Number(row.conversions || 0);

    if (!name) continue;

    let campaignId;
    try {
      campaignId = await ensureCampaignId(hs, name);
    } catch (e) {
      console.error(`❌ Ensure campaign failed for "${name}": ${e.message || e}`);
      continue;
    }

    // 1) Spend item (idempotent by order=YYYYMMDD)
    if (spend > 0) {
      if (DRY) {
        console.log(`[DRY] spend ${name} ${isoDate} £${spend.toFixed(2)}`);
      } else {
        try {
          await hs.createSpendItem(campaignId, { isoDate, amountMajor: spend, source: 'Bing' });
          console.log(`💷 spend: ${name} ${isoDate} £${spend.toFixed(2)} (created/ok)`);
        } catch (e) {
          console.error(`❌ Spend item failed for "${name}": ${e.message || e}`);
        }
      }
    }

    // 2) Add totals (NOT overwrite)
    if (clicks || imps || convs) {
      if (DRY) {
        console.log(`[DRY] totals ${name} +clicks ${clicks} +imps ${imps} +conv ${convs}`);
      } else {
        try {
          await hs.addTotals(campaignId, { addClicks: clicks, addImps: imps, addConvs: convs }, isoDate);
          console.log(`✅ totals: ${name} clicks+${clicks} imps+${imps} conv+${convs}`);
        } catch (e) {
          console.error(`❌ Totals failed for "${name}": ${e.message || e}`);
        }
      }
    }
  }
}

async function main() {
  console.log(`Backfill ALL (${SRC}) ${argv.from} → ${argv.to}${DRY ? ' [DRY RUN]' : ''}`);
  const hs = getHubspotClient();

  let days = 0, spendItems = 0, totalsAdded = 0, failures = 0;

  for (const ymd of dayRange(argv.from, argv.to)) {
    try {
      await processDay(hs, ymd);
      days++;
    } catch (e) {
      failures++;
      console.error(`❌ Day ${ymd} failed: ${e.message || e}`);
    }
  }

  console.log(`Done. Days=${days} TotalsAdded=${totalsAdded} SpendItems=${spendItems} Failures=${failures} ${DRY ? '(DRY)' : ''}`);
}

if (require.main === module) {
  main().catch(err => {
    console.error(err);
    process.exit(1);
  });
}

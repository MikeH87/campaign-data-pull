// File: backfill-all.js
require('dotenv').config();
const { getHubspotClient } = require('./src/hubspotClient');
const { getDailyCampaignRows: getDailyBingRows } = require('./src/msadsReport');

function ymdRange(from, to) {
  const out = [];
  let d = new Date(from + 'T00:00:00Z');
  const end = new Date(to + 'T00:00:00Z');
  while (d <= end) {
    out.push(d.toISOString().slice(0,10));
    d.setUTCDate(d.getUTCDate() + 1);
  }
  return out;
}

function moneyToMajor(amount) {
  // ensure (e.g.) 23.15 is number not string; do NOT *100
  const n = Number(amount);
  return Number.isFinite(n) ? Number(n.toFixed(2)) : 0;
}

async function processBingDay(hs, isoDate, { dryRun=false } = {}) {
  const rows = await getDailyBingRows(isoDate);

  if (!rows || rows.length === 0) {
    console.log(`- ${isoDate}: no data (skipped)`);
    return { added: 0, spend: 0, failures: 0 };
  }

  let added = 0, spendCount = 0, failures = 0;

  for (const r of rows) {
    const name = r.campaignName || r.name;
    const spendMajor = moneyToMajor(r.spend || 0);
    const addClicks = Number(r.clicks || 0);
    const addImps   = Number(r.impressions || 0);
    const addConvs  = Number(r.conversions || 0);

    let campaign;
    try {
      campaign = await hs.ensureCampaign(name);
    } catch (e) {
      console.error(`❌ Ensure campaign failed for "${name}": ${e.message}`);
      failures++;
      continue;
    }

    // 1) spend line item
    try {
      if (!dryRun) {
        await hs.createSpendItem(campaign.id, { isoDate, amountMajor: spendMajor, source: 'Bing' });
        console.log(`💷 spend: ${name} ${isoDate} £${spendMajor.toFixed(2)} (created or existed)`);
      } else {
        console.log(`[DRY] spend ${name} ${isoDate} £${spendMajor.toFixed(2)}`);
      }
      spendCount++;
    } catch (e) {
      console.error(`❌ Spend item failed for "${name}": ${e.message || e}`);
      failures++;
      // keep going — totals can still proceed
    }

    // 2) additive totals (NO overwrite)
    try {
      if (!dryRun) {
        await hs.addToTotals(campaign.id, {
          addClicks, addImps, addConvs
        }, isoDate);
        console.log(`✅ totals: ${name} +clicks ${addClicks} +imps ${addImps} +conv ${addConvs}`);
      } else {
        console.log(`[DRY] totals ${name} +clicks ${addClicks} +imps ${addImps} +conv ${addConvs}`);
      }
      added++;
    } catch (e) {
      console.error(`❌ Totals failed for "${name}": ${e.response?.data ? JSON.stringify(e.response.data) : e.message}`);
      failures++;
    }
  }

  return { added, spend: spendCount, failures };
}

async function main() {
  const argv = require('yargs/yargs')(process.argv.slice(2))
    .option('source', { type: 'string', choices: ['bing'], default: 'bing' })
    .option('from',   { type: 'string', demandOption: true })
    .option('to',     { type: 'string', demandOption: true })
    .option('dryRun', { type: 'boolean', default: false })
    .strict()
    .help(false).argv;

  const { source, from, to, dryRun } = argv;

  const hs = getHubspotClient();

  console.log(`Backfill ALL (${source}) ${from} → ${to}${dryRun ? ' [DRY RUN]' : ''}`);

  const days = ymdRange(from, to);
  let totalAdded = 0, totalSpend = 0, totalFailures = 0;

  for (const day of days) {
    try {
      if (source === 'bing') {
        const { added, spend, failures } = await processBingDay(hs, day, { dryRun });
        totalAdded += added;
        totalSpend += spend;
        totalFailures += failures;
      }
    } catch (e) {
      console.error(`❌ Day ${day} failed: ${e.message || e}`);
      totalFailures++;
    }
  }

  console.log(`Done. Days=${days.length} TotalsAdded=${totalAdded} SpendItems=${totalSpend} Failures=${totalFailures}${dryRun ? ' (DRY)' : ''}`);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});

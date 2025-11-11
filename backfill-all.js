# Overwrite backfill-all.js with the full correct code
@'
require('dotenv').config();
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const { getDailyCampaignRows } = require('./src/msadsReport'); // Bing/MS Ads
const { getHubspotClient } = require('./src/hubspotClient');

function* dateRange(fromYmd, toYmd) {
  const start = new Date(`${fromYmd}T00:00:00Z`);
  const end   = new Date(`${toYmd}T00:00:00Z`);
  for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    yield d.toISOString().slice(0,10);
  }
}

async function processDayBing(hs, isoDate, opt) {
  const rows = await getDailyCampaignRows(isoDate);
  if (!rows || rows.length === 0) {
    console.log(`- ${isoDate}: no data (skipped)`);
    return { day: isoDate, updated: 0, spendItems: 0, skipped: true };
  }

  let updated = 0;
  let spendItems = 0;

  for (const r of rows) {
    const name = r.campaignName || r.name;
    const clicks = r.clicks || 0;
    const imps   = r.impressions || 0;
    const convs  = r.conversions || 0;
    const spend  = r.spend || 0;

    let camp;
    try {
      camp = await hs.ensureCampaign(name);
    } catch (e) {
      console.error(`❌ Ensure campaign failed for "${name}": ${e.message || e}`);
      continue;
    }

    if (!opt.dryRun) {
      try {
        if (spend && Number(spend) > 0) {
          await hs.createSpendItem(camp.id, { isoDate, amountMajor: Number(spend), source: 'Bing' });
          spendItems++;
          console.log(`💷 spend: ${name} ${isoDate} £${Number(spend).toFixed(2)} (created)`);
        }

        const res = await hs.addDailyTotalsIfNew(
          camp.id,
          { clicks, impressions: imps, conversions: convs },
          isoDate
        );

        if (res?.updated) {
          updated++;
          console.log(`✅ totals: ${name} +clicks ${clicks} +imps ${imps} +conv ${convs}`);
        } else {
          console.log(`↩️  totals: ${name} skipped (already processed ${isoDate})`);
        }

      } catch (e) {
        console.error(`❌ Failed for "${name}" on ${isoDate}:`, e?.response?.data || e.message || e);
      }
    } else {
      if (spend && Number(spend) > 0) {
        console.log(`[DRY] spend ${name} ${isoDate} £${Number(spend).toFixed(2)}`);
      }
      console.log(`[DRY] totals ${name} +clicks ${clicks} +imps ${imps} +conv ${convs}`);
    }
  }
  return { day: isoDate, updated, spendItems, skipped: false };
}

async function main() {
  const argv = yargs(hideBin(process.argv))
    .option('source', { type: 'string', default: 'bing' })
    .option('from', { type: 'string', demandOption: true })
    .option('to', { type: 'string', demandOption: true })
    .option('dryRun', { type: 'boolean', default: false })
    .argv;

  const from = argv.from;
  const to   = argv.to;
  const source = (argv.source || 'bing').toLowerCase();
  const dryRun = !!argv.dryRun;

  console.log(`Backfill ALL (${source}) ${from} → ${to}${dryRun ? ' [DRY RUN]' : ''}`);

  const hs = getHubspotClient();

  let days = 0, totals = 0, spends = 0, failures = 0;

  for (const d of dateRange(from, to)) {
    days++;
    try {
      if (source === 'bing') {
        const res = await processDayBing(hs, d, { dryRun });
        if (!res.skipped) {
          totals += res.updated;
          spends += res.spendItems;
        }
      } else {
        console.log(`Source "${source}" not implemented in this run mode.`);
      }
    } catch (e) {
      failures++;
      console.error(`❌ Day ${d} failed:`, e?.response?.data || e.message || e);
    }
  }

  console.log(`Done. Days=${days} TotalsAdded=${totals} SpendItems=${spends} Failures=${failures}${dryRun ? ' (DRY)' : ''}`);
}

if (require.main === module) {
  main().catch(e => {
    console.error(e?.response?.data || e.message || e);
    process.exit(1);
  });
}
'@ | Set-Content -Encoding UTF8 .\backfill-all.js

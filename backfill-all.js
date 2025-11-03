// File: backfill-all.js
require('dotenv').config();
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const { getHubspotClient } = require('./src/hubspotClient');
const { getDailyCampaignRows } = require('./src/msadsReport'); // Bing/MS Ads

function ymd(date) { return date.toISOString().slice(0,10); }
function* datesBetween(fromYmd, toYmd) {
  const start = new Date(`${fromYmd}T00:00:00Z`);
  const end   = new Date(`${toYmd}T00:00:00Z`);
  for (let d=new Date(start); d<=end; d.setUTCDate(d.getUTCDate()+1)) {
    yield ymd(d);
  }
}

const argv = yargs(hideBin(process.argv))
  .option('source', { type: 'string', choices: ['bing'], default: 'bing', describe: 'Data source to backfill' })
  .option('from',   { type: 'string', demandOption: true, describe: 'Start date (YYYY-MM-DD)' })
  .option('to',     { type: 'string', demandOption: true, describe: 'End date (YYYY-MM-DD)' })
  .option('dryRun', { type: 'boolean', default: false })
  .strict()
  .parse();

async function main() {
  const { source, from, to, dryRun } = argv;
  console.log(`Backfill ALL (${source}) ${from} → ${to}${dryRun ? ' [DRY RUN]' : ''}`);

  const hs = getHubspotClient();
  let days = 0, totalsAdded = 0, spendItems = 0, failures = 0;

  for (const day of datesBetween(from, to)) {
    days++;
    try {
      const rows = await getDailyCampaignRows(day);
      if (!rows || rows.length === 0) {
        console.log(`- ${day}: no data (skipped)`);
        continue;
      }

      for (const r of rows) {
        const name = r.campaignName || r.name || `Bing ${r.campaignId}`;
        const clicks = Number(r.clicks || 0);
        const imps   = Number(r.impressions || 0);
        const convs  = Number(r.conversions || 0);
        const spend  = Number(r.spend || 0); // already major units

        // 1) ensure HubSpot campaign
        let campaign;
        try {
          campaign = await hs.ensureCampaign(name);
        } catch (e) {
          failures++;
          console.error(`❌ Ensure campaign failed for "${name}": ${e.message || e}`);
          continue;
        }

        // 2) create spend item (tolerate duplicates via 409)
        if (spend > 0) {
          if (dryRun) {
            console.log(`[DRY] spend ${name} ${day} £${spend.toFixed(2)}`);
          } else {
            try {
              await hs.createSpendItem(campaign.id, { isoDate: day, amountMajor: spend, source: 'Bing' });
              spendItems++;
              console.log(`💷 spend: ${name} ${day} £${spend.toFixed(2)} (created)`);
            } catch (e) {
              failures++;
              console.error(`❌ Spend item failed for "${name}": ${e.message || e}`);
            }
          }
        }

        // 3) ADDITIVE totals
        if (clicks || imps || convs) {
          if (dryRun) {
            console.log(`[DRY] totals ${name} +clicks ${clicks} +imps ${imps} +conv ${convs}`);
            totalsAdded++;
          } else {
            try {
              await hs.addTotals(campaign.id, { clicks, impressions: imps, conversions: convs }, day);
              totalsAdded++;
              console.log(`✅ totals: ${name} clicks+${clicks} imps+${imps} conv+${convs}`);
            } catch (e) {
              failures++;
              console.error(`❌ Totals failed for "${name}": ${e.response?.data || e.message || e}`);
            }
          }
        }
      }
    } catch (e) {
      failures++;
      console.error(`❌ Day ${day} failed: ${e.message || e}`);
    }
  }

  console.log(`Done. Days=${days} TotalsAdded=${totalsAdded} SpendItems=${spendItems} Failures=${failures}${dryRun ? ' (DRY)' : ''}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});

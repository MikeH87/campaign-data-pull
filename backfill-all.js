// backfill-all.js
// Backfill ALL sources (Bing + Twitter) into HubSpot:
//  - ensure campaign exists (by name)
//  - create daily spend line item (major units)
//  - increment channel-specific totals + set "last_*" props for that day
//
// Usage:
//   node backfill-all.js --from=YYYY-MM-DD --to=YYYY-MM-DD [--dryRun]
//
// ENV already used in your project for Bing + HubSpot.
// Additional ENV for Twitter:
//   TWITTER_BEARER_TOKEN=...
//   TWITTER_ACCOUNT_ID=...

require('dotenv').config();
const { DateTime } = require('luxon');
const { getDailyCampaignRows: getMsadsRows } = require('./src/msadsReport');
const { getDailyCampaignRows: getTwitterRows } = require('./src/twitterAdsReport');
const {
  getHubspotClient,
  ensureCampaignByName,
  addSpendItemMajor,
  incrementTotals,
  setLastProps
} = require('./src/hubspotClient');

const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
  .option('from', { type: 'string', demandOption: true })
  .option('to', { type: 'string', demandOption: true })
  .option('dryRun', { type: 'boolean', default: false })
  .parse();

function* daysInclusive(fromYMD, toYMD) {
  let d = DateTime.fromISO(fromYMD, { zone: 'Europe/London' });
  const end = DateTime.fromISO(toYMD, { zone: 'Europe/London' });
  while (d <= end) {
    yield d.toISODate();
    d = d.plus({ days: 1 });
  }
}

async function processProvider(providerName, getRowsFn, propMap, dateYMD, dryRun) {
  const rows = await getRowsFn(dateYMD);

  if (!rows || rows.length === 0) {
    console.log(`- ${providerName} ${dateYMD}: no data (skipped)`);
    return { spend: 0, totals: 0 };
  }

  let spendCount = 0, totalsCount = 0;

  for (const r of rows) {
    const name = r.campaignName || r.name || `Unknown-${r.campaignId}`;

    // 1) Ensure campaign exists (by name)
    let campaignId;
    try {
      campaignId = await ensureCampaignByName(name, { dryRun });
      if (!campaignId) throw new Error('ensureCampaignByName returned empty id');
    } catch (e) {
      console.error(`❌ Ensure campaign failed for "${name}": ${e.message || e}`);
      continue;
    }

    // 2) Spend item (MAJOR units)
    if (!dryRun) {
      try {
        await addSpendItemMajor(campaignId, `${providerName} ${dateYMD}`, r.spend, dateYMD);
        console.log(`💷 spend: ${name} ${dateYMD} £${r.spend.toFixed(2)} (created)`);
        spendCount++;
      } catch (e) {
        console.error(`❌ Spend item failed for "${name}": ${e.message || e}`);
      }
    } else {
      console.log(`[DRY] spend ${name} ${dateYMD} £${r.spend.toFixed(2)}`);
      spendCount++;
    }

    // 3) Totals + last props
    const totals = {
      clicks: r.clicks || 0,
      impressions: r.impressions || 0,
      conversions: r.conversions || 0
    };
    const last = {
      avg_cpc: r.average_cpc ?? null,
      cpl: r.all_cost_per_conversion ?? null,
      status: r.campaign_status || 'Unknown',
      processedYMD: dateYMD
    };

    if (!dryRun) {
      try {
        await incrementTotals(campaignId, propMap, totals);
        await setLastProps(campaignId, propMap, last);
        console.log(`✅ totals: ${name} clicks+${totals.clicks} imps+${totals.impressions} conv+${totals.conversions}`);
        totalsCount++;
      } catch (e) {
        console.error(`❌ Totals failed for "${name}": ${e.response?.data ? JSON.stringify(e.response.data) : (e.message || e)}`);
      }
    } else {
      console.log(`[DRY] totals ${name} +clicks ${totals.clicks} +imps ${totals.impressions} +conv ${totals.conversions}`);
      totalsCount++;
    }
  }

  return { spend: spendCount, totals: totalsCount };
}

(async function main() {
  const from = argv.from;
  const to = argv.to;
  const dryRun = !!argv.dryRun;

  console.log(`Backfill ALL (spend items + ADD totals) ${from} → ${to}${dryRun ? ' [DRY RUN]' : ''}`);

  // HubSpot client init early to fail-fast on creds
  await getHubspotClient();

  // Property maps (HubSpot property names) — Bing uses your current names; Twitter uses new ones.
  const bingProps = {
    totals: {
      clicks: process.env.HSPROP_TOTAL_CLICKS || 'total_clicks',
      impressions: process.env.HSPROP_TOTAL_IMPRESSIONS || 'total_impressions',
      conversions: process.env.HSPROP_TOTAL_CONVERSIONS || 'total_conversions',
    },
    last: {
      avg_cpc: process.env.HSPROP_LAST_AVG_CPC || 'avg_cpc_last',
      cpl: process.env.HSPROP_LAST_CPL || 'cpl_last',
      status: process.env.HSPROP_LAST_STATUS || 'bing_last_status',
      processed: process.env.HSPROP_LAST_BING_DATE || 'bing_last_processed',
    }
  };

  const twitterProps = {
    totals: {
      clicks: 'twitter_click_total',
      impressions: 'twitter_impression_total',
      conversions: 'twitter_conversion_total',
    },
    last: {
      avg_cpc: 'twitter_avg_cpc_last',
      cpl: 'twitter_cpl_last',
      status: 'twitter_last_status',
      processed: 'twitter_last_processed',
    }
  };

  let dayCount = 0, spendItems = 0, totalsAdded = 0, skipped = 0;

  for (const d of daysInclusive(from, to)) {
    dayCount++;

    try {
      const bRes = await processProvider('Bing Ads', getMsadsRows, bingProps, d, dryRun);
      spendItems += bRes.spend; totalsAdded += bRes.totals;
    } catch (e) {
      console.error(`❌ Day ${d} (Bing) failed: ${e.stack || e}`);
    }

    try {
      const tRes = await processProvider('Twitter Ads', getTwitterRows, twitterProps, d, dryRun);
      spendItems += tRes.spend; totalsAdded += tRes.totals;
    } catch (e) {
      console.error(`❌ Day ${d} (Twitter) failed: ${e.stack || e}`);
    }
  }

  console.log(`Done. Days=${dayCount} TotalsAdded=${totalsAdded} SpendItems=${spendItems} Skipped=${skipped}${dryRun ? ' (DRY)' : ''}`);
})().catch(e => {
  console.error(e);
  process.exit(1);
});

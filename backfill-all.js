require('dotenv').config();
const { getDailyCampaignRows } = require('./src/msadsReport'); // Bing/MS Ads
const { getHubspotClient } = require('./src/hubspotClient');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

function ymdRange(from, to) {
  const out = [];
  const d0 = new Date(from + 'T00:00:00Z');
  const d1 = new Date(to   + 'T00:00:00Z');
  for (let d = d0; d <= d1; d.setUTCDate(d.getUTCDate() + 1)) {
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

async function processBingDay(hs, isoDate, opts) {
  opts = opts || {};
  const dryRun = !!opts.dryRun;
  const noSpend = !!opts.noSpend;

  const rows = await getDailyCampaignRows(isoDate); // [{campaignName, spend, clicks, impressions, conversions, date}, ...]
  if (!rows || !rows.length) {
    console.log('- ' + isoDate + ': no data');
    return { totalsAdded: 0, spendItems: 0 };
  }

  let totalsAdded = 0;
  let spendItems = 0;

  for (const r of rows) {
    const name = r.campaignName;
    const spend = Number(r.spend || 0); // major units (GBP)
    const clicks = Number(r.clicks || 0);
    const imps = Number(r.impressions || 0);
    const convs = Number(r.conversions || 0);

    let campaignId;
    try {
      campaignId = hs.ensureCampaignIdForName(name);
    } catch (e) {
      console.error('? Ensure campaign failed for "' + name + '": ' + e.message);
      continue;
    }

    // Spend item (idempotent via order)
    if (noSpend) {
      console.log('[NO-SPEND] skip spend ' + name + ' ' + isoDate + ' £' + spend.toFixed(2));
    } else if (dryRun) {
      console.log('[DRY] spend ' + name + ' ' + isoDate + ' £' + spend.toFixed(2));
      spendItems++;
    } else {
      try {
        await hs.createSpendItem(campaignId, { isoDate: isoDate, amountMajor: spend, source: 'Bing' });
        console.log('?? spend: ' + name + ' ' + isoDate + ' £' + spend.toFixed(2) + ' (created or exists)');
        spendItems++;
      } catch (e) {
        console.error('? Spend item failed for "' + name + '": ' + e.message);
      }
    }

    // ADD totals (never overwrite)
    if (clicks || imps || convs) {
      if (dryRun) {
        console.log('[DRY] totals ' + name + ' +clicks ' + clicks + ' +imps ' + imps + ' +conv ' + convs);
      } else {
        try {
          await hs.addTotalsDelta(
            campaignId,
            { clicks: clicks, impressions: imps, conversions: convs },
            isoDate
          );
          console.log('? totals: ' + name + ' clicks+' + clicks + ' imps+' + imps + ' conv+' + convs);
        } catch (e) {
          console.error('? Totals failed for "' + name + '": ' + e.message);
        }
      }
      totalsAdded++;
    }
  }

  return { totalsAdded: totalsAdded, spendItems: spendItems };
}

async function main() {
  const argv = yargs(hideBin(process.argv))
    .option('source', { type: 'string', default: 'bing' })
    .option('from',   { type: 'string', demandOption: true })
    .option('to',     { type: 'string', demandOption: true })
    .option('dryRun', { type: 'boolean', default: false })
    .option('noSpend',{ type: 'boolean', default: false })
    .argv;

  const source = argv.source;
  const from   = argv.from;
  const to     = argv.to;
  const dryRun = !!argv.dryRun;
  const noSpend= !!argv.noSpend;

  const hs = getHubspotClient();

  if (source !== 'bing') {
    console.log('Source "' + source + '" not supported in this run. Use --source=bing');
    return;
  }

  console.log(
    'Backfill ALL (bing) ' +
    from + ' ? ' + to +
    (dryRun ? ' [DRY RUN]' : '') +
    (noSpend ? ' [NO SPEND]' : '')
  );

  const days = ymdRange(from, to);
  let totals = 0, spends = 0, failures = 0;

  for (const isoDate of days) {
    try {
      const r = await processBingDay(hs, isoDate, { dryRun: dryRun, noSpend: noSpend });
      totals += r.totalsAdded;
      spends += r.spendItems;
    } catch (e) {
      failures++;
      console.error('? Day ' + isoDate + ' failed: ' + e.message);
    }
  }

  console.log(
    'Done. Days=' + days.length +
    ' TotalsAdded=' + totals +
    ' SpendItems=' + spends +
    ' Failures=' + failures +
    (dryRun ? ' (DRY)' : '')
  );
}

if (require.main === module) {
  main().catch(function(err) {
    console.error(err);
    process.exit(1);
  });
}
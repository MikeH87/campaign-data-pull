// recalc-totals.js
// Usage: node recalc-totals.js --from=YYYY-MM-DD --to=YYYY-MM-DD
require("dotenv").config();
const { getCampaignSummaryForDate } = require("./src/msadsReport");
const { upsertCampaignIdByName, updateCampaignProps } = require("./src/hubspotMarketing");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");

const {
  HUBSPOT_PRIVATE_APP_TOKEN,
  HSPROP_TOTAL_CLICKS = "total_clicks",
  HSPROP_TOTAL_IMPRESSIONS = "total_impressions",
  HSPROP_TOTAL_CONVERSIONS = "total_conversions",
} = process.env;

if (!HUBSPOT_PRIVATE_APP_TOKEN) {
  console.error("HUBSPOT_PRIVATE_APP_TOKEN missing");
  process.exit(1);
}

function parseArgs() {
  const argv = yargs(hideBin(process.argv))
    .option("from", { type: "string", demandOption: true })
    .option("to", { type: "string", demandOption: true })
    .strict().argv;
  return { from: argv.from, to: argv.to };
}

function* dateRange(from, to) {
  const s = new Date(from + "T00:00:00Z");
  const e = new Date(to + "T00:00:00Z");
  for (let d = s; d <= e; d = new Date(d.getTime() + 86400000)) {
    yield d.toISOString().slice(0, 10);
  }
}

function sumInto(map, name, clicks, imps, conv) {
  const cur = map.get(name) || { clicks: 0, impressions: 0, conversions: 0 };
  cur.clicks += Number(clicks || 0);
  cur.impressions += Number(imps || 0);
  cur.conversions += Number(conv || 0);
  map.set(name, cur);
}

(async () => {
  const { from, to } = parseArgs();
  console.log(`Recalculating totals from ${from} to ${to}`);

  // 1) Pull MS Ads per day and build sums
  const totalsByCampaign = new Map();

  for (const day of dateRange(from, to)) {
    try {
      const { items } = await getCampaignSummaryForDate(day);
      // Skip blank days automatically
      if (!items || !items.length) {
        process.stdout.write("-");
        continue;
      }
      for (const it of items) {
        sumInto(totalsByCampaign, it.name, it.clicks, it.impressions, it.conversions);
      }
      process.stdout.write(".");
    } catch (e) {
      process.stdout.write("x"); // mark error and continue
    }
  }
  console.log(`\nBuilt totals for ${totalsByCampaign.size} campaign(s).`);

  if (totalsByCampaign.size === 0) {
    console.log("Nothing to update.");
    return;
  }

  // 2) Upsert campaigns by name and SET totals (hard set for the whole range)
  let updated = 0;
  for (const [name, totals] of totalsByCampaign.entries()) {
    try {
      const id = await upsertCampaignIdByName(HUBSPOT_PRIVATE_APP_TOKEN, name);
      const props = {};
      props[HSPROP_TOTAL_CLICKS] = String(totals.clicks);
      props[HSPROP_TOTAL_IMPRESSIONS] = String(totals.impressions);
      props[HSPROP_TOTAL_CONVERSIONS] = String(totals.conversions);
      await updateCampaignProps(HUBSPOT_PRIVATE_APP_TOKEN, id, props);
      updated++;
      console.log(`Updated totals: ${name}  clicks=${totals.clicks}  imps=${totals.impressions}  conv=${totals.conversions}`);
    } catch (e) {
      const code = e.response?.status;
      const body = e.response?.data;
      console.warn(`Failed to update ${name}: ${code || ""} ${e.message}`);
      if (body) console.warn("Body:", JSON.stringify(body));
    }
  }

  console.log(`✅ Done. Totals updated for ${updated} campaign(s).`);
})();

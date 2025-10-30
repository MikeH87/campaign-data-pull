/**
 * recalc-totals-simple.js
 * Updates ONLY totals (clicks, impressions, conversions) for campaigns we already know,
 * using a local name->ID map (campaign-map.json). No search, no create. Skips days with zero spend.
 *
 * Usage:
 *   node recalc-totals-simple.js --from=YYYY-MM-DD --to=YYYY-MM-DD
 */

require("dotenv").config();
const fs = require("fs");
const path = require("path");
const axios = require("axios");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");

// Wrapper that filters to spend > 0 and keeps your existing report logic untouched
const { getNonZeroSummaryForDate } = require("./src/nonZeroReport");

const {
  HUBSPOT_PRIVATE_APP_TOKEN,
  HUBSPOT_BUSINESS_UNIT_ID, // optional
  HSPROP_TOTAL_CLICKS = "total_clicks",
  HSPROP_TOTAL_IMPRESSIONS = "total_impressions",
  HSPROP_TOTAL_CONVERSIONS = "total_conversions",
} = process.env;

if (!HUBSPOT_PRIVATE_APP_TOKEN) {
  console.error("HUBSPOT_PRIVATE_APP_TOKEN missing");
  process.exit(1);
}

// ---- Load local name->ID map (no API lookups) ----
const MAP_PATH = path.join(__dirname, "campaign-map.json");
if (!fs.existsSync(MAP_PATH)) {
  console.error("campaign-map.json not found. Create it first (name -> HubSpot campaign ID).");
  process.exit(1);
}
let NAME_TO_ID;
try {
  NAME_TO_ID = JSON.parse(fs.readFileSync(MAP_PATH, "utf8"));
} catch (e) {
  console.error("Failed to read campaign-map.json:", e.message);
  process.exit(1);
}

function hsHeaders() {
  const h = {
    Authorization: `Bearer ${HUBSPOT_PRIVATE_APP_TOKEN}`,
    Accept: "application/json",
    "Content-Type": "application/json",
  };
  if (HUBSPOT_BUSINESS_UNIT_ID) h["X-HubSpot-Business-Unit-Id"] = HUBSPOT_BUSINESS_UNIT_ID;
  return h;
}

function baseUrl() {
  return "https://api.hubapi.com"; // axios follows 308 region redirects
}

async function updateCampaignProps(campaignId, properties) {
  const url = `${baseUrl()}/marketing/v3/campaigns/${encodeURIComponent(campaignId)}`;
  await axios.patch(url, { properties }, { headers: hsHeaders(), maxRedirects: 5, timeout: 30000 });
}

function* dateRange(from, to) {
  const s = new Date(`${from}T00:00:00Z`);
  const e = new Date(`${to}T00:00:00Z`);
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
  const argv = yargs(hideBin(process.argv))
    .option("from", { type: "string", demandOption: true })
    .option("to", { type: "string", demandOption: true })
    .strict().argv;

  const from = argv.from;
  const to = argv.to;
  console.log(`Recalculating totals (non-zero spend days only) from ${from} to ${to}`);

  // 1) Build totals per campaign name from Bing for the date range
  const totalsByName = new Map();

  for (const day of dateRange(from, to)) {
    try {
      const { items } = await getNonZeroSummaryForDate(day);
      if (!items || !items.length) { process.stdout.write("-"); continue; } // dash = no spend that day
      for (const it of items) {
        sumInto(totalsByName, it.name, it.clicks, it.impressions, it.conversions);
      }
      process.stdout.write("."); // dot = had spend and included
    } catch (e) {
      process.stdout.write("x"); // x = transient error that day; continue
    }
  }

  console.log(`\nBuilt totals for ${totalsByName.size} campaign(s).`);

  // 2) Update HubSpot totals ONLY for campaigns present in our local map
  let updated = 0, skippedUnknown = 0, failed = 0;

  for (const [name, totals] of totalsByName.entries()) {
    const campaignId = NAME_TO_ID[name];
    if (!campaignId) {
      console.warn(`Skipping unknown campaign (not in campaign-map.json): ${name}`);
      skippedUnknown++;
      continue;
    }
    try {
      const props = {};
      props[HSPROP_TOTAL_CLICKS] = String(totals.clicks);
      props[HSPROP_TOTAL_IMPRESSIONS] = String(totals.impressions);
      props[HSPROP_TOTAL_CONVERSIONS] = String(totals.conversions);
      await updateCampaignProps(campaignId, props);
      updated++;
      console.log(`Updated totals: ${name}  clicks=${totals.clicks}  imps=${totals.impressions}  conv=${totals.conversions}`);
    } catch (e) {
      failed++;
      const code = e.response?.status;
      console.warn(`Failed to update ${name} (${campaignId}): ${code || ""} ${e.message}`);
      if (e.response?.data) console.warn("Body:", JSON.stringify(e.response.data));
    }
  }

  console.log(`✅ Done. Updated=${updated}  SkippedUnknown=${skippedUnknown}  Failed=${failed}`);
})();

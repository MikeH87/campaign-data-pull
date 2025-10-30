// bulk-before.js
require("dotenv").config();
const minimist = require("minimist");
const { addDays, format, isValid, parseISO } = require("date-fns");
const {
  createHubSpotClient,
  findCampaignByName,
  createCampaign,
  getCampaign,
  updateCampaign,
  ensureDailySpendItem,
} = require("./src/hubspotClient");
const { getCampaignSummaryForDate } = require("./src/msadsReport");

const {
  HSPROP_TOTAL_CLICKS = "total_clicks",
  HSPROP_TOTAL_IMPRESSIONS = "total_impressions",
  HSPROP_TOTAL_CONVERSIONS = "total_conversions",
  HSPROP_LAST_AVG_CPC = "avg_cpc_last", // not used in bulk, but we leave here if you ever want to set an average
  HSPROP_LAST_CPL = "cpl_last",          // same as above
  HSPROP_LAST_STATUS = "bing_last_status",
  HSPROP_LAST_BING_DATE = "bing_last_processed",
  HUBSPOT_BUSINESS_UNIT_ID,
  CAMPAIGN_NAME_PREFIX = "",
} = process.env;

function ymd(d) { return format(d, "yyyy-MM-dd"); }
function parseYMD(s) { const d = parseISO(s); if (!isValid(d)) throw new Error(`Invalid date: ${s}`); return d; }
function toNumber(v){ if(v==null) return 0; const n=Number(String(v).replace(/,/g,"")); return Number.isFinite(n)?n:0; }

async function ensureHubSpotCampaign(hs, originalName) {
  const hsName = `${CAMPAIGN_NAME_PREFIX}${originalName}`;
  let c = await findCampaignByName(hs, hsName);
  if (!c) {
    c = await createCampaign(hs, hsName, HUBSPOT_BUSINESS_UNIT_ID);
    console.log(`Created HubSpot campaign: ${hsName} (id ${c.id})`);
  }
  return { id: c.id, hsName };
}

async function main() {
  const argv = minimist(process.argv.slice(2));
  // Usage:
  //   node bulk-before.js --start=YYYY-MM-DD --end=YYYY-MM-DD
  // Example (everything before Apr 1, 2025):
  //   node bulk-before.js --start=2024-01-01 --end=2025-03-31

  if (!argv.start || !argv.end) {
    console.error("Please provide --start=YYYY-MM-DD and --end=YYYY-MM-DD");
    process.exit(1);
  }

  const start = parseYMD(argv.start);
  const end   = parseYMD(argv.end);
  if (end < start) { console.error("end date is before start date"); process.exit(1); }

  const hs = createHubSpotClient();

  // 1) Aggregate per-campaign totals across the window (day-by-day pulls, but we only CREATE ONE bulk spend item)
  const totals = new Map(); // name -> { spend, clicks, impressions, conversions, status }
  for (let d = start; d <= end; d = addDays(d, 1)) {
    const day = ymd(d);
    try {
      const { items } = await getCampaignSummaryForDate(day);
      for (const it of items) {
        const key = it.name;
        const cur = totals.get(key) || {
          spend: 0, clicks: 0, impressions: 0, conversions: 0, status: it.campaign_status || "",
        };
        cur.spend       += toNumber(it.spend);
        cur.clicks      += toNumber(it.clicks);
        cur.impressions += toNumber(it.impressions);
        cur.conversions += toNumber(it.conversions);
        if (!cur.status && it.campaign_status) cur.status = it.campaign_status;
        totals.set(key, cur);
      }
      // polite pause to avoid rate limits
      await new Promise(r => setTimeout(r, 800));
    } catch (e) {
      const msg = e?.response
        ? `HTTP ${e.response.status} ${e.response.statusText} ${JSON.stringify(e.response.data)}`
        : e?.message || String(e);
      console.error(`Day ${day} failed: ${msg}`);
      // continue to next day
    }
  }

  console.log(`\nAggregated ${totals.size} campaign(s) for ${argv.start} → ${argv.end}`);

  // 2) For each campaign, create/update ONE spend item named "Spend through <end> (Bing bulk)"
  const bulkName = `Spend through ${ymd(end)} (Bing bulk)`;
  let created = 0, updated = 0, unchanged = 0;

  for (const [campaignName, agg] of totals.entries()) {
    try {
      const { id: hsId, hsName } = await ensureHubSpotCampaign(hs, campaignName);

      // create/update bulk spend item
      const spendAmount = +toNumber(agg.spend).toFixed(2);
      const desc = `Bing Ads bulk spend up to ${ymd(end)} for ${campaignName}`;
      const res = await ensureDailySpendItem(hs, hsId, { name: bulkName, amount: spendAmount, description: desc });
      if (res.action === "created") created++;
      else if (res.action === "updated") updated++;
      else unchanged++;

      // update cumulative totals (add the bulk window counts once)
      const current = await getCampaign(hs, hsId);
      const props = {};
      if (HSPROP_TOTAL_CLICKS)       props[HSPROP_TOTAL_CLICKS]      = toNumber(current[HSPROP_TOTAL_CLICKS])      + agg.clicks;
      if (HSPROP_TOTAL_IMPRESSIONS)  props[HSPROP_TOTAL_IMPRESSIONS] = toNumber(current[HSPROP_TOTAL_IMPRESSIONS]) + agg.impressions;
      if (HSPROP_TOTAL_CONVERSIONS)  props[HSPROP_TOTAL_CONVERSIONS] = toNumber(current[HSPROP_TOTAL_CONVERSIONS]) + agg.conversions;
      if (HSPROP_LAST_STATUS && agg.status) props[HSPROP_LAST_STATUS] = String(agg.status);

      if (Object.keys(props).length > 0) {
        try { await updateCampaign(hs, hsId, props); }
        catch (e) {
          if (e.response) {
            console.error(`⚠️ Totals update warning for ${hsName}`, e.response.status, e.response.statusText, JSON.stringify(e.response.data));
          } else {
            console.error(`⚠️ Totals update warning for ${hsName}: ${e.message}`);
          }
        }
      }
    } catch (e) {
      if (e.response) console.error(`❌ HubSpot error for ${campaignName}`, e.response.status, e.response.statusText, e.response.data);
      else console.error(`❌ Error for ${campaignName}:`, e.message);
    }
  }

  console.log(`\nBulk summary for ${argv.start} → ${argv.end}: created=${created}, updated=${updated}, unchanged=${unchanged}`);
}

main().catch(e => {
  console.error("Fatal error:", e?.message || e);
  process.exit(1);
});

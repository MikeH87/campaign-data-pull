// src/syncBingToHubspot.js
require("dotenv").config();
const {
  createHubSpotClient,
  findCampaignByName,
  createCampaign,
  getCampaign,
  updateCampaign,
  ensureDailySpendItem,
} = require("./hubspotClient");
const { getYesterdayCampaignSummary, getCampaignSummaryForDate } = require("./msadsReport");

const {
  // NOTE: HubSpot auto-sums spend items into this; we DO NOT write it.
  HSPROP_TOTAL_SPEND = "hs_spend_items_sum_amount",
  HSPROP_TOTAL_CLICKS = "total_clicks",
  HSPROP_TOTAL_IMPRESSIONS = "total_impressions",
  HSPROP_TOTAL_CONVERSIONS = "total_conversions",
  HSPROP_LAST_AVG_CPC = "avg_cpc_last",
  HSPROP_LAST_CPL = "cpl_last",
  HSPROP_LAST_STATUS = "bing_last_status",        // custom text
  HSPROP_LAST_BING_DATE = "bing_last_processed",  // custom date picker
  HUBSPOT_BUSINESS_UNIT_ID,
  CAMPAIGN_NAME_PREFIX = "", // e.g. "Bing – "
} = process.env;

const toNumber = (v) => {
  if (v == null) return 0;
  const n = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : 0;
};

function ymdToEpochMs(ymd) {
  const [Y, M, D] = ymd.split("-").map(Number);
  return Date.UTC(Y, M - 1, D); // midnight UTC in ms
}

async function ensureHubSpotCampaign(hs, originalName) {
  const hsName = `${CAMPAIGN_NAME_PREFIX}${originalName}`;
  let c = await findCampaignByName(hs, hsName);
  if (!c) {
    c = await createCampaign(hs, hsName, HUBSPOT_BUSINESS_UNIT_ID);
    console.log(`Created HubSpot campaign: ${hsName} (id ${c.id})`);
  }
  return { id: c.id, hsName };
}

async function upsertForOneCampaign(hs, summaryItem, summaryDate) {
  const srcName = summaryItem.name;
  const { id: hsId, hsName } = await ensureHubSpotCampaign(hs, srcName);

  // 1) Idempotent daily spend item
  const spendAmount = +toNumber(summaryItem.spend).toFixed(2);
  const spendName = `Spend ${summaryDate} (Bing)`;
  const spendDesc = `Bing Ads daily spend for ${srcName} on ${summaryDate}`;
  const spendRes = await ensureDailySpendItem(hs, hsId, {
    name: spendName,
    amount: spendAmount,
    description: spendDesc,
  });

  // 2) Cumulative totals: read current, then add yesterday's
  const current = await getCampaign(hs, hsId);
  const clicksCum = toNumber(current[HSPROP_TOTAL_CLICKS]) + toNumber(summaryItem.clicks);
  const impsCum   = toNumber(current[HSPROP_TOTAL_IMPRESSIONS]) + toNumber(summaryItem.impressions);
  const convsCum  = toNumber(current[HSPROP_TOTAL_CONVERSIONS]) + toNumber(summaryItem.conversions);

  const props = {};
  if (HSPROP_TOTAL_CLICKS)       props[HSPROP_TOTAL_CLICKS] = clicksCum;
  if (HSPROP_TOTAL_IMPRESSIONS)  props[HSPROP_TOTAL_IMPRESSIONS] = impsCum;
  if (HSPROP_TOTAL_CONVERSIONS)  props[HSPROP_TOTAL_CONVERSIONS] = convsCum;
  if (HSPROP_LAST_AVG_CPC)       props[HSPROP_LAST_AVG_CPC] = +toNumber(summaryItem.average_cpc).toFixed(4);
  if (HSPROP_LAST_CPL)           props[HSPROP_LAST_CPL] = +toNumber(summaryItem.all_cost_per_conversion).toFixed(4);
  if (HSPROP_LAST_STATUS)        props[HSPROP_LAST_STATUS] = String(summaryItem.campaign_status || "");
  if (HSPROP_LAST_BING_DATE)     props[HSPROP_LAST_BING_DATE] = ymdToEpochMs(summaryDate); // epoch ms

  try {
    if (Object.keys(props).length > 0) {
      await updateCampaign(hs, hsId, props);
    }
  } catch (e) {
    if (e.response) {
      console.error(
        `⚠️ Campaign props update warning for ${hsName}`,
        e.response.status,
        e.response.statusText,
        JSON.stringify(e.response.data)
      );
    } else {
      console.error(`⚠️ Campaign props update warning for ${hsName}: ${e.message}`);
    }
  }

  return { hsId, hsName, spendAction: spendRes.action };
}

async function runForDate(ymd) {
  const hs = createHubSpotClient();
  const { date, items } = await getCampaignSummaryForDate(ymd);
  console.log(`Bing summary for ${date}: ${items.length} campaign(s).`);
  if (items.length === 0) {
    console.log("Nothing to sync for that date (no spend).");
    return { date, created: 0, updated: 0, unchanged: 0 };
  }
  let created = 0, updated = 0, unchanged = 0;
  for (const it of items) {
    try {
      const r = await upsertForOneCampaign(hs, it, date);
      if (r.spendAction === "created") created++;
      else if (r.spendAction === "updated") updated++;
      else unchanged++;
    } catch (e) {
      if (e.response) console.error(`❌ HubSpot error for ${it.name}`, e.response.status, e.response.statusText, e.response.data);
      else console.error(`❌ Error for ${it.name}:`, e.message);
    }
  }
  console.log(`Summary: spend created=${created}, updated=${updated}, unchanged=${unchanged}`);
  return { date, created, updated, unchanged };
}

async function runForYesterday() {
  const { date, items } = await require("./msadsReport").getYesterdayCampaignSummary();
  const hs = createHubSpotClient();
  console.log(`Bing summary for ${date}: ${items.length} campaign(s).`);
  if (items.length === 0) {
    console.log("Nothing to sync today (no spend yesterday).");
    return { date, created: 0, updated: 0, unchanged: 0 };
  }
  let created = 0, updated = 0, unchanged = 0;
  for (const it of items) {
    const r = await upsertForOneCampaign(hs, it, date);
    if (r.spendAction === "created") created++;
    else if (r.spendAction === "updated") updated++;
    else unchanged++;
  }
  console.log(`Summary: spend created=${created}, updated=${updated}, unchanged=${unchanged}`);
  return { date, created, updated, unchanged };
}

module.exports = { syncBingToHubspot: runForYesterday, syncBingForDate: runForDate };

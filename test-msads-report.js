const { getYesterdayCampaignSummary } = require("./src/msadsReport");

(async () => {
  try {
    const { date, items } = await getYesterdayCampaignSummary();
    console.log(`✅ Got Microsoft Ads report for ${date}`);
    if (items.length === 0) {
      console.log("No rows returned (no spend yesterday or empty account).");
      return;
    }
    // Print a small table
    for (const it of items) {
      console.log(
        `${it.name} | clicks:${it.clicks} | spend:${it.spend.toFixed(2)} | imp:${it.impressions} | avgCPC:${it.average_cpc.toFixed(2)} | conv:${it.conversions} | CPL:${it.all_cost_per_conversion.toFixed(2)} | status:${it.campaign_status}`
      );
    }
  } catch (e) {
    if (e.response) {
      console.error("❌ HTTP", e.response.status, e.response.statusText, e.response.data);
    } else {
      console.error("❌ Error", e.message);
    }
    process.exit(1);
  }
})();

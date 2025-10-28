const { getCampaignSummaryForDate } = require("./src/msadsReport");

// CHANGE THIS DATE to the day you saw spend in the UI (London date)
const YMD = "2025-10-27";

(async () => {
  try {
    const res = await getCampaignSummaryForDate(YMD);
    console.log(`✅ Report for ${res.date} — rows: ${res.rowCount}`);
    console.log(`Saved ZIP: ${res.zipPath}`);
    console.log(`Saved CSV: ${res.csvPath}`);
    if (res.items.length === 0) {
      console.log("No items parsed — open the CSV to inspect headers/values.");
    } else {
      console.log("First few items:");
      console.log(res.items.slice(0, 5));
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

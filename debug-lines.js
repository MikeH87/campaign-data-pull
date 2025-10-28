require("dotenv").config();
const { getCampaignSummaryForDate, debugGetRawCsvTextForDate } = require("./src/msadsReport");

const YMD = "2025-10-27";

(async () => {
  try {
    const res = await getCampaignSummaryForDate(YMD);
    console.log(`Parsed rowCount: ${res.rowCount}`);
    console.log("Detected delimiter/header info:", {
      headerIndex: res._debug.headerIndex,
      headerLine: res._debug.headerLine,
      delimiter: JSON.stringify(res._debug.delimiter),
      sampleRow: res._debug.sampleRow
    });

    if (res.items.length > 0) {
      console.log("First item:", res.items[0]);
    } else {
      console.log("No items parsed; printing first 20 lines of raw CSV for inspection:");
      const raw = await debugGetRawCsvTextForDate(YMD);
      const lines = raw.replace(/\r\n/g, "\n").split("\n").slice(0, 20);
      console.log(lines);
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

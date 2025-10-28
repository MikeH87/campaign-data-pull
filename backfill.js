require("dotenv").config();
const { syncBingForDate } = require("./src/syncBingToHubspot");

// Usage:
//   node backfill.js --date=2025-10-27
//   node backfill.js --from=2025-09-01 --to=2025-10-27

function parseArgs() {
  const args = Object.fromEntries(
    process.argv.slice(2).map((p) => {
      const [k, v] = p.replace(/^--/, "").split("=");
      return [k, v ?? true];
    })
  );
  return args;
}

function* dateRange(fromYmd, toYmd) {
  const from = new Date(fromYmd + "T00:00:00Z");
  const to = new Date(toYmd + "T00:00:00Z");
  for (let d = new Date(from); d <= to; d.setUTCDate(d.getUTCDate() + 1)) {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    yield `${y}-${m}-${dd}`;
  }
}

(async () => {
  try {
    const { date, from, to } = parseArgs();

    if (date) {
      console.log(`Backfill single day: ${date}`);
      const res = await syncBingForDate(date);
      console.log("✅ Done:", res);
      return;
    }

    if (from && to) {
      console.log(`Backfill range: ${from} → ${to}`);
      let totalUpdated = 0;
      let totalSkipped = 0;
      for (const ymd of dateRange(from, to)) {
        console.log(`\n=== ${ymd} ===`);
        const res = await syncBingForDate(ymd);
        totalUpdated += res.updated;
        totalSkipped += res.skipped;
      }
      console.log(`\n✅ Range complete. updated=${totalUpdated}, skipped=${totalSkipped}`);
      return;
    }

    console.error("Usage:\n  node backfill.js --date=YYYY-MM-DD\n  node backfill.js --from=YYYY-MM-DD --to=YYYY-MM-DD");
    process.exit(1);
  } catch (e) {
    if (e.response) {
      console.error("❌ HTTP", e.response.status, e.response.statusText, e.response.data);
    } else {
      console.error("❌ Error", e.message);
    }
    process.exit(1);
  }
})();

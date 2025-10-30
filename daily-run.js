// daily-run.js
require("dotenv").config();
const { execFileSync } = require("node:child_process");

function yyyymmddUTCYesterday() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
}

(async () => {
  const date = process.argv[2] || yyyymmddUTCYesterday();
  console.log(`Running daily pipeline for ${date}`);

  // 1) Create/ensure spend line items for that day (your existing script)
  //    Adjust the path/filename below to the exact script you use in your repo (e.g. backfill.js)
  execFileSync(process.execPath, ["backfill.js", `--date=${date}`], {
    stdio: "inherit",
  });

  // 2) Add one-day metrics into cumulative totals (the script from Step 1)
  execFileSync(process.execPath, ["add-totals-for-day.js", `--date=${date}`], {
    stdio: "inherit",
  });

  console.log("✅ Daily pipeline complete.");
})();

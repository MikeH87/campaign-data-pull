// backfill-range.js
require("dotenv").config();
const minimist = require("minimist");
const { addDays, format, isValid, parseISO } = require("date-fns");
const { syncBingForDate } = require("./src/syncBingToHubspot");

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function ymd(d) {
  return format(d, "yyyy-MM-dd");
}

function parseYMD(s) {
  const d = parseISO(s);
  if (!isValid(d)) {
    throw new Error(`Invalid date: ${s}. Use YYYY-MM-DD`);
  }
  return d;
}

/**
 * Build an inclusive list of YYYY-MM-DD strings from start to end.
 */
function buildDateRange(startStr, endStr) {
  const start = parseYMD(startStr);
  const end = parseYMD(endStr);
  if (end < start) throw new Error("end date is before start date");

  const days = [];
  for (let d = start; d <= end; d = addDays(d, 1)) {
    days.push(ymd(d));
  }
  return days;
}

async function main() {
  const argv = minimist(process.argv.slice(2));
  // Options:
  //   --start=YYYY-MM-DD --end=YYYY-MM-DD   (inclusive)
  //   --since=YYYY-MM-DD                    (inclusive) to today-1
  //   --days=N                              (last N days ending yesterday)
  //   --pauseMs=1500                        (pause between days)
  //   --maxRetries=3                        (per day)
  const pauseMs = Number(argv.pauseMs ?? 1500);
  const maxRetries = Number(argv.maxRetries ?? 3);

  // Choose a date range
  let dates = [];
  if (argv.start && argv.end) {
    dates = buildDateRange(argv.start, argv.end);
  } else if (argv.since) {
    const start = argv.since;
    const yesterday = ymd(addDays(new Date(), -1));
    dates = buildDateRange(start, yesterday);
  } else {
    const days = Number(argv.days ?? 7);
    const end = addDays(new Date(), -1);
    const start = addDays(end, -(days - 1));
    dates = buildDateRange(ymd(start), ymd(end));
  }

  console.log(`Backfilling ${dates.length} day(s): ${dates[0]} → ${dates[dates.length - 1]}`);

  let totals = { created: 0, updated: 0, unchanged: 0, failedDays: 0 };

  for (const d of dates) {
    let attempt = 0;
    let success = false;
    while (attempt < maxRetries && !success) {
      attempt++;
      try {
        console.log(`\n=== ${d} (attempt ${attempt}/${maxRetries}) ===`);
        const res = await syncBingForDate(d);
        console.log(`✔ ${d} result:`, res);
        totals.created += res.created ?? 0;
        totals.updated += res.updated ?? 0;
        totals.unchanged += res.unchanged ?? 0;
        success = true;
      } catch (e) {
        const msg = e?.response
          ? `HTTP ${e.response.status} ${e.response.statusText} ${JSON.stringify(e.response.data)}`
          : e?.message || String(e);
        console.error(`✖ ${d} failed: ${msg}`);
        if (attempt < maxRetries) {
          const wait = Math.min(pauseMs * attempt, 10_000);
          console.log(`…waiting ${wait}ms, will retry`);
          await sleep(wait);
        }
      }
    }
    if (!success) {
      totals.failedDays += 1;
      console.log(`⚠ giving up on ${d} after ${maxRetries} attempts`);
    }
    // polite pause between days to avoid rate limits
    await sleep(pauseMs);
  }

  console.log("\n==== Backfill summary ====");
  console.log(totals);
}

main().catch((e) => {
  console.error("Fatal error:", e?.message || e);
  process.exit(1);
});

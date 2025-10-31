// run-bing-daily.js
// Computes "yesterday" based on Europe/London and runs the additive Bing backfill for that date.
// It will also guard so you can safely schedule it hourly and it'll only RUN when London time is 00:10±2min.

require('dotenv').config();
const { spawnSync } = require('child_process');

function londonNow() {
  const s = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  }).format(new Date()); // e.g. "31/10/2025, 00:10:12"
  // s = "dd/mm/yyyy, HH:MM:SS"
  const [datePart, timePart] = s.split(',').map(t => t.trim());
  const [dd, mm, yyyy] = datePart.split('/').map(Number);
  const [HH, MM, SS] = timePart.split(':').map(Number);
  return { yyyy, mm, dd, HH, MM, SS };
}

function ymdFromLondon({ yyyy, mm, dd }) {
  const y = String(yyyy);
  const m = String(mm).padStart(2, '0');
  const d = String(dd).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function londonYesterdayYMD() {
  // Build a Date for *today midnight London*, then subtract 1 day to get yesterday
  const now = londonNow();
  const todayMidLondon = Date.UTC(now.yyyy, now.mm - 1, now.dd, 0, 0, 0);
  const yestUTC = new Date(todayMidLondon - 86400000); // minus 1 day in ms
  // Re-format yestUTC in London:
  const s = new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit' }).format(yestUTC);
  const [dd, mm, yyyy] = s.split('/').map(Number);
  return `${yyyy}-${String(mm).padStart(2,'0')}-${String(dd).padStart(2,'0')}`;
}

// Optional: guard so you can run this on an hourly cron "at minute 10", and it will only *do work* at ~00:10 London.
function shouldRunNow() {
  const { HH, MM } = londonNow();
  // Run only if hour==0 and minute is between 08..12 (buffer for cron drift)
  return (HH === 0) && (MM >= 8 && MM <= 12);
}

(async () => {
  // If you set your Render cron to run hourly at :10, keep this guard = true.
  // If you set your Render cron exactly once per day at the right UTC minute, you can disable the guard.
  const USE_GUARD = true;
  if (USE_GUARD && !shouldRunNow()) {
    console.log('[run-bing-daily] Outside 00:10 London window, skipping.');
    process.exit(0);
  }

  const ymd = londonYesterdayYMD();
  console.log(`[run-bing-daily] Running Bing backfill for ${ymd}`);

  const args = [
    'backfill-all.js',
    `--source=bing`,
    `--from=${ymd}`,
    `--to=${ymd}`
  ];
  const r = spawnSync('node', args, { stdio: 'inherit' });
  process.exit(r.status || 0);
})();

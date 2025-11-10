// File: run-bing-daily.js
require('dotenv').config();
const { spawn } = require('child_process');

function ymdInLondon(date) {
  // Get YYYY-MM-DD for a JS Date as if in Europe/London
  const fmt = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year: 'numeric', month: '2-digit', day: '2-digit'
  });
  const [{ value: day }, , { value: month }, , { value: year }] = fmt.formatToParts(date);
  return `${year}-${month}-${day}`;
}

function getYesterdayLondonYMD() {
  const now = new Date();
  // “Yesterday” relative to London’s current date
  const londonTodayYMD = ymdInLondon(now);
  const d = new Date(`${londonTodayYMD}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10);
}

async function main() {
  const y = getYesterdayLondonYMD();
  console.log(`[run-bing-daily] Running Bing backfill for ${y}`);

  await new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath, // node
      ['backfill-all.js', '--source=bing', `--from=${y}`, `--to=${y}`],
      { stdio: 'inherit' }
    );
    child.on('exit', code => code === 0 ? resolve() : reject(new Error(`backfill-all exited ${code}`)));
    child.on('error', reject);
  });
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});

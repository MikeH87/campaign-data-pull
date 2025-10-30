// backfill-spend-range.js
// Runs your existing backfill.js for each date in a range (inclusive).
// - Sequential, polite delays, and simple retries.
// - Uses your existing backfill.js which is already idempotent and skips zero-spend days.

require('dotenv').config();

const { spawn } = require('child_process');

function parseArgs() {
  const args = process.argv.slice(2);
  const out = {};
  for (const a of args) {
    const m = a.match(/^--(from|to)=(.+)$/i);
    if (m) out[m[1]] = m[2];
  }
  if (!out.from || !out.to) {
    console.error('Usage: node backfill-spend-range.js --from=YYYY-MM-DD --to=YYYY-MM-DD');
    process.exit(1);
  }
  return out;
}

function toDateParts(s) {
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) throw new Error(`Bad date: ${s}`);
  return { y: +m[1], m: +m[2], d: +m[3] };
}

function formatDate(d) {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function addDaysUTC(d, days) {
  const copy = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

async function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }

async function runBackfillOnce(dateStr) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, ['backfill.js', `--date=${dateStr}`], {
      stdio: 'inherit',
      env: process.env,
    });
    child.on('close', (code) => resolve(code === 0));
  });
}

async function runWithRetries(dateStr, attempts = 3) {
  for (let i = 1; i <= attempts; i++) {
    const ok = await runBackfillOnce(dateStr);
    if (ok) return true;
    console.warn(`✖ ${dateStr} failed (attempt ${i}/${attempts}). Waiting before retry…`);
    await sleep(5000 * i); // back-off
  }
  return false;
}

(async () => {
  const { from, to } = parseArgs();
  const f = toDateParts(from);
  const t = toDateParts(to);
  let cur = new Date(Date.UTC(f.y, f.m - 1, f.d));
  const end = new Date(Date.UTC(t.y, t.m - 1, t.d));

  console.log(`Backfilling spend lines from ${from} to ${to} (inclusive)…`);

  let okCount = 0, failCount = 0;
  while (cur <= end) {
    const ds = formatDate(cur);
    console.log(`\n=== ${ds} ===`);
    const ok = await runWithRetries(ds, 3);
    if (ok) okCount++; else failCount++;
    await sleep(800); // small pause between days to be polite to APIs
    cur = addDaysUTC(cur, 1);
  }

  console.log(`\n✅ Range complete. Successes=${okCount}  Failures=${failCount}`);
  process.exit(failCount > 0 ? 2 : 0);
})();

// daily-run-all.js
// Run for "yesterday in UK time" (Bing Ads only) and write to HubSpot.
require('dotenv').config();
const { DateTime } = require('luxon');
const { spawn } = require('child_process');
(async () => {
  const nowUK = DateTime.now().setZone('Europe/London');
  const y = nowUK.minus({ days: 1 }).toISODate();
  const p = spawn(process.execPath, ['backfill-all.js', `--from=${y}`, `--to=${y}`], { stdio: 'inherit' });
  p.on('exit', (code) => process.exit(code));
})();

// daily-run-all.js
// Run for "yesterday in UK time" for both Bing + Twitter, writing to HubSpot.

require('dotenv').config();
const { DateTime } = require('luxon');
const main = require('./backfill-all');

(async () => {
  const nowUK = DateTime.now().setZone('Europe/London');
  const y = nowUK.minus({ days: 1 }).toISODate();
  // Call backfill-all by spawning a child process for clarity:
  const { spawn } = require('child_process');
  const p = spawn(process.execPath, ['backfill-all.js', `--from=${y}`, `--to=${y}`], { stdio: 'inherit' });
  p.on('exit', (code) => process.exit(code));
})();

// delete-campaigns-by-name.js
// Usage: node delete-campaigns-by-name.js "SSAS-BAD-JAN-24" "FIC-BAD-MAR-24"

require('dotenv').config();
const axios = require('axios');

const BASE = 'https://api.hubapi.com/marketing/v3/campaigns';
const TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;

if (!TOKEN) {
  console.error('Missing HUBSPOT_PRIVATE_APP_TOKEN in .env');
  process.exit(1);
}

const wantedNames = process.argv.slice(2).filter(Boolean);
if (wantedNames.length === 0) {
  console.error('Provide at least one campaign name to delete.\nExample: node delete-campaigns-by-name.js "SSAS-BAD-JAN-24"');
  process.exit(1);
}

const headers = { Authorization: `Bearer ${TOKEN}` };

async function listAllCampaigns() {
  const results = [];
  let after = undefined;
  do {
    const url = new URL(BASE);
    url.searchParams.set('limit', '100');
    if (after) url.searchParams.set('after', after);
    const res = await axios.get(url.toString(), { headers, validateStatus: () => true });
    if (res.status !== 200) throw new Error(`List campaigns failed: ${res.status} ${JSON.stringify(res.data)}`);
    const items = res.data?.results || [];
    results.push(...items);
    after = res.data?.paging?.next?.after;
  } while (after);
  return results;
}

async function main() {
  console.log('Fetching campaigns…');
  const all = await listAllCampaigns();
  const byName = new Map();
  for (const c of all) {
    const name = c?.properties?.hs_name || '';
    if (name) byName.set(name, c);
  }

  for (const name of wantedNames) {
    const found = byName.get(name);
    if (!found) {
      console.log(`- Not found: ${name} (already gone)`);
      continue;
    }
    const id = found.id;
    console.log(`- Deleting ${name} (${id})…`);
    const res = await axios.delete(`${BASE}/${id}`, { headers, validateStatus: () => true });
    if (res.status === 204) {
      console.log(`  ✓ Deleted ${name}`);
    } else {
      console.log(`  ✗ Failed to delete ${name}: ${res.status} ${JSON.stringify(res.data)}`);
    }
  }

  console.log('Done.');
}

main().catch(e => {
  console.error(e?.stack || e);
  process.exit(1);
});

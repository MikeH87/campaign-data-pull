// File: sanity-check-add.js
require('dotenv').config();
const { getHubspotClient } = require('./src/hubspotClient');

(async () => {
  const hs = getHubspotClient();
  const name = process.argv[2];
  const date = process.argv[3] || '2025-01-01';
  const clicks = Number(process.argv[4] || 3);
  const imps = Number(process.argv[5] || 100);
  const convs = Number(process.argv[6] || 1);

  if (!name) {
    console.error('Usage: node sanity-check-add.js "Campaign Name" 2025-01-01 3 100 1');
    process.exit(1);
  }

  const { id } = await hs.ensureCampaign(name);
  const before = await hs.getCampaignById(id);
  const props = before.properties || {};
  const keyClicks = process.env.HSPROP_TOTAL_CLICKS || 'total_clicks';
  const keyImps   = process.env.HSPROP_TOTAL_IMPRESSIONS || 'total_impressions';
  const keyConvs  = process.env.HSPROP_TOTAL_CONVERSIONS || 'total_conversions';

  console.log('BEFORE:', {
    clicks: props[keyClicks] || 0,
    imps: props[keyImps] || 0,
    convs: props[keyConvs] || 0,
  });

  await hs.addDailyTotalsAccumulative(id, { clicks, impressions: imps, conversions: convs, dateISO: date });

  const after = await hs.getCampaignById(id);
  const propsAfter = after.properties || {};
  console.log('AFTER:', {
    clicks: propsAfter[keyClicks] || 0,
    imps: propsAfter[keyImps] || 0,
    convs: propsAfter[keyConvs] || 0,
  });
})().catch(e => { console.error(e?.response?.data || e); process.exit(1); });

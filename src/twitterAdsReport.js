// src/twitterAdsReport.js
require('dotenv').config();
const { adsGet, accountPath } = require('./twitterAdsClient');

// Convert micros (if API returns in micros) or strings to number with 2dp
function parseAmount(val) {
  if (val == null) return 0;
  if (typeof val === 'number') return val;
  // Twitter often returns spend as a string in major units. If we ever see micros, detect big numbers.
  const s = String(val).trim();
  if (/^\d+$/.test(s) && s.length > 6) {
    // assume micros
    return Math.round(Number(s) / 10000) / 100; // micros -> major (divide by 1e6), then round 2dp
  }
  const n = Number(s);
  return Math.round(n * 100) / 100;
}

/**
 * Fetch daily campaign rows for a given ISO date (YYYY-MM-DD).
 * Returns [{ date, campaignId, campaignName, impressions, clicks, conversions, spend }]
 */
async function getDailyCampaignRows(isoDate) {
  // Twitter Ads stats API (time_range per day)
  // We’ll hit the campaigns endpoint to map id -> name, then stats per-campaign (entity=CAMPAIGN).
  // 1) List campaigns (active/historical) so we can label names
  const campaignsData = await adsGet(accountPath('/campaigns'), {
    with_deleted: true,
    count: 200
  });

  const idToName = new Map();
  if (Array.isArray(campaignsData?.data)) {
    for (const c of campaignsData.data) {
      if (c?.id) idToName.set(c.id, c.name || c.id);
    }
  }

  // 2) Stats for the single day
  const start = `${isoDate} 00:00:00`;
  const end = `${isoDate} 23:59:59`;

  const stats = await adsGet(accountPath('/stats'), {
    entity: 'CAMPAIGN',
    start_time: start,
    end_time: end,
    granularity: 'DAY',
    metric_groups: 'ENGAGEMENT,BILLING,CONVERSION',
    placement: 'ALL_ON_TWITTER',
    // include_deleted helps backfills if old items were paused/deleted
    with_deleted: true,
  });

  const rows = [];
  const data = stats?.data || [];
  for (const item of data) {
    const id = item?.id;
    const metrics = item?.metrics || {};
    // Twitter returns arrays for day granularity; take first index (single day)
    const impressions = Number(metrics?.impressions?.[0] || 0);
    const clicks = Number(metrics?.clicks?.[0] || 0);
    const conversions =
      Number(metrics?.conversion_purchases?.[0] || 0) || // if configured
      Number(metrics?.follows?.[0] || 0) || 0; // fallback — replace with your desired conversion signal

    // spend can be "billed_charge_local_micro" (micros) or "billed_charge_local" (string/major units)
    let spend = 0;
    if (metrics?.billed_charge_local_micro?.[0] != null) {
      spend = parseAmount(metrics.billed_charge_local_micro[0]); // micros → major handled
    } else if (metrics?.billed_charge_local?.[0] != null) {
      spend = parseAmount(metrics.billed_charge_local[0]);
    }

    // Skip truly empty rows
    if (!impressions && !clicks && !conversions && !spend) continue;

    rows.push({
      date: isoDate,
      campaignId: id,
      campaignName: idToName.get(id) || id,
      impressions,
      clicks,
      conversions,
      spend
    });
  }

  return rows;
}

module.exports = { getDailyCampaignRows };

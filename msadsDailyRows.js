// msadsDailyRows.js
require('dotenv').config();

const { getDailyCampaignRows: getFromSrc } = require('./src/msadsReport');

/**
 * Normalised access point used by other scripts.
 * @param {string} isoDate - YYYY-MM-DD
 * @returns {Promise<Array<{ name:string, campaignName:string, campaignId?:string, clicks:number, impressions:number, conversions:number, spend:number, average_cpc?:number, all_cost_per_conversion?:number, campaign_status?:string, date:string }>>}
 */
async function getDailyCampaignRows(isoDate) {
  if (!isoDate || !/^\d{4}-\d{2}-\d{2}$/.test(isoDate)) {
    throw new Error(`getDailyCampaignRows: expected YYYY-MM-DD, got "${isoDate}"`);
  }
  const rows = await getFromSrc(isoDate);
  return (rows || []).map(r => ({
    // keep both name keys for compatibility with older code
    name: r.name ?? r.campaignName ?? '',
    campaignName: r.campaignName ?? r.name ?? '',
    campaignId: r.campaignId,
    clicks: Number(r.clicks || 0),
    impressions: Number(r.impressions || 0),
    conversions: Number(r.conversions || 0),
    spend: Number(r.spend || 0),
    average_cpc: r.average_cpc != null ? Number(r.average_cpc) : undefined,
    all_cost_per_conversion: r.all_cost_per_conversion != null ? Number(r.all_cost_per_conversion) : undefined,
    campaign_status: r.campaign_status,
    date: isoDate,
  }));
}

module.exports = { getDailyCampaignRows };

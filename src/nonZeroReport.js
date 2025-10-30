// src/nonZeroReport.js — simple “spend > 0” filter wrapper
'use strict';

const { getCampaignSummaryForDate } = require('./msadsReport');

/**
 * Returns only rows with spend > 0 for the given ISO date.
 * Row shape is whatever msadsReport returns (we keep it unchanged).
 */
async function getNonZeroSummaryForDate(isoDate) {
  const rows = await getCampaignSummaryForDate(isoDate);
  return (rows || []).filter(r => Number(r?.spend) > 0);
}

// For compatibility with earlier calls that used this name:
async function getDailyCampaignRows(isoDate) {
  return getNonZeroSummaryForDate(isoDate);
}

module.exports = { getNonZeroSummaryForDate, getDailyCampaignRows, getCampaignSummaryForDate };

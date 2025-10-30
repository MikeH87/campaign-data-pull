// src/twitterAdsReport.js
// Pull daily campaign-level metrics from Twitter (X) Ads API.
// Returns rows shaped like msadsReport.getDailyCampaignRows so the rest of the pipeline can reuse it.
//
// ENV required:
//   TWITTER_BEARER_TOKEN=...        (OAuth2 Bearer, Ads API enabled on the app)
//   TWITTER_ACCOUNT_ID=...          (numeric account id, starts with "gq..." in API docs; here just the id string)
// Optional:
//   TWITTER_API_BASE=https://ads-api.twitter.com/13
//   MSADS_DEBUG=1 to log

const axios = require('axios');
const { DateTime } = require('luxon');

const API_BASE = process.env.TWITTER_API_BASE || 'https://ads-api.twitter.com/13';
const ACCOUNT_ID = process.env.TWITTER_ACCOUNT_ID;
const BEARER = process.env.TWITTER_BEARER_TOKEN;

function dbg(...args) {
  if (process.env.MSADS_DEBUG) console.log('[TWADS]', ...args);
}

function authHeaders() {
  if (!BEARER) throw new Error('Missing TWITTER_BEARER_TOKEN');
  return { Authorization: `Bearer ${BEARER}` };
}

/**
 * Shape output exactly like msadsReport.getDailyCampaignRows:
 * [
 *  {
 *    date: 'YYYY-MM-DD',
 *    campaignId: 'string',
 *    campaignName: 'string',
 *    campaign_status: 'Active'|'Paused'|... (best-effort),
 *    impressions: number,
 *    clicks: number,
 *    conversions: number,
 *    spend: number,            // in MAJOR units (GBP etc)
 *    average_cpc: number|null,
 *    all_cost_per_conversion: number|null
 *  }
 * ]
 */
async function getDailyCampaignRows(dateYMD) {
  if (!ACCOUNT_ID) throw new Error('Missing TWITTER_ACCOUNT_ID');

  // Twitter Ads time window requires full iso; we’ll compute the London-local day window
  const start = DateTime.fromISO(dateYMD, { zone: 'Europe/London' }).startOf('day');
  const end   = start.endOf('day');

  // Twitter metrics: we’ll request campaign entity metrics for 1 day
  // Docs (v13 at time of writing): /stats/jobs/accounts/:account_id — but for simplicity and speed we use synchronous stats:
  // GET /stats/accounts/:account_id
  //   ?entity=CAMPAIGN
  //   &entity_ids=all (we’ll discover via campaigns list)
  //   &start_time=&end_time=
  //   &granularity=DAY
  //   &placement=ALL_ON_TWITTER
  //   &metric_groups=ENGAGEMENT,BILLING,WEB_CONVERSION
  //
  // We fetch campaigns first so we can also map status + name.

  // 1) List campaigns (paged)
  const campaigns = await listAllCampaigns();

  if (campaigns.length === 0) return [];

  const entityIds = campaigns.map(c => c.id).join(',');
  const params = {
    entity: 'CAMPAIGN',
    entity_ids: entityIds,
    start_time: start.toUTC().toISO(), // ISO 8601 UTC
    end_time: end.toUTC().toISO(),
    granularity: 'DAY',
    placement: 'ALL_ON_TWITTER',
    metric_groups: 'ENGAGEMENT,BILLING,WEB_CONVERSION' // clicks, impressions, billed_charge_local_micro etc.
  };

  dbg('Stats params', { start: params.start_time, end: params.end_time });

  const statRes = await axios.get(
    `${API_BASE}/stats/accounts/${encodeURIComponent(ACCOUNT_ID)}`,
    { headers: authHeaders(), params, validateStatus: () => true }
  );

  if (statRes.status !== 200) {
    throw new Error(`Twitter stats failed (${statRes.status}) ${JSON.stringify(statRes.data)}`);
  }

  // Response has data for each entity id. We’ll reduce it to target date’s bucket.
  // Spend comes as billed_charge_local_micro in micro (1e-6) of local currency; convert to MAJOR.
  const rows = [];

  for (const ent of statRes.data.data || []) {
    const id = ent.id;
    const camp = campaigns.find(c => c.id === id);
    const name = camp?.name || id;

    // metrics are arrays aligned to time series. We expect exactly one bucket for the day.
    const getFirst = (path) => {
      const arr = path?.[0];
      // The API can return strings; coerce to number safely
      if (arr == null) return 0;
      const n = Number(arr);
      return Number.isFinite(n) ? n : 0;
    };

    const impressions = getFirst(ent.metrics?.impressions);
    const clicks      = getFirst(ent.metrics?.clicks);
    const convs       = getFirst(ent.metrics?.promoted_conversions || ent.metrics?.website_conversions);
    const micro       = getFirst(ent.metrics?.billed_charge_local_micro);
    const spend       = Math.round((micro / 1_000_000) * 100) / 100; // to major, 2dp

    const avgCpc = clicks > 0 ? Math.round((spend / clicks) * 100) / 100 : null;
    const cpl    = convs > 0 ? Math.round((spend / convs) * 100) / 100 : null;

    rows.push({
      date: dateYMD,
      campaignId: id,
      campaignName: name,
      campaign_status: mapStatus(camp?.entity_status),
      impressions,
      clicks,
      conversions: convs,
      spend,
      average_cpc: avgCpc,
      all_cost_per_conversion: cpl
    });
  }

  dbg('TW rows', { isoDate: dateYMD, count: rows.length });
  return rows;
}

function mapStatus(s) {
  // Twitter entity_status: ACTIVE, PAUSED, DELETED, DRAFT
  switch (String(s || '').toUpperCase()) {
    case 'ACTIVE': return 'Active';
    case 'PAUSED': return 'Paused';
    case 'DELETED': return 'Deleted';
    case 'DRAFT': return 'Draft';
    default: return 'Unknown';
  }
}

async function listAllCampaigns() {
  const headers = authHeaders();
  const out = [];
  let cursor = undefined;

  while (true) {
    const params = {
      with_deleted: false,
      count: 200,
    };
    if (cursor) params.cursor = cursor;

    const res = await axios.get(
      `${API_BASE}/accounts/${encodeURIComponent(ACCOUNT_ID)}/campaigns`,
      { headers, params, validateStatus: () => true }
    );
    if (res.status !== 200) {
      throw new Error(`Twitter campaigns failed (${res.status}) ${JSON.stringify(res.data)}`);
    }
    const data = res.data?.data || [];
    for (const c of data) out.push({ id: c.id, name: c.name, entity_status: c.entity_status });

    const next = res.data?.next_cursor;
    if (!next) break;
    cursor = next;
  }
  dbg('campaigns', out.length);
  return out;
}

module.exports = {
  getDailyCampaignRows, // for parity with msadsReport
};

'use strict';
require('dotenv').config();
const axios = require('axios');

const {
  MSADS_CLIENT_ID,
  MSADS_CLIENT_SECRET,
  MSADS_REFRESH_TOKEN,
  MSADS_DEVELOPER_TOKEN,
  MSADS_ACCOUNT_ID,
  MSADS_CUSTOMER_ID,
  MSADS_PUBLIC_CLIENT,
  MSADS_REPORT_TIMEOUT_MS,
  MSADS_REPORT_POLL_MS,
  MSADS_REPORT_RETRY_ATTEMPTS,
  MSADS_DEBUG,
} = process.env;

// ---------- Sanity checks ----------
function assertEnv() {
  const missing = [];
  if (!MSADS_CLIENT_ID)      missing.push('MSADS_CLIENT_ID');
  if (!MSADS_REFRESH_TOKEN)  missing.push('MSADS_REFRESH_TOKEN');
  if (!MSADS_DEVELOPER_TOKEN)missing.push('MSADS_DEVELOPER_TOKEN');
  if (!MSADS_ACCOUNT_ID)     missing.push('MSADS_ACCOUNT_ID');
  if (!MSADS_CUSTOMER_ID)    missing.push('MSADS_CUSTOMER_ID');
  if (missing.length) throw new Error(`Missing Microsoft Ads env vars: ${missing.join(', ')}`);
}
assertEnv();

const TOTAL_TIMEOUT_MS = Number(MSADS_REPORT_TIMEOUT_MS ?? 12 * 60 * 1000);
const POLL_INTERVAL_MS = Number(MSADS_REPORT_POLL_MS ?? 5000);
const RETRY_ATTEMPTS   = Number(MSADS_REPORT_RETRY_ATTEMPTS ?? 2);
const DEBUG = String(MSADS_DEBUG || '').toLowerCase() === '1';

const SUBMIT_URL = 'https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Submit';
const POLL_URL   = 'https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Poll';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const dbg = (...a) => { if (DEBUG) console.log('[MSADS]', ...a); };
const num = (v) => {
  if (v === null || v === undefined || v === '') return 0;
  const n = Number(String(v).replace(/,/g, ''));
  return Number.isFinite(n) ? n : 0;
};

// ---------- CSV parsing ----------
function pickDelimiter(l) {
  const counts = { ',': (l.match(/,/g)||[]).length, ';': (l.match(/;/g)||[]).length, '\t': (l.match(/\t/g)||[]).length };
  return Object.entries(counts).sort((a,b)=>b[1]-a[1])[0][0] || ',';
}

function splitQuoted(line, delim) {
  const arr = [];
  let cur = '';
  let q = false;
  for (let i=0;i<line.length;i++) {
    const ch = line[i];
    if (ch === '"') {
      if (q && line[i+1] === '"') { cur += '"'; i++; }
      else q = !q;
    } else if (ch === delim && !q) {
      arr.push(cur); cur = '';
    } else {
      cur += ch;
    }
  }
  arr.push(cur);
  return arr.map(s => s.trim());
}

function normaliseHeaderToken(t) {
  return t.toLowerCase().replace(/[^a-z0-9]/g,'');
}

function findHeaderIndex(lines) {
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i].trim();
    if (!raw) continue;
    if (/^report name:/i.test(raw)) continue;
    if (/^report generated at/i.test(raw)) continue;
    if (/^account:/i.test(raw)) continue;

    const delim = pickDelimiter(raw);
    const cols = splitQuoted(raw, delim).map(normaliseHeaderToken);
    if (cols.includes('timeperiod') && cols.includes('campaignname') && cols.includes('clicks')) {
      return { index: i, delim, colsRaw: splitQuoted(raw, delim) };
    }
  }
  return null;
}

function parseDailyCsv(isoDate, csv) {
  if (!csv) return [];
  if (csv.charCodeAt(0) === 0xFEFF) csv = csv.slice(1);
  const lines = String(csv).split(/\r?\n/);
  if (lines.length < 2) return [];

  const headerInfo = findHeaderIndex(lines);
  if (!headerInfo) {
    // Some “success/no data” responses can be very short – treat as no rows
    if (lines.length <= 5) return [];
    throw new Error(`CSV missing expected header row.`);
  }

  const { index: headerLineIdx, delim } = headerInfo;
  const headerRaw = lines[headerLineIdx];
  const headerCells = splitQuoted(headerRaw, delim);
  const headerNorm  = headerCells.map(normaliseHeaderToken);
  const colIdx = Object.fromEntries(headerNorm.map((h, i) => [h, i]));

  const need = {
    timeperiod: ['timeperiod'],
    campaignid: ['campaignid'],
    campaignname: ['campaignname'],
    campaignstatus: ['campaignstatus'],
    impressions: ['impressions'],
    clicks: ['clicks'],
    averagecpc: ['averagecpc','avgcpc'],
    spend: ['spend','cost'],
    conversions: ['conversions','allconversions'],
    allcostperconversion: ['allcostperconversion','costperconversion'],
  };

  const col = {};
  for (const [k, variants] of Object.entries(need)) {
    const i = variants.findIndex(v => v in colIdx);
    col[k] = i >= 0 ? colIdx[variants[i]] : -1;
  }

  const rows = [];
  for (let i = headerLineIdx + 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line || /^total:/i.test(line)) break;
    const cells = splitQuoted(line, delim);
    if (cells.length < headerCells.length) continue;

    const row = {
      date: isoDate,
      campaignId: cells[col.campaignid] || '',
      campaignName: cells[col.campaignname] || '',
      campaign_status: cells[col.campaignstatus] || '',
      impressions: num(cells[col.impressions]),
      clicks: num(cells[col.clicks]),
      conversions: num(cells[col.conversions]),
      spend: num(cells[col.spend]),
      average_cpc: num(cells[col.averagecpc]),
      all_cost_per_conversion: num(cells[col.allcostperconversion]),
    };
    if (row.campaignId || row.campaignName) rows.push(row);
  }

  dbg('Rows parsed', { isoDate, count: rows.length });
  return rows;
}

// ---------- Auth ----------
async function getAccessToken() {
  const tokenUrl = 'https://login.microsoftonline.com/common/oauth2/v2.0/token';
  const params = new URLSearchParams({
    client_id: MSADS_CLIENT_ID,
    grant_type: 'refresh_token',
    refresh_token: MSADS_REFRESH_TOKEN,
    scope: 'https://ads.microsoft.com/msads.manage offline_access',
  });
  const isPublic = String(MSADS_PUBLIC_CLIENT || '').toLowerCase() === 'true';
  if (!isPublic && MSADS_CLIENT_SECRET) params.set('client_secret', MSADS_CLIENT_SECRET);

  const res = await axios.post(tokenUrl, params.toString(), {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    timeout: 30000,
  });
  if (res.status !== 200 || !res.data?.access_token) {
    throw new Error(`Failed to get access token: ${res.status} ${res.statusText} ${JSON.stringify(res.data)}`);
  }
  return res.data.access_token;
}

function authHeaders(accessToken){
  return {
    Authorization: `Bearer ${accessToken}`,
    DeveloperToken: MSADS_DEVELOPER_TOKEN,
    CustomerId: String(MSADS_CUSTOMER_ID),
    CustomerAccountId: String(MSADS_ACCOUNT_ID),
    'Content-Type': 'application/json',
    Accept: 'application/json',
  };
}

// ---------- Build report payload (NO ReportTimeZone) ----------
function buildSubmitBody(dateYMD) {
  const [Y,M,D] = [Number(dateYMD.slice(0,4)), Number(dateYMD.slice(5,7)), Number(dateYMD.slice(8,10))];
  return {
    ReportRequest: {
      Type: 'CampaignPerformanceReportRequest',
      Format: 'Csv',
      ReportName: `CampaignPerf ${dateYMD}`,
      ReturnOnlyCompleteData: false,
      Aggregation: 'Daily',
      Scope: { AccountIds: [ String(MSADS_ACCOUNT_ID) ] },
      Time: {
        CustomDateRangeStart: { Day: D, Month: M, Year: Y },
        CustomDateRangeEnd:   { Day: D, Month: M, Year: Y }
        // Intentionally NOT including ReportTimeZone
      },
      Columns: [
        'TimePeriod','AccountId','AccountName','CampaignId','CampaignName',
        'CampaignStatus','Impressions','Clicks','AverageCpc','Spend',
        'Conversions','AllCostPerConversion'
      ],
    }
  };
}

// ---------- Submit with soft-skip on 2010 ----------
async function submitReport(accessToken, dateYMD) {
  dbg('Submit', { dateYMD });

  const attemptSubmit = async () => {
    const body = buildSubmitBody(dateYMD);
    const res = await axios.post(SUBMIT_URL, body, {
      headers: authHeaders(accessToken),
      timeout: 30000,
      validateStatus: s => s < 500 || s === 429,
    });
    if (res.status === 200 && res.data?.ReportRequestId) return res.data.ReportRequestId;
    const raw = JSON.stringify(res.data);
    throw new Error(`Submit report failed: ${res.status} ${res.statusText} ${raw}`);
  };

  try {
    return await attemptSubmit();
  } catch (e) {
    const msg = String(e.message || '');
    // If Bing says the end date is invalid (2010), treat this day as "no data" and SKIP.
    if (msg.includes('InvalidCustomDateRangeEnd') || msg.includes('"Code":2010')) {
      dbg('Soft-skip day due to 2010 (invalid end date).');
      return null; // signal "no report for this day"
    }
    // Otherwise retry once (network hiccup etc.)
    dbg('Attempt 1 failed:', e.message);
    await sleep(4000);
    try {
      return await attemptSubmit();
    } catch (e2) {
      const msg2 = String(e2.message || '');
      if (msg2.includes('InvalidCustomDateRangeEnd') || msg2.includes('"Code":2010')) {
        dbg('Soft-skip day on retry (2010).');
        return null;
      }
      throw e2;
    }
  }
}

// ---------- Poll + download ----------
async function pollForUrl(accessToken, requestId, startedAt){
  if (!requestId) return null; // soft-skip
  dbg('Poll start', { requestId });
  for (;;) {
    const res = await axios.post(POLL_URL, { ReportRequestId: requestId }, {
      headers: authHeaders(accessToken),
      timeout: 30000,
    });
    const status = res.data?.ReportRequestStatus?.Status;
    const url    = res.data?.ReportRequestStatus?.ReportDownloadUrl;
    dbg('Poll status', { status, hasUrl: !!url });

    if (res.status === 200 && status === 'Success') {
      // Some early days return Success with no URL => treat as "no rows"
      return url || null;
    }
    if (res.status === 200 && (status === 'Error' || status === 'Failed')) {
      // Treat as no rows rather than bombing the batch
      return null;
    }

    if (Date.now() - startedAt > TOTAL_TIMEOUT_MS) {
      // If it drags on, also treat as no rows to keep batch moving
      return null;
    }
    await sleep(POLL_INTERVAL_MS);
  }
}

async function downloadCsv(url, accessToken) {
  if (!url) return ''; // no data
  dbg('Download try #1 (anon)');
  let res = await axios.get(url, { responseType: 'arraybuffer', timeout: 60000 });
  if (res.status === 403 || res.status === 401) {
    dbg(`Download #1 rejected (${res.status}). Retrying with Bearer…`);
    res = await axios.get(url, {
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: 'arraybuffer', timeout: 60000,
    });
  }

  if (res.status !== 200) {
    let tail = '';
    try { tail = Buffer.from(res.data || '').toString('utf8').slice(0, 320); } catch {}
    // No data: return empty rather than throwing
    return '';
  }

  const buf = Buffer.from(res.data);
  if (buf.slice(0, 2).toString('hex') === '504b') {
    const AdmZip = require('adm-zip');
    const zip = new AdmZip(buf);
    const entry = zip.getEntries().find(e => /\.csv$/i.test(e.entryName));
    return entry ? entry.getData().toString('utf8') : '';
  }
  if (buf[0] === 0x1f && buf[1] === 0x8b) {
    const zlib = require('zlib');
    return zlib.gunzipSync(buf).toString('utf8');
  }
  return buf.toString('utf8');
}

// ---------- Public API ----------
async function getCampaignSummaryForDate(isoDate) {
  const token = await getAccessToken();
  const reqId = await submitReport(token, isoDate);
  if (!reqId) return []; // soft-skip day

  const url   = await pollForUrl(token, reqId, Date.now());
  if (!url)   return []; // treat as no rows

  const csv   = await downloadCsv(url, token);
  if (!csv)   return []; // treat as no rows

  const rows  = parseDailyCsv(isoDate, csv);
  return rows;
}

async function getDailyCampaignRows(isoDate) {
  return getCampaignSummaryForDate(isoDate);
}

function isoInLondon(daysOffset = 0) {
  const tz = 'Europe/London';
  const now = new Date();
  const shifted = new Date(Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate() + daysOffset
  ));
  const fmt = new Intl.DateTimeFormat('en-GB', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' });
  const [{ value: dd }, , { value: mm }, , { value: yyyy }] = fmt.formatToParts(shifted);
  return `${yyyy}-${mm}-${dd}`;
}

async function getYesterdayCampaignSummary() {
  return getCampaignSummaryForDate(isoInLondon(-1));
}

module.exports = {
  getDailyCampaignRows,
  getCampaignSummaryForDate,
  getYesterdayCampaignSummary,
};

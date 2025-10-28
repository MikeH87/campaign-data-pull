// src/msadsReport.js
require("dotenv").config();
const axios = require("axios");
const unzipper = require("unzipper");
const { parse } = require("csv-parse");
const { getMsAdsAccessToken } = require("./msadsAuth");
const { getYesterdayLondonYMD } = require("./timezone");

const {
  MSADS_DEVELOPER_TOKEN,
  MSADS_CUSTOMER_ID,
  MSADS_ACCOUNT_ID
} = process.env;

function requireEnv(name, val) {
  if (!val) throw new Error(`Missing required env var: ${name}`);
}
requireEnv("MSADS_DEVELOPER_TOKEN", MSADS_DEVELOPER_TOKEN);
requireEnv("MSADS_CUSTOMER_ID", MSADS_CUSTOMER_ID);
requireEnv("MSADS_ACCOUNT_ID", MSADS_ACCOUNT_ID);

// Endpoints
const SUBMIT_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Submit";
const POLL_URL   = "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Poll";

// ---------- Build/submit/poll ----------
function buildReportBodyForDate(ymd) {
  const [Y, M, D] = ymd.split("-").map(Number);
  return {
    ReportRequest: {
      Type: "CampaignPerformanceReportRequest",
      ReportName: `CampaignPerf ${ymd}`,
      Format: "Csv",
      FormatVersion: "2.0",
      ReturnOnlyCompleteData: false,
      Aggregation: "Daily",
      Columns: [
        "TimePeriod",
        "AccountId",
        "AccountName",
        "CampaignId",
        "CampaignName",
        "CampaignStatus",
        "Impressions",
        "Clicks",
        "AverageCpc",
        "Spend",
        "Conversions",
        "AllCostPerConversion"
      ],
      Scope: { AccountIds: [ Number(MSADS_ACCOUNT_ID) ] },
      Time: {
        CustomDateRangeStart: { Year: Y, Month: M, Day: D },
        CustomDateRangeEnd:   { Year: Y, Month: M, Day: D },
        ReportTimeZone: "GreenwichMeanTimeDublinEdinburghLisbonLondon"
      }
    }
  };
}

async function submitCampaignReportForDate(ymd) {
  const accessToken = await getMsAdsAccessToken();
  const body = buildReportBodyForDate(ymd);
  const headers = {
    Authorization: `Bearer ${accessToken}`,
    DeveloperToken: MSADS_DEVELOPER_TOKEN,
    CustomerId: MSADS_CUSTOMER_ID,
    CustomerAccountId: MSADS_ACCOUNT_ID,
    "Content-Type": "application/json"
  };
  const resp = await axios.post(SUBMIT_URL, body, { headers, timeout: 30000, maxRedirects: 5 });
  const reportRequestId = resp.data?.ReportRequestId;
  if (!reportRequestId) throw new Error(`No ReportRequestId in Submit response: ${JSON.stringify(resp.data)}`);
  return reportRequestId;
}

async function pollUntilReady(reportRequestId, { maxAttempts = 20, delayMs = 3000 } = {}) {
  const accessToken = await getMsAdsAccessToken();
  const headers = {
    Authorization: `Bearer ${accessToken}`,
    DeveloperToken: MSADS_DEVELOPER_TOKEN,
    CustomerId: MSADS_CUSTOMER_ID,
    CustomerAccountId: MSADS_ACCOUNT_ID,
    "Content-Type": "application/json"
  };
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const resp = await axios.post(POLL_URL, { ReportRequestId: reportRequestId }, { headers, timeout: 30000 });
    const status = resp.data?.ReportRequestStatus?.Status;
    const url = resp.data?.ReportRequestStatus?.ReportDownloadUrl;
    if (status === "Success" && url) return String(url).replace(/&amp;/g, "&");
    if (status === "Error" || status === "Failed") throw new Error(`Report generation failed. Status=${status}`);
    await new Promise(r => setTimeout(r, delayMs));
  }
  throw new Error("Report polling timed out");
}

// ---------- Download & parse (memory), skip preamble ----------
async function downloadCsvText(reportUrl) {
  const resp = await axios.get(reportUrl, { responseType: "stream", timeout: 60000 });
  const entryStream = resp.data.pipe(unzipper.ParseOne());
  const chunks = [];
  await new Promise((resolve, reject) => {
    entryStream.on("data", (c) => chunks.push(c));
    entryStream.on("end", resolve);
    entryStream.on("error", reject);
  });
  // Handle BOM if present
  let text = Buffer.concat(chunks).toString("utf8");
  if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
  return text;
}

function detectHeaderAndDelimiter(csvText) {
  const lines = csvText.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");
  let idx = lines.findIndex(l => l.trim().startsWith("TimePeriod"));
  // Some exports might quote the header: "TimePeriod,AccountId,..."
  if (idx === -1) idx = lines.findIndex(l => l.trim().startsWith('"TimePeriod'));
  const headerLine = idx >= 0 ? lines[idx] : "";
  const delimiter = headerLine.includes("\t") ? "\t" : ",";
  return { lines, headerIndex: idx, headerLine, delimiter };
}

async function parseCsvWithPreamble(csvText) {
  const { lines, headerIndex, headerLine, delimiter } = detectHeaderAndDelimiter(csvText);
  if (headerIndex === -1) return { rows: [], debug: { headerIndex, headerLine, delimiter, firstLines: lines.slice(0, 30) } };

  const tailLines = [];
  for (let i = headerIndex; i < lines.length; i++) {
    const line = lines[i];
    if (/©\d{4}\s+Microsoft Corporation/i.test(line)) break;
    if (line.trim() === "") continue;
    tailLines.push(line);
  }
  const dataCsv = tailLines.join("\n");

  const rows = [];
  await new Promise((resolve, reject) => {
    parse(dataCsv, {
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      trim: true,
      delimiter
    })
      .on("data", row => rows.push(row))
      .on("end", resolve)
      .on("error", reject);
  });

  return { rows, debug: { headerIndex, headerLine, delimiter, firstLines: lines.slice(0, 15), sampleRow: rows[0] } };
}

// ---------- Summarise ----------
function summarisePerCampaign(rows) {
  const byName = new Map();
  const toNumber = (v) => {
    if (v == null) return 0;
    const s = String(v).replace(/,/g, "");
    const n = Number(s);
    return Number.isFinite(n) ? n : 0;
  };

  for (const r of rows) {
    const name = r["CampaignName"] || r["Campaign Name"] || r["Campaign"] || r["Campaign name"];
    if (!name) continue;

    const clicks = toNumber(r["Clicks"]);
    const spend = toNumber(r["Spend"]);
    const impressions = toNumber(r["Impressions"]);
    const avgCpc = toNumber(r["AverageCpc"] || r["Average CPC"]);
    const conversions = toNumber(r["Conversions"]);
    const cpl = toNumber(r["AllCostPerConversion"] || r["All Cost Per Conversion"]);
    const status = r["CampaignStatus"] || r["Campaign Status"] || r["Status"] || "";
    const date = r["TimePeriod"] || r["Time Period"] || r["Date"] || "";

    const existing = byName.get(name) || {
      campaignId: r["CampaignId"] || r["Campaign Id"] || "",
      clicks: 0,
      spend: 0,
      impressions: 0,
      average_cpc_sum: 0,
      average_cpc_count: 0,
      conversions: 0,
      all_cost_per_conversion_sum: 0,
      all_cost_per_conversion_count: 0,
      status,
      date
    };

    existing.clicks += clicks;
    existing.spend += spend;
    existing.impressions += impressions;

    if (avgCpc > 0) { existing.average_cpc_sum += avgCpc; existing.average_cpc_count += 1; }
    if (cpl > 0) { existing.all_cost_per_conversion_sum += cpl; existing.all_cost_per_conversion_count += 1; }
    if (!existing.status && status) existing.status = status;
    if (!existing.date && date) existing.date = date;

    byName.set(name, existing);
  }

  for (const [name, v] of byName) {
    v.average_cpc = v.average_cpc_count > 0 ? v.average_cpc_sum / v.average_cpc_count : 0;
    v.all_cost_per_conversion = v.all_cost_per_conversion_count > 0
      ? v.all_cost_per_conversion_sum / v.all_cost_per_conversion_count
      : 0;
    delete v.average_cpc_sum;
    delete v.average_cpc_count;
    delete v.all_cost_per_conversion_sum;
    delete v.all_cost_per_conversion_count;
  }

  return byName;
}

// ---------- Public API ----------
async function getCampaignSummaryForDate(ymd) {
  const reportRequestId = await submitCampaignReportForDate(ymd);
  const url = await pollUntilReady(reportRequestId);
  const csvText = await downloadCsvText(url);
  const { rows, debug } = await parseCsvWithPreamble(csvText);
  const map = summarisePerCampaign(rows);
  const items = [...map.entries()].map(([name, v]) => ({
    name,
    campaignId: String(v.campaignId || ""),
    clicks: Number(v.clicks || 0),
    spend: Number(v.spend || 0),
    impressions: Number(v.impressions || 0),
    average_cpc: Number(v.average_cpc || 0),
    conversions: Number(v.conversions || 0),
    all_cost_per_conversion: Number(v.all_cost_per_conversion || 0),
    campaign_status: String(v.status || ""),
    date: ymd
  }));
  return { date: ymd, items, rowCount: rows.length, _debug: debug };
}

async function getYesterdayCampaignSummary() {
  const ymd = getYesterdayLondonYMD();
  return getCampaignSummaryForDate(ymd);
}

// Extra: expose a debug helper that returns raw CSV for a date
async function debugGetRawCsvTextForDate(ymd) {
  const reportRequestId = await submitCampaignReportForDate(ymd);
  const url = await pollUntilReady(reportRequestId);
  return downloadCsvText(url);
}

module.exports = {
  getCampaignSummaryForDate,
  getYesterdayCampaignSummary,
  debugGetRawCsvTextForDate
};

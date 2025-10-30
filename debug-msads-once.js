// debug-msads-once.js
require("dotenv").config();
const axios = require("axios");

const {
  MSADS_CLIENT_ID,
  MSADS_CLIENT_SECRET,
  MSADS_REFRESH_TOKEN,
  MSADS_DEVELOPER_TOKEN,
  MSADS_ACCOUNT_ID,
  MSADS_CUSTOMER_ID,
  MSADS_PUBLIC_CLIENT,
} = process.env;

function ymdFrom(dateStr){
  // expects YYYY-MM-DD
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) throw new Error("date must be YYYY-MM-DD");
  return dateStr;
}

function buildSubmitBody(dateYMD){
  const [Y,M,D] = [Number(dateYMD.slice(0,4)), Number(dateYMD.slice(5,7)), Number(dateYMD.slice(8,10))];
  return {
    ReportRequest: {
      Type: "CampaignPerformanceReportRequest",
      Format: "Csv",
      ReportName: `DEBUG CampaignPerf ${dateYMD}`,
      ReturnOnlyCompleteData: false,
      Aggregation: "Daily",
      Scope: { AccountIds: [ String(MSADS_ACCOUNT_ID) ] },
      Time: {
        CustomDateRangeStart: { Day: D, Month: M, Year: Y },
        CustomDateRangeEnd:   { Day: D, Month: M, Year: Y },
      },
      Columns: [
        "TimePeriod","AccountId","AccountName","CampaignId","CampaignName",
        "CampaignStatus","Impressions","Clicks","AverageCpc","Spend",
        "Conversions","AllCostPerConversion"
      ]
    }
  };
}

async function getAccessToken(){
  const tokenUrl = "https://login.microsoftonline.com/common/oauth2/v2.0/token";
  const params = new URLSearchParams({
    client_id: MSADS_CLIENT_ID,
    grant_type: "refresh_token",
    refresh_token: MSADS_REFRESH_TOKEN,
    scope: "https://ads.microsoft.com/msads.manage offline_access",
  });
  const isPublic = String(MSADS_PUBLIC_CLIENT||"").toLowerCase()==="true";
  if (!isPublic && MSADS_CLIENT_SECRET) params.set("client_secret", MSADS_CLIENT_SECRET);

  const r = await axios.post(tokenUrl, params.toString(), {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    timeout: 25000,
    validateStatus: s => s < 500 || s === 429,
  });
  if (r.status !== 200 || !r.data?.access_token) {
    throw new Error(`TOKEN FAIL: ${r.status} ${r.statusText} ${JSON.stringify(r.data)}`);
  }
  return r.data.access_token;
}

function authHeaders(token){
  return {
    Authorization: `Bearer ${token}`,
    DeveloperToken: MSADS_DEVELOPER_TOKEN,
    CustomerId: String(MSADS_CUSTOMER_ID),
    CustomerAccountId: String(MSADS_ACCOUNT_ID),
    "Content-Type": "application/json",
    Accept: "application/json",
  };
}

async function submit(token, dateYMD){
  const SUBMIT_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Submit";
  const body = buildSubmitBody(dateYMD);
  const r = await axios.post(SUBMIT_URL, body, {
    headers: authHeaders(token),
    timeout: 25000,
    validateStatus: s => s < 500 || s === 429,
  });
  return { status: r.status, data: r.data };
}

async function pollOnce(token, requestId){
  const POLL_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Poll";
  const r = await axios.post(POLL_URL, { ReportRequestId: requestId }, {
    headers: authHeaders(token),
    timeout: 25000,
    validateStatus: s => s < 500 || s === 429,
  });
  return { status: r.status, data: r.data };
}

(async () => {
  try {
    const dateYMD = ymdFrom(process.argv[2] || "2025-06-11");

    console.log("🟦 Step A: getting access token…");
    const token = await getAccessToken();
    console.log("✅ got token (len):", token.length);

    console.log("🟦 Step B: submitting JSON report…");
    const sub = await submit(token, dateYMD);
    console.log("Submit HTTP:", sub.status, "Body keys:", Object.keys(sub.data||{}));
    console.log("Submit body:", JSON.stringify(sub.data));

    const reqId = sub.data?.ReportRequestId;
    if (!reqId) {
      console.error("❌ No ReportRequestId returned. Full body above.");
      process.exit(2);
    }
    console.log("✅ ReportRequestId:", reqId);

    console.log("🟦 Step C: single poll…");
    const p = await pollOnce(token, reqId);
    console.log("Poll HTTP:", p.status, "Body:", JSON.stringify(p.data));

    console.log("🎉 Done (one-pass debug).");
  } catch (e) {
    if (e.response) {
      console.error("❌ HTTP error:", e.response.status, e.response.statusText);
      console.error("Body:", JSON.stringify(e.response.data));
    } else {
      console.error("❌ Error:", e.message);
    }
    process.exit(1);
  }
})();

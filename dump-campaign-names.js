require("dotenv").config();
const axios = require("axios");

const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
const BU = process.env.HUBSPOT_BUSINESS_UNIT_ID; // optional

function hsHeaders() {
  const h = {
    Authorization: `Bearer ${token}`,
    Accept: "application/json",
  };
  if (BU) h["X-HubSpot-Business-Unit-Id"] = BU;
  return h;
}

async function list(after) {
  const url = "https://api.hubapi.com/marketing/v3/campaigns";
  const params = { limit: 50 };
  if (after) params.after = after;
  const resp = await axios.get(url, { headers: hsHeaders(), params, maxRedirects: 5, timeout: 30000 });
  return resp.data; // results: [{id}], paging?.next?.after
}

async function details(id) {
  const url = `https://api.hubapi.com/marketing/v3/campaigns/${encodeURIComponent(id)}`;
  const resp = await axios.get(url, { headers: hsHeaders(), maxRedirects: 5, timeout: 30000 });
  return resp.data; // { id, name?, properties? }
}

(async () => {
  if (!token) {
    console.error("HUBSPOT_PRIVATE_APP_TOKEN missing");
    process.exit(1);
  }
  let after;
  let count = 0;
  let shown = 0;

  do {
    const page = await list(after);
    const ids = (page.results || []).map(r => r.id);
    count += ids.length;
    for (const id of ids) {
      const d = await details(id);
      const hs_name = d?.properties?.hs_name;
      const prop_name = d?.properties?.name;
      const top = d?.name;
      console.log(`id=${id} hs_name=${JSON.stringify(hs_name)} prop.name=${JSON.stringify(prop_name)} top.name=${JSON.stringify(top)}`);
      if (++shown >= 30) break; // avoid too much output
    }
    if (shown >= 30) break;
    after = page?.paging?.next?.after;
  } while (after);

  console.log(`-- inspected ${shown} / listed ${count} campaigns (stopped after 30 for brevity)`);
})();

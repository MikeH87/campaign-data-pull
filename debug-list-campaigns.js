require("dotenv").config();
const axios = require("axios");

function hsHeaders(token) {
  return {
    Authorization: `Bearer ${token}`,
    Accept: "application/json",
  };
}

(async () => {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
  if (!token) {
    console.error("No HUBSPOT_PRIVATE_APP_TOKEN");
    process.exit(1);
  }
  const url = "https://api.hubapi.com/marketing/v3/campaigns";
  const params = { limit: 5 }; // just a small sample
  try {
    const resp = await axios.get(url, { headers: hsHeaders(token), params, maxRedirects: 5, timeout: 30000 });
    const results = resp.data?.results || [];
    console.log("Sample count:", results.length);
    for (const r of results) {
      console.log(JSON.stringify({
        id: r.id,
        topLevelName: r.name,                    // may be undefined
        hs_name: r.properties?.hs_name,          // likely here
        anyProps: Object.keys(r.properties || {})
      }, null, 2));
    }
  } catch (e) {
    console.error("❌ Failed:", e.response?.status, e.message);
    if (e.response?.data) console.error("Body:", JSON.stringify(e.response.data));
    process.exit(1);
  }
})();

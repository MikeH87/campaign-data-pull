const { createHubSpotClient } = require("./src/hubspotClient");

(async () => {
  try {
    const hs = createHubSpotClient();
    const resp = await hs.get("/marketing/v3/campaigns?limit=1");
    console.log("✅ HubSpot client ok. Keys:", Object.keys(resp.data));
  } catch (e) {
    if (e.response) {
      console.error("❌ HTTP", e.response.status, e.response.statusText, e.response.data);
    } else {
      console.error("❌ Error", e.message);
    }
    process.exit(1);
  }
})();

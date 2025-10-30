require("dotenv").config();
const hubspot = require("@hubspot/api-client");

const NAME = process.argv[2] || "SSAS-BAD-JAN-24";

(async () => {
  try {
    const client = new hubspot.Client({ accessToken: process.env.HUBSPOT_PRIVATE_APP_TOKEN });
    const resp = await client.crm.objects.searchApi.doSearch("campaigns", {
      filterGroups: [{ filters: [{ propertyName: "hs_name", operator: "EQ", value: NAME }] }],
      limit: 5
    });
    if (resp.results && resp.results.length) {
      console.log("✅ Found:", resp.results.map(r => ({ id: r.id, hs_name: r.properties?.hs_name })));
    } else {
      console.log("ℹ️ Not found");
    }
  } catch (e) {
    console.error("❌ Search failed:", e.message);
    if (e.response?.body) console.error("Body:", JSON.stringify(e.response.body));
    process.exit(1);
  }
})();

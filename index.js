require("dotenv").config();
const axios = require("axios");

async function main() {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
  if (!token) {
    console.error("HUBSPOT_PRIVATE_APP_TOKEN missing. Check your .env file.");
    process.exit(1);
  }

  const url = "https://api.hubapi.com/marketing/v3/campaigns?limit=1";

  try {
    const resp = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
      },
      maxRedirects: 5,
      timeout: 15000,
    });

    const { total, results } = resp.data || {};
    console.log("✅ Node request ok.");
    console.log("total:", total);
    if (Array.isArray(results) && results.length > 0) {
      console.log("first campaign id:", results[0].id || "(none)");
      console.log("first campaign name:", results[0].name || "(none)");
    } else {
      console.log("No campaigns returned (that can be normal if none exist).");
    }
  } catch (err) {
    if (err.response) {
      console.error("❌ HTTP error:", err.response.status, err.response.statusText);
      console.error("Body:", JSON.stringify(err.response.data));
    } else {
      console.error("❌ Request failed:", err.message);
    }
    process.exit(2);
  }
}

main();

require("dotenv").config();
const axios = require("axios");

async function getAccessToken() {
  const {
    MSADS_CLIENT_ID,
    MSADS_REFRESH_TOKEN
  } = process.env;

  if (!MSADS_CLIENT_ID || !MSADS_REFRESH_TOKEN) {
    console.error("Missing MSADS_CLIENT_ID or MSADS_REFRESH_TOKEN in .env");
    process.exit(1);
  }

  const tokenUrl = "https://login.microsoftonline.com/common/oauth2/v2.0/token";

  const body = new URLSearchParams({
    client_id: MSADS_CLIENT_ID,
    grant_type: "refresh_token",
    refresh_token: MSADS_REFRESH_TOKEN,
    redirect_uri: "https://login.microsoftonline.com/common/oauth2/nativeclient",
    scope: "https://ads.microsoft.com/msads.manage offline_access",
  });

  try {
    const resp = await axios.post(tokenUrl, body, {
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
    });
    console.log("✅ Refresh token exchange successful!");
    console.log("Access token (first 100 chars):", resp.data.access_token.slice(0, 100), "...");
  } catch (err) {
    if (err.response) {
      console.error("❌ HTTP error:", err.response.status, err.response.statusText);
      console.error("Body:", err.response.data);
    } else {
      console.error("❌ Request failed:", err.message);
    }
  }
}

getAccessToken();

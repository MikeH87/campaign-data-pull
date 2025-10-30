// src/msadsAuth.js
require("dotenv").config();
const axios = require("axios");

/**
 * Exchange Microsoft Ads refresh token for a new access token (public client).
 * Requires MSADS_CLIENT_ID and MSADS_REFRESH_TOKEN in environment.
 */
async function getMsAdsAccessToken() {
  const { MSADS_CLIENT_ID, MSADS_REFRESH_TOKEN } = process.env;
  if (!MSADS_CLIENT_ID || !MSADS_REFRESH_TOKEN) {
    throw new Error("Missing MSADS_CLIENT_ID or MSADS_REFRESH_TOKEN in .env");
  }

  const tokenUrl = "https://login.microsoftonline.com/common/oauth2/v2.0/token";
  const body = new URLSearchParams({
    client_id: MSADS_CLIENT_ID,
    grant_type: "refresh_token",
    refresh_token: MSADS_REFRESH_TOKEN,
    redirect_uri: "https://login.microsoftonline.com/common/oauth2/nativeclient",
    scope: "https://ads.microsoft.com/msads.manage offline_access",
  });

  const resp = await axios.post(tokenUrl, body, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    timeout: 20000,
    maxRedirects: 5,
  });

  if (!resp.data || !resp.data.access_token) {
    throw new Error("No access_token in Microsoft token response");
  }
  return resp.data.access_token;
}

module.exports = { getMsAdsAccessToken };

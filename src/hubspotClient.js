// src/hubspotClient.js
require("dotenv").config();
const axios = require("axios");

const HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!HUBSPOT_TOKEN) throw new Error("HUBSPOT_PRIVATE_APP_TOKEN missing in env");

function createHubSpotClient() {
  const instance = axios.create({
    baseURL: "https://api.hubapi.com",
    timeout: 20000,
    headers: {
      Authorization: `Bearer ${HUBSPOT_TOKEN}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    maxRedirects: 5,
    validateStatus: (s) => s < 500 || s === 429,
  });

  // Simple retry on 429/5xx with backoff
  instance.interceptors.response.use(async (res) => {
    if (res.status === 429 || res.status >= 500) {
      const retryAfter = Number(res.headers["retry-after"] || 1);
      await new Promise((r) => setTimeout(r, (retryAfter || 1) * 1000));
      return instance.request(res.config);
    }
    return res;
  });

  return instance;
}

/**
 * Robust exact-name finder:
 * 1) Try server-side filter (?name=...) and match hs_name exactly.
 * 2) If not found, paginate through all campaigns requesting hs_name property explicitly.
 */
async function findCampaignByName(hs, exactName, pageLimit = 5000) {
  // Attempt 1: server-side filter by name
  try {
    const res = await hs.get("/marketing/v3/campaigns", {
      params: {
        name: exactName,
        limit: 100,
        properties: "hs_name",
      },
    });
    if (res.status === 200 && Array.isArray(res.data?.results)) {
      const found = res.data.results.find(
        (c) => c?.properties?.hs_name === exactName
      );
      if (found) return { id: found.id, ...found.properties };
    }
  } catch (_) {
    // ignore and fall back to pagination
  }

  // Attempt 2: paginate everything (request hs_name explicitly)
  const limit = 100;
  let after = undefined;
  let checked = 0;

  while (true) {
    const params = { limit, properties: "hs_name" };
    if (after) params.after = after;

    const res = await hs.get("/marketing/v3/campaigns", { params });
    if (res.status !== 200) {
      throw new Error(
        `Campaign list failed: ${res.status} ${res.statusText} ${JSON.stringify(res.data)}`
      );
    }

    const list = Array.isArray(res.data?.results) ? res.data.results : [];
    const found = list.find((c) => c?.properties?.hs_name === exactName);
    if (found) return { id: found.id, ...found.properties };

    checked += list.length;
    if (checked >= pageLimit) return null; // safety cap

    const next = res.data?.paging?.next?.after;
    if (!next) return null;
    after = next;
  }
}

/** Create campaign with hs_name */
async function createCampaign(hs, hsName, businessUnitId = undefined) {
  const body = { properties: { hs_name: hsName } };
  if (businessUnitId) body.businessUnits = [{ id: Number(businessUnitId) }];
  const res = await hs.post("/marketing/v3/campaigns", body);
  if (res.status !== 201) {
    throw new Error(
      `Create campaign failed: ${res.status} ${res.statusText} ${JSON.stringify(res.data)}`
    );
  }
  return { id: res.data.id, ...res.data.properties };
}

/** Read a campaign to get its current properties */
async function getCampaign(hs, campaignId) {
  const res = await hs.get(`/marketing/v3/campaigns/${encodeURIComponent(campaignId)}`);
  if (res.status !== 200) {
    throw new Error(
      `Get campaign failed: ${res.status} ${res.statusText} ${JSON.stringify(res.data)}`
    );
  }
  return { id: res.data.id, ...res.data.properties };
}

/** PATCH properties */
async function updateCampaign(hs, campaignId, properties) {
  const res = await hs.patch(`/marketing/v3/campaigns/${encodeURIComponent(campaignId)}`, { properties });
  if (res.status !== 200) {
    throw new Error(
      `Update campaign failed: ${res.status} ${res.statusText} ${JSON.stringify(res.data)}`
    );
  }
  return { id: res.data.id, ...res.data.properties };
}

/** Budget/spend */
async function getCampaignBudget(hs, campaignId) {
  const res = await hs.get(`/marketing/v3/campaigns/${encodeURIComponent(campaignId)}/budget/totals`);
  if (res.status !== 200) {
    throw new Error(
      `Get budget failed: ${res.status} ${res.statusText} ${JSON.stringify(res.data)}`
    );
  }
  return res.data;
}

async function createSpendItem(hs, campaignId, { name, amount, description = "", order = 0 }) {
  const res = await hs.post(`/marketing/v3/campaigns/${encodeURIComponent(campaignId)}/spend`, {
    amount,
    name,
    description,
    order,
  });
  if (res.status !== 201) {
    throw new Error(
      `Create spend item failed: ${res.status} ${res.statusText} ${JSON.stringify(res.data)}`
    );
  }
  return res.data;
}

async function updateSpendItem(hs, campaignId, spendId, { name, amount, description = "", order }) {
  const res = await hs.put(
    `/marketing/v3/campaigns/${encodeURIComponent(campaignId)}/spend/${encodeURIComponent(spendId)}`,
    { amount, name, description, ...(order != null ? { order } : {}) }
  );
  if (res.status !== 200) {
    throw new Error(
      `Update spend item failed: ${res.status} ${res.statusText} ${JSON.stringify(res.data)}`
    );
  }
  return res.data;
}

/** Idempotent: one spend item per day by name */
async function ensureDailySpendItem(hs, campaignId, { name, amount, description = "" }) {
  const budget = await getCampaignBudget(hs, campaignId);
  const existing = Array.isArray(budget.spendItems)
    ? budget.spendItems.find((s) => s?.name === name)
    : null;

  if (!existing) {
    await createSpendItem(hs, campaignId, { name, amount, description, order: 0 });
    return { action: "created" };
  }

  const currentAmount = Number(existing.amount || 0);
  if (Number.isFinite(currentAmount) && Math.abs(currentAmount - amount) < 0.0001) {
    return { action: "unchanged" };
  }

  await updateSpendItem(hs, campaignId, existing.id, {
    name,
    amount,
    description,
    order: existing.order ?? 0,
  });
  return { action: "updated" };
}

module.exports = {
  createHubSpotClient,
  findCampaignByName,
  createCampaign,
  getCampaign,
  updateCampaign,
  ensureDailySpendItem,
};

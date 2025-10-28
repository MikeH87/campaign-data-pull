// src/timezone.js
// Utilities to get yesterday's date in Europe/London as YYYY-MM-DD (DST-safe, no external libs)

/**
 * Get today's date string in Europe/London as YYYY-MM-DD using Intl.
 * We use 'en-CA' because it formats dates as ISO-like yyyy-mm-dd.
 */
function getTodayLondonYMD() {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/London",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  // Example: "2025-10-28"
  return fmt.format(new Date());
}

/**
 * Return yesterday's date in Europe/London as YYYY-MM-DD.
 * We compute the calendar day, not "now minus 24h", so DST transitions are handled correctly.
 */
function getYesterdayLondonYMD() {
  const today = getTodayLondonYMD(); // e.g., "2025-10-28"
  const [Y, M, D] = today.split("-").map(Number);
  // Create a UTC midnight Date for "today" and subtract 1 day
  const utcMidnight = Date.UTC(Y, M - 1, D); // 00:00:00 UTC of today's London date
  const yesterdayUtc = new Date(utcMidnight - 24 * 60 * 60 * 1000);
  const yY = yesterdayUtc.getUTCFullYear();
  const yM = String(yesterdayUtc.getUTCMonth() + 1).padStart(2, "0");
  const yD = String(yesterdayUtc.getUTCDate()).padStart(2, "0");
  return `${yY}-${yM}-${yD}`;
}

module.exports = {
  getYesterdayLondonYMD,
};

// GitHub Pages uses the Zeabur API. When the site is served by Zeabur, use the
// same origin so the API service can host both frontend and backend.
window.WEI_API_BASE = location.hostname.endsWith("github.io")
  ? "https://wei-strategy-api.zeabur.app/api/v1"
  : "/api/v1";

// GitHub Pages is deployed as a self-contained static site. Leave the API base
// empty there so the built-in password gate and repository data are used.
window.WEI_API_BASE = location.hostname.endsWith("github.io")
  ? ""
  : "/api/v1";

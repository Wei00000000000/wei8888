const OWNER = "Wei00000000000";
const REPOSITORY = "wei8888";
const WORKFLOW = "update-signals.yml";
const VERSION = "2026-07-02-backup-scan-dispatch-v2";
const BINANCE_HOSTS = {
  fapi: "https://fapi.binance.com",
  fdata: "https://fapi.binance.com/futures/data",
};
const ALLOWED_PATHS = new Set([
  "/fapi/v1/exchangeInfo",
  "/fapi/v1/ticker/24hr",
  "/fapi/v1/ticker/price",
  "/fapi/v1/klines",
  "/openInterestHist",
  "/takerlongshortRatio",
  "/globalLongShortAccountRatio",
  "/topLongShortAccountRatio",
  "/topLongShortPositionRatio",
]);

async function dispatch(env) {
  if (!env.GITHUB_TOKEN) {
    throw new Error("GITHUB_TOKEN secret is not configured");
  }
  const response = await fetch(
    `https://api.github.com/repos/${OWNER}/${REPOSITORY}/actions/workflows/${WORKFLOW}/dispatches`,
    {
      method: "POST",
      headers: {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
        "User-Agent": "wei-signal-trigger",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({
        ref: "main",
        inputs: {
          run_backup_scan: true,
        },
      }),
    },
  );
  if (!response.ok) {
    throw new Error(`GitHub workflow dispatch failed: ${response.status} ${await response.text()}`);
  }
}

function proxyAuthorized(request, env) {
  return !env.PROXY_TOKEN || request.headers.get("X-Wei-Proxy-Key") === env.PROXY_TOKEN;
}

async function binanceRequest(operation) {
  const base = BINANCE_HOSTS[operation.base];
  const path = String(operation.path || "");
  if (!base || !ALLOWED_PATHS.has(path)) {
    return { ok: false, status: 400, error: "Unsupported Binance operation" };
  }
  const url = new URL(`${base}${path}`);
  for (const [key, value] of Object.entries(operation.params || {})) {
    if (value !== null && value !== undefined && value !== "") url.searchParams.set(key, String(value));
  }
  const response = await fetch(url, {
    headers: { "Accept": "application/json", "User-Agent": "wei-signal-proxy/1.0" },
  });
  const text = await response.text();
  if (!response.ok) return { ok: false, status: response.status, error: text.slice(0, 500) };
  try {
    return { ok: true, status: response.status, data: JSON.parse(text) };
  } catch {
    return { ok: false, status: 502, error: "Invalid Binance JSON" };
  }
}

async function binanceBatch(request, env) {
  if (!proxyAuthorized(request, env)) return Response.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  const payload = await request.json();
  const operations = Array.isArray(payload.operations) ? payload.operations.slice(0, 40) : [];
  if (!operations.length) return Response.json({ ok: false, error: "No operations" }, { status: 400 });
  const results = [];
  // Workers allow only a small number of simultaneous outbound connections.
  for (let start = 0; start < operations.length; start += 6) {
    results.push(...await Promise.all(operations.slice(start, start + 6).map(binanceRequest)));
  }
  return Response.json({ ok: results.every(item => item.ok), results });
}

export default {
  async scheduled(_controller, env, ctx) {
    ctx.waitUntil(dispatch(env));
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return Response.json({
        ok: true,
        version: VERSION,
        workflow: WORKFLOW,
        githubTokenConfigured: Boolean(env.GITHUB_TOKEN),
        proxyTokenConfigured: Boolean(env.PROXY_TOKEN),
      });
    }
    if (url.pathname === "/dispatch") {
      await dispatch(env);
      return Response.json({ ok: true, version: VERSION, dispatched: true, workflow: WORKFLOW });
    }
    if (url.pathname === "/binance/batch" && request.method === "POST") {
      return binanceBatch(request, env);
    }
    return Response.json({ ok: true, version: VERSION, health: "/health" });
  },
};

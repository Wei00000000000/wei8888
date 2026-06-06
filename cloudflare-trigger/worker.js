const OWNER = "Wei00000000000";
const REPOSITORY = "wei8888";
const WORKFLOW = "update-signals.yml";
const VERSION = "2026-06-06-signal-time-v2";

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
      body: JSON.stringify({ ref: "main" }),
    },
  );
  if (!response.ok) {
    throw new Error(`GitHub workflow dispatch failed: ${response.status} ${await response.text()}`);
  }
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
      });
    }
    return Response.json({ ok: true, version: VERSION, health: "/health" });
  },
};

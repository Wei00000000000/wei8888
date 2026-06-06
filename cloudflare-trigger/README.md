# Wei Signal Trigger

This Cloudflare Worker wakes the GitHub Actions signal scanner every five minutes.
The Worker does not contain the scanner or any API keys.

## Required secret

Create a fine-grained GitHub personal access token with:

- Repository access: `Wei00000000000/wei8888`
- Actions: Read and write
- Contents: Read-only

Save it in the Worker as the encrypted secret `GITHUB_TOKEN`.

## Cloudflare settings

- Worker name: `wei-signal-trigger`
- Cron trigger: `*/5 * * * *`
- Entrypoint: `worker.js`

The `/health` route returns a small readiness response. The scheduled handler
calls GitHub's `workflow_dispatch` endpoint for `update-signals.yml`.

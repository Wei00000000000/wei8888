# Security policy

## Secrets

Never commit API keys, passwords, JWT secrets, database URLs, tokens, dumps or `.env` files. Production secrets belong in Zeabur environment variables. Rotate any secret that appears in a screenshot, chat, log or commit.

## Production controls

- The GitHub Pages frontend contains no production secret.
- Protected data requires a backend session.
- Administrative routes require a separate admin token and CSRF validation.
- CORS and trusted hosts use explicit allowlists.
- Authentication and expensive routes are rate limited.
- Logs must not contain passwords, cookies, authorization headers or API keys.
- Entry, SL, TP, strategy version and trigger time are immutable after a signal is created.
- Scanner failures preserve the last successful database state.

## Reporting

Do not open public issues containing credentials or private trading data. Revoke exposed credentials first, then report privately to the repository owner.


# Wei Strategy Room backend

The backend moves authentication, market collection, signal persistence, full-history statistics and scheduling away from GitHub Pages.

## Zeabur services

Deploy this repository with four services in one Zeabur project:

1. `postgres` using Zeabur PostgreSQL.
2. `api` from this GitHub repository. Zeabur automatically matches `Dockerfile.api` when the service is named `api`.
3. `worker` from the same repository. Zeabur automatically matches `Dockerfile.worker` when the service is named `worker`.
4. Redis can be added later for distributed rate limiting and job queues; it is not required for the first migration.

The API start command is:

```text
uvicorn backend.app.main:app --host 0.0.0.0 --port $PORT
```

## Required environment variables

Copy names from `backend/.env.example` into Zeabur. Never commit real values.

- `APP_ENV=production`
- `DATABASE_URL=${POSTGRES_CONNECTION_STRING}` from the PostgreSQL service reference
- `JWT_SECRET` random 32+ character secret
- `APP_PASSWORD_HASH` generated with `python backend/scripts/hash_password.py`
- `ADMIN_TOKEN` a different random 32+ character secret
- `CORS_ORIGINS=https://wei00000000000.github.io`
- `ALLOWED_HOSTS` set to the exact Zeabur API hostname
- `COOKIE_SECURE=true`
- `COOKIE_SAMESITE=none`
- `COINGLASS_API_KEY`

The scanner preserves database data when an upstream source fails. A PostgreSQL advisory lock prevents duplicate scans when workers restart or overlap.

## Historical data

Run once after PostgreSQL is connected:

```text
python -m backend.scripts.import_history
```

The importer is idempotent. It keeps the original signal ID, entry, SL, TP and trigger time immutable, while allowing current price, state and PnL to update.

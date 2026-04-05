# media-gallery-2

Rewrite target for the legacy `MediaGallery` project.

## Migration Baseline

Feature parity requirements and AGENTS mindset notes are documented in:

- `MIGRATION_BASELINE.md`

Phase 1 execution artifacts are documented in:

- `server/PHASE1_BACKLOG.md`
- `server/API_CONTRACT.md`
- `server/sql/001_init.sql`

## Server Config Location (new)

Unlike the legacy project, runtime config and infra definitions live under `server/`:

- `server/config.yaml`
- `server/docker-compose.yml`

## Infrastructure

`server/docker-compose.yml` currently provisions:

- PostgreSQL
- Redis

Run from `media-gallery-2/server`:

```powershell
docker compose up -d
```

## Backend Scaffold (Scala)

Server scaffold is in `server/` with a minimal HTTP4s entrypoint:

- `server/build.sbt`
- `server/src/main/scala/mg2/Main.scala`

Implemented Phase 1 shell endpoints:

- `GET /v1/health`
- `GET /v1/profiles`
- `PUT /v1/profiles/active`
- `GET /v1/bootstrap-status`
- `GET /v1/scan-status`
- `POST /v1/scans`
- `GET /v1/scan-events` (SSE snapshot endpoint)
- `GET /v1/assets` (empty response skeleton)
- `GET /v1/media`
- `GET /v1/app-config`

Server behavior now uses PostgreSQL raw SQL for profiles, scan runs, and asset listing.

Run (requires JDK + sbt):

```powershell
cd server
sbt run
```

Before running the backend, start infra from `server/`:

```powershell
docker compose up -d
```

## Frontend Scaffold (TypeScript + Snabbdom + Sass)

Client scaffold is in `client/` and is framework-light:

- `client/src/main.ts`
- `client/src/styles/app.scss`

Run:

```powershell
cd client
npm install
npm run dev
```

The client uses a Vite proxy for `/v1` to `http://localhost:3001`.
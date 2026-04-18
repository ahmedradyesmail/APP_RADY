# FastAPI Authentication Backend

This project provides backend-only authentication with:

- Username/password login
- JWT access + refresh tokens
- SQLite database
- Device binding for non-admin users via `X-Device-Id`
- Admin-only user creation endpoint
- Admin bootstrap account from environment variables at startup

## Core Behavior

- Non-admin user first successful login binds that user to the device ID from `X-Device-Id`.
- After binding, the same user cannot log in or refresh tokens from another device.
- Admin accounts are not device restricted.
- If admin user does not exist, it is created on startup using `ADMIN_USERNAME` and `ADMIN_PASSWORD`.

## Project Structure

- `main.py`: FastAPI app, startup table creation, admin bootstrap
- `config.py`: environment-based settings
- `db.py`: SQLAlchemy engine/session/base
- `models/user.py`: user table model
- `routers/auth.py`: login + refresh endpoints
- `routers/admin.py`: admin user management endpoints
- `dependencies/auth.py`: auth/admin/device dependencies
- `services/security.py`: password hashing + JWT utilities
- `schemas/*.py`: request/response schemas

## Run Locally

### 1) Create and activate virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies

```bash
pip install -r requirements.txt
```

### 3) Configure environment variables

Copy `.env.example` and set values:

```bash
cp .env.example .env
```

Export values from `.env` (or set directly in your shell). Example:

```bash
export APP_NAME="Auth Backend"
export DATABASE_URL="postgresql://USER:PASSWORD@HOST:5432/DBNAME"
# Optional fallback for local SQLite only when DATABASE_URL is empty:
# export SQLITE_DB_URL="sqlite:///./app.db"
export JWT_SECRET_KEY="super-secret-key"
export JWT_ALGORITHM="HS256"
export ACCESS_TOKEN_EXPIRE_MINUTES="30"
export REFRESH_TOKEN_EXPIRE_DAYS="7"
export ADMIN_USERNAME="admin"
export ADMIN_PASSWORD="admin123"
```

### 4) Start server

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 5) Verify

- Health: `GET http://127.0.0.1:8000/health`
- Docs: `http://127.0.0.1:8000/docs`

### Redis (multiple workers / production)

Set `REDIS_URL` for shared job queues across Gunicorn or uvicorn workers, and for **provider API key pools** (Gemini, ORS, Google Maps). Keys are added only from the Admin UI after Redis is available — not from `.env` or a JSON file. See `.env.example`.

## API Endpoints

### Auth

- `POST /auth/login`
  - Body: `{ "username": "string", "password": "string" }`
  - Header: `X-Device-Id: your-device-id`
  - Returns: access token + refresh token

- `POST /auth/refresh`
  - Body: `{ "refresh_token": "string" }`
  - Header: `X-Device-Id: your-device-id`
  - Returns: new access token + refresh token

### Admin (requires bearer access token from admin account)

- `POST /admin/users`
  - Body: `{ "username": "string", "password": "string", "is_admin": false }`

- `GET /admin/users`
  - Lists all users and bound device IDs

## Example Flow

1. Start app (admin account auto-created if missing).
2. Login as admin at `/auth/login` with header `X-Device-Id`.
3. Use admin token to create users at `/admin/users`.
4. User logs in first time -> their `device_id` is stored.
5. Same user from another device ID -> denied.

## Run in Cloud

Use any provider that supports Python web services (for example Render, Railway, Fly.io, or a VM).

### Required settings in cloud environment

- `APP_NAME`
- `DATABASE_URL` (recommended primary DB in production)
- `SQLITE_DB_URL` (optional local fallback only)
- `JWT_SECRET_KEY` (must be strong in production)
- `JWT_ALGORITHM`
- `ACCESS_TOKEN_EXPIRE_MINUTES`
- `REFRESH_TOKEN_EXPIRE_DAYS`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`

### Start command

```bash
uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
```

### Production Notes

- SQLite is acceptable for small deployments and prototypes.
- For serious production scale, migrate to PostgreSQL.
- First startup on a fresh PostgreSQL instance uses an empty auth DB; `bootstrap_admin` recreates the admin from `ADMIN_USERNAME` and `ADMIN_PASSWORD`.
- Old users from legacy SQLite are not copied automatically; migrate them manually if you need to preserve existing accounts.
- Keep `JWT_SECRET_KEY` private and rotate periodically.
- Use HTTPS and secure API gateway/reverse proxy.

# Security Changes

## Summary of implemented changes

| File | What changed | Why |
|---|---|---|
| `config.py` | Added `allowed_origins` + parsed `allowed_origins_list` | Move CORS policy to environment and remove wildcard browser access |
| `main.py` | Added startup validation for sensitive defaults (`JWT_SECRET_KEY`, `ADMIN_USERNAME`, `ADMIN_PASSWORD`, `ALLOWED_ORIGINS`) | Prevent insecure boot with default secrets |
| `main.py` | Replaced `allow_origins=["*"]` with `settings.allowed_origins_list` | Enforce strict CORS |
| `main.py` | Registered shared slowapi limiter + custom 429 JSON handler | Centralized rate limiting behavior |
| `.env` | Added required sensitive-variable comment blocks + added `ALLOWED_ORIGINS` | Make security-critical values explicit for Railway/prod/Android setup |
| `.env.example` | Mirrored sensitive-variable comment blocks + `ALLOWED_ORIGINS` | Keep deploy template aligned with secure runtime config |
| `requirements.txt` | Added `slowapi` | Required for auth endpoint rate limiting |
| `services/rate_limit.py` | New limiter singleton and custom 429 handler | Prevent brute-force and provide clear client response |
| `routers/auth.py` | Rate-limited `/auth/login` (5/min/IP) and `/auth/refresh` (10/min/IP) | Brute-force and token spray mitigation |
| `routers/auth.py` | Added `POST /auth/logout` to revoke refresh tokens by `user + device_id` | Immediate token invalidation on logout |
| `models/refresh_token.py` | New table model for refresh token state | DB-backed refresh token rotation and revocation |
| `models/__init__.py` | Exported `RefreshToken` model | Ensure table is part of metadata creation |
| `services/security.py` | Added `hash_token()` and `token_exp_to_datetime()` | Store refresh hashes only and persist expiry safely |
| `services/auth_service.py` | Persist refresh token hash on login | Token tracking and revocation capability |
| `services/auth_service.py` | DB validation + rotation on refresh (revoke old, issue/store new) | Replay/theft resistance for refresh tokens |
| `services/auth_service.py` | Revoke all user tokens on deactivation | Enforce admin disable action immediately |
| `services/auth_service.py` | Added helper to revoke tokens by `user + device` | Used by logout flow |
| `services/upload_security.py` | New centralized upload size validator (Excel 30MB, audio 10MB) | Reduce DoS risk from oversized uploads |
| `routers/audio.py` | Added audio size limit + hidden internal errors + server logging | Protect upload path and avoid leaking internals |
| `routers/check.py` | Added Excel size limits in upload endpoints + hidden internal errors for `detail=str(e)` case + server logging | Upload hardening and internal error shielding |
| `routers/gps.py` | Added Excel size limits in all upload endpoints + hidden internal errors for all `detail=str(e)` cases + server logging | Upload hardening and internal error shielding |
| `routers/excel.py` | Added Excel size limit + hidden internal errors + server logging | Upload hardening and internal error shielding |

## Before going to production

- [ ] Set `JWT_SECRET_KEY` to a truly random value (recommended: `openssl rand -hex 64`)
- [ ] Set strong, non-default values for `ADMIN_USERNAME` and `ADMIN_PASSWORD`
- [ ] Set `ALLOWED_ORIGINS` to real trusted frontend domains only (comma-separated)
- [ ] Verify Railway domain and custom production domain are both present if both are used
- [ ] Install updated dependencies (`pip install -r requirements.txt`) to include `slowapi`
- [ ] Confirm `/auth/login` and `/auth/refresh` return 429 after limit is exceeded
- [ ] Confirm `POST /auth/logout` revokes refresh tokens for current `X-Device-Id`
- [ ] Confirm deactivating a user via Admin invalidates all refresh tokens for that user
- [ ] Confirm large-file behavior returns `413` for files over limits (Excel > 30MB, audio > 10MB)

## Before releasing Android APK

- Android native clients do **not** enforce browser CORS, so `ALLOWED_ORIGINS` does not affect native app HTTP calls directly.
- Keep `ALLOWED_ORIGINS` configured for your web frontend (Railway/domain) even after APK release.
- Do **not** change `JWT_SECRET_KEY` just because of Android; keep one backend secret per environment.
- `ADMIN_USERNAME` and `ADMIN_PASSWORD` remain backend-only and must never be embedded in APK code.
- Ensure APK sends `X-Device-Id` consistently; refresh/logout logic now validates device binding at DB level.

## Assumptions made

- Existing authentication and admin behavior must remain unchanged, so security was layered on top (no feature removal).
- Refresh token DB table creation is handled by existing SQLAlchemy `Base.metadata.create_all(...)` startup flow.
- Existing frontend currently stores tokens in local storage; this was not replaced in this pass because task scope requested backend/security additions without breaking existing behavior.
- API clients can provide `X-Device-Id` on logout requests, matching current device-binding model.

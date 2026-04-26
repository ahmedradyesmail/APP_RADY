#!/usr/bin/env python3
"""
Local async load test for APP_RADY on localhost.

Scenarios:
1) Unauthenticated GET /health
2) Authenticated mixed GET /auth/me and GET /api/check/capabilities

Designed for quick, repeatable runs on a dev machine.
"""

from __future__ import annotations

import asyncio
import os
import random
import statistics
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import httpx


BASE_URL = os.environ.get("LOAD_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"

# Short ramp to keep local M1/8GB safe.
DEFAULT_STAGES: list[tuple[int, int]] = [
    (50, 10),
    (100, 10),
    (200, 12),
    (300, 12),
    (500, 15),
]


@dataclass
class Sample:
    status: int
    latency_ms: float
    ok: bool
    error_type: str = ""


def _read_env_key(key: str) -> str:
    if key in os.environ and str(os.environ[key]).strip():
        return str(os.environ[key]).strip()
    if not ENV_PATH.exists():
        return ""
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        if k.strip() == key:
            return v.strip()
    return ""


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    idx = int(round((len(values) - 1) * p))
    idx = max(0, min(len(values) - 1, idx))
    return sorted(values)[idx]


def _parse_stages() -> list[tuple[int, int]]:
    """
    Parse LOAD_STAGES from env as: "50:30,100:30,150:45".
    Fallback: DEFAULT_STAGES.
    """
    raw = os.environ.get("LOAD_STAGES", "").strip()
    if not raw:
        return list(DEFAULT_STAGES)
    out: list[tuple[int, int]] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        if ":" not in p:
            raise ValueError(f"Invalid LOAD_STAGES part: {p!r} (expected c:s)")
        c_s, s_s = p.split(":", 1)
        c = int(c_s.strip())
        s = int(s_s.strip())
        if c <= 0 or s <= 0:
            raise ValueError(f"Invalid LOAD_STAGES values: {p!r}")
        out.append((c, s))
    if not out:
        raise ValueError("LOAD_STAGES parsed empty.")
    return out


def _summarize(samples: Iterable[Sample]) -> dict:
    s = list(samples)
    lat = [x.latency_ms for x in s]
    codes = Counter(x.status for x in s)
    errs = Counter(x.error_type for x in s if x.error_type)
    total = len(s)
    ok = sum(1 for x in s if x.ok)
    err = total - ok
    return {
        "total": total,
        "ok": ok,
        "err": err,
        "err_rate": (err / total * 100.0) if total else 0.0,
        "avg_ms": statistics.fmean(lat) if lat else 0.0,
        "p50_ms": _pct(lat, 0.50),
        "p95_ms": _pct(lat, 0.95),
        "p99_ms": _pct(lat, 0.99),
        "codes": dict(sorted(codes.items(), key=lambda kv: kv[0])),
        "error_types": dict(sorted(errs.items(), key=lambda kv: kv[0])),
    }


async def _worker_health(client: httpx.AsyncClient, end_at: float) -> list[Sample]:
    out: list[Sample] = []
    while time.perf_counter() < end_at:
        t0 = time.perf_counter()
        try:
            r = await client.get("/health")
            dt = (time.perf_counter() - t0) * 1000.0
            out.append(
                Sample(status=r.status_code, latency_ms=dt, ok=(r.status_code == 200))
            )
        except Exception as e:
            dt = (time.perf_counter() - t0) * 1000.0
            out.append(
                Sample(status=0, latency_ms=dt, ok=False, error_type=type(e).__name__)
            )
        await asyncio.sleep(0)
    return out


async def _worker_auth_mix(
    client: httpx.AsyncClient, end_at: float, cookies: dict[str, str]
) -> list[Sample]:
    out: list[Sample] = []
    paths = ["/auth/me", "/api/check/capabilities"]
    while time.perf_counter() < end_at:
        path = random.choice(paths)
        t0 = time.perf_counter()
        try:
            r = await client.get(path, cookies=cookies)
            dt = (time.perf_counter() - t0) * 1000.0
            out.append(
                Sample(status=r.status_code, latency_ms=dt, ok=(r.status_code == 200))
            )
        except Exception as e:
            dt = (time.perf_counter() - t0) * 1000.0
            out.append(
                Sample(status=0, latency_ms=dt, ok=False, error_type=type(e).__name__)
            )
        await asyncio.sleep(0)
    return out


async def run_stage_health(concurrency: int, seconds: int) -> dict:
    timeout = httpx.Timeout(15.0, connect=5.0)
    limits = httpx.Limits(
        max_connections=max(200, concurrency * 4),
        max_keepalive_connections=max(100, concurrency * 2),
    )
    async with httpx.AsyncClient(
        base_url=BASE_URL, timeout=timeout, limits=limits
    ) as client:
        end_at = time.perf_counter() + seconds
        tasks = [asyncio.create_task(_worker_health(client, end_at)) for _ in range(concurrency)]
        res = await asyncio.gather(*tasks)
    flat = [x for chunk in res for x in chunk]
    summ = _summarize(flat)
    summ["concurrency"] = concurrency
    summ["seconds"] = seconds
    return summ


async def _login_once() -> dict[str, str]:
    username = _read_env_key("ADMIN_USERNAME")
    password = _read_env_key("ADMIN_PASSWORD")
    if not username or not password:
        raise RuntimeError("ADMIN_USERNAME/ADMIN_PASSWORD not found in env.")
    timeout = httpx.Timeout(20.0, connect=5.0)
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
        r = await client.post(
            "/auth/login",
            headers={"X-Device-Id": "load-test-device-local"},
            json={"username": username, "password": password},
        )
        if r.status_code != 200:
            raise RuntimeError(f"Login failed: HTTP {r.status_code} {r.text[:200]}")
        return dict(client.cookies.items())


async def run_stage_auth(concurrency: int, seconds: int, cookies: dict[str, str]) -> dict:
    timeout = httpx.Timeout(20.0, connect=5.0)
    limits = httpx.Limits(
        max_connections=max(200, concurrency * 4),
        max_keepalive_connections=max(100, concurrency * 2),
    )
    async with httpx.AsyncClient(
        base_url=BASE_URL, timeout=timeout, limits=limits
    ) as client:
        end_at = time.perf_counter() + seconds
        tasks = [
            asyncio.create_task(_worker_auth_mix(client, end_at, cookies))
            for _ in range(concurrency)
        ]
        res = await asyncio.gather(*tasks)
    flat = [x for chunk in res for x in chunk]
    summ = _summarize(flat)
    summ["concurrency"] = concurrency
    summ["seconds"] = seconds
    return summ


def print_summary(title: str, rows: list[dict]) -> None:
    print(f"\n=== {title} ===")
    print(" conc | secs | requests | req/s | err% | avg(ms) | p50 | p95 | p99")
    for x in rows:
        rps = (x["total"] / x["seconds"]) if x["seconds"] else 0.0
        codes = ", ".join(f"{k}:{v}" for k, v in x.get("codes", {}).items())
        print(
            f"{x['concurrency']:>5} | {x['seconds']:>4} | {x['total']:>8} | "
            f"{rps:>5.1f} | {x['err_rate']:>4.1f}% | {x['avg_ms']:>7.1f} | "
            f"{x['p50_ms']:>4.1f} | {x['p95_ms']:>4.1f} | {x['p99_ms']:>4.1f}"
        )
        print(f"      status_codes: {codes}")
        if x.get("error_types"):
            et = ", ".join(f"{k}:{v}" for k, v in x["error_types"].items())
            print(f"      error_types: {et}")


async def main() -> None:
    stages = _parse_stages()
    print(f"Base URL: {BASE_URL}")
    print("Stages:", stages)

    health_rows: list[dict] = []
    for c, s in stages:
        print(f"[health] running c={c}, s={s} ...")
        health_rows.append(await run_stage_health(c, s))
    print_summary("Scenario A: /health", health_rows)

    print("\n[auth] logging in once for cookie session ...")
    cookies = await _login_once()
    auth_rows: list[dict] = []
    for c, s in stages:
        print(f"[auth] running c={c}, s={s} ...")
        auth_rows.append(await run_stage_auth(c, s, cookies))
    print_summary("Scenario B: /auth/me + /api/check/capabilities", auth_rows)


if __name__ == "__main__":
    asyncio.run(main())

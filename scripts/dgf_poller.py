#!/usr/bin/env python3
"""DGF / api.minenergia.cl ("API Energías Renovables") poller — Trello card #56.

Acquires Universidad de Chile · DGF (Departamento de Geofísica) + Ministerio de
Energía renewable-resource data and lands each response as a timestamped file in
the bronze landing path, so the existing Auto Loader bronze ingests it with no
change (Option A in docs/card-56-bronze-landing-design.md).

Why this is automatable: the portal login (``/login/``) is a plain Django form
with **no reCAPTCHA** (verified) — only the one-time *registration* form is
reCAPTCHA-gated. So ongoing ingestion just needs the double-submit CSRF dance:
GET ``/login/`` for the token+cookie, POST credentials for a session cookie, then
GET the ``/api/`` endpoints.

STATUS: the account (fuad.onate@evalueserve.com) is still pending admin approval,
so the concrete ``/api/`` paths and payload shape are not yet known. Once you can
log in, set ``DGF_ENDPOINTS`` (and tweak ``land()`` if the content type is exotic)
— the auth + landing scaffolding below is ready to run unchanged. Until then,
``--self-test`` exercises everything that does not require an approved account.

Config (environment or .env — .env is gitignored, keep credentials there only):
  DGF_BASE_URL   base URL                 (default https://api.minenergia.cl)
  DGF_USERNAME   registered email
  DGF_PASSWORD   account password
  DGF_LANDING    output dir / Volume path (default ./_dgf_landing for local tests;
                 on Databricks use /Volumes/<catalog>/<bronze_schema>/lookup/dgf/)
  DGF_ENDPOINTS  comma-separated /api/ paths to poll (fill after first login)

Usage:
  python scripts/dgf_poller.py --self-test   # verify the CSRF handshake (no creds)
  python scripts/dgf_poller.py --dry-run     # log in + report sizes, write nothing
  python scripts/dgf_poller.py               # log in, poll DGF_ENDPOINTS, land files
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

try:  # optional: load DGF_* from a local .env
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover - dotenv is a convenience, not a requirement
    pass

BASE = os.environ.get("DGF_BASE_URL", "https://api.minenergia.cl").rstrip("/")
LOGIN_URL = f"{BASE}/login/"
API_ROOT = f"{BASE}/api/"
USER_AGENT = "poc-databricks-evalueserve/dgf-poller (card-56)"
_CSRF_RE = re.compile(r'name="csrfmiddlewaretoken"\s+value="([^"]+)"')
_REDIRECTS = (301, 302, 303, 307, 308)


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def get_csrf(session: requests.Session) -> str:
    """GET the login page to set the ``csrftoken`` cookie and read the form token."""
    r = session.get(LOGIN_URL, timeout=20)
    r.raise_for_status()
    m = _CSRF_RE.search(r.text)
    if not m:
        raise RuntimeError("csrfmiddlewaretoken not found on the login page")
    return m.group(1)


def is_authenticated(session: requests.Session) -> bool:
    """Authenticated iff ``/api/`` does not bounce us to ``/login``."""
    r = session.get(API_ROOT, timeout=20, allow_redirects=False)
    if r.status_code in _REDIRECTS:
        return "/login" not in r.headers.get("Location", "")
    return r.status_code == 200


def login(session: requests.Session, username: str, password: str) -> bool:
    """Django double-submit CSRF login. Returns True if the session is authenticated."""
    token = get_csrf(session)
    session.post(
        LOGIN_URL,
        data={"csrfmiddlewaretoken": token, "username": username, "password": password},
        headers={"Referer": LOGIN_URL},  # Django requires a same-origin Referer for HTTPS POST
        timeout=20,
        allow_redirects=True,
    )
    return is_authenticated(session)


def slugify(path: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", path.strip("/")) or "root"


def poll_endpoint(session: requests.Session, path: str) -> tuple[str, requests.Response]:
    url = path if path.startswith("http") else f"{BASE}/{path.lstrip('/')}"
    r = session.get(url, timeout=60)
    r.raise_for_status()
    return url, r


def land(resp: requests.Response, landing: Path, slug: str) -> Path:
    """Write the response to ``landing`` with a UTC-stamped, type-aware filename."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ctype = resp.headers.get("Content-Type", "")
    ext = "json" if "json" in ctype else "csv" if "csv" in ctype else "txt"
    landing.mkdir(parents=True, exist_ok=True)
    out = landing / f"dgf_{slug}_{ts}.{ext}"
    out.write_bytes(resp.content)
    return out


def self_test() -> int:
    """Prove the CSRF handshake without credentials (works while approval is pending)."""
    session = make_session()
    r = session.get(LOGIN_URL, timeout=20)
    has_cookie = "csrftoken" in session.cookies
    has_token = bool(_CSRF_RE.search(r.text))
    api = session.get(API_ROOT, timeout=20, allow_redirects=False)
    print(f"GET {LOGIN_URL} -> HTTP {r.status_code}")
    print(f"  csrftoken cookie set ......... {has_cookie}")
    print(f"  csrfmiddlewaretoken in form .. {has_token}")
    print(f"GET {API_ROOT} (anonymous) -> HTTP {api.status_code} "
          f"{api.headers.get('Location', '')}".rstrip())
    ok = has_cookie and has_token
    print("RESULT:", "handshake OK — ready to authenticate once the account is approved"
          if ok else "handshake FAILED — login page shape changed, revisit get_csrf()")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="DGF api.minenergia.cl poller (card #56)")
    ap.add_argument("--self-test", action="store_true",
                    help="verify the CSRF handshake without credentials")
    ap.add_argument("--dry-run", action="store_true",
                    help="authenticate and report, but write no files")
    args = ap.parse_args()

    if args.self_test:
        return self_test()

    username = os.environ.get("DGF_USERNAME")
    password = os.environ.get("DGF_PASSWORD")
    if not username or not password:
        print("set DGF_USERNAME and DGF_PASSWORD (env or .env) to authenticate", file=sys.stderr)
        return 2

    session = make_session()
    if not login(session, username, password):
        print("login failed — account likely still pending approval, or wrong credentials",
              file=sys.stderr)
        return 1
    print("authenticated ✓")

    endpoints = [e.strip() for e in os.environ.get("DGF_ENDPOINTS", "").split(",") if e.strip()]
    if not endpoints:
        print("logged in, but DGF_ENDPOINTS is empty — browse /api/ and set it.", file=sys.stderr)
        return 0

    landing = Path(os.environ.get("DGF_LANDING", "./_dgf_landing"))
    failures = 0
    for ep in endpoints:
        try:
            url, resp = poll_endpoint(session, ep)
            if args.dry_run:
                print(f"[dry-run] {url} -> {resp.status_code} "
                      f"{resp.headers.get('Content-Type', '')} ({len(resp.content)} bytes)")
            else:
                print(f"landed {url} -> {land(resp, landing, slugify(ep))}")
        except Exception as exc:  # keep polling the rest even if one endpoint fails
            failures += 1
            print(f"ERROR polling {ep}: {exc}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

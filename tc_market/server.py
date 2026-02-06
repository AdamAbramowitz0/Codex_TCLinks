"""HTTP API server and static frontend host."""

from __future__ import annotations

import json
import os
import re
import secrets
from datetime import datetime, timezone
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlencode, urlparse

from tc_market.jobs import JobService
from tc_market.market import MarketService
from tc_market.model_agents import ModelRunner
from tc_market.oauth_google import GoogleOAuthClient
from tc_market.storage import Storage
from tc_market.twilio_client import TwilioClient

URL_PATTERN = re.compile(r"https?://[^\s]+")
WEB_ROOT = Path(__file__).resolve().parent.parent / "web"


def _split_path(path: str) -> list[str]:
    return [part for part in path.split("/") if part]


def _extract_first_url(text: str) -> str | None:
    match = URL_PATTERN.search(text or "")
    if not match:
        return None
    return match.group(0)


def _json_bytes(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload).encode("utf-8")


class APIHandler(BaseHTTPRequestHandler):
    storage: Storage
    market: MarketService
    model_runner: ModelRunner
    jobs: JobService
    google_oauth: GoogleOAuthClient | None
    twilio_client: TwilioClient | None

    session_cookie_name = "tc_session"

    def _send_json(
        self,
        status: int,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        body = _json_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _send_text(
        self,
        status: int,
        text: str,
        content_type: str = "text/plain; charset=utf-8",
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, location: str, headers: Optional[Dict[str, str]] = None) -> None:
        self.send_response(HTTPStatus.FOUND)
        self.send_header("Location", location)
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()

    def _read_payload(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b""
        if not raw:
            return {}

        content_type = (self.headers.get("Content-Type", "") or "").lower()
        if "application/json" in content_type:
            return json.loads(raw.decode("utf-8"))
        if "application/x-www-form-urlencoded" in content_type:
            parsed = parse_qs(raw.decode("utf-8"), keep_blank_values=True)
            return {key: values[0] if len(values) == 1 else values for key, values in parsed.items()}
        return {}

    def _parse_query(self) -> Dict[str, str]:
        query = parse_qs(urlparse(self.path).query)
        return {k: v[0] for k, v in query.items()}

    def _get_session_token(self) -> Optional[str]:
        cookie_header = self.headers.get("Cookie")
        if not cookie_header:
            return None

        cookie = SimpleCookie()
        cookie.load(cookie_header)
        morsel = cookie.get(self.session_cookie_name)
        if morsel is None:
            return None
        return morsel.value

    def _get_current_user(self):
        token = self._get_session_token()
        if not token:
            return None
        return self.storage.get_user_by_session(token)

    def _session_cookie_header(self, token: str, max_age_seconds: int = 1209600) -> str:
        secure = os.getenv("COOKIE_SECURE", "0") == "1"
        parts = [
            f"{self.session_cookie_name}={token}",
            "Path=/",
            "HttpOnly",
            "SameSite=Lax",
            f"Max-Age={max_age_seconds}",
        ]
        if secure:
            parts.append("Secure")
        return "; ".join(parts)

    def _clear_session_cookie_header(self) -> str:
        return (
            f"{self.session_cookie_name}=; Path=/; HttpOnly; SameSite=Lax; "
            "Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT"
        )

    def _require_user(self):
        user = self._get_current_user()
        if user is None:
            self._send_json(401, {"error": "Authentication required"})
            return None
        return user

    def _require_job_token(self, query_token: Optional[str] = None) -> bool:
        token = os.getenv("JOB_AUTH_TOKEN", "")
        if not token:
            return True
        provided = (
            self.headers.get("X-Job-Token")
            or self.headers.get("Authorization", "").removeprefix("Bearer ").strip()
            or query_token
            or ""
        )
        if secrets.compare_digest(token, provided):
            return True
        self._send_json(401, {"error": "Invalid job token"})
        return False

    def _serve_static(self, rel_path: str, content_type: str) -> None:
        file_path = WEB_ROOT / rel_path
        if not file_path.exists() or not file_path.is_file():
            self._send_json(404, {"error": "Not found"})
            return
        self._send_text(200, file_path.read_text(encoding="utf-8"), content_type=content_type)

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            parts = _split_path(parsed.path)
            query = parse_qs(parsed.query)

            if parts == []:
                self._serve_static("index.html", "text/html; charset=utf-8")
                return
            if parts == ["app.js"]:
                self._serve_static("app.js", "application/javascript; charset=utf-8")
                return
            if parts == ["styles.css"]:
                self._serve_static("styles.css", "text/css; charset=utf-8")
                return

            if parts == ["auth", "google", "start"]:
                if self.google_oauth is None:
                    self._send_json(500, {"error": "Google OAuth not configured"})
                    return

                redirect_to = query.get("redirect", ["/"])[0]
                state = self.storage.create_oauth_state(redirect_to=redirect_to)
                auth_url = self.google_oauth.build_authorize_url(state)
                self._redirect(auth_url)
                return

            if parts == ["auth", "google", "callback"]:
                if self.google_oauth is None:
                    self._send_json(500, {"error": "Google OAuth not configured"})
                    return

                state = query.get("state", [""])[0]
                code = query.get("code", [""])[0]
                if not state or not code:
                    self._send_json(400, {"error": "Missing code/state"})
                    return

                redirect_to = self.storage.consume_oauth_state(state)
                if redirect_to is None:
                    self._send_json(400, {"error": "Invalid or expired state"})
                    return

                token_payload = self.google_oauth.exchange_code(code)
                userinfo = self.google_oauth.fetch_userinfo(token_payload["access_token"])

                user = self.storage.get_or_create_google_user(
                    google_sub=userinfo["sub"],
                    email=userinfo["email"],
                    display_name=userinfo.get("name") or userinfo.get("email", "User"),
                )
                session_token = self.storage.create_session(user.id)

                self._redirect(
                    redirect_to,
                    headers={"Set-Cookie": self._session_cookie_header(session_token)},
                )
                return

            if parts == ["api", "health"]:
                self._send_json(200, {"ok": True})
                return

            if parts == ["api", "jobs", "daily-faucet"]:
                query_token = query.get("token", [""])[0]
                if not self._require_job_token(query_token):
                    return
                as_of_date = query.get("as_of_date", [""])[0] or None
                force = query.get("force", ["0"])[0] == "1"
                result = self.jobs.run_daily_faucet(as_of_date=as_of_date, force=force)
                self._send_json(200, result)
                return

            if parts == ["api", "jobs", "sync-assorted-links"]:
                query_token = query.get("token", [""])[0]
                if not self._require_job_token(query_token):
                    return
                force = query.get("force", ["0"])[0] == "1"
                limit = int(query.get("limit", ["10"])[0])
                max_feed_pages = int(query.get("max_feed_pages", ["1"])[0])
                if max_feed_pages < 1:
                    raise ValueError("max_feed_pages must be >= 1")
                result = self.jobs.sync_assorted_links(
                    force=force,
                    limit=limit,
                    max_feed_pages=max_feed_pages,
                )
                self._send_json(200, result)
                return

            if parts == ["api", "jobs", "models"]:
                query_token = query.get("token", [""])[0]
                if not self._require_job_token(query_token):
                    return
                force = query.get("force", ["0"])[0] == "1"
                cycle_id = query.get("cycle_id", [""])[0] or None
                result = self.jobs.run_models(cycle_id=cycle_id, force=force)
                self._send_json(200, result)
                return

            if parts == ["api", "jobs", "curation-awards"]:
                query_token = query.get("token", [""])[0]
                if not self._require_job_token(query_token):
                    return
                force = query.get("force", ["0"])[0] == "1"
                cycle_id = query.get("cycle_id", [""])[0] or None
                min_age_hours = int(query.get("min_age_hours", ["24"])[0])
                result = self.jobs.run_curation_rewards(
                    cycle_id=cycle_id,
                    force=force,
                    min_age_hours=min_age_hours,
                )
                self._send_json(200, result)
                return

            if parts == ["api", "me"]:
                user = self._get_current_user()
                open_cycle = self.storage.get_open_cycle()
                self._send_json(
                    200,
                    {
                        "user": user.__dict__ if user else None,
                        "open_cycle": open_cycle.__dict__ if open_cycle else None,
                    },
                )
                return

            if parts == ["api", "cycles", "current"]:
                cycle = self.storage.get_open_cycle()
                self._send_json(200, {"cycle": cycle.__dict__ if cycle else None})
                return

            if len(parts) == 4 and parts[:2] == ["api", "cycles"] and parts[3] == "probabilities":
                cycle_id = parts[2]
                probs = self.market.compute_market_probabilities(cycle_id)
                self._send_json(200, {"cycle_id": cycle_id, "probabilities": probs})
                return

            if len(parts) == 4 and parts[:2] == ["api", "cycles"] and parts[3] == "candidates":
                cycle_id = parts[2]
                rows = self.storage.list_cycle_candidates_with_submitter(cycle_id)
                self._send_json(200, {"cycle_id": cycle_id, "candidates": rows})
                return

            if parts == ["api", "leaderboard"]:
                board_type = query.get("type", ["all"])[0]
                if board_type == "ai":
                    rows = self.storage.list_leaderboard(account_type="AI")
                elif board_type == "human":
                    rows = self.storage.list_leaderboard(account_type="HUMAN")
                elif board_type == "curation":
                    rows = self.storage.list_curation_leaderboard()
                else:
                    rows = self.storage.list_leaderboard()
                self._send_json(200, {"leaderboard": rows, "type": board_type})
                return

            if len(parts) == 5 and parts[:2] == ["api", "models"] and parts[3] == "picks":
                model_id = parts[2]
                cycle_id = parts[4]
                model_user = self.storage.get_user_by_email(f"model:{model_id}@local")
                if model_user is None:
                    self._send_json(404, {"error": "Model user not found"})
                    return
                predictions = self.storage.list_model_predictions(cycle_id, model_user.id)
                self._send_json(
                    200,
                    {
                        "model_id": model_id,
                        "cycle_id": cycle_id,
                        "predictions": [prediction.__dict__ for prediction in predictions],
                    },
                )
                return

            if parts == ["api", "archive", "links"]:
                q = query.get("q", [""])[0]
                domain = query.get("domain", [""])[0]
                rows = self.storage.search_archive_links(query=q, domain=domain)
                self._send_json(200, {"results": rows})
                return

            if parts == ["api", "archive", "posts"]:
                limit = int(query.get("limit", ["100"])[0])
                offset = int(query.get("offset", ["0"])[0])
                if limit < 1:
                    raise ValueError("limit must be >= 1")
                if offset < 0:
                    raise ValueError("offset must be >= 0")
                rows = self.storage.list_source_posts(limit=limit, offset=offset)
                total = self.storage.count_source_posts()
                self._send_json(
                    200,
                    {
                        "results": rows,
                        "limit": limit,
                        "offset": offset,
                        "total": total,
                    },
                )
                return

            if len(parts) == 2 and parts[0] == "r":
                candidate_id = parts[1]
                user = self._get_current_user()
                client_ip = self.headers.get("X-Forwarded-For", self.client_address[0] if self.client_address else "")
                user_agent = self.headers.get("User-Agent", "")
                fingerprint = f"{user.id if user else 'anon'}|{client_ip}|{user_agent}"
                self.storage.record_click(
                    candidate_id=candidate_id,
                    fingerprint_source=fingerprint,
                    clicked_by_user_id=(user.id if user else None),
                )
                destination = self.storage.get_candidate_url(candidate_id)
                self._redirect(destination)
                return

            self._send_json(404, {"error": "Not found"})
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover
            self._send_json(500, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        try:
            parts = _split_path(urlparse(self.path).path)
            payload = self._read_payload()

            if parts == ["api", "auth", "logout"]:
                session_token = self._get_session_token()
                if session_token:
                    self.storage.delete_session(session_token)
                self._send_json(
                    200,
                    {"ok": True},
                    headers={"Set-Cookie": self._clear_session_cookie_header()},
                )
                return

            if parts == ["api", "users"]:
                user = self.storage.create_user(
                    display_name=payload["display_name"],
                    email=payload["email"],
                    account_type=payload.get("account_type", "HUMAN"),
                )
                self._send_json(201, {"user": user.__dict__})
                return

            if parts == ["api", "phones", "link", "start"]:
                user = self._require_user()
                if user is None:
                    return
                phone_number = payload.get("phone_number", "")
                if not phone_number:
                    self._send_json(400, {"error": "phone_number is required"})
                    return

                normalized = self.storage._normalize_phone(phone_number)
                if self.twilio_client is not None:
                    twilio_result = self.twilio_client.start_verification(normalized)
                    challenge_id = self.storage.create_phone_verification_challenge(
                        user_id=user.id,
                        phone_number=normalized,
                        provider="twilio",
                        provider_sid=str(twilio_result.get("sid", "")),
                    )
                    self._send_json(
                        200,
                        {
                            "challenge_id": challenge_id,
                            "phone_number": normalized,
                            "provider": "twilio",
                            "status": twilio_result.get("status", "pending"),
                        },
                    )
                    return

                code = f"{secrets.randbelow(900000) + 100000}"
                challenge_id = self.storage.create_phone_verification_challenge(
                    user_id=user.id,
                    phone_number=normalized,
                    provider="local",
                    otp_code=code,
                )
                self._send_json(
                    200,
                    {
                        "challenge_id": challenge_id,
                        "phone_number": normalized,
                        "provider": "local",
                        "dev_code": code,
                    },
                )
                return

            if parts == ["api", "phones", "link", "verify"]:
                user = self._require_user()
                if user is None:
                    return

                challenge_id = payload.get("challenge_id", "")
                code = str(payload.get("code", "")).strip()
                if not challenge_id or not code:
                    self._send_json(400, {"error": "challenge_id and code are required"})
                    return

                challenge = self.storage.get_phone_verification_challenge(challenge_id)
                if challenge is None:
                    self._send_json(404, {"error": "Challenge not found"})
                    return
                if challenge["user_id"] != user.id:
                    self._send_json(403, {"error": "Challenge does not belong to current user"})
                    return
                if challenge["status"] != "PENDING":
                    self._send_json(400, {"error": "Challenge is not pending"})
                    return
                expires_at = challenge["expires_at"]
                if expires_at.endswith("Z"):
                    expires_at = expires_at.replace("Z", "+00:00")
                if datetime.fromisoformat(expires_at) <= datetime.now(timezone.utc):
                    self._send_json(400, {"error": "Challenge expired"})
                    return

                self.storage.mark_phone_challenge_attempt(challenge_id)
                success = False
                if challenge["provider"] == "twilio":
                    if self.twilio_client is None:
                        self._send_json(500, {"error": "Twilio is not configured"})
                        return
                    check = self.twilio_client.check_verification(challenge["phone_number"], code)
                    success = str(check.get("status", "")).lower() == "approved"
                else:
                    success = secrets.compare_digest(str(challenge.get("otp_code", "")), code)

                if not success:
                    self._send_json(400, {"error": "Invalid verification code"})
                    return

                self.storage.mark_phone_challenge_verified(challenge_id)
                linked = self.storage.link_phone(user.id, challenge["phone_number"])
                self._send_json(200, {"linked_phone": linked, "ok": True})
                return

            if parts == ["api", "faucet", "run"]:
                if not self._require_job_token():
                    return
                result = self.jobs.run_daily_faucet(
                    as_of_date=payload.get("as_of_date"),
                    force=bool(payload.get("force", False)),
                )
                self._send_json(200, result)
                return

            if parts == ["api", "jobs", "sync-assorted-links"]:
                if not self._require_job_token():
                    return
                limit = int(payload.get("limit", 10))
                max_feed_pages = int(payload.get("max_feed_pages", 1))
                if max_feed_pages < 1:
                    raise ValueError("max_feed_pages must be >= 1")
                result = self.jobs.sync_assorted_links(
                    force=bool(payload.get("force", False)),
                    limit=limit,
                    max_feed_pages=max_feed_pages,
                )
                self._send_json(200, result)
                return

            if parts == ["api", "jobs", "models"]:
                if not self._require_job_token():
                    return
                result = self.jobs.run_models(
                    cycle_id=payload.get("cycle_id"),
                    force=bool(payload.get("force", False)),
                )
                self._send_json(200, result)
                return

            if parts == ["api", "jobs", "curation-awards"]:
                if not self._require_job_token():
                    return
                result = self.jobs.run_curation_rewards(
                    cycle_id=payload.get("cycle_id"),
                    force=bool(payload.get("force", False)),
                    min_age_hours=int(payload.get("min_age_hours", 24)),
                )
                self._send_json(200, result)
                return

            if parts == ["api", "cycles"]:
                cycle = self.storage.create_cycle(payload.get("cycle_date"))
                self._send_json(201, {"cycle": cycle.__dict__})
                return

            if len(parts) == 4 and parts[:2] == ["api", "cycles"] and parts[3] == "candidates":
                user = self._require_user()
                if user is None:
                    return
                cycle_id = parts[2]
                candidate = self.market.submit_candidate(
                    cycle_id=cycle_id,
                    user_id=user.id,
                    url=payload["url"],
                    title=payload.get("title", ""),
                )
                self._send_json(201, {"candidate": candidate})
                return

            if parts == ["api", "submissions", "web"]:
                user = self._require_user()
                if user is None:
                    return

                cycle_id = payload.get("cycle_id")
                if not cycle_id:
                    cycle = self.storage.get_open_cycle()
                    if cycle is None:
                        self._send_json(400, {"error": "No open cycle. Create one first."})
                        return
                    cycle_id = cycle.id

                candidate = self.market.submit_candidate(
                    cycle_id=cycle_id,
                    user_id=user.id,
                    url=payload["url"],
                    title=payload.get("title", ""),
                )
                self._send_json(201, {"candidate": candidate, "source": "web"})
                return

            if parts == ["api", "submissions", "sms", "webhook"]:
                cycle_id = payload.get("cycle_id")
                if not cycle_id:
                    cycle = self.storage.get_open_cycle()
                    if cycle is None:
                        self._send_json(400, {"error": "No open cycle. Create one first."})
                        return
                    cycle_id = cycle.id

                from_phone = payload.get("From") or payload.get("from_phone")
                text = payload.get("Body") or payload.get("text", "")
                if not from_phone:
                    self._send_json(400, {"error": "Missing phone number"})
                    return

                user = self.storage.get_user_by_phone(from_phone)
                if user is None:
                    self._send_json(403, {"error": "Phone number is not linked to any user"})
                    return

                url = payload.get("url") or _extract_first_url(text)
                if not url:
                    self._send_json(400, {"error": "No URL found in SMS payload"})
                    return

                candidate = self.market.submit_candidate(
                    cycle_id=cycle_id,
                    user_id=user.id,
                    url=url,
                    title=payload.get("title", ""),
                )
                self._send_json(
                    201,
                    {
                        "candidate": candidate,
                        "source": "sms",
                        "user_id": user.id,
                    },
                )
                return

            if len(parts) == 4 and parts[:2] == ["api", "cycles"] and parts[3] == "settle":
                if not self._require_job_token():
                    return
                cycle_id = parts[2]
                settlement = self.market.settle_cycle(cycle_id=cycle_id, winner_urls=payload.get("winner_urls", []))
                self._send_json(200, {"settlement": settlement})
                return

            if parts == ["api", "models", "reload"]:
                configs = self.model_runner.reload_configs()
                self._send_json(
                    200,
                    {
                        "models": [
                            {
                                "id": config.id,
                                "provider": config.provider,
                                "model_name": config.model_name,
                                "enabled": config.enabled,
                                "strategy_profile": config.strategy_profile,
                            }
                            for config in configs
                        ]
                    },
                )
                return

            if parts == ["api", "models", "run"]:
                cycle_id = payload.get("cycle_id")
                if not cycle_id:
                    cycle = self.storage.get_open_cycle()
                    if cycle is None:
                        self._send_json(400, {"error": "No open cycle"})
                        return
                    cycle_id = cycle.id

                result = self.model_runner.run_cycle(cycle_id)
                self._send_json(200, {"model_run": result})
                return

            self._send_json(404, {"error": "Not found"})
        except KeyError as exc:
            self._send_json(400, {"error": f"Missing field: {exc}"})
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover
            self._send_json(500, {"error": str(exc)})

    def do_PUT(self) -> None:  # noqa: N802
        try:
            parts = _split_path(urlparse(self.path).path)
            payload = self._read_payload()

            if len(parts) == 4 and parts[:2] == ["api", "cycles"] and parts[3] == "picks":
                user = self._require_user()
                if user is None:
                    return

                cycle_id = parts[2]
                picks = self.market.set_ranked_picks(
                    cycle_id=cycle_id,
                    user_id=user.id,
                    candidate_ids=payload.get("candidate_ids", []),
                )
                self._send_json(200, {"picks": picks})
                return

            self._send_json(404, {"error": "Not found"})
        except KeyError as exc:
            self._send_json(400, {"error": f"Missing field: {exc}"})
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover
            self._send_json(500, {"error": str(exc)})


def create_app(db_path: str, config_path: str) -> type[APIHandler]:
    storage = Storage(db_path)
    market = MarketService(storage)
    model_runner = ModelRunner(storage, market, config_path)
    jobs = JobService(storage, market, model_runner)

    class BoundAPIHandler(APIHandler):
        pass

    BoundAPIHandler.storage = storage
    BoundAPIHandler.market = market
    BoundAPIHandler.model_runner = model_runner
    BoundAPIHandler.jobs = jobs
    BoundAPIHandler.google_oauth = GoogleOAuthClient.from_env()
    BoundAPIHandler.twilio_client = TwilioClient.from_env()
    return BoundAPIHandler


def run_server(host: str, port: int, db_path: str, config_path: str) -> None:
    handler_class = create_app(db_path=db_path, config_path=config_path)
    server = ThreadingHTTPServer((host, port), handler_class)
    print(f"Serving market API on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

"""Persistence layer for the Tyler Cowen links market.

Supports SQLite by default and PostgreSQL when `db_path` is a postgres URL.
"""

from __future__ import annotations

import json
import secrets
import sqlite3
import uuid
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from typing import Any, Dict, List, Optional

from tc_market.constants import (
    CURATION_RANK_REWARDS,
    DAILY_CHIPS,
    MAX_PICKS_PER_CYCLE,
    STARTING_CHIPS,
)
from tc_market.models import CandidateLink, Cycle, ModelPrediction, Pick, User
from tc_market.url_utils import canonicalize_url, extract_domain


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


class Storage:
    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        self.is_postgres = db_path.startswith("postgres://") or db_path.startswith("postgresql://")

        if self.is_postgres:
            try:
                import psycopg
                from psycopg.rows import dict_row
            except ImportError as exc:  # pragma: no cover - only hits without optional dep
                raise RuntimeError(
                    "PostgreSQL requested but psycopg is not installed. Add psycopg[binary] to dependencies."
                ) from exc

            self.conn = psycopg.connect(db_path, row_factory=dict_row)
        else:
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA foreign_keys = ON")

        self._create_schema()

    def close(self) -> None:
        self.conn.close()

    def _adapt_sql(self, sql: str) -> str:
        if not self.is_postgres:
            return sql
        return sql.replace("?", "%s")

    def _execute(self, sql: str, params: Iterable[Any] = ()):
        return self.conn.execute(self._adapt_sql(sql), tuple(params))

    def _executescript(self, script: str) -> None:
        if not self.is_postgres:
            self.conn.executescript(script)
            return

        statements = [statement.strip() for statement in script.split(";") if statement.strip()]
        for statement in statements:
            self._execute(statement)

    @staticmethod
    def _id(prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:12]}"

    @staticmethod
    def _parse_date(value: str | date) -> date:
        if isinstance(value, date):
            return value
        return date.fromisoformat(value)

    def _create_schema(self) -> None:
        self._executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                google_sub TEXT UNIQUE,
                account_type TEXT NOT NULL CHECK(account_type IN ('HUMAN', 'AI')),
                current_chips INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                last_daily_credit_date TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS oauth_states (
                state TEXT PRIMARY KEY,
                redirect_to TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS phone_verification_challenges (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                phone_number TEXT NOT NULL,
                provider TEXT NOT NULL,
                provider_sid TEXT,
                otp_code TEXT,
                status TEXT NOT NULL CHECK(status IN ('PENDING', 'VERIFIED', 'EXPIRED')),
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                verified_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS user_phones (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                phone_number TEXT NOT NULL UNIQUE,
                verified_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS chip_ledger (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                cycle_id TEXT,
                event_type TEXT NOT NULL,
                chips_delta INTEGER NOT NULL,
                metadata TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS cycles (
                id TEXT PRIMARY KEY,
                cycle_date TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('OPEN', 'SETTLED')),
                opened_at TEXT NOT NULL,
                closed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS candidate_links (
                id TEXT PRIMARY KEY,
                cycle_id TEXT NOT NULL,
                submitted_by_user_id TEXT NOT NULL,
                original_url TEXT NOT NULL,
                canonical_url TEXT NOT NULL,
                domain TEXT NOT NULL,
                title TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(cycle_id, canonical_url),
                FOREIGN KEY(cycle_id) REFERENCES cycles(id),
                FOREIGN KEY(submitted_by_user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS picks (
                id TEXT PRIMARY KEY,
                cycle_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                rank INTEGER NOT NULL,
                picked_at TEXT NOT NULL,
                UNIQUE(cycle_id, user_id, rank),
                UNIQUE(cycle_id, user_id, candidate_id),
                FOREIGN KEY(cycle_id) REFERENCES cycles(id),
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(candidate_id) REFERENCES candidate_links(id)
            );

            CREATE TABLE IF NOT EXISTS cycle_results (
                cycle_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                is_winner INTEGER NOT NULL,
                PRIMARY KEY(cycle_id, candidate_id),
                FOREIGN KEY(cycle_id) REFERENCES cycles(id),
                FOREIGN KEY(candidate_id) REFERENCES candidate_links(id)
            );

            CREATE TABLE IF NOT EXISTS click_events (
                id TEXT PRIMARY KEY,
                cycle_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                clicked_by_user_id TEXT,
                fingerprint_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(candidate_id, fingerprint_hash),
                FOREIGN KEY(cycle_id) REFERENCES cycles(id),
                FOREIGN KEY(candidate_id) REFERENCES candidate_links(id),
                FOREIGN KEY(clicked_by_user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS curation_rewards (
                cycle_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                rank INTEGER NOT NULL,
                unique_clicks INTEGER NOT NULL,
                reward_chips INTEGER NOT NULL,
                awarded_at TEXT NOT NULL,
                PRIMARY KEY(cycle_id, user_id),
                FOREIGN KEY(cycle_id) REFERENCES cycles(id),
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS model_predictions (
                cycle_id TEXT NOT NULL,
                model_user_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                probability REAL NOT NULL,
                explanation TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(cycle_id, model_user_id, candidate_id),
                FOREIGN KEY(cycle_id) REFERENCES cycles(id),
                FOREIGN KEY(model_user_id) REFERENCES users(id),
                FOREIGN KEY(candidate_id) REFERENCES candidate_links(id)
            );

            CREATE TABLE IF NOT EXISTS source_posts (
                id TEXT PRIMARY KEY,
                source_post_url TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                published_at TEXT NOT NULL,
                extracted_links_json TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS archive_links (
                id TEXT PRIMARY KEY,
                post_date TEXT NOT NULL,
                url TEXT NOT NULL,
                canonical_url TEXT NOT NULL,
                domain TEXT NOT NULL,
                title TEXT NOT NULL,
                source_post_url TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(post_date, canonical_url)
            );

            CREATE TABLE IF NOT EXISTS job_runs (
                id TEXT PRIMARY KEY,
                job_name TEXT NOT NULL,
                run_key TEXT NOT NULL,
                status TEXT NOT NULL,
                details_json TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(job_name, run_key)
            );

            CREATE INDEX IF NOT EXISTS idx_cycles_status ON cycles(status);
            CREATE INDEX IF NOT EXISTS idx_candidate_cycle ON candidate_links(cycle_id);
            CREATE INDEX IF NOT EXISTS idx_picks_cycle ON picks(cycle_id);
            CREATE INDEX IF NOT EXISTS idx_ledger_user ON chip_ledger(user_id);
            CREATE INDEX IF NOT EXISTS idx_archive_domain ON archive_links(domain);
            CREATE INDEX IF NOT EXISTS idx_user_phones_phone ON user_phones(phone_number);
            CREATE INDEX IF NOT EXISTS idx_clicks_cycle_candidate ON click_events(cycle_id, candidate_id);
            CREATE INDEX IF NOT EXISTS idx_source_posts_published ON source_posts(published_at);
            CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
            """
        )
        self.conn.commit()

    def create_user(
        self,
        display_name: str,
        email: str,
        account_type: str = "HUMAN",
        created_date: date | str | None = None,
        google_sub: Optional[str] = None,
    ) -> User:
        created = self._parse_date(created_date) if created_date else date.today()
        user_id = self._id("usr")
        now = _now_iso()
        with self.conn:
            self._execute(
                """
                INSERT INTO users(id, display_name, email, google_sub, account_type, current_chips, created_at, last_daily_credit_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    display_name,
                    email,
                    google_sub,
                    account_type,
                    STARTING_CHIPS,
                    now,
                    created.isoformat(),
                ),
            )
            self._execute(
                """
                INSERT INTO chip_ledger(id, user_id, cycle_id, event_type, chips_delta, metadata, created_at)
                VALUES (?, ?, NULL, 'signup_bonus', ?, ?, ?)
                """,
                (
                    self._id("led"),
                    user_id,
                    STARTING_CHIPS,
                    json.dumps({"reason": "starting_chips"}),
                    now,
                ),
            )

        return self.get_user(user_id)

    def get_or_create_google_user(self, google_sub: str, email: str, display_name: str) -> User:
        row = self._execute("SELECT id FROM users WHERE google_sub = ?", (google_sub,)).fetchone()
        if row:
            return self.get_user(row["id"])

        row = self._execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if row:
            with self.conn:
                self._execute(
                    "UPDATE users SET google_sub = ?, display_name = ? WHERE id = ?",
                    (google_sub, display_name, row["id"]),
                )
            return self.get_user(row["id"])

        return self.create_user(display_name=display_name, email=email, google_sub=google_sub)

    def get_user(self, user_id: str) -> User:
        row = self._execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if row is None:
            raise KeyError(f"User not found: {user_id}")
        return User(**dict(row))

    def get_user_by_email(self, email: str) -> Optional[User]:
        row = self._execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if row is None:
            return None
        return User(**dict(row))

    def create_session(self, user_id: str, ttl_days: int = 14) -> str:
        token = secrets.token_urlsafe(32)
        created = _now()
        expires = created + timedelta(days=ttl_days)
        with self.conn:
            self._execute(
                "INSERT INTO sessions(token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
                (
                    token,
                    user_id,
                    created.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    expires.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                ),
            )
        return token

    def delete_session(self, token: str) -> None:
        with self.conn:
            self._execute("DELETE FROM sessions WHERE token = ?", (token,))

    def purge_expired_sessions(self, now: Optional[datetime] = None) -> int:
        reference = now or _now()
        rows = self._execute("SELECT token, expires_at FROM sessions").fetchall()
        expired_tokens = [row["token"] for row in rows if _parse_iso(row["expires_at"]) <= reference]
        if not expired_tokens:
            return 0

        placeholders = ",".join("?" for _ in expired_tokens)
        with self.conn:
            self._execute(f"DELETE FROM sessions WHERE token IN ({placeholders})", expired_tokens)
        return len(expired_tokens)

    def get_user_by_session(self, token: str) -> Optional[User]:
        row = self._execute(
            """
            SELECT u.*, s.expires_at AS session_expires_at
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token = ?
            """,
            (token,),
        ).fetchone()
        if row is None:
            return None

        if _parse_iso(row["session_expires_at"]) <= _now():
            self.delete_session(token)
            return None

        payload = dict(row)
        payload.pop("session_expires_at", None)
        return User(**payload)

    def create_oauth_state(self, redirect_to: str = "/") -> str:
        state = secrets.token_urlsafe(24)
        created = _now()
        expires = created + timedelta(minutes=10)
        with self.conn:
            self._execute(
                "INSERT INTO oauth_states(state, redirect_to, created_at, expires_at) VALUES (?, ?, ?, ?)",
                (
                    state,
                    redirect_to,
                    created.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    expires.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                ),
            )
        return state

    def consume_oauth_state(self, state: str) -> Optional[str]:
        row = self._execute("SELECT redirect_to, expires_at FROM oauth_states WHERE state = ?", (state,)).fetchone()
        if row is None:
            return None

        with self.conn:
            self._execute("DELETE FROM oauth_states WHERE state = ?", (state,))

        if _parse_iso(row["expires_at"]) <= _now():
            return None
        return row["redirect_to"]

    @staticmethod
    def _normalize_phone(phone_number: str) -> str:
        digits = "".join(ch for ch in phone_number if ch.isdigit())
        if not digits:
            raise ValueError("Invalid phone number")
        if len(digits) == 10:
            digits = "1" + digits
        return "+" + digits

    def create_phone_verification_challenge(
        self,
        user_id: str,
        phone_number: str,
        provider: str,
        provider_sid: Optional[str] = None,
        otp_code: Optional[str] = None,
        ttl_minutes: int = 10,
    ) -> str:
        challenge_id = self._id("otp")
        normalized = self._normalize_phone(phone_number)
        created = _now()
        expires = created + timedelta(minutes=ttl_minutes)
        with self.conn:
            # Expire previous open challenges for this user+phone
            self._execute(
                """
                UPDATE phone_verification_challenges
                SET status = 'EXPIRED'
                WHERE user_id = ? AND phone_number = ? AND status = 'PENDING'
                """,
                (user_id, normalized),
            )
            self._execute(
                """
                INSERT INTO phone_verification_challenges(
                    id, user_id, phone_number, provider, provider_sid, otp_code,
                    status, created_at, expires_at, verified_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, NULL, 0)
                """,
                (
                    challenge_id,
                    user_id,
                    normalized,
                    provider,
                    provider_sid,
                    otp_code,
                    created.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    expires.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                ),
            )
        return challenge_id

    def get_phone_verification_challenge(self, challenge_id: str) -> Optional[Dict[str, Any]]:
        row = self._execute(
            "SELECT * FROM phone_verification_challenges WHERE id = ?",
            (challenge_id,),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def mark_phone_challenge_attempt(self, challenge_id: str) -> None:
        with self.conn:
            self._execute(
                "UPDATE phone_verification_challenges SET attempts = attempts + 1 WHERE id = ?",
                (challenge_id,),
            )

    def mark_phone_challenge_verified(self, challenge_id: str) -> None:
        with self.conn:
            self._execute(
                """
                UPDATE phone_verification_challenges
                SET status = 'VERIFIED', verified_at = ?
                WHERE id = ?
                """,
                (_now_iso(), challenge_id),
            )

    def link_phone(self, user_id: str, phone_number: str) -> str:
        normalized = self._normalize_phone(phone_number)
        with self.conn:
            self._execute(
                """
                INSERT INTO user_phones(id, user_id, phone_number, verified_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(phone_number) DO UPDATE
                SET user_id = excluded.user_id, verified_at = excluded.verified_at
                """,
                (self._id("ph"), user_id, normalized, _now_iso()),
            )
        return normalized

    def get_user_by_phone(self, phone_number: str) -> Optional[User]:
        normalized = self._normalize_phone(phone_number)
        row = self._execute(
            """
            SELECT u.*
            FROM users u
            JOIN user_phones p ON p.user_id = u.id
            WHERE p.phone_number = ?
            """,
            (normalized,),
        ).fetchone()
        if row is None:
            return None
        return User(**dict(row))

    def get_or_create_ai_user(self, model_id: str) -> User:
        email = f"model:{model_id}@local"
        existing = self.get_user_by_email(email)
        if existing:
            return existing
        return self.create_user(display_name=model_id, email=email, account_type="AI")

    def credit_user_chips(
        self,
        user_id: str,
        chips_delta: int,
        event_type: str,
        cycle_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = _now_iso()
        with self.conn:
            if chips_delta != 0:
                self._execute(
                    "UPDATE users SET current_chips = current_chips + ? WHERE id = ?",
                    (chips_delta, user_id),
                )
            self._execute(
                """
                INSERT INTO chip_ledger(id, user_id, cycle_id, event_type, chips_delta, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self._id("led"),
                    user_id,
                    cycle_id,
                    event_type,
                    chips_delta,
                    json.dumps(metadata or {}),
                    now,
                ),
            )

    def apply_daily_faucet(self, as_of_date: date | str | None = None) -> Dict[str, int]:
        as_of = self._parse_date(as_of_date) if as_of_date else date.today()
        rows = self._execute("SELECT id, last_daily_credit_date FROM users").fetchall()
        credited: Dict[str, int] = {}

        with self.conn:
            for row in rows:
                user_id = row["id"]
                last_credit = date.fromisoformat(row["last_daily_credit_date"])
                missed_days = (as_of - last_credit).days
                if missed_days <= 0:
                    continue

                chips = missed_days * DAILY_CHIPS
                credited[user_id] = chips

                self._execute(
                    """
                    UPDATE users
                    SET current_chips = current_chips + ?, last_daily_credit_date = ?
                    WHERE id = ?
                    """,
                    (chips, as_of.isoformat(), user_id),
                )
                self._execute(
                    """
                    INSERT INTO chip_ledger(id, user_id, cycle_id, event_type, chips_delta, metadata, created_at)
                    VALUES (?, ?, NULL, 'daily_faucet', ?, ?, ?)
                    """,
                    (
                        self._id("led"),
                        user_id,
                        chips,
                        json.dumps({"missed_days": missed_days}),
                        _now_iso(),
                    ),
                )

        return credited

    def create_cycle(self, cycle_date: date | str | None = None) -> Cycle:
        cycle_id = self._id("cyc")
        d = self._parse_date(cycle_date) if cycle_date else date.today()
        now = _now_iso()
        with self.conn:
            self._execute(
                """
                INSERT INTO cycles(id, cycle_date, status, opened_at, closed_at)
                VALUES (?, ?, 'OPEN', ?, NULL)
                """,
                (cycle_id, d.isoformat(), now),
            )
        return self.get_cycle(cycle_id)

    def get_cycle(self, cycle_id: str) -> Cycle:
        row = self._execute("SELECT * FROM cycles WHERE id = ?", (cycle_id,)).fetchone()
        if row is None:
            raise KeyError(f"Cycle not found: {cycle_id}")
        return Cycle(**dict(row))

    def list_cycles(self, limit: int = 100) -> List[Cycle]:
        rows = self._execute(
            "SELECT * FROM cycles ORDER BY opened_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [Cycle(**dict(row)) for row in rows]

    def get_open_cycle(self) -> Optional[Cycle]:
        row = self._execute(
            "SELECT * FROM cycles WHERE status = 'OPEN' ORDER BY opened_at DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return Cycle(**dict(row))

    def create_candidate(
        self,
        cycle_id: str,
        submitted_by_user_id: str,
        url: str,
        title: str = "",
    ) -> CandidateLink:
        candidate_id = self._id("lnk")
        canonical = canonicalize_url(url)
        domain = extract_domain(canonical)
        now = _now_iso()

        with self.conn:
            try:
                self._execute(
                    """
                    INSERT INTO candidate_links(
                        id, cycle_id, submitted_by_user_id, original_url, canonical_url, domain, title, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate_id,
                        cycle_id,
                        submitted_by_user_id,
                        url,
                        canonical,
                        domain,
                        title,
                        now,
                    ),
                )
            except Exception:
                row = self._execute(
                    """
                    SELECT * FROM candidate_links
                    WHERE cycle_id = ? AND canonical_url = ?
                    """,
                    (cycle_id, canonical),
                ).fetchone()
                if row is None:
                    raise
                return CandidateLink(**dict(row))

        return self.get_candidate(candidate_id)

    def get_candidate(self, candidate_id: str) -> CandidateLink:
        row = self._execute("SELECT * FROM candidate_links WHERE id = ?", (candidate_id,)).fetchone()
        if row is None:
            raise KeyError(f"Candidate not found: {candidate_id}")
        return CandidateLink(**dict(row))

    def list_candidates(self, cycle_id: str) -> List[CandidateLink]:
        rows = self._execute(
            "SELECT * FROM candidate_links WHERE cycle_id = ? ORDER BY created_at ASC",
            (cycle_id,),
        ).fetchall()
        return [CandidateLink(**dict(row)) for row in rows]

    def set_ranked_picks(self, cycle_id: str, user_id: str, candidate_ids: Iterable[str]) -> List[Pick]:
        ids = list(candidate_ids)
        if len(ids) > MAX_PICKS_PER_CYCLE:
            raise ValueError(f"You can only pick {MAX_PICKS_PER_CYCLE} links per cycle")
        if len(set(ids)) != len(ids):
            raise ValueError("Picks must be unique")

        if ids:
            placeholders = ",".join("?" for _ in ids)
            params = [cycle_id, *ids]
            row = self._execute(
                f"SELECT COUNT(*) AS c FROM candidate_links WHERE cycle_id = ? AND id IN ({placeholders})",
                params,
            ).fetchone()
            if row["c"] != len(ids):
                raise ValueError("Some picks do not belong to this cycle")

        with self.conn:
            self._execute("DELETE FROM picks WHERE cycle_id = ? AND user_id = ?", (cycle_id, user_id))
            now = _now_iso()
            for idx, candidate_id in enumerate(ids, start=1):
                self._execute(
                    """
                    INSERT INTO picks(id, cycle_id, user_id, candidate_id, rank, picked_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (self._id("pk"), cycle_id, user_id, candidate_id, idx, now),
                )

        return self.list_user_picks(cycle_id, user_id)

    def list_user_picks(self, cycle_id: str, user_id: str) -> List[Pick]:
        rows = self._execute(
            """
            SELECT * FROM picks
            WHERE cycle_id = ? AND user_id = ?
            ORDER BY rank ASC
            """,
            (cycle_id, user_id),
        ).fetchall()
        return [Pick(**dict(row)) for row in rows]

    def list_picks(self, cycle_id: str) -> List[Pick]:
        rows = self._execute(
            "SELECT * FROM picks WHERE cycle_id = ? ORDER BY picked_at ASC, rank ASC",
            (cycle_id,),
        ).fetchall()
        return [Pick(**dict(row)) for row in rows]

    def save_cycle_results(self, cycle_id: str, winner_candidate_ids: Iterable[str]) -> None:
        winner_set = set(winner_candidate_ids)
        candidate_rows = self._execute(
            "SELECT id FROM candidate_links WHERE cycle_id = ?",
            (cycle_id,),
        ).fetchall()

        with self.conn:
            self._execute("DELETE FROM cycle_results WHERE cycle_id = ?", (cycle_id,))
            for row in candidate_rows:
                candidate_id = row["id"]
                self._execute(
                    """
                    INSERT INTO cycle_results(cycle_id, candidate_id, is_winner)
                    VALUES (?, ?, ?)
                    """,
                    (cycle_id, candidate_id, 1 if candidate_id in winner_set else 0),
                )
            self._execute(
                "UPDATE cycles SET status = 'SETTLED', closed_at = ? WHERE id = ?",
                (_now_iso(), cycle_id),
            )

    def list_winner_candidate_ids(self, cycle_id: str) -> List[str]:
        rows = self._execute(
            """
            SELECT candidate_id
            FROM cycle_results
            WHERE cycle_id = ? AND is_winner = 1
            """,
            (cycle_id,),
        ).fetchall()
        return [row["candidate_id"] for row in rows]

    def upsert_model_prediction(
        self,
        cycle_id: str,
        model_user_id: str,
        candidate_id: str,
        probability: float,
        explanation: str,
    ) -> None:
        with self.conn:
            self._execute(
                """
                INSERT INTO model_predictions(cycle_id, model_user_id, candidate_id, probability, explanation, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(cycle_id, model_user_id, candidate_id)
                DO UPDATE SET probability = excluded.probability, explanation = excluded.explanation, created_at = excluded.created_at
                """,
                (cycle_id, model_user_id, candidate_id, probability, explanation, _now_iso()),
            )

    def list_model_predictions(
        self,
        cycle_id: str,
        model_user_id: Optional[str] = None,
    ) -> List[ModelPrediction]:
        if model_user_id:
            rows = self._execute(
                """
                SELECT cycle_id, model_user_id, candidate_id, probability, explanation
                FROM model_predictions
                WHERE cycle_id = ? AND model_user_id = ?
                ORDER BY probability DESC
                """,
                (cycle_id, model_user_id),
            ).fetchall()
        else:
            rows = self._execute(
                """
                SELECT cycle_id, model_user_id, candidate_id, probability, explanation
                FROM model_predictions
                WHERE cycle_id = ?
                ORDER BY probability DESC
                """,
                (cycle_id,),
            ).fetchall()

        return [ModelPrediction(**dict(row)) for row in rows]

    def list_leaderboard(self, limit: int = 100, account_type: Optional[str] = None) -> List[Dict[str, Any]]:
        if account_type:
            rows = self._execute(
                """
                SELECT id, display_name, account_type, current_chips
                FROM users
                WHERE account_type = ?
                ORDER BY current_chips DESC, created_at ASC
                LIMIT ?
                """,
                (account_type, limit),
            ).fetchall()
        else:
            rows = self._execute(
                """
                SELECT id, display_name, account_type, current_chips
                FROM users
                ORDER BY current_chips DESC, created_at ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        output: List[Dict[str, Any]] = []
        for idx, row in enumerate(rows, start=1):
            item = dict(row)
            item["rank"] = idx
            output.append(item)
        return output

    def list_curation_leaderboard(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT u.id, u.display_name, u.account_type, COALESCE(SUM(r.reward_chips), 0) AS curation_chips
            FROM users u
            LEFT JOIN curation_rewards r ON r.user_id = u.id
            GROUP BY u.id, u.display_name, u.account_type
            ORDER BY curation_chips DESC, u.created_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        output = []
        for idx, row in enumerate(rows, start=1):
            item = dict(row)
            item["rank"] = idx
            output.append(item)
        return output

    def record_click(
        self,
        candidate_id: str,
        fingerprint_source: str,
        clicked_by_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        candidate = self._execute(
            "SELECT cycle_id, submitted_by_user_id FROM candidate_links WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        if candidate is None:
            raise KeyError(f"Candidate not found: {candidate_id}")

        if clicked_by_user_id and clicked_by_user_id == candidate["submitted_by_user_id"]:
            return {"counted": False, "reason": "self_click"}

        fingerprint_hash = sha256(fingerprint_source.encode("utf-8")).hexdigest()

        try:
            with self.conn:
                self._execute(
                    """
                    INSERT INTO click_events(id, cycle_id, candidate_id, clicked_by_user_id, fingerprint_hash, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self._id("clk"),
                        candidate["cycle_id"],
                        candidate_id,
                        clicked_by_user_id,
                        fingerprint_hash,
                        _now_iso(),
                    ),
                )
            return {"counted": True, "reason": "unique"}
        except Exception:
            return {"counted": False, "reason": "duplicate"}

    def has_curation_rewards(self, cycle_id: str) -> bool:
        row = self._execute(
            "SELECT COUNT(*) AS c FROM curation_rewards WHERE cycle_id = ?",
            (cycle_id,),
        ).fetchone()
        return bool(row and row["c"] > 0)

    def compute_curation_reward_rows(self, cycle_id: str) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT c.submitted_by_user_id AS user_id, COUNT(e.id) AS unique_clicks
            FROM candidate_links c
            LEFT JOIN click_events e ON e.candidate_id = c.id
            WHERE c.cycle_id = ?
            GROUP BY c.submitted_by_user_id
            HAVING COUNT(e.id) > 0
            ORDER BY unique_clicks DESC, user_id ASC
            """,
            (cycle_id,),
        ).fetchall()

        sorted_rows = [dict(row) for row in rows]
        if not sorted_rows:
            return []

        reward_rows: List[Dict[str, Any]] = []
        idx = 0
        next_rank = 1
        top_rank_positions = sorted(CURATION_RANK_REWARDS.keys())
        max_rank = max(top_rank_positions)

        while idx < len(sorted_rows) and next_rank <= max_rank:
            click_count = sorted_rows[idx]["unique_clicks"]
            tie_group = []
            while idx < len(sorted_rows) and sorted_rows[idx]["unique_clicks"] == click_count:
                tie_group.append(sorted_rows[idx])
                idx += 1

            start_rank = next_rank
            end_rank = min(start_rank + len(tie_group) - 1, max_rank)
            eligible_positions = [
                rank for rank in range(start_rank, end_rank + 1) if rank in CURATION_RANK_REWARDS
            ]

            if not eligible_positions:
                break

            total_pool = sum(CURATION_RANK_REWARDS[rank] for rank in eligible_positions)
            split_reward = round(total_pool / len(tie_group))
            for row in tie_group:
                reward_rows.append(
                    {
                        "user_id": row["user_id"],
                        "rank": start_rank,
                        "unique_clicks": click_count,
                        "reward_chips": split_reward,
                    }
                )

            next_rank += len(tie_group)

        return reward_rows

    def apply_curation_rewards(self, cycle_id: str) -> List[Dict[str, Any]]:
        if self.has_curation_rewards(cycle_id):
            return []

        reward_rows = self.compute_curation_reward_rows(cycle_id)
        if not reward_rows:
            return []

        with self.conn:
            for row in reward_rows:
                self._execute(
                    """
                    INSERT INTO curation_rewards(cycle_id, user_id, rank, unique_clicks, reward_chips, awarded_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cycle_id,
                        row["user_id"],
                        row["rank"],
                        row["unique_clicks"],
                        row["reward_chips"],
                        _now_iso(),
                    ),
                )
                self.credit_user_chips(
                    user_id=row["user_id"],
                    chips_delta=row["reward_chips"],
                    event_type="curation_reward",
                    cycle_id=cycle_id,
                    metadata={"rank": row["rank"], "unique_clicks": row["unique_clicks"]},
                )

        return reward_rows

    def upsert_archive_link(self, post_date: str, url: str, title: str, source_post_url: str) -> None:
        canonical = canonicalize_url(url)
        domain = extract_domain(canonical)
        with self.conn:
            self._execute(
                """
                INSERT INTO archive_links(id, post_date, url, canonical_url, domain, title, source_post_url, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(post_date, canonical_url)
                DO UPDATE SET title = excluded.title, source_post_url = excluded.source_post_url
                """,
                (
                    self._id("arc"),
                    post_date,
                    url,
                    canonical,
                    domain,
                    title,
                    source_post_url,
                    _now_iso(),
                ),
            )

    def search_archive_links(self, query: str = "", domain: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        where_parts = []
        params: List[Any] = []

        if query:
            where_parts.append("(title LIKE ? OR url LIKE ?)")
            q = f"%{query}%"
            params.extend([q, q])
        if domain:
            where_parts.append("domain = ?")
            params.append(domain)

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        params.append(limit)

        rows = self._execute(
            f"""
            SELECT post_date, url, canonical_url, domain, title, source_post_url
            FROM archive_links
            {where_sql}
            ORDER BY post_date DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def source_post_seen(self, source_post_url: str) -> bool:
        row = self._execute(
            "SELECT 1 AS one FROM source_posts WHERE source_post_url = ?",
            (source_post_url,),
        ).fetchone()
        return row is not None

    def mark_source_post_processed(
        self,
        source_post_url: str,
        title: str,
        published_at: str,
        extracted_links: List[str],
    ) -> None:
        with self.conn:
            self._execute(
                """
                INSERT INTO source_posts(
                    id, source_post_url, title, published_at, extracted_links_json, processed_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_post_url)
                DO UPDATE SET
                    title = excluded.title,
                    published_at = excluded.published_at,
                    extracted_links_json = excluded.extracted_links_json,
                    processed_at = excluded.processed_at
                """,
                (
                    self._id("src"),
                    source_post_url,
                    title,
                    published_at,
                    json.dumps(extracted_links),
                    _now_iso(),
                    _now_iso(),
                ),
            )

    def list_source_posts(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT source_post_url, title, published_at, extracted_links_json, processed_at
            FROM source_posts
            ORDER BY published_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        payload = []
        for row in rows:
            item = dict(row)
            item["extracted_links"] = json.loads(item.pop("extracted_links_json", "[]"))
            payload.append(item)
        return payload

    def claim_job_run(self, job_name: str, run_key: str, details: Optional[Dict[str, Any]] = None) -> bool:
        try:
            with self.conn:
                self._execute(
                    """
                    INSERT INTO job_runs(id, job_name, run_key, status, details_json, created_at)
                    VALUES (?, ?, ?, 'DONE', ?, ?)
                    """,
                    (self._id("job"), job_name, run_key, json.dumps(details or {}), _now_iso()),
                )
            return True
        except Exception:
            return False

    def get_candidate_url(self, candidate_id: str) -> str:
        row = self._execute(
            "SELECT original_url FROM candidate_links WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"Candidate not found: {candidate_id}")
        return row["original_url"]

    def get_candidate_cycle_id(self, candidate_id: str) -> str:
        row = self._execute(
            "SELECT cycle_id FROM candidate_links WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"Candidate not found: {candidate_id}")
        return row["cycle_id"]

    def list_cycle_candidates_with_submitter(self, cycle_id: str) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT c.id, c.original_url, c.canonical_url, c.domain, c.title, c.submitted_by_user_id,
                   u.display_name AS submitted_by_name
            FROM candidate_links c
            JOIN users u ON u.id = c.submitted_by_user_id
            WHERE c.cycle_id = ?
            ORDER BY c.created_at ASC
            """,
            (cycle_id,),
        ).fetchall()
        return [dict(row) for row in rows]

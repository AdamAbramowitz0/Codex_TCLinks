"""Google OAuth helper."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Dict
from urllib.parse import urlencode
from urllib.request import Request, urlopen

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"


@dataclass
class GoogleOAuthConfig:
    client_id: str
    client_secret: str
    redirect_uri: str


class GoogleOAuthClient:
    def __init__(self, config: GoogleOAuthConfig) -> None:
        self.config = config

    @staticmethod
    def from_env() -> "GoogleOAuthClient | None":
        client_id = os.getenv("GOOGLE_CLIENT_ID", "")
        client_secret = os.getenv("GOOGLE_CLIENT_SECRET", "")
        redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "")
        if not client_id or not client_secret or not redirect_uri:
            return None
        return GoogleOAuthClient(
            GoogleOAuthConfig(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
            )
        )

    def build_authorize_url(self, state: str) -> str:
        params = {
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "state": state,
            "prompt": "consent",
            "access_type": "online",
        }
        return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"

    def exchange_code(self, code: str) -> Dict[str, str]:
        body = urlencode(
            {
                "code": code,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "redirect_uri": self.config.redirect_uri,
                "grant_type": "authorization_code",
            }
        ).encode("utf-8")
        req = Request(
            GOOGLE_TOKEN_URL,
            data=body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urlopen(req, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))

        if "access_token" not in payload:
            raise RuntimeError("Google token exchange failed")
        return payload

    def fetch_userinfo(self, access_token: str) -> Dict[str, str]:
        req = Request(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        with urlopen(req, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))

        required = ["sub", "email"]
        missing = [key for key in required if key not in payload]
        if missing:
            raise RuntimeError(f"Google userinfo missing keys: {', '.join(missing)}")
        return payload

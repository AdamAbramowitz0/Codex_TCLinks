"""Twilio Verify + Messaging integration."""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Dict
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass
class TwilioConfig:
    account_sid: str
    auth_token: str
    verify_service_sid: str
    messaging_service_sid: str


class TwilioClient:
    def __init__(self, config: TwilioConfig) -> None:
        self.config = config

    @staticmethod
    def from_env() -> "TwilioClient | None":
        account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        verify_service_sid = os.getenv("TWILIO_VERIFY_SERVICE_SID", "")
        messaging_service_sid = os.getenv("TWILIO_MESSAGING_SERVICE_SID", "")

        if not account_sid or not auth_token or not verify_service_sid:
            return None

        return TwilioClient(
            TwilioConfig(
                account_sid=account_sid,
                auth_token=auth_token,
                verify_service_sid=verify_service_sid,
                messaging_service_sid=messaging_service_sid,
            )
        )

    def _auth_header(self) -> str:
        creds = f"{self.config.account_sid}:{self.config.auth_token}".encode("utf-8")
        return "Basic " + base64.b64encode(creds).decode("ascii")

    def _post_form(self, url: str, payload: Dict[str, str]) -> Dict[str, object]:
        body = urlencode(payload).encode("utf-8")
        req = Request(
            url,
            data=body,
            method="POST",
            headers={
                "Authorization": self._auth_header(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        with urlopen(req, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))

    def start_verification(self, phone_number: str) -> Dict[str, object]:
        url = (
            f"https://verify.twilio.com/v2/Services/{self.config.verify_service_sid}/Verifications"
        )
        return self._post_form(url, {"To": phone_number, "Channel": "sms"})

    def check_verification(self, phone_number: str, code: str) -> Dict[str, object]:
        url = (
            f"https://verify.twilio.com/v2/Services/{self.config.verify_service_sid}/VerificationCheck"
        )
        return self._post_form(url, {"To": phone_number, "Code": code})

    def send_sms(self, phone_number: str, body: str) -> Dict[str, object]:
        if not self.config.messaging_service_sid:
            raise RuntimeError("TWILIO_MESSAGING_SERVICE_SID is required to send direct SMS")

        url = f"https://api.twilio.com/2010-04-01/Accounts/{self.config.account_sid}/Messages.json"
        return self._post_form(
            url,
            {
                "To": phone_number,
                "Body": body,
                "MessagingServiceSid": self.config.messaging_service_sid,
            },
        )

"""Vercel serverless entrypoint."""

from __future__ import annotations

from tc_market.runtime import get_handler_class

# Vercel Python runtime expects a module-level `handler` for HTTP routing.
handler = get_handler_class()

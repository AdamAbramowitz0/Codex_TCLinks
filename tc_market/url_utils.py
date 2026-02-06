"""URL normalization helpers."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

TRACKING_PARAM_PREFIXES = ("utm_",)
TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "ref_src",
}


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    query_items = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        key_lower = key.lower()
        if key_lower in TRACKING_PARAMS:
            continue
        if any(key_lower.startswith(prefix) for prefix in TRACKING_PARAM_PREFIXES):
            continue
        query_items.append((key, value))

    query = urlencode(sorted(query_items))
    path = parsed.path or "/"

    canonical = urlunparse((scheme, netloc, path.rstrip("/") or "/", "", query, ""))
    return canonical


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host

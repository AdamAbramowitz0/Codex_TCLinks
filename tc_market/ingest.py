"""Marginal Revolution assorted-links ingestion."""

from __future__ import annotations

import json
import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from tc_market.market import MarketService
from tc_market.storage import Storage
from tc_market.url_utils import canonicalize_url

ASSORTED_LINKS_PATTERN = re.compile(r"assorted links", re.IGNORECASE)
HREF_PATTERN = re.compile(r"href=[\"'](https?://[^\"'#]+)", re.IGNORECASE)


@dataclass
class AssortedLinksPost:
    title: str
    url: str
    published_at: str
    links: List[str]


class MarginalRevolutionIngestor:
    def __init__(self, feed_url: str | None = None) -> None:
        self.feed_url = feed_url or os.getenv("MR_FEED_URL", "https://marginalrevolution.com/feed")

    def _fetch_text(self, url: str) -> str:
        req = Request(url, headers={"User-Agent": "tc-links-market/1.0"})
        with urlopen(req, timeout=20) as response:
            return response.read().decode("utf-8", errors="replace")

    @staticmethod
    def _normalize_published(value: str) -> str:
        value = value.strip()
        if not value:
            return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        # RFC822, RSS pubDate
        for fmt in [
            "%a, %d %b %Y %H:%M:%S %z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
        ]:
            try:
                dt = datetime.strptime(value, fmt)
                if dt.tzinfo is None:
                    return dt.isoformat() + "Z"
                return dt.astimezone().replace(microsecond=0).isoformat().replace("+00:00", "Z")
            except ValueError:
                continue

        return value

    @staticmethod
    def _extract_post_entries(feed_xml: str) -> List[Dict[str, str]]:
        root = ET.fromstring(feed_xml)
        entries: List[Dict[str, str]] = []

        # RSS format
        for item in root.findall("./channel/item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            if title and link:
                entries.append({"title": title, "link": link, "published": pub_date})

        # Atom fallback
        if not entries:
            namespace = "{http://www.w3.org/2005/Atom}"
            for item in root.findall(f"{namespace}entry"):
                title = (item.findtext(f"{namespace}title") or "").strip()
                link_node = item.find(f"{namespace}link")
                link = ""
                if link_node is not None:
                    link = (link_node.attrib.get("href") or "").strip()
                pub_date = (
                    item.findtext(f"{namespace}published")
                    or item.findtext(f"{namespace}updated")
                    or ""
                ).strip()
                if title and link:
                    entries.append({"title": title, "link": link, "published": pub_date})

        return entries

    @staticmethod
    def _extract_outbound_links(post_url: str, html: str) -> List[str]:
        seen = set()
        links: List[str] = []

        post_host = urlparse(post_url).netloc.lower()
        for match in HREF_PATTERN.findall(html):
            try:
                canonical = canonicalize_url(match)
            except Exception:
                continue

            host = urlparse(canonical).netloc.lower()
            if not host or host == post_host:
                continue
            if "marginalrevolution.com" in host:
                continue
            if canonical in seen:
                continue
            seen.add(canonical)
            links.append(canonical)

        return links

    def fetch_recent_assorted_posts(self, limit: int = 10) -> List[AssortedLinksPost]:
        feed_xml = self._fetch_text(self.feed_url)
        entries = self._extract_post_entries(feed_xml)

        filtered = [entry for entry in entries if ASSORTED_LINKS_PATTERN.search(entry["title"])][:limit]

        posts: List[AssortedLinksPost] = []
        for entry in filtered:
            html = self._fetch_text(entry["link"])
            links = self._extract_outbound_links(entry["link"], html)
            posts.append(
                AssortedLinksPost(
                    title=entry["title"],
                    url=entry["link"],
                    published_at=self._normalize_published(entry["published"]),
                    links=links,
                )
            )

        posts.sort(key=lambda post: post.published_at)
        return posts

    def sync(self, storage: Storage, market: MarketService, limit: int = 10) -> Dict[str, object]:
        posts = self.fetch_recent_assorted_posts(limit=limit)
        if not posts:
            if storage.get_open_cycle() is None:
                storage.create_cycle()
            return {"processed": 0, "settlements": []}

        unseen_posts = [post for post in posts if not storage.source_post_seen(post.url)]
        if not unseen_posts:
            if storage.get_open_cycle() is None:
                # If bootstrapped from historical data only, ensure an active market exists.
                latest = posts[-1].published_at[:10]
                storage.create_cycle(latest)
            return {"processed": 0, "settlements": []}

        current_open_cycle = storage.get_open_cycle()
        settlements = []

        bootstrap_mode = current_open_cycle is None
        for post in unseen_posts:
            post_date = post.published_at[:10]

            for link in post.links:
                storage.upsert_archive_link(
                    post_date=post_date,
                    url=link,
                    title=post.title,
                    source_post_url=post.url,
                )

            if not bootstrap_mode and current_open_cycle is not None:
                settlement = market.settle_cycle(current_open_cycle.id, post.links)
                settlements.append(
                    {
                        "cycle_id": current_open_cycle.id,
                        "post_url": post.url,
                        "winner_count": settlement["winner_count"],
                    }
                )
                current_open_cycle = storage.create_cycle(post_date)

            storage.mark_source_post_processed(
                source_post_url=post.url,
                title=post.title,
                published_at=post.published_at,
                extracted_links=post.links,
            )

        if bootstrap_mode:
            latest_date = unseen_posts[-1].published_at[:10]
            storage.create_cycle(latest_date)

        return {
            "processed": len(unseen_posts),
            "settlements": settlements,
            "bootstrap_mode": bootstrap_mode,
        }

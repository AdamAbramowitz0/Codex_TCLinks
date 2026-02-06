from __future__ import annotations

import unittest

from tc_market.ingest import MarginalRevolutionIngestor


class FakeIngestor(MarginalRevolutionIngestor):
    def __init__(self, feed_url: str, responses: dict[str, str]) -> None:
        super().__init__(feed_url=feed_url)
        self.responses = responses

    def _fetch_text(self, url: str) -> str:
        return self.responses[url]


class IngestTests(unittest.TestCase):
    def test_feed_pagination_and_unlimited_limit(self) -> None:
        feed1_url = "https://marginalrevolution.com/feed"
        feed2_url = "https://marginalrevolution.com/feed?paged=2"
        post1_url = "https://marginalrevolution.com/assorted-links-1"
        post2_url = "https://marginalrevolution.com/assorted-links-2"

        responses = {
            feed1_url: """
            <rss version="2.0"><channel>
              <item><title>Assorted Links A</title><link>https://marginalrevolution.com/assorted-links-1</link><pubDate>Fri, 06 Feb 2026 12:00:00 +0000</pubDate></item>
              <item><title>Not Assorted</title><link>https://marginalrevolution.com/other</link><pubDate>Fri, 06 Feb 2026 11:00:00 +0000</pubDate></item>
            </channel></rss>
            """,
            feed2_url: """
            <rss version="2.0"><channel>
              <item><title>Assorted Links B</title><link>https://marginalrevolution.com/assorted-links-2</link><pubDate>Thu, 05 Feb 2026 12:00:00 +0000</pubDate></item>
            </channel></rss>
            """,
            post1_url: '<a href="https://example.com/a">a</a>',
            post2_url: '<a href="https://example.com/b">b</a>',
        }

        ingestor = FakeIngestor(feed_url=feed1_url, responses=responses)
        posts = ingestor.fetch_recent_assorted_posts(limit=0, max_feed_pages=2)

        self.assertEqual(len(posts), 2)
        self.assertEqual(posts[0].url, post2_url)
        self.assertEqual(posts[1].url, post1_url)

    def test_feed_url_for_page_preserves_existing_query_params(self) -> None:
        ingestor = MarginalRevolutionIngestor(feed_url="https://marginalrevolution.com/feed?foo=bar")
        self.assertEqual(
            ingestor._feed_url_for_page(3),
            "https://marginalrevolution.com/feed?foo=bar&paged=3",
        )


if __name__ == "__main__":
    unittest.main()

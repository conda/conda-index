import re
import time
import unittest

from conda_index.index import rss

_DAY = 24 * 60 * 60


class rssTest(unittest.TestCase):
    def setUp(self) -> None:
        rss.time.time = lambda: 1656741161.774336
        self.channeldata = {
            "channeldata_version": 1,
            "packages": {
                "example1": {
                    "description": "Long description.",
                    "dev_url": None,
                    "doc_source_url": None,
                    "doc_url": "https://anaconda.org/anaconda/example1",
                    "home": "http://example1.org/",
                    "license": "LGPL",
                    "source_git_url": None,
                    "source_url": "http://example1.org/package_sources.zip/download",
                    "subdirs": ["win-32", "win-64"],
                    "summary": "Short description",
                    "timestamp": rss.time.time() - 1 * _DAY,
                    "version": "123",
                },
                "example2": {
                    "description": "Long description.",
                    "dev_url": None,
                    "doc_source_url": None,
                    "doc_url": "https://anaconda.org/anaconda/example2",
                    "home": "http://www.example2.com/",
                    "license": "LGPL",
                    "source_git_url": None,
                    "source_url": "http://example2.com/src.tar.gz",
                    "subdirs": ["win-32", "osx-64", "osx-64", "linux-64"],
                    "summary": "Short description",
                    "timestamp": rss.time.time() - 3 * _DAY,
                    "version": "1.2.3.4",
                },
            },
            "packages.conda": {
                "conda.example1": {
                    "description": "Long description.",
                    "dev_url": None,
                    "doc_source_url": None,
                    "doc_url": "https://anaconda.org/anaconda/example1",
                    "home": "http://example1.org/",
                    "license": "LGPL",
                    "source_git_url": None,
                    "source_url": "http://example1.org/package_sources.zip/download",
                    "subdirs": ["win-32", "win-64"],
                    "summary": "Short description",
                    "timestamp": rss.time.time() - 14 * _DAY,
                    "version": "123",
                },
            },
        }
        self.maxDiff = None

    def tearDown(self) -> None:
        rss.time.time = time.time

    def testGetRecentPackages(self):
        actual = rss.get_recent_packages(self.channeldata, 2)
        expected = [("example1", self.channeldata["packages"]["example1"])]
        self.assertDictEqual(actual[0][1], expected[0][1])

    def testGetChannel(self):
        packages = rss.get_recent_packages(self.channeldata, 2)
        actual = rss._get_channel("example", packages)
        expected = {
            "title": "anaconda.org/example",
            "link": "https://conda.anaconda.org/example",
            "description": "The most recent 2 updates for example.",
            "pubDate": rss._iso822(time.time()),
            "lastBuildDate": rss._iso822(time.time()),
        }
        self.assertDictEqual(actual, expected)

    def testGetTitle(self):
        actual = rss._get_title("example2", "213", ["win-32", "linux-s390x"])
        expected = "example2 213 [linux-s390x, win-32]"
        self.assertEqual(actual, expected)

    def testIso822(self):
        self.assertEqual(rss._iso822(0), "Thu, 01 Jan 1970 00:00:00 GMT")
        self.assertEqual(
            rss._iso822(1656717698.601216), "Fri, 01 Jul 2022 23:21:38 GMT"
        )

    def testGetItems(self):
        packages = [
            (
                "example1",
                {
                    "description": "Long description.",
                    "dev_url": None,
                    "doc_source_url": None,
                    "doc_url": "https://anaconda.org/anaconda/example1",
                    "home": "http://example1.org/",
                    "license": "LGPL",
                    "source_git_url": None,
                    "source_url": "http://example1.org/package_sources.zip/download",
                    "subdirs": ["win-32", "win-64"],
                    "summary": "Short description",
                    "timestamp": time.time() - 1 * _DAY,
                    "version": "123",
                },
            ),
            (  # coverage for 'has no description or summary' fallback
                "nondescript",
                {
                    "dev_url": None,
                    "doc_source_url": None,
                    "doc_url": "https://anaconda.org/anaconda/example1",
                    "home": "http://example1.org/",
                    "license": "LGPL",
                    "source_git_url": None,
                    "source_url": "http://example1.org/package_sources.zip/download",
                    "subdirs": ["win-32", "win-64"],
                    "timestamp": time.time() - 1 * _DAY,
                    "version": "123",
                },
            ),
        ]
        actual = rss._get_items(packages)
        expected = [
            {
                "title": "example1 123 [win-32, win-64]",
                "description": "Long description.",
                "link": "https://anaconda.org/anaconda/example1",
                "guid": "http://example1.org/package_sources.zip/download",
                "pubDate": "Fri, 01 Jul 2022 05:52:41 GMT",
                "source": "http://example1.org/",
            }
        ]
        self.assertDictEqual(expected[0], actual[0])

    def testGetRss(self):
        actual = rss.get_rss("example", self.channeldata)
        expected = """<?xml version="1.0" ?>
<rss version="2.0">
    <channel>
        <title>anaconda.org/example</title>
        <link>https://conda.anaconda.org/example</link>
        <description>The most recent 2 updates for example.</description>
        <pubDate>Sat, 02 Jul 2022 05:52:41 GMT</pubDate>
        <lastBuildDate>Sat, 02 Jul 2022 05:52:41 GMT</lastBuildDate>
        <item>
            <title>example1 123 [win-32, win-64]</title>
            <description>Long description.</description>
            <link>https://anaconda.org/anaconda/example1</link>
            <guid>http://example1.org/package_sources.zip/download</guid>
            <pubDate>Fri, 01 Jul 2022 05:52:41 GMT</pubDate>
            <source>http://example1.org/</source>
        </item>
        <item>
            <title>example2 1.2.3.4 [linux-64, osx-64, win-32]</title>
            <description>Long description.</description>
            <link>https://anaconda.org/anaconda/example2</link>
            <guid>http://example2.com/src.tar.gz</guid>
            <pubDate>Wed, 29 Jun 2022 05:52:41 GMT</pubDate>
            <source>http://www.example2.com/</source>
        </item>
    </channel>
</rss>
"""

        def dedent(text):
            return re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        self.assertEqual(dedent(actual), dedent(expected))


if __name__ == "__main__":
    unittest.main()

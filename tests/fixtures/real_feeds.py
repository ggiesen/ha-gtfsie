"""Registry of real-world GTFS feeds, downloaded on demand.

Why these exist alongside the synthetic feeds
---------------------------------------------

The synthetic feeds in ``gtfs_feed.py`` reproduce failures we already understand.
Real feeds catch the ones we do not: agencies that omit optional columns, ship
non-UTF-8 text, use ``calendar_dates.txt`` exclusively, run trips past 26:00, or
simply publish enough rows to make a missing index matter.

Selection is deliberate rather than arbitrary. Each entry is either a feed the
maintainer works with, a feed named repeatedly in the issue tracker, or one we
depend on ourselves:

===============  =======  ===========================================
feed             country  why it is here
===============  =======  ===========================================
sncf             FR       the maintainer's own feed (TER); 23 mentions
lemet            FR       small French feed, quick to fetch
mbta             US       most-cited feed in the issue tracker
rnv              DE       cited in realtime reports -- URL DEAD, excluded
up_express       CA       ours; small, ~6 month validity
go_transit       CA       ours; large, ~6 week validity
metro_north      US       cited in feed-parsing reports
===============  =======  ===========================================

How they are used
-----------------

Never for exact assertions. A real feed's departures change every time the
agency republishes, so a test asserting "the 08:14 to Union exists" is a test
that fails on a Tuesday. These feeds are used for two things:

* **invariants** -- the feed loads, the helpers return the right *shape*, and
  nothing raises. A crash on real data is a bug regardless of the data.
* **characterisation** -- recording which real-world shapes actually occur, so
  the synthetic feeds can be checked against reality rather than against our
  assumptions about it.

They are opt-in (``--real-feeds``) and skipped by default, because CI should not
depend on third-party servers being reachable.

What the feeds actually showed (measured 2026-07-28)
----------------------------------------------------

Recorded here because it corrects assumptions this suite was built on:

* **calendar_dates.txt-only is the dominant shape, not a corner case.** Four of
  the six live feeds define service that way -- go_transit, metro_north,
  up_express and *sncf, the maintainer's own*. Two ship both. **None uses
  calendar.txt alone.** The branch where issue #164 lives is therefore the
  common path, which is worth knowing before triaging it as an edge case.
* **Times past 24:00 are universal**, 0.78%-6.47% of stop_times rows depending
  on the agency. Max hour observed: 35 (sncf), 28 (go_transit), 26 (mbta,
  metro_north). None of the midnight-handling logic is speculative.
* **stop_timezone is empty in every feed**, and parent_station in five of six --
  which is why the synthetic builder omits them.
* **Validity windows vary by an order of magnitude**: go_transit ~6 weeks,
  lemet ~4 weeks, mbta ~7 weeks, metro_north ~4 months, sncf and up_express
  ~6 months. Short windows are why a silently-expired feed (#170) is routine
  rather than exotic.
"""

from __future__ import annotations

import os
import urllib.request
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RealFeed:
    key: str
    name: str
    country: str
    url: str
    note: str = ""
    #: False when the published URL is known to be dead. The entry is kept for
    #: the record (and so nobody re-adds it), but excluded from the default set
    #: so it does not produce a permanent skip on every run.
    available: bool = True


FEEDS: dict[str, RealFeed] = {
    "sncf": RealFeed(
        "sncf",
        "SNCF TGV, Intercites et TER",
        "FR",
        "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip",
        "The maintainer's feed. Referenced throughout the issue tracker as 'TER'.",
    ),
    "lemet": RealFeed(
        "lemet",
        "LE MET' (Metz)",
        "FR",
        "https://data.lemet.fr/documents/LEMET-gtfs.zip",
        "Small French urban feed; cheap to fetch, useful as a fast French case.",
    ),
    "mbta": RealFeed(
        "mbta",
        "MBTA (Boston)",
        "US",
        "https://cdn.mbta.com/MBTA_GTFS.zip",
        "Most-cited feed in the issue tracker. Large, heavy use of optional fields.",
    ),
    "rnv": RealFeed(
        "rnv",
        "Rhein-Neckar-Verkehr",
        "DE",
        "https://rnv-dds-prod-gtfs.azurewebsites.net/latest/gtfs.zip",
        "Cited in several realtime reports. URL is DEAD as of 2026-07-28 -- the "
        "azurewebsites.net host no longer resolves (connection refused, not 404), "
        "so this is a retired endpoint rather than a transient outage. Kept for "
        "the record; set available=True once a current URL is confirmed. This was "
        "the only German feed, so replacing it restores DE coverage.",
        available=False,
    ),
    "up_express": RealFeed(
        "up_express",
        "Metrolinx UP Express",
        "CA",
        "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/UP-GTFS.zip",
        "Small rail feed, route_type 2, roughly six-month validity window.",
    ),
    "go_transit": RealFeed(
        "go_transit",
        "Metrolinx GO Transit",
        "CA",
        "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip",
        "Large regional feed with a roughly six-week validity window, so it "
        "expires often. Its silent expiry is what motivated issue #170.",
    ),
    "metro_north": RealFeed(
        "metro_north",
        "MTA Metro-North",
        "US",
        "https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip",
        "Cited in feed-parsing reports.",
    ),
}


def cache_dir() -> Path:
    """Where downloaded feeds live between runs.

    Overridable with ``GTFSIE_TEST_FEED_CACHE`` so CI can point at a persistent
    cache and avoid re-downloading tens of megabytes on every job.
    """
    env = os.environ.get("GTFSIE_TEST_FEED_CACHE")
    base = Path(env) if env else Path.home() / ".cache" / "gtfsie-test-feeds"
    base.mkdir(parents=True, exist_ok=True)
    return base


def fetch_out_of_process(feed: RealFeed, *, timeout: int = 300) -> Path:
    """Download a feed in a subprocess, returning the cached path.

    Why not just call :func:`fetch`? ``pytest-homeassistant-custom-component``
    installs pytest-socket and calls::

        pytest_socket.socket_allow_hosts(["127.0.0.1"])
        pytest_socket.disable_socket(allow_unix_socket=True)

    That is a good default -- it makes an accidental network call in a unit test
    impossible -- but it blocks these deliberate ones too. Undoing it in-process
    means restoring pytest-socket's private ``_true_connect`` (``enable_socket()``
    alone is not enough; it restores ``socket.socket`` but not the guarded
    ``connect``), and it opens a window where any other test could reach the
    network.

    A subprocess avoids both problems: the guard patches ``socket`` in the *test*
    interpreter only, so a child downloads normally while the suite keeps its
    no-network guarantee. No private APIs, no version coupling, nothing to
    restore afterwards.
    """
    import subprocess
    import sys

    target = cache_dir() / f"{feed.key}.zip"
    if target.exists() and target.stat().st_size > 0:
        return target

    # URL and destination are passed as argv, never interpolated into the
    # program text -- the registry is ours, but this keeps the habit.
    program = (
        "import sys, urllib.request, pathlib\n"
        "url, dest, ua = sys.argv[1], sys.argv[2], sys.argv[3]\n"
        "req = urllib.request.Request(url, headers={'User-Agent': ua})\n"
        "tmp = pathlib.Path(dest + '.part')\n"
        f"with urllib.request.urlopen(req, timeout={int(timeout)}) as r:\n"
        "    tmp.write_bytes(r.read())\n"
        "tmp.replace(pathlib.Path(dest))\n"
    )
    subprocess.run(
        [
            sys.executable,
            "-c",
            program,
            feed.url,
            str(target),
            "ha-gtfsie-tests/1.0 (+https://gitlab.com/ggiesen/ha-gtfsie)",
        ],
        check=True,
        capture_output=True,
        timeout=timeout + 30,
    )
    return target


def fetch(feed: RealFeed, *, timeout: int = 300, force: bool = False) -> Path:
    """Download a feed if not already cached, and return its path.

    Deliberately tolerant: these are third-party servers and a failure here is
    an environment problem, not a defect in this integration. Callers are expected to skip
    rather than fail when this raises.
    """
    target = cache_dir() / f"{feed.key}.zip"
    if target.exists() and target.stat().st_size > 0 and not force:
        return target

    tmp = target.with_suffix(".zip.part")
    request = urllib.request.Request(
        feed.url,
        headers={
            "User-Agent": "ha-gtfsie-tests/1.0 (+https://gitlab.com/ggiesen/ha-gtfsie)"
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        tmp.write_bytes(response.read())
    tmp.replace(target)
    return target


def available_feeds() -> list[str]:
    """Feed keys with a live URL, sorted.

    Dead endpoints are excluded here rather than skipped at runtime: a
    permanently-skipping test is indistinguishable from a broken one at a glance,
    and this suite otherwise reports zero skips.
    """
    return sorted(k for k, f in FEEDS.items() if f.available)

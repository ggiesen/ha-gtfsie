"""One datasource: its files, its import, and read access to the result.

Everything expensive lives here, and none of it runs on the event loop. The
import is a background task so setup returns immediately -- a first import of a
national feed is measured in hours, and a config entry that blocked on it would
hold up Home Assistant's entire startup.

Read access goes through :meth:`Datasource.async_query`, which hands a callable a
connection inside an executor. Callers pass a function rather than receiving a
connection so a SQLite object cannot escape onto the loop, where using it would
block invisibly.
"""

from __future__ import annotations

import asyncio
import logging
import random
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, TypeVar

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_change, async_track_time_interval
from homeassistant.util import dt as dt_util
from pygtfsie.db.queries import feed_service_bounds
from pygtfsie.db.schema_feed import open_for_read as open_feed_for_read
from pygtfsie.db.schema_index import open_for_read, read_meta
from pygtfsie.exceptions import FeedInvalid, FeedUnavailable, GtfsieError
from pygtfsie.ingest.download import Credential, fetch
from pygtfsie.ingest.guard import should_replace
from pygtfsie.ingest.materialise import Scope, materialise
from pygtfsie.ingest.worker import IngestRequest, run_ingest

from .const import (
    CONF_BACK_DAYS,
    CONF_HORIZON_DAYS,
    CONF_REFRESH_HOURS,
    CONF_SCOPE_KIND,
    CONF_SCOPE_VALUES,
    CONF_SOURCE_URL,
    DATA_SUBDIR,
    DEFAULT_BACK_DAYS,
    DEFAULT_HORIZON_DAYS,
    DEFAULT_REFRESH_HOURS,
)

_LOGGER = logging.getLogger(__name__)

T = TypeVar("T")


class DatasourceState:
    """Where a datasource is in its life.

    Distinct from "has departures". A user seeing nothing needs to know whether
    the feed is still importing, failed, or simply has no service tonight, and
    those have entirely different answers.
    """

    IDLE = "idle"
    IMPORTING = "importing"
    READY = "ready"
    FAILED = "failed"


@dataclass(slots=True)
class DatasourcePaths:
    root: Path
    archive: Path
    feed: Path
    feed_build: Path
    feed_candidate: Path
    index: Path
    index_build: Path
    progress: Path


def paths_for(hass: HomeAssistant, entry_id: str) -> DatasourcePaths:
    """Where one datasource keeps its files.

    Under the configuration directory, in a directory of its own named for the
    entry. Per-entry rather than per-feed-name so removing one datasource cannot
    reach another's files: cleanup deletes a directory it owns outright, instead
    of matching a name prefix -- which is how deleting "dublin" once destroyed
    the working "dublin-bus-gtfs".
    """
    root = Path(hass.config.path(DATA_SUBDIR)) / entry_id
    return DatasourcePaths(
        root=root,
        archive=root / "feed.zip",
        feed=root / "feed.sqlite",
        feed_build=root / "feed.building",
        feed_candidate=root / "feed.candidate",
        index=root / "index.sqlite",
        index_build=root / "index.building",
        progress=root / "progress.jsonl",
    )


class Datasource:
    """A feed, its databases, and the one connection that reads them."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass = hass
        self.entry = entry
        self.paths = paths_for(hass, entry.entry_id)
        self.state: str = DatasourceState.IDLE
        self.error: str | None = None
        self.meta: dict[str, str] = {}
        self._conn: sqlite3.Connection | None = None
        self._listeners: list[Callable[[], None]] = []
        # One writer per datasource. A manual service call and the scheduled
        # refresh therefore cannot overlap, which is what makes "database is
        # locked" unreachable rather than merely unlikely.
        self._lock = asyncio.Lock()
        # Set once the datasource is torn down. A refresh can already be
        # queued when an entry is unloaded or reloaded, and running it
        # afterwards reaches for an executor that has been shut down --
        # "cannot schedule new futures after shutdown", raised from a
        # background task where nothing is waiting to catch it.
        self._closed = False
        # One thread, and every use of the connection goes through it.
        #
        # sqlite3 connections are bound to the thread that created them. Home
        # Assistant's shared executor is a pool, so opening on one worker and
        # querying from another raises ProgrammingError -- and because that is a
        # subclass of sqlite3.Error it is easy to swallow into an empty result,
        # which presents as "the datasource failed" with a perfectly good
        # database sitting on disk.
        #
        # A dedicated single-thread executor fixes it without relaxing
        # check_same_thread, and serialising reads is what one connection wants
        # anyway. It also keeps long queries off the shared pool, where they
        # would compete with every other integration.
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"gtfsie-{entry.entry_id[:8]}")

    # --- lifecycle -------------------------------------------------------

    async def async_setup(self) -> None:
        """Open what exists, schedule the future, and do only the work needed now.

        Opening first matters on restart: a datasource whose index is already
        built serves departures immediately rather than waiting on a refresh, and
        it has to work even when the source archive is long gone.

        What happens next depends on what is actually missing. Downloading a feed
        on every Home Assistant restart would be slow and rude to the agency
        serving it, so a restart with current data does nothing at all.
        """
        if await self.hass.async_add_executor_job(self.paths.index.is_file):
            await self._async_open()

        if self.state != DatasourceState.READY:
            reason, roll_only = "first import", False
        elif not self.window_covers_today:
            # The index covers a fixed range of dates, so it goes stale by time
            # passing alone: a machine that was off for a week comes back with a
            # window that ended days ago. Rebuilding needs no download, because
            # feed.sqlite is still perfectly good.
            reason, roll_only = "window no longer covers today", True
        else:
            reason, roll_only = "", False

        if reason:
            self.entry.async_create_background_task(
                self.hass,
                self.async_refresh(reason=reason, roll_only=roll_only),
                f"gtfsie {reason} {self.entry.entry_id}",
            )

        self._schedule()

    def _schedule(self) -> None:
        """Roll the window nightly, and re-fetch the feed on its own interval.

        Two schedules, because they answer different questions. The roll keeps
        the horizon ahead of today and costs no network; the fetch asks whether
        the publisher has issued anything new.

        The roll runs in the small hours with a random offset of up to fifteen
        minutes. Not to spread load on this machine -- a rebuild is seconds --
        but so that every installation does not wake at the same minute, which
        for a popular feed is a self-inflicted stampede on somebody's server.
        """
        options = {**self.entry.data, **self.entry.options}
        jitter = random.randint(0, 15)  # noqa: S311 - spreading load, not a secret

        # Registered through the entry rather than a list of our own. Home
        # Assistant then cancels them on unload *and* on a setup that fails
        # partway, which a manual list does not: a timer surviving a failed
        # setup fires against a datasource that was never finished.
        self.entry.async_on_unload(
            async_track_time_change(self.hass, self._on_roll_due, hour=3, minute=15 + jitter, second=0)
        )

        hours = max(1, int(options.get(CONF_REFRESH_HOURS, DEFAULT_REFRESH_HOURS)))
        self.entry.async_on_unload(async_track_time_interval(self.hass, self._on_refresh_due, timedelta(hours=hours)))

    # Both listeners are decorated rather than written as lambdas. Home Assistant
    # infers a plain callable as HassJobType.Executor and runs it on a worker
    # thread, and hass.async_create_task from off the loop is not thread-safe --
    # its own runtime check calls it out as able to crash or corrupt. The
    # decorator is what says "run this on the loop".

    @callback
    def _on_roll_due(self, _now) -> None:
        self.hass.async_create_task(self.async_refresh(reason="nightly window roll", roll_only=True))

    @callback
    def _on_refresh_due(self, _now) -> None:
        self.hass.async_create_task(self.async_refresh(reason="scheduled feed refresh"))

    async def async_shutdown(self) -> None:
        self._closed = True
        await self._in_store(self._close)
        self._executor.shutdown(wait=False)

    def _close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def add_listener(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Register for "the data changed", returning an unsubscribe."""
        self._listeners.append(callback)

        def _remove() -> None:
            if callback in self._listeners:
                self._listeners.remove(callback)

        return _remove

    def _notify(self) -> None:
        for listener in list(self._listeners):
            listener()

    # --- reading ---------------------------------------------------------

    async def async_query(self, fn: Callable[[sqlite3.Connection], T]) -> T | None:
        """Run a query in an executor. ``None`` when there is nothing to read yet."""
        if self._conn is None:
            return None
        return await self._in_store(self._run, fn)

    def _run(self, fn: Callable[[sqlite3.Connection], T]) -> T | None:
        conn = self._conn
        if conn is None:
            return None
        try:
            return fn(conn)
        except sqlite3.Error as err:
            # A promoted file is replaced by rename, so a reader can be holding
            # an unlinked inode. Reporting rather than raising keeps one bad
            # query from taking the entity unavailable when the next poll will
            # reopen and succeed.
            _LOGGER.debug("query against %s failed: %s", self.paths.index, err)
            return None

    async def _in_store(self, fn, *args):
        """Run something on the one thread that owns the connection."""
        if self._closed and fn is not self._close:
            return None
        return await self.hass.loop.run_in_executor(self._executor, fn, *args)

    async def _async_open(self) -> None:
        await self._in_store(self._open)

    def _open(self) -> None:
        self._close()
        try:
            self._conn = open_for_read(self.paths.index)
            self.meta = read_meta(self._conn)
            self.state = DatasourceState.READY
            self.error = None
        except sqlite3.Error as err:
            self._conn = None
            self.state = DatasourceState.FAILED
            self.error = str(err)
            _LOGGER.warning("could not open %s: %s", self.paths.index, err)

    # --- importing -------------------------------------------------------

    @property
    def window(self) -> tuple[Any, Any]:
        """The dates to materialise, in Home Assistant's own timezone.

        The only clock reading in the whole chain. The engine is deliberately
        clock-free, so "today" is decided once, here, and passed down as two
        explicit dates.
        """
        options = {**self.entry.data, **self.entry.options}
        back = int(options.get(CONF_BACK_DAYS, DEFAULT_BACK_DAYS))
        horizon = int(options.get(CONF_HORIZON_DAYS, DEFAULT_HORIZON_DAYS))
        today = dt_util.now().date()
        return today - timedelta(days=back), today + timedelta(days=horizon)

    @property
    def window_covers_today(self) -> bool:
        """Whether the promoted index still reaches today.

        A materialised window is a fixed range of dates, so it goes stale simply
        by time passing: an index built for today plus three days covers nothing
        four days later. Checking this at startup is what stops a Home Assistant
        that has been off for a week coming back with an empty timetable.
        """
        end = self.meta.get("window_end_date") or ""
        if not end.isdigit():
            return False
        today = dt_util.now().date()
        return int(end) >= today.year * 10000 + today.month * 100 + today.day

    async def async_refresh(self, *, reason: str, roll_only: bool = False) -> None:
        """Bring the datasource up to date. Never raises; failure becomes state.

        ``roll_only`` rebuilds the index from the feed already on disk, with no
        download and no CSV parsing. That is the nightly window roll, and it is
        the same code path as a full build -- there is no prune, no in-place
        edit and no second implementation to keep correct.

        Serialised by a lock, so a manual service call and the scheduled refresh
        cannot overlap. One writer per datasource is what makes "database is
        locked" unreachable rather than merely unlikely.
        """
        if self._closed:
            return
        if self._lock.locked():
            _LOGGER.debug("%s: a refresh is already running, skipping %s", self.entry.title, reason)
            return

        async with self._lock:
            self.state = DatasourceState.IMPORTING
            self._notify()
            try:
                await self.hass.async_add_executor_job(self._refresh, roll_only)
            except GtfsieError as err:
                # Expected and handled: unreachable, or not a feed. A traceback
                # would put a stack trace in a user's log for a network blip,
                # every night, and teach them to ignore the log.
                self.state = DatasourceState.FAILED
                self.error = str(err)
                _LOGGER.warning("%s failed for %s: %s", reason, self.entry.title, err)
            except Exception as err:  # noqa: BLE001 - a background task must not vanish silently
                # Anything else is a defect rather than a condition, and the
                # traceback is the point.
                self.state = DatasourceState.FAILED
                self.error = f"{type(err).__name__}: {err}"
                _LOGGER.exception("unexpected error during %s for %s", reason, self.entry.title)
            else:
                await self._async_open()
            self._notify()

    def _refresh(self, roll_only: bool) -> None:
        """The whole refresh, synchronously, off the loop."""
        self.paths.root.mkdir(parents=True, exist_ok=True)
        if not roll_only:
            self._fetch_and_load()
        if not self.paths.feed.is_file():
            raise FeedUnavailable("no feed database to build an index from")
        self._materialise()

    def _fetch_and_load(self) -> None:
        """Download a candidate archive and load it, if it should replace what is there."""
        options = {**self.entry.data, **self.entry.options}
        url = str(options[CONF_SOURCE_URL])

        result = fetch(
            url,
            self.paths.root,
            credential=Credential(),
            etag=self.meta.get("source_etag") or None,
            last_modified=self.meta.get("source_last_modified") or None,
        )
        if result.path is None:
            # 304. The publisher says nothing changed, so there is nothing to
            # load; the index is still rebuilt, because the window may have moved
            # even when the feed has not.
            return

        try:
            ingest = run_ingest(
                IngestRequest(
                    datasource_id=self.entry.entry_id,
                    zip_path=str(result.path),
                    feed_path=str(self.paths.feed_candidate),
                    feed_build_path=str(self.paths.feed_build),
                    progress_path=str(self.paths.progress),
                    source_url=url,
                    local_timezone=str(self.hass.config.time_zone or "UTC"),
                )
            )
            if not ingest.ok:
                # Preserve the distinction the worker drew: a feed that is not a
                # feed will fail identically forever, while an unavailable one is
                # worth retrying on the next schedule.
                detail = f"{ingest.error_key}: {ingest.error_detail}"
                raise FeedInvalid(detail) if ingest.error_key == "feed_invalid" else FeedUnavailable(detail)

            if not self._should_promote():
                # The candidate is discarded and the existing data is untouched.
                # Loading into a candidate file rather than over the live one is
                # what makes that possible: the guard runs against real parsed
                # dates, and refusing costs nothing already in use.
                self.paths.feed_candidate.unlink(missing_ok=True)
                _LOGGER.warning(
                    "%s: the downloaded feed is entirely future-dated, so the existing timetable has been kept",
                    self.entry.title,
                )
                return

            self.paths.feed_candidate.replace(self.paths.feed)
        finally:
            # The archive has done its job and is the largest thing here; the
            # databases are rebuildable from the source URL.
            result.path.unlink(missing_ok=True)
            self.paths.archive.unlink(missing_ok=True)
            self.paths.feed_candidate.unlink(missing_ok=True)

    def _should_promote(self) -> bool:
        """Ask the guard whether the candidate may replace the live feed.

        A failure to answer is not a reason to destroy working data, so any
        error reading the candidate keeps what is already there -- which is the
        opposite of the usual default and deliberate.
        """
        is_first = not self.paths.feed.is_file()
        try:
            conn = open_feed_for_read(self.paths.feed_candidate)
            try:
                first, last = feed_service_bounds(conn)
            finally:
                conn.close()
        except sqlite3.Error as err:
            _LOGGER.warning(
                "%s: could not read the candidate's service dates (%s); keeping the existing feed",
                self.entry.title,
                err,
            )
            return is_first

        return bool(
            should_replace(
                first_service_date=_as_date(first),
                last_service_date=_as_date(last),
                today=dt_util.now().date(),
                is_first_import=is_first,
            )
        )

    def _materialise(self) -> None:
        """Rebuild the index for the current window and promote it."""
        options = {**self.entry.data, **self.entry.options}
        start, end = self.window
        materialise(
            self.paths.feed,
            self.paths.index_build,
            window_start=start,
            window_end=end,
            scope=Scope(
                str(options.get(CONF_SCOPE_KIND, "all")),
                tuple(options.get(CONF_SCOPE_VALUES, ()) or ()),
            ),
            datasource_id=self.entry.entry_id,
            generation=int(self.meta.get("generation", "0") or 0) + 1,
            built_utc=int(dt_util.utcnow().timestamp()),
        )
        # Promotion is a rename, so a reader either sees the whole old file or
        # the whole new one. It never sees a half-built index.
        self.paths.index_build.replace(self.paths.index)


def _as_date(value: int | None):
    """YYYYMMDD to a date, or None."""
    if not value:
        return None
    try:
        return date(value // 10000, (value // 100) % 100, value % 100)
    except ValueError:
        return None

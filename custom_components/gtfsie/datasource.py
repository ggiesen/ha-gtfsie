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

import logging
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, TypeVar

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pygtfsie.db.schema_index import open_for_read, read_meta
from pygtfsie.exceptions import FeedInvalid, FeedUnavailable, GtfsieError
from pygtfsie.ingest.download import Credential, fetch
from pygtfsie.ingest.materialise import Scope, materialise
from pygtfsie.ingest.worker import IngestRequest, run_ingest

from .const import (
    CONF_BACK_DAYS,
    CONF_HORIZON_DAYS,
    CONF_SCOPE_KIND,
    CONF_SCOPE_VALUES,
    CONF_SOURCE_URL,
    DATA_SUBDIR,
    DEFAULT_BACK_DAYS,
    DEFAULT_HORIZON_DAYS,
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
        """Open an existing index if there is one, and import in the background.

        Opening first matters on restart: a datasource whose index is already
        built must serve departures immediately rather than waiting on a refresh,
        and I-14 requires it to work even when the source archive is long gone.
        """
        if await self.hass.async_add_executor_job(self.paths.index.is_file):
            await self._async_open()

        self.entry.async_create_background_task(self.hass, self._async_import(), f"gtfsie import {self.entry.entry_id}")

    async def async_shutdown(self) -> None:
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

    async def _async_import(self) -> None:
        """Fetch, load and materialise. Never raises; failure becomes state."""
        self.state = DatasourceState.IMPORTING
        self._notify()
        try:
            await self.hass.async_add_executor_job(self._import)
        except GtfsieError as err:
            # Expected and handled: unreachable, or not a feed. A traceback here
            # would put a stack trace in a user's log for a network blip, every
            # night, and teach them to ignore the log.
            self.state = DatasourceState.FAILED
            self.error = str(err)
            _LOGGER.warning("import failed for %s: %s", self.entry.title, err)
        except Exception as err:  # noqa: BLE001 - a background task must not vanish silently
            # Anything else is a defect rather than a condition, and the
            # traceback is the point.
            self.state = DatasourceState.FAILED
            self.error = f"{type(err).__name__}: {err}"
            _LOGGER.exception("unexpected error importing %s", self.entry.title)
        else:
            await self._async_open()
        self._notify()

    def _import(self) -> None:
        """The whole import, synchronously, off the loop."""
        options = {**self.entry.data, **self.entry.options}
        url = str(options[CONF_SOURCE_URL])
        self.paths.root.mkdir(parents=True, exist_ok=True)

        result = fetch(url, self.paths.root, credential=Credential())
        if result.path is None:
            # A 304 with nothing on disk means there is nothing to do and
            # nothing to serve; with an index already present it means the data
            # is current, which is a success.
            self.error = None if self.paths.index.is_file() else "not_modified_without_data"
            return

        ingest = run_ingest(
            IngestRequest(
                datasource_id=self.entry.entry_id,
                zip_path=str(result.path),
                feed_path=str(self.paths.feed),
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
        # The archive has done its job; the databases are rebuildable from the
        # source URL and the archive is the largest thing here.
        self.paths.archive.unlink(missing_ok=True)
        result.path.unlink(missing_ok=True)

"""Per-watch polling.

One coordinator per route subentry. It reads the datasource's index, applies the
watch's offset and lookback, and hands the presenter a list of rows plus a
status. Nothing here formats anything and nothing here touches a database
directly -- both belong to layers either side.

The clock is read exactly once per refresh, and the resulting instant is passed
down explicitly. Every departure in one update therefore agrees about what "now"
is, which a query that consulted the clock per statement could not guarantee.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from homeassistant.config_entries import ConfigEntry, ConfigSubentry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util
from pygtfsie.db.queries import (
    DepartureRow,
    departures_at_stops,
    departures_origin_to_destination,
    over_fetch,
    resolve_stop_ids,
)

from .const import (
    CONF_DEST_STOP_IDS,
    CONF_DIRECTION_ID,
    CONF_LIMIT,
    CONF_LOOKBACK_MINUTES,
    CONF_OFFSET_MINUTES,
    CONF_ORIGIN_STOP_IDS,
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_MINUTES,
    DEFAULT_OFFSET_MINUTES,
    DOMAIN,
)
from .datasource import Datasource, DatasourceState

_LOGGER = logging.getLogger(__name__)

#: How often to recompute. Departures move only as the clock does, so this is
#: about how promptly an elapsed one disappears rather than about fetching.
UPDATE_INTERVAL = timedelta(seconds=60)


class Status:
    OK = "ok"
    NO_DEPARTURES = "no_departures"
    STOP_NOT_IN_FEED = "stop_not_in_feed"
    WINDOW_EXHAUSTED = "window_exhausted"
    EXTRACTING = "extracting"
    DATASOURCE_FAILED = "datasource_failed"


@dataclass(slots=True)
class WatchData:
    rows: list[DepartureRow] = field(default_factory=list)
    status: str = Status.NO_DEPARTURES
    now_utc: int = 0
    truncated: bool = False
    window_end_utc: int | None = None
    missing_stop_ids: tuple[str, ...] = ()


class RouteCoordinator(DataUpdateCoordinator[WatchData]):
    """Keeps one watched stop pair up to date."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        subentry: ConfigSubentry,
        datasource: Datasource,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN} {subentry.title}",
            update_interval=UPDATE_INTERVAL,
            config_entry=entry,
        )
        self.subentry = subentry
        self.datasource = datasource
        self._unsub = datasource.add_listener(self._on_datasource_change)

    def _on_datasource_change(self) -> None:
        """Refresh when the import finishes, rather than waiting for the timer.

        Without this a first import that completes just after a poll leaves the
        entity reporting "extracting" for another whole interval, which reads as
        the import having hung.

        ``async_refresh`` rather than ``async_request_refresh``: this fires once
        per import, not per poll, so there is nothing for a debouncer to
        coalesce and its cooldown would only delay the one update the user is
        waiting for.
        """
        self.hass.async_create_task(self.async_refresh())

    async def async_shutdown(self) -> None:
        self._unsub()
        await super().async_shutdown()

    @property
    def options(self) -> dict[str, Any]:
        return dict(self.subentry.data)

    async def _async_update_data(self) -> WatchData:
        now_utc = int(dt_util.utcnow().timestamp())

        if self.datasource.state == DatasourceState.FAILED:
            return WatchData(status=Status.DATASOURCE_FAILED, now_utc=now_utc)
        if self.datasource.state in (DatasourceState.IDLE, DatasourceState.IMPORTING):
            return WatchData(status=Status.EXTRACTING, now_utc=now_utc)

        options = self.options
        origins = list(options.get(CONF_ORIGIN_STOP_IDS) or [])
        destinations = list(options.get(CONF_DEST_STOP_IDS) or [])
        limit = int(options.get(CONF_LIMIT, DEFAULT_LIMIT))
        offset = int(options.get(CONF_OFFSET_MINUTES, DEFAULT_OFFSET_MINUTES))
        lookback = int(options.get(CONF_LOOKBACK_MINUTES, DEFAULT_LOOKBACK_MINUTES))
        direction = options.get(CONF_DIRECTION_ID)
        direction_id = int(direction) if str(direction or "").strip().isdigit() else None

        # The offset excludes departures too soon to reach; the lookback keeps
        # ones already due, because a late vehicle has not left yet. They move
        # the same bound in opposite directions and both are the user's choice.
        from_utc = now_utc + offset * 60 - lookback * 60
        window_end = self._window_end_utc()
        until_utc = window_end if window_end is not None else from_utc + 86400 * 30

        def _query(conn) -> tuple[Any, dict[str, int]]:
            resolved = resolve_stop_ids(conn, origins + destinations)
            origin_uids = [resolved[s] for s in origins if s in resolved]
            dest_uids = [resolved[s] for s in destinations if s in resolved]
            if not origin_uids:
                return None, resolved
            want = over_fetch(limit)
            if dest_uids:
                page = departures_origin_to_destination(
                    conn,
                    origin_uids,
                    dest_uids,
                    from_utc,
                    until_utc,
                    want,
                    direction_id=direction_id,
                )
            else:
                page = departures_at_stops(conn, origin_uids, from_utc, until_utc, want, direction_id=direction_id)
            return page, resolved

        result = await self.datasource.async_query(_query)
        if result is None:
            return WatchData(status=Status.DATASOURCE_FAILED, now_utc=now_utc)

        page, resolved = result
        missing = tuple(s for s in origins + destinations if s not in resolved)
        if page is None:
            return WatchData(status=Status.STOP_NOT_IN_FEED, now_utc=now_utc, missing_stop_ids=missing)

        # Trim to the presentation limit here rather than in SQL. The extra rows
        # exist so a realtime overlay can reorder before this cut is made; until
        # phase 6 there is nothing to reorder, and the cut is the same either way.
        rows = page.rows[:limit]
        if rows:
            status = Status.OK
        elif page.exhausted and window_end is not None:
            status = Status.WINDOW_EXHAUSTED
        else:
            status = Status.NO_DEPARTURES

        return WatchData(
            rows=rows,
            status=status,
            now_utc=now_utc,
            truncated=len(page.rows) > limit or page.truncated,
            window_end_utc=window_end,
            missing_stop_ids=missing,
        )

    def _window_end_utc(self) -> int | None:
        value = self.datasource.meta.get("window_end_utc") or ""
        try:
            return int(value)
        except ValueError:
            return None

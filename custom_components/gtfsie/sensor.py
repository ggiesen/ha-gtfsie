"""Sensor entities.

Two per watch. A timestamp sensor whose state is the next departure, and a
status sensor saying why that is what it is.

The status sensor is not decoration. "No departures" has half a dozen causes
with entirely different fixes -- still importing, stop not in the feed, nothing
runs tonight, the prepared window ran out -- and an entity that reports all of
them as an empty state is unactionable. A second entity costs nothing and makes
the difference visible to an automation as well as a person.
"""

from __future__ import annotations

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import DOMAIN, SubentryKind
from .coordinator import RouteCoordinator, Status, WatchData
from .presenter import state_attributes


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Create entities for each route subentry.

    Entities are added against their own subentry, so Home Assistant attributes
    them to the right child and removing that child removes them.
    """
    store = hass.data[DOMAIN][entry.entry_id]
    for subentry_id, coordinator in store["coordinators"].items():
        subentry = entry.subentries[subentry_id]
        if subentry.subentry_type != SubentryKind.ROUTE.value:
            continue
        async_add_entities(
            [DepartureSensor(coordinator), WatchStatusSensor(coordinator)],
            config_subentry_id=subentry_id,
        )


class _Base(CoordinatorEntity[RouteCoordinator], SensorEntity):
    _attr_has_entity_name = True

    def __init__(self, coordinator: RouteCoordinator) -> None:
        super().__init__(coordinator)
        self._subentry_id = coordinator.subentry.subentry_id

    @property
    def _data(self) -> WatchData:
        return self.coordinator.data or WatchData()


class DepartureSensor(_Base):
    """The next departure, as an instant.

    A timestamp rather than a formatted string or a countdown. Home Assistant
    renders a timestamp in the viewer's own locale and keeps the relative
    display current without the integration polling to move a number, and an
    automation can compare it directly.
    """

    _attr_device_class = SensorDeviceClass.TIMESTAMP
    _attr_translation_key = "next_departure"

    def __init__(self, coordinator: RouteCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{self._subentry_id}_next_departure"
        self._attr_name = "Next departure"

    @property
    def native_value(self):
        rows = self._data.rows
        if not rows:
            # None, not a fabricated instant. "Unknown" is honest; a made-up
            # time would be indistinguishable from a real one and would fire
            # every automation watching it.
            return None
        return dt_util.utc_from_timestamp(rows[0].departure_utc)

    @property
    def icon(self) -> str | None:
        rows = self._data.rows
        return rows[0].icon if rows else "mdi:transit-connection-variant"

    @property
    def extra_state_attributes(self):
        data = self._data
        meta = self.coordinator.datasource.meta
        return state_attributes(
            data.rows,
            now_utc=data.now_utc or int(dt_util.utcnow().timestamp()),
            status=data.status,
            fallback_tz=str(self.hass.config.time_zone or "UTC"),
            feed_valid_from=meta.get("feed_start_date", ""),
            feed_valid_to=meta.get("feed_end_date", ""),
            feed_imported=meta.get("built_utc", ""),
            truncated=data.truncated,
            window_end_utc=data.window_end_utc,
        )


class WatchStatusSensor(_Base):
    """Why the departure sensor says what it says."""

    _attr_translation_key = "status"
    _attr_entity_registry_enabled_default = True

    def __init__(self, coordinator: RouteCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{self._subentry_id}_status"
        self._attr_name = "Status"

    @property
    def native_value(self) -> str:
        return self._data.status

    @property
    def extra_state_attributes(self):
        data = self._data
        attributes = {"departure_count": len(data.rows)}
        if data.missing_stop_ids:
            # Named, because "stop not in feed" without the id leaves a user
            # comparing their configuration against a feed by hand.
            attributes["missing_stop_ids"] = list(data.missing_stop_ids)
        if self.coordinator.datasource.error:
            attributes["error"] = self.coordinator.datasource.error
        return attributes

    @property
    def icon(self) -> str:
        return {
            Status.OK: "mdi:check-circle-outline",
            Status.EXTRACTING: "mdi:database-import-outline",
            Status.DATASOURCE_FAILED: "mdi:alert-circle-outline",
            Status.STOP_NOT_IN_FEED: "mdi:map-marker-question-outline",
            Status.WINDOW_EXHAUSTED: "mdi:calendar-end-outline",
        }.get(self._data.status, "mdi:information-outline")


@callback
def _unused() -> None:  # pragma: no cover
    """Keeps the callback import honest until services land."""

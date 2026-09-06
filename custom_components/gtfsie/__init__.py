"""The gtfsie integration.

A config entry is one datasource. Its route subentries each get a coordinator,
and the sensor platform turns those into entities.

Setup never waits for an import. A first import of a national feed runs for
hours, so it is started as a background task and the entities report
``extracting`` until it finishes -- a state a user can understand, and far
better than a startup that appears to hang.
"""

from __future__ import annotations

import logging
import shutil

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr

from .const import DOMAIN, SubentryKind
from .coordinator import RouteCoordinator
from .datasource import Datasource, paths_for

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up a datasource and a coordinator per watch."""
    datasource = Datasource(hass, entry)
    coordinators = {
        subentry_id: RouteCoordinator(hass, entry, subentry, datasource)
        for subentry_id, subentry in entry.subentries.items()
        if subentry.subentry_type == SubentryKind.ROUTE.value
    }
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
        "datasource": datasource,
        "coordinators": coordinators,
    }

    # Registered here, before the platform loads, because a watch device names
    # this one as its ``via_device`` and that link is dropped silently if the
    # referenced device does not already exist. There is no datasource-level
    # entity to bring it into being as a side effect, so it is created outright.
    dr.async_get(hass).async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={(DOMAIN, entry.entry_id)},
        name=entry.title,
        model="GTFS datasource",
        entry_type=dr.DeviceEntryType.SERVICE,
    )

    await datasource.async_setup()
    for coordinator in coordinators.values():
        # Deliberately not async_config_entry_first_refresh, which fails setup
        # when the first update produces no data. "Still importing" is a real
        # answer the entities are built to display, not a reason to refuse to
        # load -- and refusing would hide the one state the user needs to see.
        await coordinator.async_refresh()

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_reload))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Tear a datasource down, leaving its databases on disk.

    Unload runs on every reload and every restart. Deleting gigabytes that took
    hours to build is a separate action with its own confirmation, so nothing
    here touches a file.
    """
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    store = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    if store:
        for coordinator in store["coordinators"].values():
            await coordinator.async_shutdown()
        await store["datasource"].async_shutdown()
    return unloaded


async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Delete this datasource's directory once the entry is removed for good.

    Scoped to the directory named for this entry, so it cannot reach another
    datasource's files -- the failure that once destroyed a working
    "dublin-bus-gtfs" while deleting "dublin". Missing files are not an error:
    an entry removed before its first import never had any.
    """
    root = paths_for(hass, entry.entry_id).root
    await hass.async_add_executor_job(shutil.rmtree, root, True)


async def _async_reload(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload when the entry or one of its subentries changes.

    A reload rebuilds the coordinators, which is how an added or removed watch
    gains or loses its entities. It does not re-import: the databases are keyed
    to the feed, not to the configuration.
    """
    await hass.config_entries.async_reload(entry.entry_id)

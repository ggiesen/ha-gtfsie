"""The gtfsie integration.

Minimal for now: a datasource entry loads and unloads cleanly, which is what the
config and subentry flows need in order to be exercised end to end. The
coordinator, the import manager and the entity platforms arrive with phase 4
proper.
"""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN

PLATFORMS: list[str] = []


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up a datasource."""
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {}
    entry.async_on_unload(entry.add_update_listener(_async_reload))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Tear a datasource down, leaving its databases on disk.

    Removing the entry is a separate action from deleting gigabytes that took
    hours to build, so unload never touches the files.
    """
    hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    return True


async def _async_reload(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)

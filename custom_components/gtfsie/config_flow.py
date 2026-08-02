"""Config and subentry flows.

One config entry per datasource -- a GTFS feed, its schedule, and its databases.
Route and vicinity watches are ``ConfigSubentry`` children of it.

That structure is the point. A datasource is expensive and a watch is cheap, and
every watch belongs to exactly one datasource. Modelling that natively means
Home Assistant handles deletion cascading and startup ordering, and there is no
cross-entry `datasource_id` for anything to get out of step with -- which is the
"which datasource is this entry using" defect class from the upstream history.
"""

from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    ConfigSubentryFlow,
    SubentryFlowResult,
)
from homeassistant.core import callback
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.selector import (
    EntitySelector,
    EntitySelectorConfig,
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import (
    CONF_DATASOURCE_NAME,
    CONF_DEST_STOP_IDS,
    CONF_DIRECTION_ID,
    CONF_HORIZON_DAYS,
    CONF_LIMIT,
    CONF_LOOKBACK_MINUTES,
    CONF_MAX_STOPS,
    CONF_OFFSET_MINUTES,
    CONF_ORIGIN_STOP_IDS,
    CONF_RADIUS_M,
    CONF_REFRESH_HOURS,
    CONF_ROUTE_ID,
    CONF_SOURCE_URL,
    CONF_TRACKER_ENTITY_ID,
    DEFAULT_HORIZON_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_MINUTES,
    DEFAULT_MAX_STOPS,
    DEFAULT_OFFSET_MINUTES,
    DEFAULT_RADIUS_M,
    DEFAULT_REFRESH_HOURS,
    DOMAIN,
    SubentryKind,
)


def _split_ids(raw: str | list[str] | None) -> list[str]:
    """Parse a comma-separated stop id list without damaging the ids.

    Only commas separate and only surrounding whitespace is trimmed. Stop ids
    are opaque and routinely contain colons, spaces and non-ASCII --
    ``de:08221:1234:1:A`` and ``MTA NYCT_308209`` are both real -- so anything
    cleverer than this corrupts them.
    """
    if raw is None:
        return []
    parts = raw.split(",") if isinstance(raw, str) else list(raw)
    return [text for part in parts if (text := str(part).strip())]


_URL = TextSelector(TextSelectorConfig(type=TextSelectorType.URL))
_TEXT = TextSelector(TextSelectorConfig(type=TextSelectorType.TEXT))


def _number(minimum: float, maximum: float, step: float = 1) -> NumberSelector:
    return NumberSelector(NumberSelectorConfig(min=minimum, max=maximum, step=step, mode=NumberSelectorMode.BOX))


DATASOURCE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_DATASOURCE_NAME): _TEXT,
        vol.Required(CONF_SOURCE_URL): _URL,
        vol.Optional(CONF_REFRESH_HOURS, default=DEFAULT_REFRESH_HOURS): _number(1, 720),
        vol.Optional(CONF_HORIZON_DAYS, default=DEFAULT_HORIZON_DAYS): _number(1, 30),
    }
)

ROUTE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_ORIGIN_STOP_IDS): _TEXT,
        vol.Optional(CONF_DEST_STOP_IDS, default=""): _TEXT,
        vol.Optional(CONF_ROUTE_ID, default=""): _TEXT,
        vol.Optional(CONF_DIRECTION_ID, default=""): _TEXT,
        vol.Optional(CONF_OFFSET_MINUTES, default=DEFAULT_OFFSET_MINUTES): _number(0, 1440),
        vol.Optional(CONF_LOOKBACK_MINUTES, default=DEFAULT_LOOKBACK_MINUTES): _number(0, 120),
        vol.Optional(CONF_LIMIT, default=DEFAULT_LIMIT): _number(1, 50),
    }
)

VICINITY_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_TRACKER_ENTITY_ID): EntitySelector(
            EntitySelectorConfig(domain=["device_tracker", "person", "zone"])
        ),
        vol.Optional(CONF_RADIUS_M, default=DEFAULT_RADIUS_M): _number(50, 5000, 10),
        vol.Optional(CONF_MAX_STOPS, default=DEFAULT_MAX_STOPS): _number(1, 100),
        vol.Optional(CONF_OFFSET_MINUTES, default=DEFAULT_OFFSET_MINUTES): _number(0, 1440),
        vol.Optional(CONF_LIMIT, default=DEFAULT_LIMIT): _number(1, 50),
    }
)


class GtfsieConfigFlow(ConfigFlow, domain=DOMAIN):
    """Adds a datasource: one feed, its schedule and its databases."""

    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            url = str(user_input[CONF_SOURCE_URL]).strip()
            if not url.lower().startswith(("http://", "https://")):
                errors[CONF_SOURCE_URL] = "invalid_url"
            else:
                # The URL identifies the datasource, so adding the same feed
                # twice is caught here rather than producing two imports of
                # identical data under different names.
                await self.async_set_unique_id(url)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=str(user_input[CONF_DATASOURCE_NAME]).strip() or url,
                    data={**user_input, CONF_SOURCE_URL: url},
                )

        return self.async_show_form(
            step_id="user",
            data_schema=self.add_suggested_values_to_schema(DATASOURCE_SCHEMA, user_input or {}),
            errors=errors,
        )

    @classmethod
    @callback
    def async_get_supported_subentry_types(cls, config_entry: ConfigEntry) -> dict[str, type[ConfigSubentryFlow]]:
        """Declare the children a datasource can have."""
        return {
            SubentryKind.ROUTE.value: RouteSubentryFlow,
            SubentryKind.VICINITY.value: VicinitySubentryFlow,
        }


class RouteSubentryFlow(ConfigSubentryFlow):
    """A watched origin, and optionally a destination, on one datasource."""

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> SubentryFlowResult:
        return await self._async_form(user_input, reconfigure=False)

    async def async_step_reconfigure(self, user_input: dict[str, Any] | None = None) -> SubentryFlowResult:
        """Change a watch in place.

        Reconfigure rather than delete-and-recreate, so the entities keep their
        unique ids and with them their history, customisations and every
        dashboard reference pointing at them.
        """
        return await self._async_form(user_input, reconfigure=True)

    async def _async_form(self, user_input: dict[str, Any] | None, *, reconfigure: bool) -> SubentryFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            origins = _split_ids(user_input.get(CONF_ORIGIN_STOP_IDS))
            if not origins:
                errors[CONF_ORIGIN_STOP_IDS] = "no_origin"
            else:
                data = {
                    **user_input,
                    CONF_ORIGIN_STOP_IDS: origins,
                    CONF_DEST_STOP_IDS: _split_ids(user_input.get(CONF_DEST_STOP_IDS)),
                }
                title = " to ".join(filter(None, [", ".join(origins), ", ".join(data[CONF_DEST_STOP_IDS])]))
                if reconfigure:
                    return self.async_update_and_abort(
                        self._get_entry(),
                        self._get_reconfigure_subentry(),
                        data=data,
                        title=title,
                    )
                return self.async_create_entry(title=title, data=data)

        suggested = user_input or (dict(self._get_reconfigure_subentry().data) if reconfigure else {})
        return self.async_show_form(
            step_id="reconfigure" if reconfigure else "user",
            data_schema=self.add_suggested_values_to_schema(ROUTE_SCHEMA, _as_form_values(suggested)),
            errors=errors,
        )


class VicinitySubentryFlow(ConfigSubentryFlow):
    """Stops near a tracked entity, on one datasource."""

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> SubentryFlowResult:
        return await self._async_form(user_input, reconfigure=False)

    async def async_step_reconfigure(self, user_input: dict[str, Any] | None = None) -> SubentryFlowResult:
        return await self._async_form(user_input, reconfigure=True)

    async def _async_form(self, user_input: dict[str, Any] | None, *, reconfigure: bool) -> SubentryFlowResult:
        if user_input is not None:
            title = str(user_input[CONF_TRACKER_ENTITY_ID])
            if reconfigure:
                return self.async_update_and_abort(
                    self._get_entry(),
                    self._get_reconfigure_subentry(),
                    data=user_input,
                    title=title,
                )
            return self.async_create_entry(title=title, data=user_input)

        suggested = user_input or (dict(self._get_reconfigure_subentry().data) if reconfigure else {})
        return self.async_show_form(
            step_id="reconfigure" if reconfigure else "user",
            data_schema=self.add_suggested_values_to_schema(VICINITY_SCHEMA, suggested),
        )


def _as_form_values(data: dict[str, Any]) -> dict[str, Any]:
    """Render stored values back into what the form expects.

    Stop ids are stored as a list and edited as a comma-separated string. Handing
    the list straight back would render as a Python repr in the text box, which a
    user would then save verbatim.
    """
    out = dict(data)
    for key in (CONF_ORIGIN_STOP_IDS, CONF_DEST_STOP_IDS):
        value = out.get(key)
        if isinstance(value, list):
            out[key] = ", ".join(value)
    return out


__all__ = ["GtfsieConfigFlow", "RouteSubentryFlow", "VicinitySubentryFlow", "cv"]

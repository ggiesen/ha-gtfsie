"""The config flow and its subentry flows.

The structural claim under test is the one that was hard to reverse: a
datasource is a config entry, and a watch is a subentry of it. If these pass,
deletion cascading and startup ordering are Home Assistant's problem rather than
ours, and no watch can be left pointing at a datasource that is gone.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.gtfsie.const import (
    CONF_DATASOURCE_NAME,
    CONF_DEST_STOP_IDS,
    CONF_ORIGIN_STOP_IDS,
    CONF_RADIUS_M,
    CONF_SOURCE_URL,
    CONF_TRACKER_ENTITY_ID,
    DOMAIN,
    SubentryKind,
)

FEED_URL = "https://transit.example/gtfs.zip"


@pytest.fixture(autouse=True)
def _enable_custom_integrations(enable_custom_integrations):
    """Without this Home Assistant will not load a component from custom_components."""
    return enable_custom_integrations


async def _add_datasource(hass: HomeAssistant, url: str = FEED_URL, name: str = "Toronto"):
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
    assert result["type"] is FlowResultType.FORM
    return await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {CONF_DATASOURCE_NAME: name, CONF_SOURCE_URL: url},
    )


class TestDatasourceFlow:
    async def test_a_datasource_can_be_added(self, hass: HomeAssistant):
        result = await _add_datasource(hass)
        assert result["type"] is FlowResultType.CREATE_ENTRY
        assert result["title"] == "Toronto"
        assert result["data"][CONF_SOURCE_URL] == FEED_URL

    @pytest.mark.parametrize("bad", ["not-a-url", "ftp://transit.example/f.zip", "  "])
    async def test_a_url_that_is_not_http_is_rejected(self, hass: HomeAssistant, bad):
        """Caught in the form rather than hours later in an import."""
        result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_DATASOURCE_NAME: "x", CONF_SOURCE_URL: bad}
        )
        assert result["type"] is FlowResultType.FORM
        assert result["errors"] == {CONF_SOURCE_URL: "invalid_url"}

    async def test_the_same_feed_cannot_be_added_twice(self, hass: HomeAssistant):
        """Two entries for one URL would mean importing identical data twice,
        at the cost of an hour and a gigabyte apiece."""
        await _add_datasource(hass)
        await hass.async_block_till_done()
        again = await _add_datasource(hass, name="Toronto again")
        assert again["type"] is FlowResultType.ABORT
        assert again["reason"] == "already_configured"

    async def test_a_different_feed_is_a_separate_datasource(self, hass: HomeAssistant):
        await _add_datasource(hass)
        await hass.async_block_till_done()
        other = await _add_datasource(hass, url="https://other.example/gtfs.zip", name="Vancouver")
        assert other["type"] is FlowResultType.CREATE_ENTRY
        assert len(hass.config_entries.async_entries(DOMAIN)) == 2

    async def test_a_blank_name_falls_back_to_the_url(self, hass: HomeAssistant):
        result = await _add_datasource(hass, name="   ")
        assert result["title"] == FEED_URL


class TestSubentryTypes:
    async def test_a_datasource_declares_both_child_types(self, hass: HomeAssistant):
        from custom_components.gtfsie.config_flow import (
            GtfsieConfigFlow,
            RouteSubentryFlow,
            VicinitySubentryFlow,
        )

        entry = MockConfigEntry(domain=DOMAIN, data={CONF_SOURCE_URL: FEED_URL})
        entry.add_to_hass(hass)
        supported = GtfsieConfigFlow.async_get_supported_subentry_types(entry)

        assert set(supported) == {SubentryKind.ROUTE.value, SubentryKind.VICINITY.value}
        assert supported[SubentryKind.ROUTE.value] is RouteSubentryFlow
        assert supported[SubentryKind.VICINITY.value] is VicinitySubentryFlow

    async def test_both_child_types_support_reconfigure(self):
        """Reconfigure is why the version floor exists, so it is asserted rather
        than assumed. Without it a user changing a stop id would have to delete
        and recreate the watch, losing its entity history."""
        from custom_components.gtfsie.config_flow import (
            RouteSubentryFlow,
            VicinitySubentryFlow,
        )

        for flow in (RouteSubentryFlow, VicinitySubentryFlow):
            assert hasattr(flow, "async_step_reconfigure"), flow.__name__


class TestRouteSubentry:
    async def _add_route(self, hass, entry, **overrides):
        result = await hass.config_entries.subentries.async_init(
            (entry.entry_id, SubentryKind.ROUTE.value),
            context={"source": config_entries.SOURCE_USER},
        )
        assert result["type"] is FlowResultType.FORM
        return await hass.config_entries.subentries.async_configure(
            result["flow_id"], {CONF_ORIGIN_STOP_IDS: "STOP_A", **overrides}
        )

    @pytest.fixture
    async def entry(self, hass: HomeAssistant):
        entry = MockConfigEntry(domain=DOMAIN, data={CONF_SOURCE_URL: FEED_URL}, title="Toronto", unique_id=FEED_URL)
        entry.add_to_hass(hass)
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        return entry

    async def test_a_route_watch_is_added_as_a_child(self, hass: HomeAssistant, entry):
        result = await self._add_route(hass, entry)
        assert result["type"] is FlowResultType.CREATE_ENTRY
        assert len(entry.subentries) == 1

    async def test_stop_ids_are_stored_as_a_list(self, hass: HomeAssistant, entry):
        result = await self._add_route(hass, entry, **{CONF_ORIGIN_STOP_IDS: "STOP_A, STOP_B"})
        assert result["data"][CONF_ORIGIN_STOP_IDS] == ["STOP_A", "STOP_B"]

    async def test_opaque_stop_ids_survive_the_form(self, hass: HomeAssistant, entry):
        """Colons and spaces are ordinary inside a stop id. Only commas separate,
        because anything cleverer corrupts ``de:08221:1234:1:A``."""
        result = await self._add_route(
            hass,
            entry,
            **{CONF_ORIGIN_STOP_IDS: "de:08221:1234:1:A, MTA NYCT_308209"},
        )
        assert result["data"][CONF_ORIGIN_STOP_IDS] == [
            "de:08221:1234:1:A",
            "MTA NYCT_308209",
        ]

    async def test_an_origin_is_required(self, hass: HomeAssistant, entry):
        result = await hass.config_entries.subentries.async_init(
            (entry.entry_id, SubentryKind.ROUTE.value),
            context={"source": config_entries.SOURCE_USER},
        )
        result = await hass.config_entries.subentries.async_configure(
            result["flow_id"], {CONF_ORIGIN_STOP_IDS: " , , "}
        )
        assert result["type"] is FlowResultType.FORM
        assert result["errors"] == {CONF_ORIGIN_STOP_IDS: "no_origin"}

    async def test_a_destination_is_optional(self, hass: HomeAssistant, entry):
        """Watching a stop without saying where you are going is the common case."""
        result = await self._add_route(hass, entry)
        assert result["data"][CONF_DEST_STOP_IDS] == []

    async def test_several_watches_hang_off_one_datasource(self, hass: HomeAssistant, entry):
        """The whole reason for the structure: many cheap children, one
        expensive parent, and no second import."""
        await self._add_route(hass, entry)
        await self._add_route(hass, entry, **{CONF_ORIGIN_STOP_IDS: "STOP_B"})
        assert len(entry.subentries) == 2
        assert len(hass.config_entries.async_entries(DOMAIN)) == 1


class TestVicinitySubentry:
    @pytest.fixture
    async def entry(self, hass: HomeAssistant):
        entry = MockConfigEntry(domain=DOMAIN, data={CONF_SOURCE_URL: FEED_URL}, title="Toronto", unique_id=FEED_URL)
        entry.add_to_hass(hass)
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        return entry

    async def test_a_vicinity_watch_is_added(self, hass: HomeAssistant, entry):
        result = await hass.config_entries.subentries.async_init(
            (entry.entry_id, SubentryKind.VICINITY.value),
            context={"source": config_entries.SOURCE_USER},
        )
        assert result["type"] is FlowResultType.FORM
        result = await hass.config_entries.subentries.async_configure(
            result["flow_id"],
            {CONF_TRACKER_ENTITY_ID: "device_tracker.phone", CONF_RADIUS_M: 300},
        )
        assert result["type"] is FlowResultType.CREATE_ENTRY
        assert result["data"][CONF_TRACKER_ENTITY_ID] == "device_tracker.phone"
        assert len(entry.subentries) == 1


class TestDeletionCascades:
    async def test_removing_a_datasource_takes_its_watches_with_it(self, hass: HomeAssistant):
        """The property that decided the design.

        With independent config entries nothing prevents deleting the datasource
        and leaving its watches pointing at a feed that is gone. As subentries
        the framework removes them together, so the orphaning cannot happen.
        """
        result = await _add_datasource(hass)
        await hass.async_block_till_done()
        entry = hass.config_entries.async_entries(DOMAIN)[0]

        sub = await hass.config_entries.subentries.async_init(
            (entry.entry_id, SubentryKind.ROUTE.value),
            context={"source": config_entries.SOURCE_USER},
        )
        await hass.config_entries.subentries.async_configure(sub["flow_id"], {CONF_ORIGIN_STOP_IDS: "STOP_A"})
        assert len(entry.subentries) == 1

        assert await hass.config_entries.async_remove(entry.entry_id)
        await hass.async_block_till_done()
        assert hass.config_entries.async_entries(DOMAIN) == []
        del result


class TestEntryLifecycle:
    async def test_an_entry_sets_up_and_unloads(self, hass: HomeAssistant):
        entry = MockConfigEntry(domain=DOMAIN, data={CONF_SOURCE_URL: FEED_URL})
        entry.add_to_hass(hass)
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        assert entry.state is config_entries.ConfigEntryState.LOADED

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
        assert entry.state is config_entries.ConfigEntryState.NOT_LOADED

    async def test_unloading_does_not_delete_anything_on_disk(self, hass: HomeAssistant):
        """Removing an entry and deleting gigabytes that took hours to build are
        separate actions, so unload never touches the files."""
        entry = MockConfigEntry(domain=DOMAIN, data={CONF_SOURCE_URL: FEED_URL})
        entry.add_to_hass(hass)
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        root = Path(hass.config.path())
        before = sorted(p.name for p in root.iterdir())
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
        assert sorted(p.name for p in root.iterdir()) == before

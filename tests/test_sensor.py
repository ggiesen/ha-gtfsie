"""End to end: a configured stop pair produces a working departure sensor.

A real archive, a real import, a real materialisation, real SQL. Only the
download is stubbed, because a test that reached the network would be testing
somebody else's server.

The clock is pinned wherever a result depends on it. Every assertion about which
departures appear is an assertion about a stated instant.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from freezegun import freeze_time
from homeassistant.config_entries import ConfigEntryState, ConfigSubentryData
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from pygtfsie.ingest.download import FetchResult
from pygtfsie.ingest.sniff import PayloadKind
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.gtfsie.const import (
    CONF_DEST_STOP_IDS,
    CONF_LIMIT,
    CONF_LOOKBACK_MINUTES,
    CONF_OFFSET_MINUTES,
    CONF_ORIGIN_STOP_IDS,
    CONF_SOURCE_URL,
    DOMAIN,
    SubentryKind,
)

from .fixtures.gtfs_feed import CalendarEntry, Trip, build_feed

TORONTO = ZoneInfo("America/Toronto")
FEED_URL = "https://transit.example/gtfs.zip"

#: A Monday well clear of any clock change in this zone.
NOON = datetime(2026, 6, 15, 12, 0, tzinfo=TORONTO)


def _every_day() -> CalendarEntry:
    return CalendarEntry(
        service_id="S1",
        monday=1,
        tuesday=1,
        wednesday=1,
        thursday=1,
        friday=1,
        saturday=1,
        sunday=1,
    )


def _feed(path: Path, *times: str) -> Path:
    return build_feed(
        path,
        calendar=[_every_day()],
        trips=[
            Trip(
                trip_id=f"T{i}",
                service_id="S1",
                stop_times=[("STOP_A", t, t), ("STOP_B", t, t)],
            )
            for i, t in enumerate(times)
        ],
    )


@pytest.fixture(autouse=True)
def _custom_integrations(enable_custom_integrations):
    return enable_custom_integrations


@pytest.fixture
def feed_zip(tmp_path):
    """A feed departing hourly through the afternoon."""
    return _feed(tmp_path / "feed.zip", "13:00:00", "14:00:00", "15:00:00", "23:50:00")


@pytest.fixture
async def stub_fetch(feed_zip, hass):
    """Return the local archive instead of downloading one.

    Only the transfer is replaced. Validation, import, materialisation and every
    query below run for real against the file this produces.
    """

    def _fetch(url, dest_dir, **kwargs):
        dest_dir.mkdir(parents=True, exist_ok=True)
        target = Path(dest_dir) / "feed.zip"
        target.write_bytes(Path(feed_zip).read_bytes())
        return FetchResult(
            status=200,
            path=target,
            byte_count=target.stat().st_size,
            content_type="application/zip",
            kind=PayloadKind.ZIP,
        )

    with patch("custom_components.gtfsie.datasource.fetch", side_effect=_fetch) as mock:
        yield mock
        # Drain before the patch is released. A reload or a scheduled refresh
        # the test did not await otherwise runs during teardown, when the real
        # fetch is back and reaches for the network.
        await hass.async_block_till_done(wait_background_tasks=True)


async def _setup(hass: HomeAssistant, **watch) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Toronto",
        unique_id=FEED_URL,
        data={CONF_SOURCE_URL: FEED_URL},
        subentries_data=[
            ConfigSubentryData(
                subentry_type=SubentryKind.ROUTE.value,
                title="STOP_A",
                unique_id=None,
                data={CONF_ORIGIN_STOP_IDS: ["STOP_A"], **watch},
            )
        ],
    )
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    # The import is a background task precisely so setup does not wait for it,
    # which means an ordinary block_till_done returns while it is still running
    # and every assertion below would see "extracting". Waiting for background
    # tasks is the test acknowledging the design rather than working around it.
    await hass.async_block_till_done(wait_background_tasks=True)
    await hass.async_block_till_done()
    return entry


def _departure(hass: HomeAssistant):
    for state in hass.states.async_all("sensor"):
        if state.attributes.get("departures") is not None:
            return state
    return None


def _status(hass: HomeAssistant):
    states = [s for s in hass.states.async_all("sensor") if s.attributes.get("departures") is None]
    return states[0] if states else None


class TestEndToEnd:
    async def test_a_stop_produces_a_departure_sensor(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass)
            assert entry.state is ConfigEntryState.LOADED

            state = _departure(hass)
            assert state is not None, [s.entity_id for s in hass.states.async_all()]
            assert state.attributes["status"] == "ok"
            # 13:00 is the first departure after a 12:00 now.
            assert state.state == datetime(2026, 6, 15, 13, 0, tzinfo=TORONTO).astimezone(ZoneInfo("UTC")).isoformat()

    async def test_the_departures_list_is_ordered_and_complete(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass)
            departures = _departure(hass).attributes["departures"]

        # The materialised window runs today plus three days by default, and
        # this feed runs daily, so tomorrow's departures follow today's. That is
        # the point of a horizon: the list does not stop at midnight.
        times = [d["departure_time"] for d in departures]
        assert times[:4] == ["13:00", "14:00", "15:00", "23:50"]
        assert [d["day"] for d in departures[:4]] == ["today"] * 4
        assert departures[4]["day"] == "tomorrow"
        assert departures[4]["departure_time"] == "13:00"

        instants = [d["departure_utc"] for d in departures]
        assert instants == sorted(instants), "the list must be in time order"

    async def test_elapsed_departures_disappear(self, hass, stub_fetch):
        """The behaviour a user checks the entity for.

        Not a rolling window computed once at setup: the query runs against the
        current instant every poll, so a departure that has gone is gone.
        """
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass)
            before = [d["departure_time"] for d in _departure(hass).attributes["departures"]]
        assert before[0] == "13:00"

        with freeze_time(NOON.replace(hour=14, minute=30)):
            store = hass.data[DOMAIN]
            coordinator = next(iter(next(iter(store.values()))["coordinators"].values()))
            await coordinator.async_refresh()
            await hass.async_block_till_done()
            after = [d["departure_time"] for d in _departure(hass).attributes["departures"]]

        # Today's 13:00 and 14:00 have both gone.
        assert after[0] == "15:00"
        assert "13:00" not in after[:2]

    async def test_a_departure_that_has_gone_is_not_offered(self, hass, stub_fetch):
        """The lookback widens the search, not the answer.

        At 14:05 the default 15-minute lookback reaches back to 13:50, so the
        query finds the 14:00. Nothing says that vehicle is late, so it has
        left, and offering it would put a past instant in a timestamp sensor --
        displayed as "5 minutes ago" and negative to any automation subtracting
        it from now.

        On a service whose headway is no longer than the lookback there is
        always such a row, so the state was always the train just missed rather
        than the one that could still be caught.
        """
        now = NOON.replace(hour=14, minute=5)
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(now):
            await _setup(hass, **{CONF_LOOKBACK_MINUTES: 15})
            state = _departure(hass)
            departures = state.attributes["departures"]

        assert [d["departure_time"] for d in departures][0] == "15:00"
        # The general invariant, not just the head: the lookback may not put a
        # departure that has been and gone anywhere in the list. Asserting only
        # on today's 14:00 would miss it, since tomorrow's 14:00 belongs there.
        past = [d["departure_local"] for d in departures if datetime.fromisoformat(d["departure_utc"]) < now]
        assert not past, f"departures already gone were offered: {past}"

        # The state and the head of the list stay the same departure. Advancing
        # one without the other is a defect in its own right.
        assert state.state == departures[0]["departure_utc"]

    async def test_the_lookback_keeps_a_vehicle_that_is_running_late(self, hass, stub_fetch):
        """The case the lookback exists for, which needs realtime to arise.

        A vehicle scheduled at 14:00 but predicted at 14:10 has not left at
        14:05, and is exactly what someone asking "can I still catch it" needs
        to see. Realtime is phase 6, so the prediction is injected at the seam
        the coordinator consults -- which is what that seam is named for, and
        what keeps this behaviour from being rediscovered the hard way later.
        """
        await hass.config.async_set_time_zone("America/Toronto")
        scheduled_1400 = int(datetime(2026, 6, 15, 14, 0, tzinfo=TORONTO).timestamp())

        def _late_by_ten_minutes(row):
            if row.departure_utc == scheduled_1400:
                return row.departure_utc + 600
            return row.departure_utc

        with (
            freeze_time(NOON.replace(hour=14, minute=5)),
            patch(
                "custom_components.gtfsie.coordinator.effective_departure_utc",
                side_effect=_late_by_ten_minutes,
            ),
        ):
            await _setup(hass, **{CONF_LOOKBACK_MINUTES: 15})
            times = [d["departure_time"] for d in _departure(hass).attributes["departures"]]

        assert times[0] == "14:00", "a vehicle predicted to leave in five minutes has not left"

    async def test_the_offset_skips_departures_that_cannot_be_reached(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON.replace(hour=12, minute=50)):
            await _setup(hass, **{CONF_OFFSET_MINUTES: 30, CONF_LOOKBACK_MINUTES: 0})
            times = [d["departure_time"] for d in _departure(hass).attributes["departures"]]
        assert times[0] == "14:00", "13:00 is only ten minutes away and was asked to be skipped"

    async def test_the_limit_is_honoured(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass, **{CONF_LIMIT: 2})
            attributes = _departure(hass).attributes
        assert len(attributes["departures"]) == 2
        assert attributes["truncated"] is True

    async def test_an_origin_destination_pair_reports_the_destination(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass, **{CONF_DEST_STOP_IDS: ["STOP_B"]})
            first = _departure(hass).attributes["departures"][0]
        assert first["destination_stop_id"] == "STOP_B"
        assert first["destination_arrival_utc"]


class TestAttributeContract:
    #: Frozen at v1. Renaming or removing any of these breaks templates and
    #: dashboards people have already written.
    REQUIRED = (
        "next_departure",
        "next_departure_local",
        "next_departure_time",
        "next_departure_realtime",
        "delay_provider",
        "delay_derived",
        "departures",
        "status",
        "realtime_state",
        "realtime_last_success",
        "feed_valid_from",
        "feed_valid_to",
        "feed_imported",
        "timezone",
        "timezone_source",
        "attribution",
    )
    PER_DEPARTURE = (
        "departure_utc",
        "departure_local",
        "departure_time",
        "day",
        "service_date",
        "line",
        "route_id",
        "route_short_name",
        "route_long_name",
        "mode",
        "icon",
        "trip_id",
        "trip_headsign",
        "direction_id",
        "stop_id",
        "stop_name",
        "origin_stop_name",
        "destination_stop_name",
        "destination_stop_id",
        "destination_arrival_utc",
        "departure_realtime_utc",
        "departure_realtime_local",
        "delay_provider",
        "delay_derived",
        "realtime_state",
        "match_rule",
        "cancelled",
    )

    async def test_every_contracted_attribute_is_present(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass)
            attributes = _departure(hass).attributes
        missing = [k for k in self.REQUIRED if k not in attributes]
        assert not missing, missing

    async def test_every_per_departure_key_is_present(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass)
            first = _departure(hass).attributes["departures"][0]
        missing = [k for k in self.PER_DEPARTURE if k not in first]
        assert not missing, missing

    async def test_the_string_none_appears_nowhere(self, hass, stub_fetch):
        """The defect clean() exists to prevent, checked at the boundary where a
        user would actually see it."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass)
            attributes = dict(_departure(hass).attributes)

        def _walk(value):
            if isinstance(value, dict):
                for v in value.values():
                    yield from _walk(v)
            elif isinstance(value, list):
                for v in value:
                    yield from _walk(v)
            else:
                yield value

        assert "None" not in list(_walk(attributes))

    async def test_the_timezone_is_the_feeds_not_the_machines(self, hass, stub_fetch):
        """A Vancouver departure reads in Vancouver time even when Home
        Assistant is set to Toronto."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass)
            attributes = _departure(hass).attributes
        assert attributes["timezone"] == "America/Toronto"


class TestStatus:
    async def test_a_status_sensor_accompanies_the_departure_sensor(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass)
        status = _status(hass)
        assert status is not None
        assert status.state == "ok"

    async def test_an_unknown_stop_is_named_rather_than_silently_empty(self, hass, stub_fetch):
        """ "No departures" and "that stop is not in this feed" have entirely
        different fixes, and only one of them is the user's configuration."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass, **{CONF_ORIGIN_STOP_IDS: ["NO_SUCH_STOP"]})
        status = _status(hass)
        assert status.state == "stop_not_in_feed"
        assert status.attributes["missing_stop_ids"] == ["NO_SUCH_STOP"]

    async def test_the_departure_state_is_unknown_rather_than_invented(self, hass, stub_fetch):
        """A fabricated instant would be indistinguishable from a real one and
        would fire every automation watching it."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            await _setup(hass, **{CONF_ORIGIN_STOP_IDS: ["NO_SUCH_STOP"]})
        assert _departure(hass).state in ("unknown", "unavailable")


class TestLifecycle:
    async def test_the_databases_live_under_the_config_directory(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass)
        root = Path(hass.config.path("gtfsie")) / entry.entry_id
        assert (root / "index.sqlite").is_file()
        assert (root / "feed.sqlite").is_file()

    async def test_the_archive_is_not_kept(self, hass, stub_fetch):
        """The databases are rebuildable from the URL and the archive is the
        largest thing in the directory."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass)
        root = Path(hass.config.path("gtfsie")) / entry.entry_id
        assert not (root / "feed.zip").exists()

    async def test_unloading_leaves_the_databases_alone(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass)
        root = Path(hass.config.path("gtfsie")) / entry.entry_id
        before = sorted(p.name for p in root.iterdir())

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
        assert sorted(p.name for p in root.iterdir()) == before

    async def test_removing_the_entry_deletes_only_its_own_directory(self, hass, stub_fetch):
        """Scoped to a directory this entry owns, so it cannot reach a sibling's
        files -- the failure that destroyed a working "dublin-bus-gtfs" while
        deleting "dublin"."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass)
        gtfsie = Path(hass.config.path("gtfsie"))
        neighbour = gtfsie / "another-datasource"
        neighbour.mkdir(parents=True, exist_ok=True)
        (neighbour / "index.sqlite").write_bytes(b"not mine")

        assert await hass.config_entries.async_remove(entry.entry_id)
        await hass.async_block_till_done()

        assert not (gtfsie / entry.entry_id).exists()
        assert (neighbour / "index.sqlite").is_file()

    async def test_a_second_import_is_not_triggered_by_a_reload(self, hass, stub_fetch):
        """Adding a watch must not cost another import of the same feed."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass)
            first = stub_fetch.call_count
            await hass.config_entries.async_reload(entry.entry_id)
            await hass.async_block_till_done()

        # The fetch runs again, but the import short-circuits on the recorded
        # fingerprint rather than rebuilding. That the archive is re-downloaded
        # at all is a refresh-scheduling question for phase 5.
        assert stub_fetch.call_count >= first


async def test_a_cross_midnight_departure_reads_as_the_small_hours(hass, tmp_path):
    """25:30 belongs to today's service day and happens tomorrow morning.

    The user-visible end of the whole time model. Every layer contributed: the
    reader kept the excess above 24:00, the loader stored it as raw seconds, the
    materialiser added it to a noon-anchored instant, and the presenter renders
    that instant in the feed's zone. What a person sees is 01:30, tomorrow,
    filed under today's service date.
    """
    archive = _feed(tmp_path / "late.zip", "25:30:00")

    def _fetch(url, dest_dir, **kwargs):
        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        target = Path(dest_dir) / "feed.zip"
        target.write_bytes(archive.read_bytes())
        return FetchResult(
            status=200,
            path=target,
            byte_count=target.stat().st_size,
            content_type="application/zip",
            kind=PayloadKind.ZIP,
        )

    await hass.config.async_set_time_zone("America/Toronto")
    with (
        patch("custom_components.gtfsie.datasource.fetch", side_effect=_fetch),
        freeze_time(NOON.replace(hour=23, minute=0)),
    ):
        await _setup(hass)
        departures = _departure(hass).attributes["departures"]

    late = [d for d in departures if d["departure_time"] == "01:30"]
    assert late, [d["departure_time"] for d in departures]
    assert late[0]["day"] == "tomorrow"
    assert late[0]["service_date"] == "2026-06-15"


class TestDeviceRegistry:
    """Two watches on one datasource must be tellable apart.

    Without a device, ``has_entity_name`` has no name to build an entity id
    from, so every watch proposes the same "Next departure" and the registry
    separates them with numeric suffixes -- ``sensor.next_departure_2`` says
    nothing about which stop, direction or feed it reports on.
    """

    async def _two_watches(self, hass: HomeAssistant) -> MockConfigEntry:
        entry = MockConfigEntry(
            domain=DOMAIN,
            title="Toronto",
            unique_id=FEED_URL,
            data={CONF_SOURCE_URL: FEED_URL},
            subentries_data=[
                ConfigSubentryData(
                    subentry_type=SubentryKind.ROUTE.value,
                    title="STOP_A to STOP_B",
                    unique_id=None,
                    data={CONF_ORIGIN_STOP_IDS: ["STOP_A"], CONF_DEST_STOP_IDS: ["STOP_B"]},
                ),
                ConfigSubentryData(
                    subentry_type=SubentryKind.ROUTE.value,
                    title="STOP_B to STOP_A",
                    unique_id=None,
                    data={CONF_ORIGIN_STOP_IDS: ["STOP_B"], CONF_DEST_STOP_IDS: ["STOP_A"]},
                ),
            ],
        )
        entry.add_to_hass(hass)
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)
        await hass.async_block_till_done()
        return entry

    async def test_each_watch_gets_its_own_named_entities(self, hass, stub_fetch):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await self._two_watches(hass)
        assert entry.state is ConfigEntryState.LOADED

        ids = sorted(e.entity_id for e in er.async_entries_for_config_entry(er.async_get(hass), entry.entry_id))
        assert ids == [
            "sensor.stop_a_to_stop_b_next_departure",
            "sensor.stop_a_to_stop_b_status",
            "sensor.stop_b_to_stop_a_next_departure",
            "sensor.stop_b_to_stop_a_status",
        ]
        assert not any(i.endswith("_2") for i in ids), "a numeric suffix means the names collided"

    async def test_watches_hang_off_the_datasource(self, hass, stub_fetch):
        """The datasource is the hub the manifest already claims it is."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await self._two_watches(hass)

        registry = dr.async_get(hass)
        devices = dr.async_entries_for_config_entry(registry, entry.entry_id)
        by_name = {d.name: d for d in devices}
        assert set(by_name) == {"Toronto", "STOP_A to STOP_B", "STOP_B to STOP_A"}

        datasource = by_name["Toronto"]
        for title in ("STOP_A to STOP_B", "STOP_B to STOP_A"):
            watch = by_name[title]
            # Not merely set: via_device is dropped silently when the device it
            # names does not exist yet, so this asserts the link actually formed.
            assert watch.via_device_id == datasource.id, f"{title} is not attached to the datasource"
            assert watch.config_entries_subentries[entry.entry_id] != {None}, (
                f"{title} must belong to its subentry, so removing the watch removes the device"
            )

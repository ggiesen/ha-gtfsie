"""Refresh, the nightly window roll, and the guard that protects live data.

The behaviour that decides whether this is something you can leave running. A
materialised window covers a fixed range of dates, so it goes stale by time
passing alone: without a roll the integration stops producing departures a few
days after it is installed, and says so correctly while being useless.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from freezegun import freeze_time
from homeassistant.config_entries import ConfigSubentryData
from homeassistant.core import HomeAssistant
from pygtfsie.db.schema_index import open_for_read, read_meta
from pygtfsie.ingest.download import FetchResult
from pygtfsie.ingest.sniff import PayloadKind
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.gtfsie.const import (
    CONF_ORIGIN_STOP_IDS,
    CONF_SOURCE_URL,
    DOMAIN,
    SubentryKind,
)

from .fixtures.gtfs_feed import CalendarEntry, Trip, build_feed

TORONTO = ZoneInfo("America/Toronto")
FEED_URL = "https://transit.example/gtfs.zip"
NOON = datetime(2026, 6, 15, 12, 0, tzinfo=TORONTO)


def _calendar(start: str = "", end: str = "") -> CalendarEntry:
    return CalendarEntry(
        service_id="S1",
        monday=1,
        tuesday=1,
        wednesday=1,
        thursday=1,
        friday=1,
        saturday=1,
        sunday=1,
        start_date=start,
        end_date=end,
    )


def _feed(path: Path, *, start: str = "", end: str = "", time: str = "13:00:00") -> Path:
    return build_feed(
        path,
        calendar=[_calendar(start, end)],
        trips=[Trip(trip_id="T0", service_id="S1", stop_times=[("STOP_A", time, time), ("STOP_B", time, time)])],
    )


@pytest.fixture(autouse=True)
def _custom_integrations(enable_custom_integrations):
    return enable_custom_integrations


class Serving:
    """A stub download that can be pointed at a different archive mid-test."""

    def __init__(self, archive: Path) -> None:
        self.archive = archive
        self.calls = 0

    def __call__(self, url, dest_dir, **kwargs):
        self.calls += 1
        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        target = Path(dest_dir) / "feed.zip"
        target.write_bytes(self.archive.read_bytes())
        return FetchResult(
            status=200,
            path=target,
            byte_count=target.stat().st_size,
            content_type="application/zip",
            kind=PayloadKind.ZIP,
        )


@pytest.fixture
def serving(tmp_path):
    return Serving(_feed(tmp_path / "feed.zip"))


async def _setup(hass: HomeAssistant, serving: Serving) -> MockConfigEntry:
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
                data={CONF_ORIGIN_STOP_IDS: ["STOP_A"]},
            )
        ],
    )
    entry.add_to_hass(hass)
    with patch("custom_components.gtfsie.datasource.fetch", side_effect=serving):
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)
        await hass.async_block_till_done()
    return entry


def _datasource(hass: HomeAssistant, entry):
    return hass.data[DOMAIN][entry.entry_id]["datasource"]


def _window_end(hass: HomeAssistant, entry) -> str:
    return read_meta(open_for_read(_datasource(hass, entry).paths.index))["window_end_date"]


class TestStartup:
    async def test_a_first_setup_downloads_and_builds(self, hass, serving):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass, serving)
        assert serving.calls == 1
        assert _datasource(hass, entry).state == "ready"

    async def test_a_restart_with_current_data_downloads_nothing(self, hass, serving):
        """Fetching on every restart would be slow and rude to the agency.

        A machine that reboots twice in an evening has no new feed to collect,
        and the scheduled refresh exists precisely so startup does not have to
        guess.
        """
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass, serving)
            assert serving.calls == 1

            with patch("custom_components.gtfsie.datasource.fetch", side_effect=serving):
                await hass.config_entries.async_reload(entry.entry_id)
                await hass.async_block_till_done(wait_background_tasks=True)

        assert serving.calls == 1, "a restart with a current window re-downloaded the feed"

    async def test_a_stale_window_is_rolled_without_downloading(self, hass, serving):
        """The case that makes this usable at all.

        A machine off for a week comes back with a window that ended days ago.
        Rebuilding the index needs no network: feed.sqlite is still perfectly
        good, and only the range of dates has moved.
        """
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass, serving)
            first_end = _window_end(hass, entry)
            assert serving.calls == 1

        later = NOON + timedelta(days=10)
        with freeze_time(later), patch("custom_components.gtfsie.datasource.fetch", side_effect=serving):
            await hass.config_entries.async_reload(entry.entry_id)
            await hass.async_block_till_done(wait_background_tasks=True)

        assert serving.calls == 1, "the window roll downloaded the feed"
        assert _window_end(hass, entry) > first_end, "the window did not move"

    async def test_the_rolled_window_covers_the_new_today(self, hass, serving):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass, serving)

        later = NOON + timedelta(days=10)
        with freeze_time(later), patch("custom_components.gtfsie.datasource.fetch", side_effect=serving):
            await hass.config_entries.async_reload(entry.entry_id)
            await hass.async_block_till_done(wait_background_tasks=True)
            assert _datasource(hass, entry).window_covers_today is True

    async def test_departures_reappear_after_a_roll(self, hass, serving):
        """The user-visible point: it still works ten days later."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass, serving)

        later = NOON + timedelta(days=10)
        with freeze_time(later), patch("custom_components.gtfsie.datasource.fetch", side_effect=serving):
            await hass.config_entries.async_reload(entry.entry_id)
            await hass.async_block_till_done(wait_background_tasks=True)
            await hass.async_block_till_done()
            states = [s for s in hass.states.async_all("sensor") if s.attributes.get("departures")]

        assert states, "no departures after the window rolled"
        assert states[0].attributes["status"] == "ok"


class TestFutureDatedGuard:
    async def test_a_future_dated_feed_does_not_replace_a_working_one(self, hass, serving, tmp_path):
        """A publisher posting next season early must not empty a live feed.

        The user's first sign would otherwise be a sensor with no departures and
        no error, weeks before the new timetable starts.
        """
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass, serving)
            before = _window_end(hass, entry)

            serving.archive = _feed(
                tmp_path / "next-season.zip",
                start="20270101",
                end="20271231",
                time="09:00:00",
            )
            with patch("custom_components.gtfsie.datasource.fetch", side_effect=serving):
                await _datasource(hass, entry).async_refresh(reason="test")
                await hass.async_block_till_done()

            conn = open_for_read(_datasource(hass, entry).paths.index)
            times = [r[0] for r in conn.execute("SELECT departure_utc FROM departure LIMIT 1")]

        assert serving.calls == 2, "the refresh should have downloaded and inspected it"
        assert times, "the existing timetable was destroyed by a future-dated feed"
        del before

    async def test_a_first_import_of_a_future_dated_feed_is_allowed(self, hass, tmp_path):
        """Nothing to lose, and refusing would make a seasonal feed impossible
        to add at all -- a ski shuttle published in October for December."""
        await hass.config.async_set_time_zone("America/Toronto")
        serving = Serving(_feed(tmp_path / "season.zip", start="20270101", end="20271231"))
        with freeze_time(NOON):
            entry = await _setup(hass, serving)
        assert _datasource(hass, entry).state == "ready"

    async def test_a_candidate_never_touches_the_live_feed_until_accepted(self, hass, serving, tmp_path):
        """Loading into a candidate file is what makes refusal possible.

        The guard runs against real parsed dates, which means the archive has to
        be loaded first -- and loading it over the live database would destroy
        the thing being protected before the decision was made.
        """
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass, serving)
            paths = _datasource(hass, entry).paths
            feed_before = paths.feed.read_bytes()

            serving.archive = _feed(tmp_path / "future.zip", start="20270101", end="20271231")
            with patch("custom_components.gtfsie.datasource.fetch", side_effect=serving):
                await _datasource(hass, entry).async_refresh(reason="test")
                await hass.async_block_till_done()

        assert paths.feed.read_bytes() == feed_before
        assert not paths.feed_candidate.exists(), "the rejected candidate was left behind"


class TestSerialisation:
    async def test_two_refreshes_do_not_overlap(self, hass, serving):
        """One writer per datasource, so a manual call and the schedule cannot
        collide. This is what makes "database is locked" unreachable rather than
        merely unlikely."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass, serving)
            datasource = _datasource(hass, entry)
            before = serving.calls

            with patch("custom_components.gtfsie.datasource.fetch", side_effect=serving):
                await asyncio.gather(
                    datasource.async_refresh(reason="a"),
                    datasource.async_refresh(reason="b"),
                )
                await hass.async_block_till_done()

        # The second call finds the lock held and returns rather than queueing,
        # because a refresh that has just run again immediately has nothing to
        # add.
        assert serving.calls == before + 1


class TestScheduling:
    async def test_setup_registers_a_roll_and_a_refresh(self, hass, serving):
        """Both schedules exist, and both are registered through the entry so
        Home Assistant cancels them even if setup fails partway."""
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass, serving)
        assert len(entry._on_unload or []) >= 2

    async def test_unloading_leaves_no_timers_behind(self, hass, serving):
        await hass.config.async_set_time_zone("America/Toronto")
        with freeze_time(NOON):
            entry = await _setup(hass, serving)
            assert await hass.config_entries.async_unload(entry.entry_id)
            await hass.async_block_till_done()
        # A lingering timer fails the test in teardown, so reaching here is the
        # assertion. Stated explicitly so the test reads as deliberate.
        assert entry.state.name == "NOT_LOADED"

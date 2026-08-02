"""The fixture builders themselves: do they produce what they claim to?

There is no integration code yet, so these are the only tests here. They are
worth having anyway. Every later test in this repository states its case through
these builders, so a builder that quietly emits a malformed archive or an
unparseable protobuf would make a whole suite agree with itself while testing
nothing -- and that failure is invisible, because the tests still pass.

Nothing here touches the network or the clock.
"""

from __future__ import annotations

import zipfile

from .fixtures.gtfs_feed import CalendarEntry, Stop, Trip, build_feed
from .fixtures.gtfs_rt import (
    StopTimeUpdate,
    TripUpdate,
    build_alerts,
    build_trip_updates,
)

#: The files a consumer is entitled to assume are present. calendar.txt is not
#: among them -- a feed may express service entirely through calendar_dates.txt --
#: which is why it is asserted separately where a test asks for one.
REQUIRED_MEMBERS = {
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
}


def _every_day(service_id: str = "S1") -> CalendarEntry:
    return CalendarEntry(
        service_id=service_id,
        monday=1,
        tuesday=1,
        wednesday=1,
        thursday=1,
        friday=1,
        saturday=1,
        sunday=1,
    )


class TestBuildFeed:
    def test_produces_a_readable_zip_with_the_required_members(self, tmp_path):
        path = build_feed(tmp_path / "feed.zip", calendar=[_every_day()])
        assert zipfile.is_zipfile(path), "the builder did not produce a valid zip"
        with zipfile.ZipFile(path) as archive:
            assert archive.testzip() is None, "the archive reports a corrupt member"
            names = set(archive.namelist())
        assert REQUIRED_MEMBERS <= names, f"missing {REQUIRED_MEMBERS - names}"

    def test_members_are_at_the_archive_root(self, tmp_path):
        """No wrapping directory.

        A feed whose files sit under a top-level folder is a real and separate
        shape that some publishers emit, and it needs its own fixture and its own
        handling. What matters here is that the *default* builder does not
        silently produce it, because then every test would be exercising the
        nested case without saying so.
        """
        path = build_feed(tmp_path / "feed.zip", calendar=[_every_day()])
        with zipfile.ZipFile(path) as archive:
            nested = [n for n in archive.namelist() if "/" in n]
        assert not nested, f"members are nested under a directory: {nested}"

    def test_a_cross_midnight_stop_time_survives_the_round_trip(self, tmp_path):
        """A 25:30 departure must reach the archive unmangled.

        If the builder normalised times past 24:00 into ordinary clock times,
        every cross-midnight test in this repository would be testing an
        ordinary daytime departure while claiming otherwise.
        """
        path = build_feed(
            tmp_path / "feed.zip",
            calendar=[_every_day()],
            trips=[
                Trip(
                    trip_id="T1",
                    service_id="S1",
                    stop_times=[
                        ("STOP_A", "25:30:00", "25:30:00"),
                        ("STOP_B", "25:45:00", "25:45:00"),
                    ],
                )
            ],
        )
        with zipfile.ZipFile(path) as archive:
            body = archive.read("stop_times.txt").decode("utf-8")
        assert "25:30:00" in body, f"the 25:30 departure did not survive:\n{body}"

    def test_the_declared_timezone_reaches_agency_txt(self, tmp_path):
        path = build_feed(
            tmp_path / "feed.zip",
            agency_timezone="Pacific/Chatham",
            calendar=[_every_day()],
        )
        with zipfile.ZipFile(path) as archive:
            assert "Pacific/Chatham" in archive.read("agency.txt").decode("utf-8")

    def test_a_calendar_dates_only_feed_omits_calendar_txt(self, tmp_path):
        """Service expressed only as explicit dates is legal GTFS.

        Pinned because the builder has to be able to *not* write calendar.txt;
        if it always wrote one, the calendar-dates-only path could never be
        tested.
        """
        path = build_feed(
            tmp_path / "feed.zip",
            calendar=None,
            calendar_dates=[("S1", "20260615", 1)],
        )
        with zipfile.ZipFile(path) as archive:
            names = set(archive.namelist())
        assert "calendar.txt" not in names
        assert "calendar_dates.txt" in names

    def test_stops_carry_the_coordinates_they_were_given(self, tmp_path):
        path = build_feed(
            tmp_path / "feed.zip",
            calendar=[_every_day()],
            stops=[
                Stop("STOP_A", "Origin", 47.5605, -52.7128),
                Stop("STOP_B", "Destination", 47.5730, -52.7050),
            ],
        )
        with zipfile.ZipFile(path) as archive:
            body = archive.read("stops.txt").decode("utf-8")
        assert "47.5605" in body and "-52.7128" in body


class TestBuildRealtime:
    def test_trip_updates_decode_back_to_what_went_in(self, tmp_path):
        """Round trip through the protobuf encoder.

        Asserting on the bytes would only prove the encoder is deterministic.
        Decoding proves the payload says what the test meant, which is the thing
        every realtime test downstream depends on.
        """
        from google.transit import gtfs_realtime_pb2

        payload = build_trip_updates(
            [
                TripUpdate(
                    trip_id="T1",
                    route_id="R1",
                    stop_time_updates=[
                        StopTimeUpdate(
                            stop_id="STOP_A", stop_sequence=1, departure_delay=120
                        ),
                    ],
                ),
            ]
        )
        message = gtfs_realtime_pb2.FeedMessage()
        message.ParseFromString(payload)

        assert len(message.entity) == 1
        update = message.entity[0].trip_update
        assert update.trip.trip_id == "T1"
        assert update.stop_time_update[0].departure.delay == 120

    def test_an_empty_feed_is_still_a_valid_feed(self):
        """Zero entities is a normal answer, not a malformed response.

        Overnight, and on any agency between service periods, a realtime feed
        legitimately carries no entities. It has to parse.
        """
        from google.transit import gtfs_realtime_pb2

        message = gtfs_realtime_pb2.FeedMessage()
        message.ParseFromString(build_trip_updates([]))
        assert list(message.entity) == []
        assert message.header.gtfs_realtime_version

    def test_alerts_decode(self):
        from google.transit import gtfs_realtime_pb2

        message = gtfs_realtime_pb2.FeedMessage()
        message.ParseFromString(build_alerts([("R1", "Elevator out of service")]))
        assert len(message.entity) == 1

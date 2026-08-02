"""Build GTFS-Realtime protobuf feeds for tests.

Realtime is the largest single category in the issue tracker -- 42 of 149 issues --
and the least testable by hand. From issue #163:

    "I am stuck, I need to know what the Realtime kicks back ... I asked for dev
    access with translink but not sure why this takes time"

Verifying realtime behaviour currently means holding credentials for an agency's
API, waiting for that agency to produce the situation of interest (a delay, a
cancellation, a trip crossing midnight), and catching it while it lasts. For
several of the open reports the maintainer does not have access at all and is
asking users to paste debug output.

None of that is necessary. GTFS-RT is protobuf with a published schema, and
``gtfs-realtime-bindings`` is already a declared dependency of this integration.
A feed describing any situation can be constructed in a few lines and serialised
to bytes, which makes every reported scenario reproducible on demand and offline.

These builders deliberately produce *wire format* bytes rather than the parsed
objects, so tests exercise the real parse path, including the response handling in
``get_gtfs_feed_entities``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

from google.transit import gtfs_realtime_pb2


@dataclass
class StopTimeUpdate:
    """One stop within a trip update.

    ``delay`` is seconds, positive for late. ``time`` is an absolute POSIX
    timestamp. Real feeds supply one, the other, or both, and disagree about
    which -- part of why realtime handling is awkward.
    """

    stop_id: str
    stop_sequence: int = 1
    arrival_delay: int | None = None
    arrival_time: int | None = None
    departure_delay: int | None = None
    departure_time: int | None = None


@dataclass
class TripUpdate:
    trip_id: str
    route_id: str = "R1"
    direction_id: int = 0
    start_time: str = ""
    start_date: str = ""
    schedule_relationship: str | None = None  # SCHEDULED, CANCELED, ADDED
    stop_time_updates: list[StopTimeUpdate] = field(default_factory=list)


@dataclass
class VehiclePosition:
    trip_id: str
    latitude: float
    longitude: float
    vehicle_id: str = "V1"
    route_id: str = "R1"
    direction_id: int = 0
    bearing: float | None = None


def build_trip_updates(
    updates: list[TripUpdate], *, timestamp: int | None = None, version: str = "2.0"
) -> bytes:
    """Serialise trip updates to GTFS-RT wire format."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = version
    feed.header.timestamp = timestamp if timestamp is not None else int(time.time())
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET

    for index, update in enumerate(updates, start=1):
        entity = feed.entity.add()
        entity.id = f"entity-{index}"
        entity.trip_update.trip.trip_id = update.trip_id
        entity.trip_update.trip.route_id = update.route_id
        entity.trip_update.trip.direction_id = update.direction_id
        if update.start_time:
            entity.trip_update.trip.start_time = update.start_time
        if update.start_date:
            entity.trip_update.trip.start_date = update.start_date
        if update.schedule_relationship:
            entity.trip_update.trip.schedule_relationship = (
                gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Value(
                    update.schedule_relationship
                )
            )
        for stop in update.stop_time_updates:
            stu = entity.trip_update.stop_time_update.add()
            stu.stop_id = stop.stop_id
            stu.stop_sequence = stop.stop_sequence
            if stop.arrival_delay is not None:
                stu.arrival.delay = stop.arrival_delay
            if stop.arrival_time is not None:
                stu.arrival.time = stop.arrival_time
            if stop.departure_delay is not None:
                stu.departure.delay = stop.departure_delay
            if stop.departure_time is not None:
                stu.departure.time = stop.departure_time

    return feed.SerializeToString()


def build_vehicle_positions(
    positions: list[VehiclePosition], *, timestamp: int | None = None
) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = timestamp if timestamp is not None else int(time.time())
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET

    for index, position in enumerate(positions, start=1):
        entity = feed.entity.add()
        entity.id = f"vehicle-{index}"
        entity.vehicle.trip.trip_id = position.trip_id
        entity.vehicle.trip.route_id = position.route_id
        entity.vehicle.trip.direction_id = position.direction_id
        entity.vehicle.vehicle.id = position.vehicle_id
        entity.vehicle.position.latitude = position.latitude
        entity.vehicle.position.longitude = position.longitude
        if position.bearing is not None:
            entity.vehicle.position.bearing = position.bearing

    return feed.SerializeToString()


def build_alerts(alerts: list[tuple[str, str]]) -> bytes:
    """``alerts`` is a list of (header_text, description_text)."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(time.time())
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET

    for index, (header_text, description) in enumerate(alerts, start=1):
        entity = feed.entity.add()
        entity.id = f"alert-{index}"
        translation = entity.alert.header_text.translation.add()
        translation.text = header_text
        translation.language = "en"
        body = entity.alert.description_text.translation.add()
        body.text = description
        body.language = "en"

    return feed.SerializeToString()


def write_feed(path: str | Path, payload: bytes) -> str:
    """Write a feed to disk and return a ``file://`` URL for it.

    ``get_gtfs_feed_entities`` mounts a local adapter for ``file://`` URLs, so a
    test can exercise the whole fetch-parse path with no HTTP mocking and no
    network. That local-file support exists in the integration for users debugging
    their own captures; it turns out to be exactly what a test harness wants.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return f"file://{path}"

"""Build small, purpose-made GTFS feeds for tests.

Every test in this suite states the feed it needs rather than relying on a
checked-in archive. A real transit feed is tens of megabytes and changes
underneath you when the agency republishes; the failures we care about are
reproducible from a handful of rows.

The feeds produced here are fed through real ``pygtfs`` rather than mocked, so
the tests exercise the SQL this integration actually issues. That matters:
several reported bugs (#164, #166, #36) live in the SQL string itself and would
survive any amount of mocking at the database layer.

Defaults describe one route, two stops and one weekday trip, which is the
smallest feed that produces a departure. Tests override only what they are
about::

    build_feed(tmp_path / "f.zip", calendar=None,
               calendar_dates=[("S1", "20260728", 1)])
"""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

# GTFS calls this "noon minus twelve hours"; in practice it is a local clock
# time that may exceed 24:00:00 for trips continuing past midnight.
Time = str


@dataclass
class Stop:
    stop_id: str
    stop_name: str
    stop_lat: float = 43.6532
    stop_lon: float = -79.3832
    # Per-stop timezone is legal GTFS and overrides the agency's. Feeds that set
    # it inconsistently are behind several timezone reports (#107, #140).
    stop_timezone: str = ""


@dataclass
class Route:
    route_id: str = "R1"
    route_short_name: str = "1"
    route_long_name: str = "Test Route"
    # 2 = rail, matching the Metrolinx feeds this integration is most used with.
    route_type: int = 2
    agency_id: str = "A1"


@dataclass
class Trip:
    trip_id: str
    service_id: str
    route_id: str = "R1"
    trip_headsign: str = "Downtown"
    direction_id: int = 0
    trip_short_name: str = ""
    # (stop_id, arrival_time, departure_time); sequence is positional.
    stop_times: list[tuple[str, Time, Time]] = field(default_factory=list)


@dataclass
class CalendarEntry:
    service_id: str = "S1"
    monday: int = 1
    tuesday: int = 1
    wednesday: int = 1
    thursday: int = 1
    friday: int = 1
    saturday: int = 0
    sunday: int = 0
    start_date: str = "20260101"
    end_date: str = "20271231"


DEFAULT_STOPS = [Stop("STOP_A", "Origin"), Stop("STOP_B", "Destination")]
DEFAULT_TRIPS = [
    Trip(
        trip_id="T1",
        service_id="S1",
        stop_times=[("STOP_A", "08:00:00", "08:00:00"), ("STOP_B", "08:30:00", "08:30:00")],
    )
]


def _csv(rows: list[dict], header: list[str]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=header, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


def build_feed(
    path: str | Path,
    *,
    agency_timezone: str = "America/Toronto",
    agency_id: str | None = "A1",
    stops: list[Stop] | None = None,
    routes: list[Route] | None = None,
    trips: list[Trip] | None = None,
    calendar: list[CalendarEntry] | None = "default",  # type: ignore[assignment]
    calendar_dates: list[tuple[str, str, int]] | None = None,
    feed_info: tuple[str, str] | None = None,
) -> Path:
    """Write a GTFS zip and return its path.

    ``calendar`` defaults to a single weekday service. Pass ``None`` to omit
    ``calendar.txt`` entirely, which is what produces a calendar_dates-only feed
    (the shape behind #164, and how several European agencies publish).

    ``calendar_dates`` entries are ``(service_id, YYYYMMDD, exception_type)``
    where 1 means "service added" and 2 means "service removed".

    ``agency_id`` may be set to ``None`` to build a feed with the column absent,
    reproducing the missing-agency handling in #132.

    ``feed_info`` is ``(feed_start_date, feed_end_date)``. Omitted by default,
    since most feeds this integration consumes do not ship it, and its absence
    is why an expired feed fails silently rather than loudly.
    """
    path = Path(path)
    stops = DEFAULT_STOPS if stops is None else stops
    routes = [Route()] if routes is None else routes
    trips = DEFAULT_TRIPS if trips is None else trips
    if calendar == "default":
        calendar = [CalendarEntry()]

    agency_header = ["agency_name", "agency_url", "agency_timezone"]
    agency_row = {
        "agency_name": "Test Transit",
        "agency_url": "https://example.invalid",
        "agency_timezone": agency_timezone,
    }
    if agency_id is not None:
        agency_header.insert(0, "agency_id")
        agency_row["agency_id"] = agency_id

    stop_header = ["stop_id", "stop_name", "stop_lat", "stop_lon"]
    if any(s.stop_timezone for s in stops):
        stop_header.append("stop_timezone")

    files: dict[str, str] = {
        "agency.txt": _csv([agency_row], agency_header),
        "stops.txt": _csv(
            [{k: getattr(s, k) for k in stop_header} for s in stops], stop_header
        ),
        "routes.txt": _csv(
            [
                {
                    "route_id": r.route_id,
                    "agency_id": r.agency_id if agency_id is not None else "",
                    "route_short_name": r.route_short_name,
                    "route_long_name": r.route_long_name,
                    "route_type": r.route_type,
                }
                for r in routes
            ],
            ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
        ),
        "trips.txt": _csv(
            [
                {
                    "route_id": t.route_id,
                    "service_id": t.service_id,
                    "trip_id": t.trip_id,
                    "trip_headsign": t.trip_headsign,
                    "trip_short_name": t.trip_short_name,
                    "direction_id": t.direction_id,
                }
                for t in trips
            ],
            [
                "route_id",
                "service_id",
                "trip_id",
                "trip_headsign",
                "trip_short_name",
                "direction_id",
            ],
        ),
        "stop_times.txt": _csv(
            [
                {
                    "trip_id": t.trip_id,
                    "arrival_time": arrival,
                    "departure_time": departure,
                    "stop_id": stop_id,
                    "stop_sequence": seq,
                }
                for t in trips
                for seq, (stop_id, arrival, departure) in enumerate(t.stop_times, start=1)
            ],
            ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
        ),
    }

    if calendar:
        files["calendar.txt"] = _csv(
            [
                {
                    "service_id": c.service_id,
                    "monday": c.monday,
                    "tuesday": c.tuesday,
                    "wednesday": c.wednesday,
                    "thursday": c.thursday,
                    "friday": c.friday,
                    "saturday": c.saturday,
                    "sunday": c.sunday,
                    "start_date": c.start_date,
                    "end_date": c.end_date,
                }
                for c in calendar
            ],
            [
                "service_id",
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
                "start_date",
                "end_date",
            ],
        )

    if calendar_dates:
        files["calendar_dates.txt"] = _csv(
            [
                {"service_id": sid, "date": date, "exception_type": kind}
                for sid, date, kind in calendar_dates
            ],
            ["service_id", "date", "exception_type"],
        )

    if feed_info:
        start, end = feed_info
        files["feed_info.txt"] = _csv(
            [
                {
                    "feed_publisher_name": "Test Transit",
                    "feed_publisher_url": "https://example.invalid",
                    "feed_lang": "en",
                    "feed_start_date": start,
                    "feed_end_date": end,
                }
            ],
            [
                "feed_publisher_name",
                "feed_publisher_url",
                "feed_lang",
                "feed_start_date",
                "feed_end_date",
            ],
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return path

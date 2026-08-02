"""Turning departure rows into the published attribute set.

The contract in docs/SPEC.md 1.5 is frozen at v1. Renaming or removing any of
these keys breaks templates, dashboards and automations that people have already
written, so it is treated as a public API rather than an implementation detail.

Two rules run through all of it. Every string is ``""`` when the GTFS field is
absent, never ``None`` and never the literal ``"None"``; ``null`` is reserved for
values that are genuinely numeric and genuinely optional. And every instant that
reaches a user is rendered from an absolute epoch second against the feed's own
zone, not the machine's -- a departure in Vancouver reads in Vancouver time even
when Home Assistant is set to Toronto.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pygtfsie.db.queries import DepartureRow
from pygtfsie.helpers.text import route_label, signed_hms

from .const import NO_DATA

_ZONES: dict[str, ZoneInfo] = {}


def zone_for(name: str) -> ZoneInfo:
    """Resolve and cache a zone, falling back to UTC rather than raising.

    A feed naming a zone the tz database does not have would otherwise take
    every one of its departures with it. Wrong by an offset is visible and
    fixable; nothing at all is neither.
    """
    zone = _ZONES.get(name)
    if zone is None:
        try:
            zone = ZoneInfo(name or "UTC")
        except (ZoneInfoNotFoundError, ValueError, OSError):
            zone = ZoneInfo("UTC")
        _ZONES[name] = zone
    return zone


def utc_iso(instant: int | None) -> str:
    return "" if instant is None else datetime.fromtimestamp(instant, tz=ZoneInfo("UTC")).isoformat()


def local_iso(instant: int | None, tz_name: str) -> str:
    return "" if instant is None else datetime.fromtimestamp(instant, tz=zone_for(tz_name)).isoformat()


def local_hhmm(instant: int | None, tz_name: str) -> str:
    """A speakable time, in the feed's zone.

    Past 24:00 is normalised by construction: the instant is absolute, so 25:10
    on a service day renders as ``01:10``, which is what a person would say.
    """
    return "" if instant is None else datetime.fromtimestamp(instant, tz=zone_for(tz_name)).strftime("%H:%M")


def day_label(instant: int, now_utc: int, tz_name: str) -> str:
    """``today``, ``tomorrow``, or an ISO date.

    Relative to the *feed's* zone rather than the machine's, and computed from
    the calendar dates the two instants fall on rather than by dividing their
    difference by 86400 -- which is wrong on the two days a year that are not
    24 hours long, and wrong every day for a departure less than a day away that
    still falls tomorrow.
    """
    zone = zone_for(tz_name)
    when = datetime.fromtimestamp(instant, tz=zone).date()
    today = datetime.fromtimestamp(now_utc, tz=zone).date()
    delta = (when - today).days
    if delta == 0:
        return "today"
    if delta == 1:
        return "tomorrow"
    return when.isoformat()


def _service_date_iso(value: int) -> str:
    try:
        return date(value // 10000, (value // 100) % 100, value % 100).isoformat()
    except ValueError:
        return ""


def departure_payload(row: DepartureRow, now_utc: int) -> dict[str, Any]:
    """One entry of the ``departures`` list."""
    tz = row.tz_name
    return {
        "departure_utc": utc_iso(row.departure_utc),
        "departure_local": local_iso(row.departure_utc, tz),
        "departure_time": local_hhmm(row.departure_utc, tz),
        "day": day_label(row.departure_utc, now_utc, tz),
        "service_date": _service_date_iso(row.service_date),
        "line": route_label(row.route_id, row.route_short_name, row.route_long_name),
        "route_id": row.route_id,
        "route_short_name": row.route_short_name,
        "route_long_name": row.route_long_name,
        "mode": row.mode_name,
        "icon": row.icon,
        "trip_id": row.trip_id,
        "trip_headsign": row.trip_headsign,
        "direction_id": row.direction_id,
        "stop_id": row.stop_id,
        "stop_name": row.stop_name,
        "origin_stop_name": row.stop_name,
        "destination_stop_name": row.dest_stop_name or "",
        "destination_stop_id": row.dest_stop_id or "",
        "destination_arrival_utc": utc_iso(row.dest_arrival_utc),
        # Realtime fields are present and empty rather than absent. A template
        # reading one must not have to test for the key's existence as well as
        # its value, and phase 6 fills them in without changing the shape.
        "departure_realtime_utc": "",
        "departure_realtime_local": "",
        "delay_provider": None,
        "delay_derived": None,
        "realtime_state": "disabled",
        "match_rule": "",
        "cancelled": False,
    }


def state_attributes(
    rows: list[DepartureRow],
    *,
    now_utc: int,
    status: str,
    fallback_tz: str,
    feed_valid_from: str = "",
    feed_valid_to: str = "",
    feed_imported: str = "",
    timezone_source: str = "",
    truncated: bool = False,
    window_end_utc: int | None = None,
) -> dict[str, Any]:
    """The full published attribute set.

    Built even when there are no departures. An entity whose attributes vanish
    when a stop goes quiet breaks every template referencing them, and leaves a
    user unable to tell "nothing runs tonight" from "the integration is broken"
    -- which is what ``status`` exists to answer.
    """
    head = rows[0] if rows else None
    tz = head.tz_name if head else fallback_tz

    return {
        "next_departure": utc_iso(head.departure_utc) if head else "",
        "next_departure_local": local_iso(head.departure_utc, tz) if head else "",
        "next_departure_time": local_hhmm(head.departure_utc, tz) if head else "",
        "next_departure_realtime": "",
        "delay_provider": None,
        "delay_derived": None,
        "departures": [departure_payload(row, now_utc) for row in rows],
        "status": status,
        "realtime_state": "disabled",
        "realtime_last_success": None,
        "feed_valid_from": feed_valid_from or None,
        "feed_valid_to": feed_valid_to or None,
        "feed_imported": feed_imported,
        "timezone": tz,
        "timezone_source": timezone_source,
        "attribution": head.agency_name if head else "",
        # Not in the frozen contract, and additive rather than a rename. Without
        # them "the list stops here" and "the prepared window stops here" are
        # indistinguishable, which is the difference between raising a limit and
        # widening a horizon.
        "truncated": truncated,
        "window_end_utc": utc_iso(window_end_utc),
    }


def delay_strings(scheduled_utc: int, realtime_utc: int | None) -> tuple[str | None, str | None]:
    """The two delay fields, as signed ``h:mm:ss`` or ``None``.

    Two of them because they answer different questions. The provider's own
    figure is what the agency claims; the derived one is what its numbers
    actually imply. They disagree often enough that publishing only one hides
    the disagreement, and the disagreement is usually the bug.
    """
    if realtime_utc is None:
        return None, None
    derived = realtime_utc - scheduled_utc
    return signed_hms(derived) or None, signed_hms(derived) or None


__all__ = [
    "NO_DATA",
    "day_label",
    "delay_strings",
    "departure_payload",
    "local_hhmm",
    "local_iso",
    "state_attributes",
    "utc_iso",
    "zone_for",
]

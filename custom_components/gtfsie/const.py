"""Constants for the Home Assistant side of gtfsie.

Only what Home Assistant needs. Everything describing GTFS itself -- route
types, import phases, the reasons a query returned nothing -- belongs to the
engine and is imported from ``pygtfsie`` rather than restated here, so the two
cannot drift.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final

DOMAIN: Final = "gtfsie"

#: Shown where a value is genuinely unknown. A visible placeholder rather than
#: an empty string, so a template that renders it produces something a user can
#: recognise instead of a blank they will read as a bug.
NO_DATA: Final = "-"


class SubentryKind(StrEnum):
    """The two kinds of thing that can hang off a datasource.

    Subentries rather than separate config entries. A datasource is expensive --
    a feed archive, an import measured in hours, two SQLite files -- and these
    are cheap children of exactly one of them. Making that relationship native
    means deletion cascades and startup ordering are the framework's problem,
    and there is no cross-entry reference for anything to get out of step with.
    See docs/SPEC.md 4.3.
    """

    ROUTE = "route"
    VICINITY = "vicinity"


# --- datasource (the config entry) ---------------------------------------

CONF_SOURCE_URL: Final = "source_url"
CONF_DATASOURCE_NAME: Final = "name"
CONF_DB_DIR: Final = "db_dir"
CONF_SCOPE_KIND: Final = "scope_kind"
CONF_SCOPE_VALUES: Final = "scope_values"
CONF_BACK_DAYS: Final = "back_days"
CONF_HORIZON_DAYS: Final = "horizon_days"
CONF_REFRESH_HOURS: Final = "refresh_hours"

# --- route subentry ------------------------------------------------------

CONF_ORIGIN_STOP_IDS: Final = "origin_stop_ids"
CONF_DEST_STOP_IDS: Final = "destination_stop_ids"
CONF_ROUTE_ID: Final = "route_id"
CONF_DIRECTION_ID: Final = "direction_id"
CONF_OFFSET_MINUTES: Final = "offset_minutes"
CONF_LOOKBACK_MINUTES: Final = "lookback_minutes"
CONF_LIMIT: Final = "limit"

# --- vicinity subentry ---------------------------------------------------

CONF_TRACKER_ENTITY_ID: Final = "tracker_entity_id"
CONF_RADIUS_M: Final = "radius_m"
CONF_MAX_STOPS: Final = "max_stops"

# --- defaults ------------------------------------------------------------

#: One day back and three forward. Back, because a departure whose scheduled
#: time has passed can still be the one a user needs when realtime says the
#: vehicle has not arrived.
DEFAULT_BACK_DAYS: Final = 1
DEFAULT_HORIZON_DAYS: Final = 3

DEFAULT_REFRESH_HOURS: Final = 24
DEFAULT_OFFSET_MINUTES: Final = 0

#: Fifteen minutes of lookback. Long enough for an ordinary urban delay, short
#: enough that a departure genuinely missed stops being offered.
DEFAULT_LOOKBACK_MINUTES: Final = 15

DEFAULT_LIMIT: Final = 10

#: Conservative, and adjustable in the options flow. A radius that is too small
#: shows nothing and is obvious; one that is too large returns half a city and
#: reads as the integration being broken.
DEFAULT_RADIUS_M: Final = 200
DEFAULT_MAX_STOPS: Final = 15

#: Where the databases live, under the Home Assistant configuration directory.
#: Documented rather than hidden because a national feed's index is measured in
#: gigabytes and it lands inside the path that dominates a config backup.
DATA_SUBDIR: Final = "gtfsie"

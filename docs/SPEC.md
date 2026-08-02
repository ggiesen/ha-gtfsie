# gtfsie -- Architecture and Requirements Specification

Version 1.0 (final). Clean-room design. No source from any existing GTFS integration was consulted; the inputs are public issue reports, the GTFS reference, the GTFS-Realtime reference and the Home Assistant developer documentation.

---

## 1. ARCHITECTURE

### 1.1 Chosen design

**gtfsie-C base ("layered store with a demand-scoped departure index"), grafted with gtfsie-B's storage engineering and gtfsie-A's control plane.**

Two of three judges chose C on correctness-against-hostile-feeds and on operational cost; the third chose B on testability. The gap between them is not the time model (all three converge on the same one) but on scope, on knowing when the store is wrong, and on how testable the ingest stages are. C wins the first two, B wins the third, and B's wins are grafts rather than rewrites. So: C's layering and its verify/repair/tzdata machinery, B's interned integer keys, `WITHOUT ROWID` clustered departure table, pure-function ingest stages and `paths.py`, A's separate `registry.sqlite` control database, full-range calendar expansion, typed no-departures diagnosis and log throttling.

The core insight, shared by all three and not the tiebreaker: **`stop_times` are resolved to absolute UTC epoch seconds at ingest**, using the GTFS service-day origin (local noon minus twelve hours), so `25:10:00` and DST days are correct by construction. Every runtime read is then an integer range scan. No `date()`, `strftime()`, `julianday()`, `localtime` or `now` appears anywhere in `store/queries.py`, and a CI grep enforces that.

What C adds and why it decided the choice:

- **Scope is demand-driven, not import-time.** Only stops someone actually asked about are expanded into the hot table, and scope grows later as an indexed join rather than a rebuild. A pair of stop entries materialises thousands of rows, not tens of millions. Adding a vicinity entry six months later costs seconds. B froze scope at import time, which is a one-way door; A materialised the whole country by default and reproduced the 9 GB reports it was written to fix.
- **`verify()` gates promotion.** A refresh that parses cleanly but drops the user's stops is the one adversarial case that atomic-swap-plus-rollback does not catch, because the import did not fail. gtfsie refuses the swap on a structurally empty result and raises a named repair issue when a configured identifier genuinely vanished from the new feed.
- **`tzdata_version` in meta forces a rebuild.** A stored absolute instant is only correct under the IANA rules in force when it was computed. This is affordable precisely because the materialised set is small.

Grafted corrections the judges demanded, all adopted:

- The `departure` table stores **no** `local_date` / `local_time` / `tz_name` text columns. Local rendering happens once, in `attributes.py`, from the epoch plus the instance's `tz_name`. The original design's own INSERT ... SELECT could not have filled those columns correctly anyway (local clock time varies per stop within a trip instance).
- All identifiers are **interned to INTEGER surrogate keys** in the hot and base tables; the opaque original strings live once, in the dimension tables. This roughly halves the base tables and, critically, shrinks `ix_stoptime_stop`, which is what scope-on-demand depends on.
- The `departure` table is `WITHOUT ROWID`, clustered on `(stop_key, departure_utc, ...)`, so the table *is* the query index. There is no secondary index on it.
- The materialisation window starts at **today minus two days** of *service date* (not today, not today-1): a `25:30` trip on yesterday's service day departs at 01:30 this morning, and some feeds legally publish times past 28:00.
- `HORIZON_EXHAUSTED` and `FEED_EXPIRED` are distinct first-class states.
- `after_dependencies` for `zone` and `person`, not `dependencies`. Vicinity mode is optional and neither should be able to fail gtfsie setup.
- Import progress travels on a `multiprocessing.Queue` plus heartbeat rows in `registry.sqlite`, not by polling the staging database while it is under bulk load.

### 1.2 On-disk layout

`paths.py` is the only module that knows the layout. Nothing anywhere derives a path from an entity's display name.

```
<config>/gtfsie/
  registry.sqlite                 control plane; never swapped
  <ds_id>/
    feed.sqlite                   live datasource database
    feed.sqlite.staging           built here, then os.replace()d into place
    feed.sqlite.prev              retained until the next import verifies
    source.zip                    last known-good archive
    source.zip.prev
    tmp/                          downloads, sqlite temp_store_directory
<config>/www/gtfsie/<ds_id>/<route>_<direction>.geojson
```

### 1.3 Module layout

```
custom_components/gtfsie/
  manifest.json
  const.py
  exceptions.py
  paths.py
  helpers/
    tz.py
    text.py
    geo.py
    logthrottle.py
  store/
    schema.py
    connection.py
    rows.py
    queries.py
    verify.py
  ingest/
    source.py
    csvreader.py
    calendar.py
    guard.py
    plan.py
    load.py
    instances.py
    materialise.py
    worker.py
    manager.py
  registry.py
  horizon.py
  repairs.py
  realtime/
    model.py
    fetch.py
    decode.py
    ids.py
    match.py
    hub.py
    geojson.py
  coordinator.py
  __init__.py
  config_flow.py
  sensor.py
  attributes.py
  diagnostics.py
  services.py
  services.yaml
  translations/en.json
```

### 1.4 Public signatures

`manifest.json` (not code, but load-bearing): `"config_flow": true`, `"iot_class": "local_polling"`, `"integration_type": "hub"`, `"dependencies": []`, `"after_dependencies": ["zone", "person"]`, `"requirements": ["gtfs-realtime-bindings==1.0.0", "protobuf>=5.28"]`, `"homeassistant": "2025.12.0"`.

**`const.py`**

```python
DOMAIN: Final = "gtfsie"
NO_DATA: Final = "-"
SCHEMA_VERSION: Final = 1

class SubentryKind(StrEnum):   ROUTE = "route"; VICINITY = "vicinity"
class FeedKind(StrEnum):       TRIP_UPDATES; VEHICLE_POSITIONS; ALERTS
class KeyLocation(StrEnum):    NONE; QUERY; HEADER
class RouteMode(StrEnum):      STOP_IDS; STATION_NAMES
class ImportPhase(StrEnum):    IDLE; DOWNLOADING; VALIDATING; LOADING; CALENDAR; \
                               INSTANCES; MATERIALISING; INDEXING; VERIFYING; \
                               PROMOTING; DONE; FAILED; ABORTED_FUTURE_DATA
class DataState(StrEnum):      OK; IMPORTING; NO_DEPARTURE; HORIZON_EXHAUSTED; \
                               FEED_EXPIRED; ERROR
class NoDeparturesReason(StrEnum):
    OK; NO_STOPS_IN_RADIUS; STOP_NOT_IN_FEED; NO_SERVICE_TODAY; \
    NO_TRIPS_AT_STOPS; NO_TIMED_EVENTS; NOT_MATERIALISED; \
    HORIZON_EXHAUSTED; FEED_EXPIRED

def route_type_name(rt: int | None) -> str      # incl. extended 100-1700 block
def route_type_icon(rt: int | None) -> str      # mdi:*; unknown -> transit-connection-variant
```

**`helpers/tz.py`** -- the only place timezone reasoning exists.

```python
def resolve_zone(agency_tz: str | None, stop_tz: str | None,
                 ha_tz: str | None) -> tuple[ZoneInfo, str]
    # returns (zone, source in {"agency","stop","homeassistant","utc"})
def day_origin_utc(service_date: date, zone: ZoneInfo) -> int
    # epoch seconds of (local noon on service_date) - 12h
def event_utc(day_origin: int, secs: int | None) -> int | None
def parse_gtfs_time(raw: str | None) -> int | None   # "25:10:00" -> 90600; blank -> None
def parse_gtfs_date(raw: str | None) -> date | None  # strips quotes/whitespace; junk -> None
def tzdata_version() -> str
```

`day_origin_utc` uses absolute arithmetic from local noon rather than naive wall-clock arithmetic. The two agree everywhere except for clock times falling inside a skipped or repeated hour; publishers are inconsistent about which they assumed, so `tz_source` is recorded per trip instance and the divergence is documented rather than papered over.

**`helpers/text.py`** -- nothing else may call `str()` on a raw GTFS value.

```python
def clean(value: str | None) -> str                       # None/whitespace -> ""
def first_non_empty(*values: str | None) -> str
def route_label(route_id: str, short: str | None, long: str | None) -> str
def omit_empty(d: Mapping[str, Any]) -> dict[str, Any]
def signed_hms(seconds: int | None) -> str                # "-0:01:30"; "" when None
def slug_component(raw: str) -> str                       # colon/space-safe filename part
```

**`helpers/geo.py`**

```python
@dataclass(frozen=True, slots=True)
class BBox: lat_min: float; lat_max: float; lon_min: float; lon_max: float

def bboxes_for(lat: float, lon: float, radius_m: float) -> list[BBox]
def haversine_m(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float
def tracked_coords(hass: HomeAssistant, entity_id: str) -> tuple[float, float] | None
def moved_enough(prev: tuple[float, float] | None,
                 cur: tuple[float, float], threshold_m: float) -> bool
```

**`store/connection.py`**

```python
class Store:
    def __init__(self, hass, db_path: Path, *, readers: int = 2) -> None
    async def async_open(self) -> None
    async def async_close(self) -> None
    async def async_run(self, fn: Callable[[sqlite3.Connection], T]) -> T
    async def async_write(self, fn: Callable[[sqlite3.Connection], T]) -> T   # single writer lock
    async def async_swap_in(self, staging: Path) -> None   # quiesce, replace, reopen
    @property
    def path(self) -> Path
    @property
    def opened(self) -> bool
```

Readers are thread-local connections in a small pool; the importer never shares a connection with them. Writers serialise behind one `asyncio.Lock` plus the registry heartbeat row, which is why a manual refresh and a scheduled refresh can never produce "database is locked".

**`store/queries.py`** -- every SQL statement in the integration lives here. All are synchronous functions taking a connection and a caller-supplied `now_utc: int`.

```python
def resolve_stop_keys(conn, stop_ids: Sequence[str]) -> dict[str, int]
def resolve_station_keys(conn, name: str) -> list[int]
def resolve_route_key(conn, route_id: str) -> int | None
def next_departures(conn, *, origin_keys, from_utc, until_utc,
                    route_key=None, direction_id=None, limit) -> list[DepartureRow]
def next_departures_pair(conn, *, origin_keys, dest_keys, from_utc, until_utc,
                         route_key=None, direction_id=None, limit) -> list[DepartureRow]
def departures_by_service_date(conn, *, origin_keys, dest_keys, from_utc,
                               limit) -> dict[date, list[DepartureRow]]
def stops_in_boxes(conn, boxes, *, limit, route_keys=None,
                   stop_keys=None) -> list[StopRow]
def trip_stop_events(conn, *, trip_id: str, service_date: date | None) -> list[DepartureRow]
def route_choices(conn, agency_id: str | None) -> list[RouteChoice]
def agency_choices(conn) -> list[tuple[str, str]]
def stop_choices_for_route(conn, route_key: int, direction_id: int | None) -> list[StopRow]
def station_name_choices(conn, prefix: str, limit: int) -> list[str]
def alias_lookup(conn, kind: str, aliases: Sequence[str]) -> dict[str, int]
def trip_keys_by_prefix(conn, prefix: str, limit: int = 8) -> list[tuple[str, int]]
def meta(conn) -> dict[str, str]
def diagnose(conn, *, stop_keys, from_utc, radius_hits: int) -> NoDeparturesReason
```

**`ingest`** -- each stage is a function over a connection and an archive, so each is testable against a fixture zip without hass, an event loop or a subprocess.

```python
# source.py
@dataclass(frozen=True, slots=True)
class FeedSource:
    kind: Literal["url", "local_zip", "local_dir"]
    location: str
    key_name: str | None
    key_value: str | None
    key_location: KeyLocation

async def async_fetch(hass, src: FeedSource, dest: Path) -> Path
def sniff(head: bytes) -> str      # zip|gzip|protobuf|html|json|xml|unknown
def validate_archive(path: Path) -> ArchiveInfo

@dataclass(frozen=True, slots=True)
class ArchiveInfo:
    members: frozenset[str]
    has_calendar: bool
    has_calendar_dates: bool
    has_frequencies: bool
    has_feed_info: bool
    encoding: str

# csvreader.py
DEFAULT_DROPS: Final = frozenset({"transfers.txt", "shapes.txt"})
REQUIRED: Final = frozenset({"agency.txt","stops.txt","routes.txt","trips.txt","stop_times.txt"})
def iter_rows(zf, member: str, *, required_cols: Sequence[str] = ()) -> Iterator[dict[str, str]]

# calendar.py
def expand_service_dates(zf, info: ArchiveInfo) -> Iterator[tuple[str, date]]
def calendar_bounds(zf, info: ArchiveInfo) -> tuple[date, date] | None
def earliest_service_date(zf, info: ArchiveInfo) -> date | None

# guard.py -- the decision that can destroy both datasets, as a pure function
def should_abort_replace(earliest: date | None, today: date,
                         is_replace: bool) -> bool

# plan.py
@dataclass(frozen=True, slots=True)
class ImportPlan:
    ds_id: str; staging_path: str; archive_path: str; is_replace: bool
    ha_timezone: str; today_iso: str; drops: tuple[str, ...]
    pinned_stop_ids: tuple[str, ...]
    window_past_h: int; window_future_h: int
    freq_expansion_cap: int; tzdata_version: str

@dataclass(frozen=True, slots=True)
class ImportResult:
    ok: bool; rows: dict[str, int]; window: tuple[int, int] | None
    calendar_bounds: tuple[str, str] | None; feed_version: str
    tz_sources: dict[str, int]; missing_pinned: tuple[str, ...]
    error: str | None; phase: ImportPhase

def run_import(plan: ImportPlan,
               report: Callable[[ImportProgress], None]) -> ImportResult

# instances.py / materialise.py
def build_instances(conn, *, from_date: date, to_date: date, ha_tz: str,
                    freq_cap: int, report) -> int
def materialise(conn, *, stop_keys: Sequence[int], from_utc: int,
                until_utc: int, report) -> int
def prune(conn, *, before_utc: int, chunk: int = 20_000) -> int
def ensure_scope(conn, *, stop_keys: Sequence[int], pinned: bool,
                 now_utc: int) -> int     # runtime scope growth

# worker.py
def worker_main(config_dir: str, plan: dict[str, Any], queue: Queue) -> dict[str, Any]

# manager.py
class ImportManager:
    async def async_start(self, *, reason: str, source: FeedSource | None = None) -> None
    async def async_recover_stale(self) -> None
    async def async_slide_window(self) -> int
    async def async_ensure_scope(self, stop_ids: Sequence[str], *, pinned: bool) -> None
    async def async_discard_and_retry(self) -> None
    async def async_delete_database(self) -> None
    @property
    def state(self) -> ImportState
    @property
    def busy(self) -> bool
```

**`store/verify.py`**

```python
@dataclass(frozen=True, slots=True)
class VerifyResult:
    ok: bool
    counts: dict[str, int]
    covers_today: bool
    missing_pinned: tuple[str, ...]    # stop_id absent from the new feed
    empty_pinned: tuple[str, ...]      # present but zero departures anywhere
    reason: str | None

def verify(conn, *, today: date, pinned_stop_ids: Sequence[str]) -> VerifyResult
```

Promotion rules: abort when any of `agency/stop/route/trip/stop_time/service_date` is zero, or when `empty_pinned` is non-empty. Do **not** abort on `missing_pinned` -- promote and raise a repair issue naming each vanished identifier, because that is a genuine feed change and blocking would freeze the user on stale data forever.

**`realtime`**

```python
# model.py -- protobuf, GTFS-RT JSON and SIRI all normalise to these at the boundary
@dataclass(frozen=True, slots=True)
class RtStopUpdate:
    stop_id: str | None; stop_sequence: int | None
    arrival_utc: datetime | None; arrival_delay: int | None
    departure_utc: datetime | None; departure_delay: int | None
    schedule_relationship: str
@dataclass(frozen=True, slots=True)
class RtTripUpdate:
    trip_id: str | None; route_id: str | None; direction_id: int | None
    start_date: date | None; trip_delay: int | None
    updates: tuple[RtStopUpdate, ...]; timestamp: datetime | None
@dataclass(frozen=True, slots=True)
class RtVehicle: trip_id; route_id; direction_id; lat; lon; bearing; label; timestamp
@dataclass(frozen=True, slots=True)
class RtAlert: header; description; cause; effect; routes; trips; stops; active
@dataclass(frozen=True, slots=True)
class RtSnapshot:
    trips: tuple[RtTripUpdate, ...]; vehicles: tuple[RtVehicle, ...]
    alerts: tuple[RtAlert, ...]
    fetched_at: Mapping[FeedKind, datetime | None]
    errors: Mapping[FeedKind, str | None]

# fetch.py
@dataclass(frozen=True, slots=True)
class RtEndpoint:
    kind: FeedKind; url: str | None; local_path: str | None
    key_name: str | None; key_value: str | None; key_location: KeyLocation
    extra_headers: Mapping[str, str]
def build_request(ep: RtEndpoint) -> tuple[str, dict[str, str]]
async def async_fetch_raw(hass, ep: RtEndpoint) -> bytes

# decode.py -- protobuf imported lazily inside the function
def decode(payload: bytes, *, url: str) -> tuple[list[RtTripUpdate],
                                                 list[RtVehicle], list[RtAlert]]
def protobuf_available() -> bool

# ids.py
PREFIX_PATTERNS: Final[tuple[re.Pattern, ...]]
def suffix_candidates(raw: str, limit: int = 4) -> list[str]
def prefix_bounds(raw: str) -> tuple[str, str]

# match.py
@dataclass(frozen=True, slots=True)
class Enriched:
    row: DepartureRow
    rt_departure_utc: datetime | None; rt_arrival_utc: datetime | None
    delay_reported: int | None; delay_derived: int | None
    matched_by: str | None
def apply(rows, snap: RtSnapshot, *, origin_stop_ids: frozenset[str],
          now: datetime) -> list[Enriched]
def alerts_for(snap, *, route_ids, trip_ids, stop_ids) -> list[RtAlert]

# hub.py
class RealtimeHub:
    async def async_snapshot(self, *, force: bool = False) -> RtSnapshot
    @property
    def last_success(self) -> Mapping[FeedKind, datetime | None]
    @property
    def last_error(self) -> Mapping[FeedKind, str | None]
```

**`coordinator.py`**

```python
@dataclass(slots=True)
class DepartureData:
    state: DataState
    departures: list[Enriched]
    alerts: list[RtAlert]
    reason: NoDeparturesReason
    agency_name: str; agency_phone: str
    route_type: int | None
    tz_name: str; tz_source: str
    window_end_utc: datetime | None
    rt_last_success: Mapping[FeedKind, datetime | None]
    rt_last_error: Mapping[FeedKind, str | None]

class DepartureCoordinator(DataUpdateCoordinator[DepartureData]): ...
class VicinityCoordinator(DataUpdateCoordinator[dict[str, DepartureData]]):
    async def async_recompute_stops(self) -> None
```

**`attributes.py`** -- the only module allowed to render an instant.

```python
DEPARTURE_KEYS: Final[tuple[str, ...]]   # fixed; every entry carries all of them
def render_instant(epoch: int | None, tz: ZoneInfo) -> tuple[str, str, str]
    # (utc_iso, local_iso, local_hhmm)
def departure_entry(e: Enriched, tz: ZoneInfo) -> dict[str, Any]
def vicinity_entry(e: Enriched, stop: StopRow, tz: ZoneInfo) -> dict[str, Any]
def build(data: DepartureData, *, limit: int) -> dict[str, Any]
```

### 1.5 Complete SQL DDL

#### 1.5.1 `registry.sqlite` (one per Home Assistant instance, never swapped)

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = FULL;
PRAGMA foreign_keys = ON;

CREATE TABLE registry_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);   -- schema_version, created_utc

CREATE TABLE datasource (
  ds_id           TEXT PRIMARY KEY,
  name            TEXT NOT NULL,
  db_path         TEXT NOT NULL,
  source_kind     TEXT NOT NULL CHECK (source_kind IN ('url','local_zip','local_dir')),
  source_loc      TEXT NOT NULL,            -- verbatim, query string preserved
  key_name        TEXT,                     -- key VALUE lives in the config entry, never here
  key_location    TEXT NOT NULL DEFAULT 'none'
                  CHECK (key_location IN ('none','query','header')),
  drop_files      TEXT NOT NULL DEFAULT 'transfers.txt,shapes.txt',
  window_past_h   INTEGER NOT NULL DEFAULT 6,
  window_future_h INTEGER NOT NULL DEFAULT 36,
  created_utc     INTEGER NOT NULL,
  last_success_utc INTEGER,
  last_error      TEXT
);

CREATE TABLE import_run (
  run_id        INTEGER PRIMARY KEY,
  ds_id         TEXT NOT NULL REFERENCES datasource(ds_id) ON DELETE CASCADE,
  pid           INTEGER NOT NULL,
  reason        TEXT NOT NULL
                CHECK (reason IN ('setup','service','schedule','window','scope','tzdata')),
  phase         TEXT NOT NULL,
  rows_done     INTEGER NOT NULL DEFAULT 0,
  rows_total    INTEGER,
  detail        TEXT NOT NULL DEFAULT '',
  started_utc   INTEGER NOT NULL,
  heartbeat_utc INTEGER NOT NULL,           -- stale => worker died
  finished_utc  INTEGER,
  ok            INTEGER,
  error         TEXT
);
CREATE INDEX ix_run_live     ON import_run(ds_id, finished_utc, heartbeat_utc);
CREATE INDEX ix_run_recent   ON import_run(ds_id, started_utc DESC);
```

#### 1.5.2 `<ds_id>/feed.sqlite`

```sql
-- Connection pragmas. page_size must precede the first write.
PRAGMA page_size    = 8192;
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA busy_timeout = 30000;
PRAGMA foreign_keys = OFF;          -- surrogate keys are assigned by the importer
PRAGMA cache_size   = -16384;       -- 16 MiB, bounded for a Pi
PRAGMA temp_store   = FILE;
PRAGMA temp_store_directory = '<config>/gtfsie/<ds_id>/tmp';   -- HA OS /tmp is a small tmpfs
PRAGMA auto_vacuum  = INCREMENTAL;

-- ---------------------------------------------------------------- meta
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT
);
-- schema_version, gtfsie_version, tzdata_version, imported_utc, source_sha256,
-- encoding_fallback, feed_version, feed_start_date, feed_end_date,
-- calendar_min_date, calendar_max_date, window_from_utc, window_to_utc,
-- tz_default, tz_source_default, agency_count, freq_expanded, verified_utc

-- ------------------------------------------------------- layer 0: feed
CREATE TABLE agency (
  agency_key      INTEGER PRIMARY KEY,
  agency_id       TEXT NOT NULL DEFAULT '',   -- '' is legal in a single-agency feed
  agency_name     TEXT NOT NULL DEFAULT '',
  agency_url      TEXT NOT NULL DEFAULT '',
  agency_timezone TEXT,                       -- NULL when absent or unparseable
  agency_lang     TEXT NOT NULL DEFAULT '',
  agency_phone    TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX ux_agency_id ON agency(agency_id);

CREATE TABLE stop (
  stop_key       INTEGER PRIMARY KEY,
  stop_id        TEXT NOT NULL,
  stop_code      TEXT NOT NULL DEFAULT '',
  stop_name      TEXT NOT NULL DEFAULT '',
  stop_lat       REAL,                        -- NULL is legal and must not abort ingest
  stop_lon       REAL,
  location_type  INTEGER NOT NULL DEFAULT 0,
  parent_key     INTEGER,                     -- resolved parent_station, NULL when none
  stop_timezone  TEXT,
  platform_code  TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX ux_stop_id   ON stop(stop_id);
CREATE INDEX ix_stop_geo         ON stop(stop_lat, stop_lon)
                                 WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL;
CREATE INDEX ix_stop_parent      ON stop(parent_key) WHERE parent_key IS NOT NULL;
CREATE INDEX ix_stop_name        ON stop(stop_name COLLATE NOCASE);

CREATE TABLE route (
  route_key        INTEGER PRIMARY KEY,
  route_id         TEXT NOT NULL,
  agency_key       INTEGER NOT NULL,          -- repaired at ingest, see R-I12
  route_short_name TEXT NOT NULL DEFAULT '',
  route_long_name  TEXT NOT NULL DEFAULT '',
  route_desc       TEXT NOT NULL DEFAULT '',
  route_type       INTEGER,                   -- NULL when absent; never defaulted to 3
  route_color      TEXT NOT NULL DEFAULT '',
  route_text_color TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX ux_route_id  ON route(route_id);
CREATE INDEX ix_route_agency     ON route(agency_key);

CREATE TABLE service (
  service_key INTEGER PRIMARY KEY,
  service_id  TEXT NOT NULL
);
CREATE UNIQUE INDEX ux_service_id ON service(service_id);

-- Expanded once over the FULL feed range, so window rollover never needs the archive.
-- calendar.txt mask INTERSECT range, UNION calendar_dates type 1, MINUS type 2,
-- de-duplicated by the primary key.
CREATE TABLE service_date (
  service_key  INTEGER NOT NULL,
  service_date INTEGER NOT NULL,              -- YYYYMMDD as INTEGER
  PRIMARY KEY (service_key, service_date)
) WITHOUT ROWID;
CREATE INDEX ix_service_date_d ON service_date(service_date);

CREATE TABLE trip (
  trip_key           INTEGER PRIMARY KEY,
  trip_id            TEXT NOT NULL,
  route_key          INTEGER NOT NULL,
  service_key        INTEGER NOT NULL,
  trip_headsign      TEXT NOT NULL DEFAULT '',
  trip_short_name    TEXT NOT NULL DEFAULT '',
  direction_id       INTEGER,                 -- NULL is legal; opaque per route
  block_id           TEXT NOT NULL DEFAULT '',
  wheelchair_accessible INTEGER,
  bikes_allowed      INTEGER,
  last_stop_key      INTEGER,                 -- destination fallback when no headsign
  first_departure_secs INTEGER,
  last_arrival_secs  INTEGER
);
CREATE UNIQUE INDEX ux_trip_id     ON trip(trip_id);
CREATE INDEX ix_trip_route_dir     ON trip(route_key, direction_id);
CREATE INDEX ix_trip_service       ON trip(service_key);

CREATE TABLE stop_time (
  trip_key       INTEGER NOT NULL,
  stop_sequence  INTEGER NOT NULL,
  stop_key       INTEGER NOT NULL,
  arrival_secs   INTEGER,                     -- seconds from service-day origin; NULL if blank
  departure_secs INTEGER,
  pickup_type    INTEGER NOT NULL DEFAULT 0,
  drop_off_type  INTEGER NOT NULL DEFAULT 0,
  stop_headsign  TEXT NOT NULL DEFAULT '',
  timepoint      INTEGER,
  PRIMARY KEY (trip_key, stop_sequence)
) WITHOUT ROWID;
-- Required by runtime scope growth and by the origin/destination EXISTS check.
CREATE INDEX ix_stoptime_stop ON stop_time(stop_key, trip_key);

CREATE TABLE frequency (
  trip_key     INTEGER NOT NULL,
  start_secs   INTEGER NOT NULL,
  end_secs     INTEGER NOT NULL,
  headway_secs INTEGER NOT NULL,
  exact_times  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (trip_key, start_secs)
) WITHOUT ROWID;

-- Precomputed realtime identifier aliases (deterministic, unit-testable).
CREATE TABLE id_alias (
  kind       TEXT NOT NULL CHECK (kind IN ('stop','trip','route')),
  alias      TEXT NOT NULL,
  target_key INTEGER NOT NULL,
  PRIMARY KEY (kind, alias)
) WITHOUT ROWID;

-- --------------------------------------------- layer 1: trip instances
-- One row per (trip, service_date, frequency offset). Absolute time origin
-- and resolved zone are fixed here and never recomputed on the read path.
CREATE TABLE trip_instance (
  inst_key         INTEGER PRIMARY KEY,
  trip_key         INTEGER NOT NULL,
  service_date     INTEGER NOT NULL,          -- YYYYMMDD
  day_origin_utc   INTEGER NOT NULL,          -- epoch secs of (local noon - 12h)
  tz_name          TEXT NOT NULL,
  tz_source        TEXT NOT NULL
                   CHECK (tz_source IN ('agency','stop','homeassistant','utc')),
  freq_offset_secs INTEGER NOT NULL DEFAULT 0,
  freq_exact       INTEGER NOT NULL DEFAULT 1
);
CREATE UNIQUE INDEX ux_inst ON trip_instance(trip_key, service_date, freq_offset_secs);
CREATE INDEX ix_inst_date   ON trip_instance(service_date);

-- ------------------------------------- layer 2: materialised departures
-- The hot table IS the query index: clustered on (stop_key, departure_utc).
-- There is no secondary index on this table. Only scoped stops appear here,
-- and only within the current window.
CREATE TABLE departure (
  stop_key      INTEGER NOT NULL,
  departure_utc INTEGER NOT NULL,             -- day_origin + freq_offset + departure_secs
  inst_key      INTEGER NOT NULL,
  stop_sequence INTEGER NOT NULL,
  arrival_utc   INTEGER,
  route_key     INTEGER NOT NULL,
  direction_id  INTEGER,
  pickup_type   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (stop_key, departure_utc, inst_key, stop_sequence)
) WITHOUT ROWID;

-- ------------------------------------------------------ scope tracking
CREATE TABLE watch_stop (
  stop_key          INTEGER PRIMARY KEY,
  pinned            INTEGER NOT NULL DEFAULT 0,   -- 1 = referenced by a live subentry
  materialised_from INTEGER,                      -- epoch secs, NULL = not yet built
  materialised_to   INTEGER,
  last_used_utc     INTEGER NOT NULL
);
CREATE INDEX ix_watch_pinned ON watch_stop(pinned);

-- Named repairs raised by verify() and surfaced as HA repair issues.
CREATE TABLE repair (
  repair_id   INTEGER PRIMARY KEY,
  kind        TEXT NOT NULL,                  -- missing_stop | missing_route | empty_pinned
  identifier  TEXT NOT NULL,
  detail      TEXT NOT NULL DEFAULT '',
  raised_utc  INTEGER NOT NULL
);
CREATE UNIQUE INDEX ux_repair ON repair(kind, identifier);
```

#### 1.5.3 The three query shapes

Departures at one or more origin stops:

```sql
SELECT d.departure_utc, d.arrival_utc, d.stop_key, d.stop_sequence,
       i.inst_key, i.service_date, i.tz_name, i.tz_source, i.freq_exact,
       t.trip_id, t.trip_headsign, t.direction_id, t.last_stop_key,
       r.route_id, r.route_short_name, r.route_long_name, r.route_type,
       a.agency_name, a.agency_phone,
       s.stop_id, s.stop_name
  FROM departure d
  JOIN trip_instance i ON i.inst_key  = d.inst_key
  JOIN trip          t ON t.trip_key  = i.trip_key
  JOIN route         r ON r.route_key = d.route_key
  JOIN agency        a ON a.agency_key = r.agency_key
  JOIN stop          s ON s.stop_key  = d.stop_key
 WHERE d.stop_key IN (/* origin keys */)
   AND d.departure_utc >= :from_utc
   AND d.departure_utc <  :until_utc
   AND (:route_key IS NULL OR d.route_key   = :route_key)
   AND (:direction_id IS NULL OR d.direction_id = :direction_id)
   AND d.pickup_type <> 1
 ORDER BY d.departure_utc
 LIMIT :limit;
```

Origin to destination adds, with no change to the scan on `departure`:

```sql
   AND EXISTS (SELECT 1 FROM stop_time st
                WHERE st.trip_key = i.trip_key
                  AND st.stop_sequence > d.stop_sequence
                  AND st.stop_key IN (/* destination keys */))
```

and the destination arrival as a correlated scalar:

```sql
       (SELECT i.day_origin_utc + i.freq_offset_secs + MIN(st.arrival_secs)
          FROM stop_time st
         WHERE st.trip_key = i.trip_key
           AND st.stop_sequence > d.stop_sequence
           AND st.stop_key IN (/* destination keys */)
           AND st.arrival_secs IS NOT NULL) AS dest_arrival_utc
```

Runtime scope growth, the only write on the read path, executed in the writer executor:

```sql
INSERT OR IGNORE INTO departure
      (stop_key, departure_utc, inst_key, stop_sequence,
       arrival_utc, route_key, direction_id, pickup_type)
SELECT st.stop_key,
       i.day_origin_utc + i.freq_offset_secs + st.departure_secs,
       i.inst_key, st.stop_sequence,
       CASE WHEN st.arrival_secs IS NULL THEN NULL
            ELSE i.day_origin_utc + i.freq_offset_secs + st.arrival_secs END,
       t.route_key, t.direction_id, st.pickup_type
  FROM stop_time    st
  JOIN trip         t ON t.trip_key = st.trip_key
  JOIN trip_instance i ON i.trip_key = st.trip_key
 WHERE st.stop_key IN (/* newly scoped keys */)
   AND st.departure_secs IS NOT NULL
   AND i.day_origin_utc + i.freq_offset_secs + st.departure_secs
       BETWEEN :from_utc AND :until_utc;
```

CI gate: `grep -nEi '\b(date|time|datetime|julianday|strftime|unixepoch)\s*\(|localtime|'"'"'now'"'"'' custom_components/gtfsie/store/queries.py` must return nothing. The build fails if it does.

---

## 2. REQUIREMENTS

Each statement is testable. **[EDGE]** marks the non-obvious cases that a fresh implementation would not guess -- these are the reason the issue corpus was mined and each one needs a dedicated regression test with a fixture.

### 2.1 Ingest

- **I1** A static feed URL is fetched verbatim, including any embedded query string, without re-encoding. (#21) **[EDGE]**
- **I2** A static feed may require authentication: a user-named key sent either as a URL query parameter or as a named HTTP header. When no key is configured, no header and no parameter is sent -- in particular never a placeholder value. (#21, #91, #92, #122) **[EDGE]**
- **I3** A local zip file or a pre-extracted directory is accepted as a source in place of a URL. (#99, #117)
- **I4** The downloaded payload is content-sniffed and validated as a zip containing the five required members before any import work begins. HTML, XML, JSON, gzip and raw protobuf are each rejected by name with the URL and the sniff result. (#67, #82, #110)
- **I5** An error message, an HTML page or a protobuf stream is never written into the database file. (#82) **[EDGE]**
- **I6** `transfers.txt` and `shapes.txt` are dropped before loading, plus any user-nominated additional drop list, so out-of-range or extension values in unused files cannot abort the import. Specifically `transfer_type=4` must not fail an import. (#94, #17) **[EDGE]**
- **I7** CSV values are read with `utf-8-sig` first, falling back to `cp1252` and then `utf-8` with replacement; the fallback used is recorded in `meta.encoding_fallback`. Diacritics survive round-trip. (#113, #127)
- **I8** Every field value is stripped of surrounding double quotes and whitespace before parsing. A fully quoted feed (`"20240517","20240531"`) parses. (#71) **[EDGE]**
- **I9** `feed_info.txt` may be absent; `feed_start_date` / `feed_end_date` may be blank; `feed_version` may be a non-date string such as `14.02.2024 01:03`. All three are treated as absent optional data and none aborts the import. (#17) **[EDGE]**
- **I10** Stops with blank or missing `stop_lat` / `stop_lon` and blank `stop_code`, including `location_type` 2, 3 and 4 rows, import with NULL coordinates and are invisible to distance queries. No numeric cast aborts the import. (#68, #84, #98, #114) **[EDGE]**
- **I11** Any row-level parse failure names the file and the 1-based line number and leaves nothing half-built. (#127)
- **I12** When `routes.txt` has no `agency_id` column or an empty value and `agency.txt` contains exactly one agency, every route is attributed to that agency by a single UPDATE at ingest. No per-poll warning is ever emitted. (#19, #132) **[EDGE]**
- **I13** `service_date` is expanded once over the full feed calendar range: `calendar.txt` weekday mask intersected with its start/end range, unioned with `calendar_dates` exception type 1, minus type 2, de-duplicated so a service described in both files yields each date once. Either file may be absent. (#71, #118) **[EDGE]**
- **I14** `frequencies.txt` is expanded into real trip instances at the defined headway offsets, capped per datasource, with `exact_times=0` instances flagged. The seed trip is never presented as the whole day, and no intermediate stop time is ever interpolated. (#120) **[EDGE]**
- **I15** A `stop_time` row with a blank `departure_secs` is loaded but never materialised into `departure`. A configuration whose only events are untimed yields diagnosis `NO_TIMED_EVENTS`, not silence. (#120) **[EDGE]**
- **I16** Import runs in a separate OS process, started via a module-level entry point that takes only picklable primitives, so a `forkserver` start method works. Nothing touches the event loop: no `listdir`, no zip handling, no database work, no feed loading. (#98, #102, #110)
- **I17** Import progress (phase, table, rows) is published through the integration's own logger at INFO and to a heartbeat row in `registry.sqlite`, so a Home Assistant OS user sees progress without container stdout. (#64, #68, #84)
- **I18** An import that is still running is reported as such; the datasource is not offered for entry creation, and no query is attempted against its database. At most one warning per import is emitted, not one per sensor per poll. (#13, #17, #64, #103, #137) **[EDGE]**
- **I19** A dead worker is detected by process absence or a heartbeat older than five minutes. The run is marked failed, the "importing" flag cleared, the partial staging database discarded, and a retry offered. This survives a hard Home Assistant restart because the state lives in `registry.sqlite`, not in the feed database. (#68, #94, #102) **[EDGE]**
- **I20** Leftover temp downloads, `-journal` and `-wal` remnants and staging files from a failed attempt are removed at startup and after any failure, and are never mistaken for an import in progress. (#17, #106) **[EDGE]**
- **I21** Before replacing an existing datasource, the earliest service date across whichever of `calendar.txt` and `calendar_dates.txt` are present is read, using `start_date` from the former and `date` from the latter. If it is later than today the replacement is aborted and the previous database and archive are kept. (#71) **[EDGE]**
- **I22** The future-only check applies only to a refresh. A brand new datasource imports its archive regardless of date range. (#71) **[EDGE]**
- **I23** If the future-only check cannot be completed (unparseable dates, missing columns), the problem is logged and the import proceeds. The system never ends in a state where both the new and the previous dataset are gone. (#71) **[EDGE]**
- **I24** The previous database *and* the previous source archive are retained until the new import verifies, and both are restored on abort or failure. A dead URL, a redirect to HTML or a corrupt zip leaves the working dataset in service. (#71, #103)
- **I25** Promotion of a staging database is gated on `verify()`: non-zero counts in every base table, `service_date` coverage for today, and a non-empty departure set for every pinned stop that still exists in the feed. A pinned stop whose `stop_id` has vanished does not block promotion but raises a named repair issue. (#71, #103, #115) **[EDGE]**
- **I26** Once the database exists, setup proceeds from it even when the source archive is gone. Window rollover uses the retained `stop_time` and `service_date` tables and never needs the archive. A genuinely missing datasource produces a named error, not a downstream attribute error, and does not abort setup of unrelated entries. (#6) **[EDGE]**
- **I27** Multiple datasources coexist, each with its own database, refresh schedule and realtime configuration. Refreshing one neither requires nor triggers a refresh of another. A provider that splits its network across several zips is configured as one datasource per zip with no cross-contamination. (#17, #70, #81)
- **I28** The dataset lives on disk and is queried incrementally. Nothing loads the feed into memory. Multi-gigabyte national feeds work on a Raspberry Pi. (#70)
- **I29** A pre-flight free-space check requires roughly 2.5x the expected final database size, counting the retained `.prev` copy and the archive, and aborts with an actionable message rather than filling the disk hours in. (#70, #102) **[EDGE]**
- **I30** SQLite temp files are directed to the datasource directory, never to `/tmp`, because Home Assistant OS mounts `/tmp` as a small tmpfs and a large `CREATE INDEX` external sort will fill it. (#102) **[EDGE]**
- **I31** Indexes required by the departure and vicinity queries are created at import time, after bulk load. A vicinity query over a large feed completes in well under a second. (#88)
- **I32** `meta.tzdata_version` is recorded at import. When the running tzdata version differs, `trip_instance` and `departure` are rebuilt (the base layer is not re-imported). (#2, #107) **[EDGE]**

### 2.2 Query and time model

- **Q1** `stop_times` values are local clock times relative to the service-day origin, defined as local noon minus twelve hours on the service date. Times of 24:00:00 and beyond are represented losslessly as seconds from that origin. (#107) **[EDGE]**
- **Q2** The zone is resolved as: the route's agency `agency_timezone`, else the stop's `stop_timezone`, else the Home Assistant instance timezone, else UTC. The link that produced it is recorded per trip instance as `tz_source` and exposed in diagnostics. (#2, #63, #107, #140) **[EDGE]**
- **Q3** All comparisons against "now" are made between absolute instants (epoch integers). A departure whose absolute instant is in the past is never returned, regardless of how its nominal clock time compares to the local wall clock. (#8, #107)
- **Q4** Candidates are ordered by absolute instant, not by nominal time, because a trip whose nominal time falls after midnight or on the following service day can convert to an earlier instant. (#107) **[EDGE]**
- **Q5** "Now" is taken fresh from `dt_util.utcnow()` on every coordinator cycle. No reference time is ever captured at entry creation. (#8, #100) **[EDGE]**
- **Q6** The materialisation window covers service dates from `date(window_from) - 2 days` through `date(window_to)`, filtered on absolute instant, so a trip departing at `25:30` on yesterday's service day is present at 01:30 this morning. (#107) **[EDGE]**
- **Q7** A configurable offset in minutes excludes departures sooner than now plus the offset, from both the state and the list. (#16, #23)
- **Q8** With "include tomorrow" enabled, the state is the earliest departure at or after now across the combined set, and each departure carries the service date it belongs to. Enabling the option never changes which departure is selected while departures remain today. (#23) **[EDGE]**
- **Q9** Departure selection filters on the configured `route_id` and direction in addition to origin and destination, so two entries sharing a stop pair on different routes return different timetables. The route reported for a departure is the route of the trip actually selected. (#115) **[EDGE]**
- **Q10** A trip qualifies when the origin stop appears at any `stop_sequence`, provided a destination stop occurs later in the same trip. The origin need not be the trip's first stop. (#107) **[EDGE]**
- **Q11** `direction_id` is an opaque per-route label with no fixed geographic meaning, may be NULL, and is never required to identify a destination. (#72) **[EDGE]**
- **Q12** All trips serving the configured stop pair are reported regardless of `route_type`, with route name and mode exposed so the user can distinguish them (for example connecting buses under a rail route). (#107) **[EDGE]**
- **Q13** A station-name selection expands to every platform-level `stop_id` carried by that station, including children resolved via `parent_station`. Realtime matches against any of them. (#25, #90, #134) **[EDGE]**
- **Q14** Identifiers are opaque strings that may contain colons, spaces, commas, parentheses, non-ASCII characters and embedded ISO timestamps. They are never parsed, split or reconstructed from a display label, including where used in entity ids, config values, file names and query parameters. (#90, #113) **[EDGE]**
- **Q15** When a query returns nothing, `diagnose()` returns a typed reason: no stops in radius, stop not in feed, no service today, no trips at these stops, no timed events, not materialised, horizon exhausted, or feed expired. (#67, #103)
- **Q16** `HORIZON_EXHAUSTED` (the index has not been extended that far yet, extension scheduled) and `FEED_EXPIRED` (`calendar_max_date` passed, nothing further exists) are distinct states with different user actions. The window end is published as an attribute. (#103) **[EDGE]**
- **Q17** When the feed's service calendar no longer covers the current date, the integration reports expiry explicitly rather than returning an empty list. (#103)
- **Q18** Stop selection around a location builds a bounding box clamped to +/-90 latitude and +/-180 longitude, splits a box crossing the antimeridian into two longitude ranges, and filters candidates by true great-circle distance against the radius in metres. (#74) **[EDGE]**

### 2.3 Realtime

- **R1** Realtime feeds are consumed as GTFS-Realtime protobuf regardless of the URL's file extension or path (`.aspx` endpoints included), and archive extraction is never attempted on a realtime response. (#15, #18, #106) **[EDGE]**
- **R2** GTFS-RT JSON and SIRI StopMonitoring JSON are also accepted, the latter reading aimed and expected arrival and departure from the monitored call. All three formats normalise to one internal shape at the boundary, so no downstream code tests protobuf field presence. (#63, #65, #99, #133) **[EDGE]**
- **R3** A response that is not a decodable realtime payload produces one clear error naming the URL and what the content looked like. The error path itself references no unassigned variable and raises no secondary exception. (#15, #18, #61, #89, #110) **[EDGE]**
- **R4** Vendor-specific protobuf extensions and unknown fields are ignored rather than fatal. (#82) **[EDGE]**
- **R5** A payload that decodes but carries zero entities means "no realtime this cycle", not an error. (#63) **[EDGE]**
- **R6** Any fetch or decode failure (parse error, connection reset, timeout, non-200) is caught per fetch, logged with the affected origin stop, the URL and the underlying error text, and leaves the sensor available with its scheduled departures intact. It never propagates as a failed coordinator update and never marks an entity unavailable. (#17, #20, #22, #63, #65, #108, #124, #126, #129)
- **R7** Trip updates, vehicle positions and service alerts are fetched and processed independently. A failure in one leaves the other two intact. Any of the three URLs may be omitted. (#63, #65, #128) **[EDGE]**
- **R8** Each feed kind is fetched and decoded at most once per update cycle and the same snapshot is reused for every departure and every nearby stop, however many. (#61, #109) **[EDGE]**
- **R9** Request construction is identical for start/end sensors, vicinity sensors and the manual action: the same builder function, the same headers, the same key placement. (#109) **[EDGE]**
- **R10** The API key is sent under its configured *name*, either in a caller-named header or as a named query parameter. The key name is never used as the lookup key for its own value. No placeholder is sent when unconfigured. (#21, #80, #99, #109, #122) **[EDGE]**
- **R11** `Accept: application/x-protobuf` and `Accept-Encoding: gzip, deflate` are sent alongside any configured authentication header. (#109, #123, #137) **[EDGE]**
- **R12** A sensor can read realtime from a previously downloaded local file instead of a live URL, for both route and vicinity entries. The path comes from configuration and is never derived from the entity's display name. (#15, #21, #61, #136) **[EDGE]**
- **R13** Matching order is: `(trip_id, stop_id)`; then `(trip_id, stop_sequence)` when the update carries no `stop_id`; then `(route_id, direction_id, stop_id)` when the trip_id is unknown to the static feed. (#93, #119) **[EDGE]**
- **R14** An absent, empty or mismatched `route_id` in the trip descriptor is never disqualifying. (#20, #90, #119) **[EDGE]**
- **R15** A `direction_id` disagreement between the static feed and the realtime descriptor for the same `trip_id` is never disqualifying. (#90) **[EDGE]**
- **R16** `stop_sequence` is never used for matching when a `stop_id` is present, because providers publish 0 for every entry in a trip. (#90) **[EDGE]**
- **R17** A `stop_time_update` for any stop other than the sensor's configured origin, including a `stop_id` absent from the static feed, is ignored silently. (#22) **[EDGE]**
- **R18** A realtime trip whose stop list does not correspond to the static trip's (extra, missing or reordered stops) simply fails to match on the affected stop rather than invalidating the trip. (#90) **[EDGE]**
- **R19** An arrival or departure `time` of 0 or absent is treated as absent, never as 1970. The predicted time is then derived from the delay applied to the scheduled time. (#90, #119) **[EDGE]**
- **R20** Both forms are supported: an absolute epoch time, and a relative delay in seconds which may be negative. A trip-level delay applies when no stop-level value is present. (#18, #90)
- **R21** A predicted time already in the past relative to now is discarded and the scheduled time stands. No negative countdown is ever shown. (#61) **[EDGE]**
- **R22** Agency-prefixed realtime identifiers match unprefixed static ones (`MTA_308209` to `308209`, `MTA NYCT_JG_A5-Weekday-SDon-035000_B63_657` to `JG_A5-Weekday-SDon-035000_B63_657`). This is done first against a deterministic alias table built at ingest, then by bounded runtime suffix-candidate generation (at most four candidates, split on `_` and space) as a fallback. The matching path used is exposed as `matched_by`. (#99) **[EDGE]**
- **R23** Service alerts match on `informed_entity` identifying a route, a trip, a stop, or a truncated/prefix `trip_id` (indexed prefix range scan). An alert matching nothing is ignored without raising. System-wide alerts with no per-stop entity are supported. (#90) **[EDGE]**
- **R24** Vehicle positions are matched to the configured route by `trip_id` when the trip descriptor omits `route_id`. (#20) **[EDGE]**
- **R25** A GeoJSON file is written per route *and* per direction on every realtime update, containing an empty `FeatureCollection` when nothing matched. It is never left absent or stale. Written via temp file plus `os.replace` with mode 0o644 under `<config>/www/gtfsie/`, so it is servable over `/local/`. (#20, #117, #128, #136) **[EDGE]**
- **R26** When a feed decodes but contains no update matching the configured trip and stop, the sensor shows an explicit "no realtime" marker, keeps publishing the scheduled time, and records when the feed was last successfully read. Far-future departures legitimately have no realtime and must not blank the entity. (#22, #90, #100, #108, #119, #124, #126) **[EDGE]**
- **R27** A reported delay of 0, empty, null or `-` is presented as "no delay reported", not as a measured zero. (#112) **[EDGE]**
- **R28** A separate derived delay is published, computed as realtime departure minus scheduled departure, formatted `h:mm:ss`, signed so early running is negative, empty when it cannot be computed. The feed-supplied delay and the derived delay are both exposed even when they disagree, with no attempt to reconcile them. (#112) **[EDGE]**

### 2.4 Sensor and attributes

- **S1** Every datetime-valued attribute is an aware UTC value in one consistent representation across `next_departures`, `next_departures_lines`, `next_departures_headsign` and every realtime equivalent. No naive local time, no bare time-of-day, no missing or empty offset. (#7, #16, #63, #107, #112) **[EDGE]**
- **S2** Alongside the UTC value, each entry carries a local-time form in the feed's zone (`departure_local` as an aware ISO string, `departure_time_local` as 24-hour `HH:MM`) directly usable in templates and text-to-speech without conversion. (#16, #138)
- **S3** Local rendering happens in exactly one module, from the epoch plus the instance's `tz_name`. No local representation is stored in the database. (#63, #107)
- **S4** An empty or absent optional GTFS text field renders as an empty string or is omitted. The literal string `None` never appears in any attribute. (#3, #13) **[EDGE]**
- **S5** The per-departure line label falls back to `route_short_name` when `route_long_name` is empty and to `route_long_name` when `route_short_name` is empty. (#13) **[EDGE]**
- **S6** Each departure exposes a destination: the trip headsign when present, otherwise the name of the trip's last stop. `route_long_name` is never split on a guessed separator. (#3, #72) **[EDGE]**
- **S7** Every list attribute is always present, empty when there is nothing to report, and every entry carries an identical key set drawn from a fixed tuple. (#72) **[EDGE]**
- **S8** `route_type` is surfaced as a human-readable mode name and drives the entity icon; the agency name is the entity attribution. (#13, #15)
- **S9** The main state is the next departure as an aware UTC datetime with device class `timestamp`. A separate enum status sensor carries `ok` / `no_departure` / `importing` / `horizon_exhausted` / `expired` / `error`, so "nothing scheduled" is distinguishable from "integration broken" without violating the timestamp device class. (#66, #138, #140) **[EDGE]**
- **S10** A vicinity stop sensor's state combines stop name and next departure time so it changes on every meaningful update and is never a constant string. (#66) **[EDGE]**
- **S11** Vicinity sensor `unique_id` is `f"{entry_id}_stop_{stop_id}"`, stable across refreshes and across tracked-location changes, so entities are reused rather than accumulating one per stop ever passed and duplicate `_2` / `_3` entities. (#75) **[EDGE]**
- **S12** A vicinity sensor's entity id carries a stable `_vicinity` suffix distinguishing it from start/end sensors, and its departure list entries carry at least departure time, date, stop_name, route, route_long, headsign, trip_id, direction_id and icon. (#95)
- **S13** Setup performs no blocking database work. Entities are created immediately with placeholder data and populate on the first refresh after startup. (#75, #109)
- **S14** Each config entry's identity incorporates datasource, route, direction, origin and destination, so a second entry sharing a stop pair neither collides with nor overwrites the first. (#115) **[EDGE]**
- **S15** Removing or disabling an entry leaves no residue that prevents an equivalent entry being created afterwards, and a newly created entry never comes up permanently unavailable. (#100, #103) **[EDGE]**
- **S16** Whenever an entity transitions to unavailable, the reason is logged at warning level. (#130)
- **S17** The departure list is capped at the configured count and further capped against Home Assistant's attribute size limit. (#131)
- **S18** Scheduled departures are consumable through sensor state and attributes. A calendar platform is explicitly out of scope. (#97)

### 2.5 Config flow

- **C1** All user-facing strings come from translation files. No English literal appears in flow code. (#30, #101)
- **C2** Selected agency, route, stop and direction values are carried as `SelectOptionDict` value/label pairs, so an identifier containing colons, commas, parentheses or non-ASCII characters survives selection and is never parsed back out of a composite label. (#113) **[EDGE]**
- **C3** The route picker labels each route with `route_id`, `route_short_name` and `route_long_name` together, so a route stays identifiable when short name is absent, meaningless, or when `route_id` does not match the publicly advertised line number. (#1) **[EDGE]**
- **C4** Route type and direction selectors show words, not raw GTFS codes. (#104)
- **C5** Two route-definition modes are offered: explicit origin and destination `stop_id`s, and origin/destination by station or city name for rail, where the name resolves against the many `stop_id`s a station carries. (#25)
- **C6** Validation succeeds when a departure exists anywhere in the materialised window, not only on the current calendar day. Configuring shortly before midnight, on a weekend, or on a holiday succeeds. (#5, #104) **[EDGE]**
- **C7** A failed validation returns to the same step with route, direction, origin and destination pre-filled via suggested values, and names the reason. It never silently redisplays a cleared form. (#105, #113) **[EDGE]**
- **C8** URLs and key values are stripped of surrounding whitespace. A realtime step submitted with blank key name and blank key value succeeds. (#92, #129) **[EDGE]**
- **C9** Any URL the user types is accepted, and a local zip is usable as an alternative. There is no built-in provider catalogue whose staleness can block a user. (#99)
- **C10** A URL that is not a static GTFS zip is rejected with a message explaining that a static feed, not a realtime feed, is required. No directory is created and no import is started. (#82, #110) **[EDGE]**
- **C11** While a datasource is importing, entry creation against it is refused with one clear message and no stack trace, and no database query is attempted. (#64, #68, #84, #102, #103)
- **C12** An existing entry can be reconfigured (route, direction, origin, destination, realtime, options) without deleting it and without re-importing the datasource. (#26)
- **C13** Deleting a datasource succeeds including after a failed or partial import, and offers a choice of whether to also delete the imported database file. (#110) **[EDGE]**
- **C14** The options flow uses the framework-provided `self.config_entry` and never assigns it. (#139)
- **C15** The flow displays an estimated database size before the user commits to a window length, and states plainly that the static feed is not refreshed automatically. (#27, #70)
- **C16** The config flow loads and a static-only configuration remains fully usable when the GTFS-Realtime protobuf bindings are not importable. Realtime imports are deferred to the point of use. (#29, #68) **[EDGE]**

### 2.6 Local stops (vicinity)

- **L1** The tracked source may be fixed coordinates, a zone, a person or a device_tracker. (#118, #125)
- **L2** When the tracked entity is unavailable or is missing *either* latitude or longitude, the entity id is logged and an empty stop list is returned without raising. Both coordinates are checked. (#73) **[EDGE]**
- **L3** Refreshes driven by tracker movement are rate-limited and suppressed when the tracked location has moved less than a configurable threshold. Both the threshold and the maximum data age are user-configurable. (#75) **[EDGE]**
- **L4** The maximum number of nearby stops is a user option, not a hardcoded constant. (#109) **[EDGE]**
- **L5** When stops found within the radius exceed the configured maximum, the flow tells the user the count and the limit and lets them reduce the radius. (#67) **[EDGE]**
- **L6** A vicinity configuration can restrict which stops and/or routes it reports on, because dashboard cards cannot filter on a nested attribute. (#87) **[EDGE]**
- **L7** When a feed imports successfully but produces no departures for the configured location, the reason is surfaced: no stops in radius, stops found but no active service today, or no trips referencing those stops. (#67)
- **L8** Newly discovered stops are scoped into the departure index on demand, as an indexed insert, without a rebuild. Unpinned scope rows unused for 24 hours are garbage-collected along with their departures. (architecture) **[EDGE]**
- **L9** Stops referenced by a live subentry are pinned, and pinning is re-asserted on every entry setup and reconfigure, so a restored backup or a rewritten stop list can never lose its scope and go empty a day later. (architecture) **[EDGE]**
- **L10** Realtime for a vicinity entry is fetched once per cycle and reused across all stops, using the same request builder as route entries. (#109)
- **L11** Vicinity departure times use the resolved feed zone. No manual timezone-correction option is offered; the resolution chain in Q2 is the fix. (#107)
- **L12** Vicinity entries produce one sensor per stop, and the set of sensors is recomputed only when the tracked location moved beyond the threshold. (#75)

### 2.7 Services (actions)

- **V1** `gtfsie.refresh_datasource` re-downloads the static feed from its original URL and rebuilds the local dataset in place, keeping config entries, entity ids and user options valid. Sensors recover on their own without a restart, reload or reconfiguration. (#27, #103, #116, #135)
- **V2** `refresh_datasource` accepts the same API key name, value and placement options as the setup flow, plus an optional `drop_files` list so a non-compliant feed loads without manual unzip-edit-rezip. (#17, #91)
- **V3** A manual refresh and the scheduled refresh can never touch the same datasource concurrently. The second is skipped with a logged message. "Database is locked" never surfaces. (#4) **[EDGE]**
- **V4** `gtfsie.download_realtime` fetches a realtime endpoint and writes the raw payload plus an optional human-readable decoded dump to a user-specified file, chmod 0o644. (#93, #136)
- **V5** `download_realtime` accepts the key in a caller-named header or a caller-named query parameter, and never appends the key to the URL when header placement is selected. (#80, #99) **[EDGE]**
- **V6** When a payload cannot be decoded (for example a zip containing a `.bin`), the call fails with a message naming the URL and the problem. It never raises from writing an output file whose name was never assigned. (#89) **[EDGE]**
- **V7** Every field any action reads from call data has a documented default or is validated by the service schema. An omitted optional field produces a clear validation message, never a `KeyError`. (#80) **[EDGE]**
- **V8** `gtfsie.extract_departures` returns response data grouped as today and tomorrow, with empty lists when there are none. (#131)
- **V9** `gtfsie.extract_trip_stops` returns the full stop list of a given trip.
- **V10** Every sensor responds to `homeassistant.update_entity` with a full refresh including realtime. No integration-specific refresh service is required for that. (#62)
- **V11** An on-demand refresh does not tear down and recreate entity data. Sensors keep their previous values and stay available while it runs. (#62) **[EDGE]**
- **V12** `services.yaml` states explicitly that the static feed is never refreshed automatically and must be scheduled by an automation calling `gtfsie.refresh_datasource`. (#27)

### 2.8 Platform and packaging

- **P1** Every third-party runtime dependency, including the GTFS-Realtime protobuf bindings, is declared in the manifest so a fresh install can open the config flow without `ImportError`. (#29, #68)
- **P2** `zone` and `person` are declared as `after_dependencies`, as separate list entries, so neither can fail gtfsie's setup and neither can block Home Assistant from starting. (#69) **[EDGE]**
- **P3** A minimum Home Assistant version is declared, and a too-old core produces an explicit logged message rather than an unopenable config flow reporting "Invalid handler specified". (#96)
- **P4** Repeated identical log lines (realtime failures, "still importing") are throttled: once per key per interval, with a suppressed-count suffix. (#103, #137) **[EDGE]**
- **P5** Diagnostics expose import state and last run, window bounds, feed meta, resolved zone and its source, per-table row counts, database file size, realtime last success and last error per feed kind, scope size, and the current `NoDeparturesReason`. (#67)
- **P6** A repair issue is raised, naming the identifier, when a configured `stop_id` or `route_id` disappears from a republished feed. Stop-name matching is never used to paper over it. (#115) **[EDGE]**

---

## 3. BUILD ORDER

Each phase is independently testable and leaves the integration in a shippable state. Phases 1 and 2 deliver a working next-departure sensor.

**Phase 0 -- skeleton and pure helpers.** `manifest.json`, `const.py`, `exceptions.py`, `paths.py`, `helpers/tz.py`, `helpers/text.py`, `helpers/geo.py`, `helpers/logthrottle.py`. Tests: `parse_gtfs_time("25:10:00") == 90600`; quoted and junk dates; `day_origin_utc` across a spring-forward and a fall-back day in `America/Toronto`, `Europe/Berlin`, `Australia/Adelaide` (30-minute offset) and `Pacific/Chatham` (45-minute offset); `clean(None) == ""`; bounding boxes at the poles and across the antimeridian; great-circle distance against known pairs. Zero Home Assistant imports in this phase; everything is a pure function. (Q1, Q2, Q18, S4, S5, P4)

**Phase 1 -- store and synchronous ingest.** `store/schema.py`, `store/connection.py`, `store/rows.py`, `ingest/source.py`, `ingest/csvreader.py`, `ingest/calendar.py`, `ingest/load.py`, `ingest/instances.py`, `ingest/materialise.py`, `ingest/plan.py`, `store/queries.py`, `store/verify.py`. `run_import` is a pure function of an `ImportPlan` returning an `ImportResult`, callable synchronously in-process against a fixture zip. Fixtures: a minimal three-stop feed; a feed with no `route_short_name`; a feed with no `route_long_name`; a fully quoted `calendar.txt`; a `feed_info.txt` with blank dates and a non-date version; a feed with no `feed_info.txt`; a feed with `location_type` 3 rows carrying no coordinates; a feed with `transfer_type=4`; a `calendar_dates`-only feed; a `calendar`-only feed; a feed with a cross-midnight `25:30` trip; a `frequencies.txt` feed; a UTF-8-BOM feed with diacritics; a feed whose `routes.txt` omits `agency_id`. Tests assert row counts, `service_date` de-duplication, the absence of a `None` string anywhere, and that the query returns the expected departures for a fixed `now`. Run the CI grep gate from here on. (I1, I4-I15, I31, Q1-Q6, Q10-Q13, I25)

**Phase 2 -- entries, coordinator, one sensor.** `__init__.py`, `registry.py`, `coordinator.py`, `sensor.py`, `attributes.py`, `config_flow.py` (datasource + route-by-stop-ids paths only, synchronous import), `translations/en.json`. Deliverable: a user adds a datasource and a stop pair and gets a working next-departure timestamp sensor, an arrival sensor, a status sensor and a full attribute set. Tests: attribute key-set stability across every fixture; state is `None` with status `no_departure` rather than a fabricated value; departures already past are never returned; the offset excludes near departures; "include tomorrow" does not change today's selection; two entries on the same stop pair with different routes return different timetables. (Q7-Q9, S1-S9, S13-S17, C2-C8, C14)

**Phase 3 -- background import and control plane.** `ingest/worker.py`, `ingest/manager.py`, progress queue, heartbeat rows, stale-run recovery, temp cleanup, "still importing" states, free-space pre-flight, `temp_store_directory`, log throttling. Tests: kill the worker mid-import and assert recovery clears the flag and offers retry; restart Home Assistant with a stale heartbeat present; assert that no filesystem or database call runs on the loop (use `pytest-homeassistant-custom-component`'s blocking detection); assert exactly one "still importing" warning per import across many polling cycles. (I16-I20, I29, I30, C11, P4)

**Phase 4 -- refresh lifecycle.** `ingest/guard.py`, `.prev` retention for both database and archive, atomic swap under live readers, `verify()` gating, `repairs.py`, `horizon.py` (window slide plus `register_need`), scope growth and GC, tzdata invalidation, `services.py` / `services.yaml` for `refresh_datasource`, `extract_departures`, `extract_trip_stops`. Tests: `should_abort_replace` truth table; a refresh with a forward-dated archive keeps the old data; a refresh with a corrupt download keeps the old data; a refresh that drops a pinned stop's departures is refused and `.prev` is restored; a refresh where a pinned `stop_id` vanished promotes and raises a repair; **read a value, swap the database file, read again, assert the new value** (getting this wrong pins readers to an unlinked inode and presents as "the refresh worked but the data is stale"); a tzdata bump triggers a rebuild of layers 1 and 2 only. (I21-I27, I32, Q15-Q17, V1-V3, V8, V9, P6)

**Phase 5 -- realtime trip updates.** `realtime/model.py`, `fetch.py`, `decode.py`, `ids.py`, `match.py`, `hub.py`; realtime steps in the config flow; `download_realtime`. Fixtures, one per reported failure: absolute epoch times; delay-only updates including negative delays; `{"delay": 0, "time": 0}`; trip-level delay with no stop-level value; empty `route_id`; `stop_sequence` 0 on every entry; `direction_id` disagreement; a `stop_id` absent from the static feed; agency-prefixed stop and trip ids; a truncated alert trip id; an HTML error page; an ASPX page; a JSON body at a `.pb` URL; a zip containing a `.bin`; an empty but valid feed; a SIRI StopMonitoring document. Tests assert that in every failure case the entity stays available with scheduled departures intact, that a past prediction falls back to schedule, that the reported and derived delays are both published, and that the feed is fetched exactly once per cycle regardless of departure count. (R1-R23, R26-R28, V4-V7, C16)

**Phase 6 -- vehicle positions, alerts, GeoJSON.** `realtime/geojson.py`, alert matching and exposure. Tests: an empty `FeatureCollection` is written when nothing matched; the file is written per route and per direction; the file is written atomically with mode 0o644; a vehicle with no `route_id` matches by `trip_id`; an alert matching nothing is ignored; a failure in the alerts feed leaves trip-update data intact. (R7, R23-R25)

**Phase 7 -- vicinity.** `VicinityCoordinator`, vicinity config-flow steps, on-demand scope growth, movement threshold, max stops, stop/route filters, per-stop sensors. Tests: a tracker reporting only latitude returns an empty list and logs the entity id; movement below the threshold does not trigger a query; `unique_id` stability across a location change; adding a vicinity entry to a datasource created for two stop pairs completes in seconds rather than rebuilding; unpinned scope GC does not delete a pinned stop's departures. (L1-L12, Q18)

**Phase 8 -- station-name mode, reconfigure, diagnostics, polish.** Station-name route mode with platform expansion; reconfigure flow; datasource deletion with the database-file choice; `diagnostics.py`; size estimation in the flow; README and the DST divergence note; performance pass on a national feed on a Pi 4. (C5, C9-C13, C15, Q13, Q14, P5, S18)

---

## 4. OPEN QUESTIONS

**Q-1. Config subentries versus separate config entries.**
Subentries (one datasource entry with route and vicinity subentries) make reconfiguring a route free, structurally prevent orphaning routes when a datasource is deleted, and give a natural place to pin scope. The subentry API is comparatively young.
*Recommendation:* use subentries, and keep a `datasource_id` field in every subentry's data so a fallback to separate entries is a migration rather than a rewrite. Build phase 2 against subentries from the start; retrofitting is worse than committing.

**Q-2. Default window length, and whether a whole-feed scope is offered at all.**
The design materialises only watched stops over a rolling window (default: 6 hours past, 36 hours future). A whole-feed option is what produced the 9 GB reports.
*Recommendation:* do not offer whole-feed scope. Make the window user-adjustable up to 7 days, driven upward automatically by `HorizonKeeper.register_need` from each entry's offset, include-tomorrow flag and departure count. If a maintainer later wants whole-feed for a research use case, add it as an advanced option with the estimated size shown first.

**Q-3. SIRI in v1.**
SIRI StopMonitoring is the only realtime option for NYC MTA bus and several European operators (#99), but it is a second format with its own identifier conventions and its own test corpus.
*Recommendation:* ship the normalisation boundary (`realtime/model.py`) in v1 so SIRI is a decoder addition, but hold the SIRI decoder itself for v1.1 unless a maintainer can supply a live key and two captured responses. Everything else in phase 5 is unaffected.

**Q-4. Frequency expansion cap and `exact_times=0` semantics.**
A headway-based whole-network feed can produce more materialised rows than a schedule-based feed ten times its size, and `exact_times=0` means the resulting instants are advisory, not promises.
*Recommendation:* cap at 5,000 expanded instances per trip per datasource (configurable), and expose `frequency_based: true` plus `exact_times` on every affected departure entry so a card can render it as "every 10 min" rather than a false precise time. Log once per import when the cap bites.

**Q-5. GeoJSON under `www/`.**
Writing into `<config>/www/` is what the reported use case needs (`geo_location` plus a map card over `/local/`), but it is a public directory and a `www` write is a side effect outside the integration's own storage.
*Recommendation:* keep it, because the alternative (a `geo_location` platform) is a larger surface and the reports specifically want map-card files. Make the path configurable, default it under `www/gtfsie/<ds_id>/`, and document that anything under `www/` is served unauthenticated.

**Q-6. Python multiprocessing start method.**
Recent Python defaults Linux to `forkserver` rather than `fork`, so the worker entry point must be importable in a fresh interpreter and must take only picklable primitives.
*Recommendation:* write `worker.py` to that constraint unconditionally (it costs nothing and is more testable anyway), bootstrap `sys.path` with the config directory in `worker_main`, and add a test that spawns the worker with `forkserver` explicitly rather than relying on the platform default.

**Q-7. Whose local time do the `*_local` attributes use?**
The feed's agency zone is correct for "when does this bus leave"; the Home Assistant zone is what a user watching a remote city might expect.
*Recommendation:* agency zone, with `timezone` and `timezone_source` published as attributes on every entity so a template can convert if it wants. Publishing both forms per departure doubles the attribute payload against the size cap for no clear benefit. Document the choice prominently, since a user tracking Rome buses from New York will notice.

**Q-8. Vicinity manual timezone correction (#107).**
The reported need was a three-hour correction for a feed whose stops are in another zone. That symptom is a bug in zone resolution, not a missing feature.
*Recommendation:* do not implement the option. Fix it with Q2's resolution chain and expose `timezone_source` so a wrong result is diagnosable. If a real feed exists whose `agency_timezone` and `stop_timezone` are both absent *and* wrong-by-inheritance, revisit with that feed as the fixture.

**Q-9. Should `stop_times` be `WITHOUT ROWID`?**
It is declared `WITHOUT ROWID` above on a `(trip_key, stop_sequence)` primary key, which removes a full extra copy of the key versus a rowid table with a unique index. The trade is page-split cost during bulk load unless rows arrive in primary-key order.
*Recommendation:* sort by `(trip_key, stop_sequence)` during the load (`stop_times.txt` is usually already in that order, so the sort is nearly free) and keep `WITHOUT ROWID`. Benchmark on a national feed in phase 8; if load time regresses more than 20 percent, fall back to a rowid table and accept the index.

**Q-10. Coexistence with an existing GTFS integration on the same instance.**
Users will run both during a transition. Domain, storage directory and `www` path are all distinct, so there is no technical conflict.
*Recommendation:* no migration tooling. Document that both can run side by side, that gtfsie will re-download and re-import rather than adopting an existing database (schemas are unrelated), and that a user should delete the old entries once satisfied.
# gtfsie - Architecture, Requirements, Build Order

Clean-room Home Assistant integration for GTFS static and GTFS-Realtime transit departures.
Nothing in this document derives from any existing implementation's source; behaviour is specified from public issue reports, the GTFS/GTFS-RT specifications and the Home Assistant developer documentation.

---

---

## 0. STATUS AND PROVENANCE

This document supersedes the previous specification (commit `00ae805`, 194
requirements). It was produced by mining upstream issue reports for behaviour,
generating three independent architectures and scoring them with three
independent judges; the two-file split won two of three votes, and section 1.1
records the reasoning.

**Nothing here derives from any existing implementation's source.** Behaviour is
specified from public issue reports, the GTFS and GTFS-Realtime specifications
and the Home Assistant developer documentation. Issue reports describe symptoms,
which is what was mined.

**Coverage.** 165 requirements are recorded below, numbered as a single sequence
1 to 165 with an area-letter prefix, verified complete and without duplicates.
They were distilled from 289 candidates mined across the whole issue history;
the difference is near-duplicates merged during synthesis.

Earlier revisions of this paragraph gave the *candidate* count as though it were
the number written down -- 194, then 214, against 136 and 137 actually recorded.
Both were wrong, and wrong in the flattering direction. The figure above is
counted from this document rather than carried across from a tool's output.

The gap the previous revision recorded -- issues #31-60, blocked twice by a
transient classifier failure -- is closed. Re-mining that range in three smaller
batches returned 75 candidates, of which 45 duplicated requirements already
present and 28 were new; those 28 are I-138 through V-165.

That pass also found three contradictions *inside* section 2. Each is resolved
by amending the older requirement rather than leaving both in place to surface
later as two tests that cannot both pass: Q-34 against the lookback window of
Q-140, Q-37 against the optional route selection of Q-143, and R-65 against the
affix normalisation of R-66 -- which were already inconsistent with each other
before this revision, independently of anything newly mined.

Two candidates were rejected on their merits, recorded here so they are not
re-mined later as gaps. "Realtime epoch timestamps must be compared in the
feed's local timezone" (#48) is confused: epoch comparison is zone-independent,
and this architecture resolves everything to absolute instants at ingest, so
there is nothing to implement. "Normalise across date, datetime and delay
representations before comparison" (#55) restates the ingest-time instant model;
building it as a comparison-time step would reintroduce precisely the class of
defect that section 1.1 exists to design out.

One finding from #32 is calibration rather than requirement: a 4 MB archive
produced a 46.5 MB database, a ratio of about 11x. That belongs in the test
fixtures for `ingest/estimate.py`, not in section 2.

**What changed from the previous specification.** The storage layer, and only
the storage layer. The earlier design used one database and mutated it in place
to slide the departure window. This one splits it: `feed.sqlite` is a pure
function of the archive, `index.sqlite` is a pure function of `(feed.sqlite,
window, resolved zone, scope)`, and neither is written again after promotion.
That removes the in-place writer, makes the "database is locked" class of
failure structurally unreachable, and lets the index be rebuilt from a
checked-in fixture hundreds of times in a single test run.

The time model is unchanged and was correct in both: absolute instants resolved
at ingest, the service day anchored at local noon minus 43200 seconds, and no
date function in any SQL statement.

**Build status.** Phase 0 is partially complete in `pygtfsie` (commit
`5a31619`): `helpers/tz.py`, `helpers/text.py`, `helpers/geo.py`,
`helpers/logthrottle.py`, `exceptions.py` and the GTFS half of `const.py`, with
285 tests running in under a second and a timezone/instant sweep enforced in CI.
The outstanding items are listed against Phase 0 in section 3.

## 1. ARCHITECTURE

### 1.1 The choice and why

**Chosen: the two-file split (candidate C), with grafts from A and B.**

Two of three judges picked C, and the reason survives scrutiny: it is the only candidate with a real functional seam. `feed.sqlite` is a pure function of the downloaded archive. `index.sqlite` is a pure function of `(feed.sqlite, window, resolved timezone, scope)`. Those two things change at completely different frequencies - a publisher re-issues an archive every few weeks, the departure window rolls every night - and every design that fuses them pays the CSV-parsing cost on every window roll and cannot recompute stored instants after a timezone correction or an anchor bugfix without a network re-download. It is also the only split that makes the date arithmetic cheaply testable: a checked-in 40 KB synthetic `feed.sqlite` fixture lets the index be rebuilt hundreds of times per test run to assert both DST transitions, `25:30:00` stop times, calendar-dates-only services and all-zero weekday flags.

All three candidates share the correct central bet, which is retained unchanged: **every `(trip, stop_time, service date)` is resolved at ingest to an absolute UTC epoch integer using the GTFS service-day anchor (local noon minus 43200 s) in the resolved IANA zone, and no SQL statement in the integration contains `date()`, `datetime()`, `strftime()`, `julianday()` or `'now'`.** DST, times past `24:00:00` and midnight-crossing service days become unreachable at query time rather than carefully handled.

Grafts, all endorsed by at least one judge:

- **From A:** the sorted insert (stage unindexed, then drain into the `WITHOUT ROWID` clustered B-tree in primary-key order with one `INSERT ... SELECT ... ORDER BY`); the free-space precheck on both the database directory and the temp directory, including tmpfs detection; the service-date lookahead capability that lets the config flow validate a route whose next service day is weeks away; the CI lint test that greps the query module for date functions; the subprocess-isolation test that asserts `homeassistant` is not importable-in-scope in the worker.
- **From B:** the four-way empty-result vocabulary (`feed_expired`, `feed_future`, `window_exhausted`, `no_service_today`); a forced index rebuild when the Home Assistant timezone changes and it was used as the fallback zone; explicit truncation/coverage flags on query results rather than a silently short list; the pre-import row and size estimate shown before download; `RECORDER_EXCLUDE` for the heavy attributes; degenerate-`direction_id` detection.
- **From C's own strengths:** change detection by fingerprinting sorted `(member name, size, crc32)` over the members actually read, which is immune to a publisher re-zipping identical content; phase checkpointing so an interrupted ingest re-runs only the index build; honest documentation of the spring-forward anchor artefact; a TTL on matched realtime entries.

Three defects the judges found in C are fixed here and are not negotiable:

1. `departure.arrival_utc` is **nullable**. Blank `arrival_time` at non-timepoint stops is legal and common.
2. There is **no in-place mutation of a live database**. The nightly window roll builds a new index generation and `os.replace()`s it, exactly as a full rebuild does. This removes C's prune-and-extend writer, removes A's and B's two-pruning-strategies problem, and makes the "database is locked" class of failure structurally unreachable because there is exactly one writer and it never writes a file a reader holds open.
3. Because nothing writes the promoted files, they are checkpointed and set to `journal_mode=DELETE` before promotion, and readers open them `mode=ro`. This avoids the read-only-WAL `-shm` hazard that all three candidates walked into (a read-only connection to a WAL database needs a writable `-shm` segment; on `/share` or an external mount with different ownership that surfaces as "attempt to write a readonly database").

`stop_seq` is dropped from the departure primary key (kept as a column). Route/agency/headsign text is **not** denormalised into departure rows; `index.sqlite` instead carries small snapshot dimension tables so it is self-contained and readers open one file on the hot path.

### 1.2 Data flow

```
source URL / local zip
   |  ingest/download.py      fetch, sniff magic bytes, conditional GET
   |  ingest/archive.py       validate layout, fingerprint members, optional member strip
   v
staging zip  ->  ingest/load_feed.py   ->  feed-<fingerprint>.building.sqlite
                                             |  os.replace  (phase checkpoint)
                                             v
                                          feed.sqlite            (normalised static copy)
                                             |
                                             |  ingest/materialise.py
                                             |  window = [today - back_days, today + horizon_days]
                                             v
                                          index-<gen>.building.sqlite
                                             |  os.replace
                                             v
                                          index.sqlite           (materialised departures)
                                             |
   db/store.py (one connection per file, single-thread executor, mode=ro)
                                             |
   db/queries.py  ->  coordinator.py  ->  presenter.py  ->  sensor.py
                          ^
                          |  rt/*  (independent poll, own failure domain)
```

Everything left of `feed.sqlite` runs in a `ProcessPoolExecutor` with a `forkserver` context and touches no Home Assistant object. Everything right of it runs in the event loop or in the store's dedicated executor thread.

### 1.3 Module layout

```
custom_components/gtfsie/
  manifest.json
  const.py
  exceptions.py
  __init__.py
  config_flow.py
  coordinator.py
  datasource.py
  presenter.py
  local_stops.py
  services.py
  services.yaml
  sensor.py
  binary_sensor.py            # datasource healthy / realtime live
  repairs.py
  diagnostics.py
  recorder.py                 # exclude_attributes hook
  strings.json
  translations/en.json
  helpers/
    text.py                   # the only place str(None) can happen
    tz.py                     # the whole timezone policy
    geo.py                    # bounding boxes, haversine
    modes.py                  # route_type -> name, icon
    ids.py                    # affix normalisation, name keys, unique_ids
  db/
    schema_feed.py            # DDL + pragmas for feed.sqlite
    schema_index.py           # DDL + pragmas for index.sqlite
    store.py                  # FeedStore, IndexStore, generation handshake
    queries.py                # every SQL statement in the integration
  ingest/
    download.py               # HTTP, sniffing, temp-file hygiene
    archive.py                # zip validation, fingerprint, member strip
    reader.py                 # tolerant CSV decode
    calendar.py               # calendar + calendar_dates -> active service dates
    load_feed.py              # CSV -> feed.sqlite
    materialise.py            # feed.sqlite -> index.sqlite
    estimate.py               # pre-import row/byte projection
    worker.py                 # subprocess entry point, HA-free
    manager.py                # HA side: locking, scheduling, progress, state
  rt/
    client.py                 # HTTP + credential placement
    protobuf.py               # deferred bindings import, decode, dump
    siri.py                   # SIRI JSON -> same snapshot
    model.py                  # format-neutral RT types + presence probes
    matcher.py                # the match ladder + effective-time rules
    geojson.py                # vehicle positions -> www
  tests/
    fixtures/                 # tiny synthetic feeds, one per pathology
    test_no_date_sql.py       # lint: no date functions in db/queries.py
    test_worker_isolation.py  # lint: worker imports without homeassistant
```

### 1.3a How the layout is distributed across two repositories

The tree above is written as one package for readability. It ships as two, and
the seam is a line this design already draws: everything that runs in the ingest
subprocess is Home-Assistant-free by construction, which is what
`test_worker_isolation.py` exists to prove.

| Repository | Contents |
| --- | --- |
| [`pygtfsie`](https://gitlab.com/ggiesen/pygtfsie), published to PyPI | `helpers/*`, `db/schema_feed.py`, `db/schema_index.py`, `db/queries.py`, all of `ingest/` except `manager.py`, all of `rt/` except the coordinators, `exceptions.py`, and the GTFS half of `const.py` |
| [`ha-gtfsie`](https://gitlab.com/ggiesen/ha-gtfsie), distributed through HACS | `manifest.json`, `__init__.py`, `config_flow.py`, `coordinator.py`, `datasource.py`, `presenter.py`, `local_stops.py`, `sensor.py`, `binary_sensor.py`, `services.py`, `repairs.py`, `diagnostics.py`, `recorder.py`, `db/store.py`, `ingest/manager.py`, translations, and the Home Assistant half of `const.py` |

`ha-gtfsie/manifest.json` declares `pygtfsie==<version>` under `requirements`.
That is the same mechanism by which `pygtfs` reaches the integration this one
replaces, so the pattern is already established in HACS and needs no special
handling.

The reason for the split is test cost. Most of the logic here is date
arithmetic, CSV tolerance, protobuf decoding and SQL, none of which needs an
event loop, a config entry or a pinned Home Assistant version to exercise. As a
library those tests run on bare pytest across every supported Python in under a
second, so the parts most likely to be wrong are also the cheapest to check.

Two signatures in section 1.4 take a `HomeAssistant` and so cannot live in the
library as written. Both are split rather than relocated wholesale:

- `db/store.py` `ReadStore.async_open(hass, path)` -- the library owns
  connection setup, pragmas, `mode=ro` handling and the generation handshake,
  taking an injected executor callable. `ha-gtfsie` supplies
  `hass.async_add_executor_job`.
- `helpers/geo.py` `tracked_coords(hass, entity_id)` -- reading a device
  tracker's state is Home Assistant's job. The geometry stays in the library and
  the state read moves out.

The boundary is enforced rather than documented: a CI job fails the build if
anything under `pygtfsie/` imports `homeassistant`. Without that gate the
boundary erodes one convenient import at a time and the fast test tier goes with
it.

### 1.4 Key public signatures

```python
# helpers/text.py
def clean(value: str | None) -> str: ...          # '' for None, 'None', 'nan', whitespace-only
def opt(value: str | None) -> str | None: ...
def route_picker_label(route_id: str, short: str, long: str) -> str: ...   # display only, never parsed
def line_label(short: str, long: str) -> str: ...  # short if long empty, long if short empty
def signed_hms(delta: timedelta | None) -> str | None: ...  # '-0:04:05', never wrapped to a clock time

# helpers/tz.py
def resolve_zone(agency_tz: str, stop_tz: str, ha_tz: str | None) -> tuple[ZoneInfo, str, str]:
    """Returns (zone, name, source) where source in {'agency','stop','homeassistant','utc'}."""
def service_day_anchor_utc(service_date: int, tz: ZoneInfo) -> int:
    """epoch seconds of (local noon on YYYYMMDD) - 43200."""
def utc_iso(ts: int | None) -> str | None: ...            # '2026-08-02T14:07:00+00:00'
def local_iso(ts: int, tz: ZoneInfo) -> str: ...          # '2026-08-02T16:07:00+02:00'
def local_hhmm(ts: int, tz: ZoneInfo) -> str: ...         # '16:07'
def day_label(ts: int, now_utc: int, ha_tz: ZoneInfo) -> str: ...   # 'today'|'tomorrow'|'2026-08-05'

# helpers/geo.py
DEG_PER_METRE: float = 360.0 / 40_000_000.0
def bounding_boxes(lat: float, lon: float, radius_m: float) -> list[tuple[float, float, float, float]]
def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float

# helpers/ids.py
def name_key(name: str) -> str: ...        # casefold + accent fold + collapse whitespace
def affixes(identifier: str) -> tuple[str, ...]:
    """Suffix/prefix candidates split on ':', '_', ' ' boundaries only. Never truncates arbitrarily."""

# db/store.py
class ReadStore:
    @classmethod
    async def async_open(cls, hass: HomeAssistant, path: Path) -> "ReadStore": ...
    async def async_run(self, fn: Callable[[sqlite3.Connection], T]) -> T: ...
    async def async_reopen(self) -> None: ...          # after a generation swap
    async def async_close(self) -> None: ...
    @property
    def generation(self) -> int: ...
    @property
    def meta(self) -> Mapping[str, str]: ...

# db/queries.py  (all synchronous, all take a connection, none contains a date function)
@dataclass(slots=True)
class DepartureRow:
    stop_uid: int; stop_id: str; stop_name: str; stop_platform: str
    departure_utc: int; arrival_utc: int | None; service_date: int; stop_seq: int
    trip_uid: int; trip_id: str; trip_headsign: str; trip_short_name: str
    route_uid: int; route_id: str; route_short_name: str; route_long_name: str
    route_type: int; direction_id: int | None
    agency_name: str; tz_name: str
    first_stop_name: str; last_stop_name: str
    dest_stop_id: str | None; dest_stop_name: str | None; dest_arrival_utc: int | None

@dataclass(slots=True)
class DeparturePage:
    rows: list[DepartureRow]
    truncated: bool                 # limit hit, more exist inside the window
    window_end_utc: int             # materialised through
    exhausted: bool                 # scan reached window_end without filling the limit

def departures_at_stops(conn, stop_uids: Sequence[int], from_utc: int, until_utc: int,
                        limit: int, route_uids: Sequence[int] | None = None,
                        direction_id: int | None = None) -> DeparturePage: ...
def departures_origin_to_destination(conn, origin_uids: Sequence[int], dest_uids: Sequence[int],
                                     from_utc: int, until_utc: int, limit: int,
                                     route_uids: Sequence[int] | None = None,
                                     direction_id: int | None = None) -> DeparturePage: ...
def trip_instance_stops(conn, trip_uid: int, service_date: int) -> list[TripStopRow]: ...
def stops_in_boxes(conn, boxes, limit: int) -> list[StopRow]: ...
def resolve_stop_ids(conn, stop_ids: Sequence[str]) -> dict[str, int]: ...
def stop_uids_for_name_key(conn, name_key: str) -> list[int]: ...

# db/queries.py, feed.sqlite side (config flow and lookahead only)
def list_agencies(conn) -> list[AgencyRow]: ...
def list_routes(conn, agency_uid: int | None, search: str | None, limit: int) -> list[RouteRow]: ...
def list_stops_on_route(conn, route_uid: int, direction_id: int | None) -> list[StopRow]: ...
def search_station_names(conn, prefix_key: str, limit: int) -> list[tuple[str, int]]: ...
def service_dates_for(conn, service_ids: Sequence[str], on_or_after: int,
                      max_days: int = 400) -> list[int]: ...
def next_service_date_for_pair(conn, origin_uids, dest_uids, route_uid: int | None,
                               direction_id: int | None, on_or_after: int,
                               max_days: int = 400) -> int | None: ...
def feed_service_bounds(conn) -> tuple[int | None, int | None]: ...
def import_problems(conn, limit: int) -> list[ProblemRow]: ...

# ingest/download.py
@dataclass(slots=True)
class FetchResult:
    status: int; path: Path | None; etag: str | None; last_modified: str | None
    not_modified: bool; byte_count: int; content_type: str

async def async_fetch(hass, url: str, dest_dir: Path, *, header_name: str | None,
                      header_value: str | None, etag: str | None, last_modified: str | None,
                      timeout_s: int = 900) -> FetchResult: ...
def sniff(path: Path) -> Literal["zip", "gzip", "html", "json", "protobuf", "empty", "unknown"]: ...

# ingest/archive.py
@dataclass(frozen=True, slots=True)
class FeedLayout:
    members: tuple[str, ...]; missing_required: tuple[str, ...]
    nested_zips: tuple[str, ...]; fingerprint: str; uncompressed_bytes: int

def validate_archive(path: Path) -> FeedLayout: ...     # raises NotAZipError / NotAGtfsFeedError
def fingerprint(path: Path) -> str: ...                 # sha256 over sorted (name, size, crc32) of read members
def rewrite_without(src: Path, dest: Path, drop: Sequence[str]) -> Path: ...

# ingest/reader.py
REQUIRED = ("agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt")
IGNORED  = ("shapes.txt", "transfers.txt", "fare_attributes.txt", "fare_rules.txt",
            "fare_products.txt", "fare_leg_rules.txt", "fare_transfer_rules.txt",
            "levels.txt", "pathways.txt", "translations.txt", "attributions.txt",
            "areas.txt", "stop_areas.txt", "networks.txt", "route_networks.txt",
            "timeframes.txt", "booking_rules.txt", "rider_categories.txt")
def rows(zf: zipfile.ZipFile, member: str) -> Iterator[dict[str, str]]: ...
def parse_gtfs_date(value: str) -> int | None: ...   # tolerates quotes/whitespace, None for blank or non-date
def parse_gtfs_secs(value: str) -> int | None: ...   # accepts '25:30:00' and 'H:MM:SS'

# ingest/worker.py   (HA-free; only primitives cross the process boundary)
@dataclass(frozen=True, slots=True)
class IngestRequest:
    datasource_id: str; job: Literal["full", "index_only"]
    zip_path: str | None; feed_path: str; feed_build_path: str
    index_path: str; index_build_path: str
    progress_path: str; log_path: str
    fingerprint: str; window_start_date: int; window_end_date: int
    ha_timezone: str; scope_kind: str; scope_values: tuple[str, ...]
    schema_version_feed: int; schema_version_index: int; generation: int

@dataclass(frozen=True, slots=True)
class IngestResult:
    ok: bool; phase: str; departure_rows: int; problems: int
    feed_start: int | None; feed_end: int | None
    error_key: str | None; error_detail: str | None

def run_ingest(request: IngestRequest) -> IngestResult: ...

class ProgressSink:
    def phase(self, name: str, done: int, total: int | None) -> None: ...
    def problem(self, member: str, identifier: str, message: str) -> None: ...

# ingest/manager.py
class ImportManager:
    async def async_setup(self) -> None: ...
    async def async_shutdown(self) -> None: ...
    async def async_refresh_source(self, ds: DatasourceRecord, *, reason: str,
                                   force: bool = False, url_override: str | None = None
                                   ) -> ImportOutcome: ...   # NOT_MODIFIED|REPLACED|SKIPPED_FUTURE_DATED|FAILED
    async def async_rebuild_index(self, ds: DatasourceRecord, *, reason: str) -> ImportOutcome: ...
    def status(self, datasource_id: str) -> DatasourceStatus: ...
    def raise_if_busy(self, datasource_id: str) -> None: ...   # raises DatasourceBusy
    @callback
    def async_add_listener(self, cb: Callable[[str, DatasourceStatus], None]) -> CALLBACK_TYPE: ...

# rt/matcher.py
@dataclass(slots=True)
class Effective:
    scheduled_utc: int; realtime_utc: int | None
    provider_delay_s: int | None; derived_delay_s: int | None
    rule: str; cancelled: bool; skipped: bool; stale: bool

class RealtimeIndex:
    @classmethod
    def build(cls, snap: RealtimeSnapshot, ctx: StaticContext) -> "RealtimeIndex": ...
    def lookup(self, dep: DepartureRow) -> tuple[StopUpdateRT | None, int | None, str]: ...
    def alerts_for(self, dep: DepartureRow) -> list[AlertRT]: ...
    def unmatched_trip_ratio(self) -> float: ...

def apply(dep: DepartureRow, upd: StopUpdateRT | None, trip_delay: int | None,
          fetched_utc: int, ttl_s: int) -> Effective: ...

# presenter.py
def departure_payload(dep: DepartureRow, eff: Effective, ha_tz: ZoneInfo,
                      feed_tz: ZoneInfo, now_utc: int) -> dict[str, Any]: ...
def entity_payload(rows: Sequence[dict], meta: EntityMeta, limit: int,
                   max_bytes: int = 12_000) -> dict[str, Any]: ...
```

### 1.5 The attribute contract

Frozen at v1. Renaming or removing any of these is a breaking change that requires a release note and a repair issue.

| Attribute | Type | Notes |
|---|---|---|
| `next_departure` | ISO-8601 UTC | same instant as the entity state |
| `next_departure_local` | ISO-8601 with feed-zone offset | for templates and TTS |
| `next_departure_time` | `HH:MM` in the feed zone | speakable |
| `next_departure_realtime` | ISO-8601 UTC or `""` | `""` means no realtime for this trip/stop |
| `delay_provider` | `h:mm:ss` signed, or `null` | `null` when the provider sent nothing, `0`, empty or null |
| `delay_derived` | `h:mm:ss` signed, or `null` | `realtime - scheduled`, may be negative |
| `departures` | list of dicts | the full per-departure payload below |
| `status` | enum | `ok, no_departures, no_service_today, feed_expired, feed_future, window_exhausted, extracting, datasource_failed` |
| `realtime_state` | enum | `ok, no_data, stale, error, disabled` |
| `realtime_last_success` | ISO-8601 UTC or `null` | |
| `feed_valid_from` / `feed_valid_to` | ISO date or `null` | |
| `feed_imported` | ISO-8601 UTC | |
| `timezone` / `timezone_source` | str | e.g. `Europe/Rome`, `agency` |
| `attribution` | str or absent | agency name |

Each entry of `departures` carries: `departure_utc`, `departure_local`, `departure_time`, `day` (`today`/`tomorrow`/ISO date), `service_date`, `line`, `route_id`, `route_short_name`, `route_long_name`, `mode`, `icon`, `trip_id`, `trip_headsign`, `direction_id`, `stop_id`, `stop_name`, `origin_stop_name`, `destination_stop_name`, `destination_stop_id`, `destination_arrival_utc`, `departure_realtime_utc`, `departure_realtime_local`, `delay_provider`, `delay_derived`, `realtime_state`, `match_rule`, `cancelled`.

Every string value is `""` when the underlying GTFS field is absent. `null` is used only for genuinely numeric/optional values (`direction_id`, the two delays, arrival times). The literal string `"None"` cannot occur because `helpers/text.clean()` is the only conversion path and every TEXT column is `NOT NULL DEFAULT ''`.

### 1.6 DDL - `feed.sqlite` (normalised static copy)

```sql
-- Build pragmas: journal_mode=OFF, synchronous=OFF, temp_store=FILE,
--   cache_size=-65536 (64 MB), page_size=8192, set before the first CREATE.
-- Before promotion: PRAGMA journal_mode=DELETE; PRAGMA optimize; VACUUM is NOT run.
-- Read pragmas: query_only=1, journal_mode is left as DELETE, busy_timeout=15000,
--   mmap_size=0 on 32-bit builds.

CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID;
-- schema_version, datasource_id, source_url, source_etag, source_last_modified,
-- fingerprint, imported_utc, feed_start_date, feed_end_date, feed_version,
-- agency_count, route_count, stop_count, trip_count, stop_time_count,
-- default_tz_name, default_tz_source, ha_timezone_at_import, problem_count

CREATE TABLE agency (
  agency_uid      INTEGER PRIMARY KEY,
  agency_id       TEXT    NOT NULL DEFAULT '' UNIQUE,  -- '' is legal for a single-agency feed
  agency_name     TEXT    NOT NULL DEFAULT '',
  agency_timezone TEXT    NOT NULL DEFAULT '',
  agency_url      TEXT    NOT NULL DEFAULT '',
  agency_lang     TEXT    NOT NULL DEFAULT '',
  agency_phone    TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE route (
  route_uid        INTEGER PRIMARY KEY,
  route_id         TEXT    NOT NULL UNIQUE,
  agency_uid       INTEGER NOT NULL REFERENCES agency(agency_uid),
  route_short_name TEXT    NOT NULL DEFAULT '',
  route_long_name  TEXT    NOT NULL DEFAULT '',
  route_desc       TEXT    NOT NULL DEFAULT '',
  route_type       INTEGER NOT NULL,
  route_color      TEXT    NOT NULL DEFAULT '',
  route_text_color TEXT    NOT NULL DEFAULT '',
  picker_label     TEXT    NOT NULL,   -- 'route_id | short | long'; display only, never parsed
  mode_name        TEXT    NOT NULL,
  icon             TEXT    NOT NULL,
  search_key       TEXT    NOT NULL    -- folded concat of the three name fields
);
CREATE INDEX route_agency_idx ON route(agency_uid);
CREATE INDEX route_search_idx ON route(search_key);

CREATE TABLE stop (
  stop_uid       INTEGER PRIMARY KEY,
  stop_id        TEXT    NOT NULL UNIQUE,   -- opaque: may contain ':', ' ', non-ASCII
  stop_code      TEXT    NOT NULL DEFAULT '',
  stop_name      TEXT    NOT NULL DEFAULT '',
  stop_desc      TEXT    NOT NULL DEFAULT '',
  stop_lat       REAL,                      -- nullable: coordinate-less nodes must import
  stop_lon       REAL,
  zone_id        TEXT    NOT NULL DEFAULT '',
  stop_url       TEXT    NOT NULL DEFAULT '',
  location_type  INTEGER NOT NULL DEFAULT 0,
  parent_station TEXT    NOT NULL DEFAULT '',
  platform_code  TEXT    NOT NULL DEFAULT '',
  stop_timezone  TEXT    NOT NULL DEFAULT '',
  name_key       TEXT    NOT NULL,          -- casefolded, accent-folded stop_name
  id_affix       TEXT    NOT NULL           -- stop_id after the last ':' or '_', for RT prefix matching
);
CREATE INDEX stop_latlon_idx  ON stop(stop_lat, stop_lon)
  WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL;
CREATE INDEX stop_namekey_idx ON stop(name_key);
CREATE INDEX stop_affix_idx   ON stop(id_affix);
CREATE INDEX stop_parent_idx  ON stop(parent_station) WHERE parent_station <> '';

CREATE TABLE trip (
  trip_uid        INTEGER PRIMARY KEY,
  trip_id         TEXT    NOT NULL UNIQUE,  -- opaque, exact match only
  route_uid       INTEGER NOT NULL REFERENCES route(route_uid),
  service_id      TEXT    NOT NULL,         -- opaque; hyphen composites are ordinary strings
  trip_headsign   TEXT    NOT NULL DEFAULT '',
  trip_short_name TEXT    NOT NULL DEFAULT '',
  direction_id    INTEGER,                  -- nullable; never required by any query
  block_id        TEXT    NOT NULL DEFAULT '',
  shape_id        TEXT    NOT NULL DEFAULT '',
  first_stop_uid  INTEGER,
  last_stop_uid   INTEGER,
  tz_name         TEXT    NOT NULL,
  tz_source       TEXT    NOT NULL,         -- 'agency'|'stop'|'homeassistant'|'utc'
  id_affix        TEXT    NOT NULL
);
CREATE INDEX trip_route_idx  ON trip(route_uid, direction_id);
CREATE INDEX trip_svc_idx    ON trip(service_id);
CREATE INDEX trip_short_idx  ON trip(trip_short_name) WHERE trip_short_name <> '';
CREATE INDEX trip_affix_idx  ON trip(id_affix);

CREATE TABLE trip_stop (
  trip_uid      INTEGER NOT NULL,
  stop_seq      INTEGER NOT NULL,
  stop_uid      INTEGER NOT NULL,
  arr_secs      INTEGER,                    -- seconds from service-day anchor; may exceed 86400
  dep_secs      INTEGER,
  pickup_type   INTEGER NOT NULL DEFAULT 0,
  drop_off_type INTEGER NOT NULL DEFAULT 0,
  stop_headsign TEXT    NOT NULL DEFAULT '',
  timepoint     INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (trip_uid, stop_seq)
) WITHOUT ROWID;
CREATE INDEX trip_stop_stop_idx ON trip_stop(stop_uid, trip_uid);

CREATE TABLE service (
  service_id TEXT PRIMARY KEY,
  mon INTEGER NOT NULL DEFAULT 0, tue INTEGER NOT NULL DEFAULT 0,
  wed INTEGER NOT NULL DEFAULT 0, thu INTEGER NOT NULL DEFAULT 0,
  fri INTEGER NOT NULL DEFAULT 0, sat INTEGER NOT NULL DEFAULT 0,
  sun INTEGER NOT NULL DEFAULT 0,
  start_date INTEGER, end_date INTEGER      -- YYYYMMDD, nullable
) WITHOUT ROWID;

CREATE TABLE service_exception (
  service_id     TEXT    NOT NULL,
  service_date   INTEGER NOT NULL,          -- YYYYMMDD from calendar_dates.date
  exception_type INTEGER NOT NULL,          -- 1 added, 2 removed
  PRIMARY KEY (service_id, service_date)
) WITHOUT ROWID;
CREATE INDEX service_exception_date_idx ON service_exception(service_date);

CREATE TABLE frequency (
  trip_uid     INTEGER NOT NULL,
  start_secs   INTEGER NOT NULL,
  end_secs     INTEGER NOT NULL,
  headway_secs INTEGER NOT NULL,
  exact_times  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (trip_uid, start_secs)
) WITHOUT ROWID;

CREATE TABLE import_problem (
  seq        INTEGER PRIMARY KEY,
  member     TEXT NOT NULL,
  identifier TEXT NOT NULL DEFAULT '',
  message    TEXT NOT NULL
);
```

There is deliberately **no** materialised `service_id x date` cross-product table. Active service dates are derived on demand by a bounded lookahead over `service` and `service_exception` (both small, both indexed). A 189-service-day national feed with tens of thousands of service ids would otherwise cost hundreds of megabytes of text-keyed B-tree before a single departure existed.

### 1.7 DDL - `index.sqlite` (the materialised store)

```sql
-- Build pragmas: journal_mode=OFF, synchronous=OFF, temp_store=FILE,
--   cache_size=-131072 (128 MB), page_size=8192.
-- SQLITE_TMPDIR is forced to the datasource directory so a sort spill lands on the
-- same filesystem that was free-space checked, never on a container tmpfs.
-- Before promotion: PRAGMA journal_mode=DELETE; PRAGMA integrity_check;
-- Read pragmas: query_only=1, busy_timeout=15000.

CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID;
-- schema_version, generation, datasource_id, feed_fingerprint, built_utc,
-- window_start_date, window_end_date, window_start_utc, window_end_utc,
-- ha_timezone_at_build, scope_kind, scope_values, departure_rows,
-- feed_start_date, feed_end_date, dst_mode, frequencies_expanded

-- Self-contained dimension snapshots. index.sqlite is the only file the hot read
-- path opens, so a generation swap replaces facts and dimensions atomically together.
CREATE TABLE dim_agency (
  agency_uid  INTEGER PRIMARY KEY,
  agency_id   TEXT NOT NULL DEFAULT '',
  agency_name TEXT NOT NULL DEFAULT '',
  agency_phone TEXT NOT NULL DEFAULT '',
  agency_url  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE dim_route (
  route_uid        INTEGER PRIMARY KEY,
  route_id         TEXT    NOT NULL UNIQUE,
  agency_uid       INTEGER NOT NULL,
  route_short_name TEXT    NOT NULL DEFAULT '',
  route_long_name  TEXT    NOT NULL DEFAULT '',
  route_type       INTEGER NOT NULL,
  route_color      TEXT    NOT NULL DEFAULT '',
  mode_name        TEXT    NOT NULL,
  icon             TEXT    NOT NULL
);

CREATE TABLE dim_stop (
  stop_uid       INTEGER PRIMARY KEY,
  stop_id        TEXT    NOT NULL UNIQUE,
  stop_code      TEXT    NOT NULL DEFAULT '',
  stop_name      TEXT    NOT NULL DEFAULT '',
  stop_lat       REAL,
  stop_lon       REAL,
  platform_code  TEXT    NOT NULL DEFAULT '',
  parent_station TEXT    NOT NULL DEFAULT '',
  name_key       TEXT    NOT NULL,
  id_affix       TEXT    NOT NULL
);
CREATE INDEX dim_stop_latlon_idx  ON dim_stop(stop_lat, stop_lon)
  WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL;
CREATE INDEX dim_stop_namekey_idx ON dim_stop(name_key);
CREATE INDEX dim_stop_affix_idx   ON dim_stop(id_affix);

-- Only trips with at least one instance inside the window.
CREATE TABLE dim_trip (
  trip_uid        INTEGER PRIMARY KEY,
  trip_id         TEXT    NOT NULL UNIQUE,
  route_uid       INTEGER NOT NULL,
  trip_headsign   TEXT    NOT NULL DEFAULT '',
  trip_short_name TEXT    NOT NULL DEFAULT '',
  direction_id    INTEGER,
  first_stop_uid  INTEGER,
  last_stop_uid   INTEGER,
  tz_name         TEXT    NOT NULL,
  id_affix        TEXT    NOT NULL
);
CREATE INDEX dim_trip_route_idx ON dim_trip(route_uid, direction_id);
CREATE INDEX dim_trip_short_idx ON dim_trip(trip_short_name) WHERE trip_short_name <> '';
CREATE INDEX dim_trip_affix_idx ON dim_trip(id_affix);

-- Date-independent trip shape, restricted to dim_trip. Answers "does this trip call
-- at the destination after the origin", supplies destination arrival times by integer
-- addition against trip_instance.start_utc, and backs extract_trip_stops.
CREATE TABLE trip_stop (
  trip_uid      INTEGER NOT NULL,
  stop_seq      INTEGER NOT NULL,
  stop_uid      INTEGER NOT NULL,
  arr_secs      INTEGER,
  dep_secs      INTEGER,
  pickup_type   INTEGER NOT NULL DEFAULT 0,
  drop_off_type INTEGER NOT NULL DEFAULT 0,
  stop_headsign TEXT    NOT NULL DEFAULT '',
  PRIMARY KEY (trip_uid, stop_seq)
) WITHOUT ROWID;
CREATE INDEX trip_stop_lookup_idx ON trip_stop(trip_uid, stop_uid);

-- One row per running instance of a trip inside the window.
-- start_utc is the absolute epoch second at which dep_secs = 0, i.e.
-- (local noon on service_date in tz_name) - 43200. Because GTFS stop times are
-- elapsed seconds from that anchor, departure_utc = start_utc + dep_secs holds
-- exactly, including across DST changes and for times at or past 24:00:00.
CREATE TABLE trip_instance (
  trip_uid     INTEGER NOT NULL,
  service_date INTEGER NOT NULL,      -- YYYYMMDD, feed service day
  start_utc    INTEGER NOT NULL,
  PRIMARY KEY (trip_uid, service_date)
) WITHOUT ROWID;

-- THE MATERIALISED STORE.
-- One row per (stop_time x active service date) where boarding is possible
-- (pickup_type <> 1). The primary key IS the table: a WITHOUT ROWID B-tree clustered
-- on (stop_uid, departure_utc), so every runtime query is one seek plus a forward
-- scan with two integer bind parameters. There is deliberately no secondary index:
-- every query anchors on a known set of stop_uids, and a second index would roughly
-- double the largest object in the database. Rows are inserted in primary-key order
-- from a sorted staging drain so the B-tree is built sequentially.
CREATE TABLE departure (
  stop_uid      INTEGER NOT NULL,
  departure_utc INTEGER NOT NULL,     -- absolute epoch seconds UTC; no nulls, no sentinels
  trip_uid      INTEGER NOT NULL,
  route_uid     INTEGER NOT NULL,
  direction_id  INTEGER,              -- nullable, mirrored from dim_trip for filtering
  service_date  INTEGER NOT NULL,
  stop_seq      INTEGER NOT NULL,
  arrival_utc   INTEGER,              -- NULL when stop_times.arrival_time was blank
  PRIMARY KEY (stop_uid, departure_utc, trip_uid)
) WITHOUT ROWID;

-- Staging table used only during the build, dropped before promotion.
CREATE TABLE departure_stage (
  stop_uid INTEGER, departure_utc INTEGER, trip_uid INTEGER, route_uid INTEGER,
  direction_id INTEGER, service_date INTEGER, stop_seq INTEGER, arrival_utc INTEGER
);
-- Drain:
--   INSERT INTO departure SELECT stop_uid, departure_utc, trip_uid, route_uid,
--          direction_id, service_date, stop_seq, arrival_utc
--     FROM departure_stage ORDER BY stop_uid, departure_utc, trip_uid;
--   DROP TABLE departure_stage;

CREATE TABLE build_phase (
  phase      TEXT PRIMARY KEY,        -- load_dims, expand_calendar, materialise, drain, finalise
  started_utc  INTEGER,
  finished_utc INTEGER,
  rows       INTEGER NOT NULL DEFAULT 0,
  detail     TEXT NOT NULL DEFAULT ''
);
```

Canonical hot-path statement, the only shape used for departures:

```sql
SELECT d.stop_uid, d.departure_utc, d.arrival_utc, d.trip_uid, d.route_uid,
       d.direction_id, d.service_date, d.stop_seq,
       s.stop_id, s.stop_name, s.platform_code,
       t.trip_id, t.trip_headsign, t.trip_short_name, t.tz_name,
       t.first_stop_uid, t.last_stop_uid,
       r.route_id, r.route_short_name, r.route_long_name, r.route_type,
       r.mode_name, r.icon, a.agency_name
  FROM departure AS d
  JOIN dim_stop  AS s ON s.stop_uid  = d.stop_uid
  JOIN dim_trip  AS t ON t.trip_uid  = d.trip_uid
  JOIN dim_route AS r ON r.route_uid = d.route_uid
  JOIN dim_agency AS a ON a.agency_uid = r.agency_uid
 WHERE d.stop_uid = ?          -- one execution per configured stop, merged in Python
   AND d.departure_utc >= ?
   AND d.departure_utc <  ?
 ORDER BY d.departure_utc
 LIMIT ?;
```

Origin-to-destination adds, against the same clustered scan:

```sql
   AND EXISTS (SELECT 1 FROM trip_stop AS ts
                WHERE ts.trip_uid = d.trip_uid
                  AND ts.stop_uid IN (/* destination uids */)
                  AND ts.stop_seq > d.stop_seq
                  AND ts.drop_off_type <> 1)
```

Destination arrival for the selected departure is `trip_instance.start_utc + trip_stop.arr_secs`, computed with one integer addition rather than a second index on the largest table.

### 1.8 Concurrency, generations and locking

- Exactly one writer process per datasource, serialised by an `asyncio.Lock` plus an on-disk lock file containing the owning PID and start time. A stale lock whose PID is gone is broken with a warning. A manual service call and the scheduled refresh therefore cannot overlap. There is no code path that opens the promoted `feed.sqlite` or `index.sqlite` for writing.
- Promotion is `os.replace()` of a fully built, integrity-checked, `journal_mode=DELETE` file. Readers holding the old inode keep serving until they reopen; the old inode is unlinked and its space is reclaimed when the last reader closes.
- `meta.generation` is a monotonically increasing integer. The `ImportManager` fires its listener callback on promotion, and each `ReadStore` reopens on that callback. As a backstop against a missed callback, `ReadStore` also stats the file's `(st_dev, st_ino, st_mtime_ns)` at most once every 60 s and reopens on change. A missed handshake therefore costs at most a minute of stale data, not a permanently stale timetable.
- The nightly window roll is a `job="index_only"` rebuild. It reads `feed.sqlite`, writes a new index generation and promotes it. There is no prune path and no in-place mutation, so there is exactly one build implementation to test.

### 1.9 Scope and window

- `window`: `[today - back_days, today + horizon_days]` in the Home Assistant timezone. Defaults `back_days = 1`, `horizon_days = 3`. Rolled nightly at 03:15 local plus a random 0-15 minute jitter, and immediately on Home Assistant start if the promoted window no longer covers today.
- `scope`: coarse and configuration-independent - `all` (default), or a filter by `agency_id`, `route_id` or `route_type`. Per-stop scope is explicitly rejected: it couples the materialised set to the configuration, breaks a moving `device_tracker`, and forces a warming state machine into existence.
- Before download, `ingest/estimate.py` projects departure rows and bytes from the archive's `stop_times.txt` uncompressed size, the calendar span and the scope filter, and shows the projection in the config flow. Above a configurable threshold (default 4 GB projected) the flow requires an explicit confirmation checkbox.
- `ingest/manager.py` refuses to start any build unless the datasource directory has `projected_bytes * 2.5` free and `SQLITE_TMPDIR` resolves to that same filesystem and is not a tmpfs.

---

## 2. REQUIREMENTS

149 issues, deduplicated and merged. Provenance in square brackets is the upstream issue number. `[EDGE]` marks a non-obvious case that a fresh implementation would not guess and that must have a dedicated regression test with a fixture feed.

### 2.1 Ingest

- **I-1** Any user-supplied static feed URL is accepted; no curated source catalogue exists. The loaded feed's validity window and import timestamp are exposed so a stale source is detectable. [#85]
- **I-2** A source URL containing a query string is fetched verbatim, with the query string neither stripped nor re-encoded; static sources requiring authentication are supported via a user-named header or a query parameter. [#21]
- **I-3** The downloaded payload is classified by magic bytes, not by URL extension or content type. A payload that is not a zip is rejected before any datasource directory or database file is created, with a message naming the URL and what was found instead. [#67][#82][#89][#90][#110] `[EDGE]` Users routinely paste a GTFS-Realtime endpoint into the static field.
- **I-4** An error string, an HTML page or a protobuf body is never written to a file with a `.sqlite` name under any failure path. [#67][#82] `[EDGE]`
- **I-5** A zip whose top level contains further zips rather than GTFS text files is rejected with a message naming the nested members; a locally extracted directory or a locally placed zip is accepted as an alternative source. [#117] `[EDGE]` SEPTA publishes a zip of two zips.
- **I-6** CSV members are decoded defensively: BOM stripped, UTF-8 first, then cp1252, then UTF-8 with replacement. A genuine decode failure raises a handled error naming the member and the byte offset, never an uncaught `UnicodeDecodeError`. [#82][#113]
- **I-7** Non-ASCII agency, stop and route names round-trip without corruption (German umlauts, Latvian and French diacritics). [#113]
- **I-8** Files the integration never queries are skipped at load time rather than parsed or validated: `shapes.txt`, `transfers.txt`, all fare files, `pathways.txt`, `levels.txt`, `translations.txt`, `attributions.txt` and the fares-v2 set. [#94] `[EDGE]` A `transfer_type=4` value must not be able to fail an import of a feed that never reads transfers.
- **I-9** `feed_info.txt` with blank `feed_start_date`/`feed_end_date`, or a `feed_version` that is not a date (`"14.02.2024 01:03"`), is treated as absent optional data. A feed with no `feed_info.txt` loads. [#17] `[EDGE]`
- **I-10** Stop records import with any combination of `stop_code`, `stop_lat`, `stop_lon`, `zone_id`, `stop_url`, `platform_code` and `parent_station` absent, for every `location_type` including 3 and 4. Stops without coordinates are simply excluded from distance searches. [#68][#84][#98][#114] `[EDGE]` MBTA `node-*-platform` rows have no code and no coordinates.
- **I-11** A single unloadable row is skipped with a warning naming the member and the record identifier, recorded in `import_problem`, and the import continues to completion. The problem count and the first 100 problems are visible in diagnostics and in the datasource attributes. [#68][#84][#94]
- **I-12** A failed import never leaves a half-written database presenting itself as usable. The build file and any temporary archive are deleted on every failure path, the datasource state becomes `failed` with the offending phase and record named, and a repair issue is raised. [#94][#98][#102][#106][#114] `[EDGE]` A leftover temporary zip must not make a retry believe an extraction is still running.
- **I-13** Extraction runs in a separate process, emits phase progress and terminal success/failure into the Home Assistant log (not only to the process's stdout), never blocks the event loop, survives runs of several hours, and resumes from the last completed phase after an interrupted run. [#13][#17][#64][#68][#70][#84][#102][#103]
- **I-14** On restart, an existing extracted database is reused even when the source archive is gone. A rebuild is triggered only when the database is absent or its schema version is stale. A genuinely missing datasource produces a named, actionable error and does not abort setup of unrelated entries. [#6]
- **I-15** Creating, editing, reloading or reconfiguring any config entry never deletes, re-extracts or rebuilds a datasource database. [#75] `[EDGE]` Adding a second entry against a freshly imported feed must not cost a second import.
- **I-16** When refreshing an existing datasource, the candidate archive is inspected first; if every service date in `calendar.txt` and `calendar_dates.txt` starts after today, the update is abandoned and the existing database and archive are retained. [#71] `[EDGE]`
- **I-17** The future-dating check parses date fields wrapped in double quotes, reads `start_date` from `calendar.txt` and `date` from `calendar_dates.txt`, and tolerates either file being absent. [#71] `[EDGE]` Some publishers quote every CSV value.
- **I-18** If the future-dating check itself errors, the existing data is kept untouched; the failure never causes deletion of the current database. [#71]
- **I-19** The future-dating check applies only to a refresh of an existing datasource, never to a first import. [#71] `[EDGE]` Otherwise a seasonal feed can never be added at all.
- **I-20** Change detection uses a fingerprint over sorted `(member name, size, crc32)` of the members actually read, in addition to conditional `ETag`/`If-Modified-Since`. A publisher re-zipping identical content does not trigger a rebuild. [C; #103][#116][#135]
- **I-21** Several static datasources run simultaneously, each with its own database, its own refresh schedule and its own name, with no cross-contamination of stops, routes or entity names. [#17][#81][#87] `[EDGE]` Providers split a city across separate bus and tram archives.
- **I-22** The extracted database is disposable and fully rebuildable from source. Its directory is configurable and defaults outside the paths that dominate a Home Assistant backup, with the location documented. [#70]
- **I-23** A build refuses to start unless the target filesystem has 2.5x the projected result free, and unless the SQLite temp directory is on that same filesystem and is not a tmpfs. [A; judge] `[EDGE]` A file-backed sort spill into a container tmpfs is an out-of-memory kill several hours into the job.
- **I-24** A projected row count and byte size is shown before the download starts, and a projection above the configured threshold requires explicit confirmation. [B]
- **I-25** When `frequencies.txt` is present, headway-based service is expanded into concrete departures for each stop (first departure plus each interval, offset by the stop's elapsed time from the trip start) and the expansion is recorded in `meta`. [#120] `[EDGE]` Otherwise a stop served every 25 minutes appears to have one departure per day.
- **I-26** A user may nominate members to drop before extraction, so a non-compliant optional file can be discarded without manually unzipping, editing and re-zipping. Members the loader reads can never be dropped. [#17]
- **I-27** No filesystem listing, file read or database call executes on the event loop; all such work runs in an executor or a subprocess. [#98][#102][#110]
- **I-138** A zip whose GTFS text members sit inside a subdirectory rather than at the archive root imports successfully. Member lookup resolves by basename anywhere in the archive; when two members share a basename in different directories the archive is rejected with a message naming both paths rather than one being chosen silently. [#42] `[EDGE]` A publisher moving its files into a subfolder between releases otherwise produces an empty database and no error.
- **I-139** A datasource is ready when a promoted database file exists whose `meta` records a completed generation and a matching schema version. Readiness is never inferred from the presence or absence of a side-effect artefact - a SQLite journal, a lock file, a staging or temporary zip, or a build file. [#41][#42] `[EDGE]` Marker-file logic reported partially imported sources as complete and complete sources as still running.

### 2.2 Query and time

- **I/Q-28** The zone for a trip is resolved in order: `agency.agency_timezone`, then the origin stop's `stop_timezone`, then the Home Assistant instance timezone, then UTC. The resolved name and its source are persisted and exposed as attributes. [#2][#63][#107] `[EDGE]` A feed with `routes.agency_id` NULL and `agency.timezone` NULL must not silently become UTC.
- **Q-29** Departure instants are computed once at ingest as `service_day_anchor_utc(service_date, zone) + dep_secs`, where the anchor is local noon minus 43200 s. Stop times at or past `24:00:00` and DST transition days therefore need no special case at query time. [#107] `[EDGE]`
- **Q-30** Timezone localisation is applied before the next departure is selected, and the entire remaining-departures list is localised on the same basis as the selected one. [#107] `[EDGE]` Localising only the chosen departure produces a correct-looking time attached to the wrong vehicle.
- **Q-31** A departure that appears to belong to the following feed service day may fall today after localisation, and vice versa; ordering against the current time is done purely on the absolute instant. [#107] `[EDGE]`
- **Q-32** Departures are computed in the timezone of the feed/agency, not that of the Home Assistant instance, including when the two differ by several hours or when the agency zone differs from the stops' zone. [#63][#107] `[EDGE]` Amtrak declares `America/New_York` while serving `America/Los_Angeles` stops.
- **Q-33** Every poll recomputes the departure set against the current wall clock. No absolute window, and no result, is cached from the time the entry was created. [#8][#100] `[EDGE]` A sensor counting down towards the hour at which it was configured the previous night.
- **Q-34** Departures whose effective instant has passed are discarded from both the state and the list on every refresh, subject to the lookback window of Q-140. [#8]
- **Q-35** With "include tomorrow" enabled, the state is the earliest departure at or after now across the combined set, each departure carries the service date it actually belongs to, and enabling the option does not change which departure is selected while departures remain today. [#23] `[EDGE]`
- **Q-36** A configurable offset in minutes excludes departures sooner than `now + offset` from both the state and the list. The offset is purely a lead-time filter and is never used to compensate for a timezone error. [#16][#23][#107]
- **Q-37** Where a route is configured, departure lookup is constrained by it and by direction as well as by the origin/destination pair, so two entries sharing the same stops on different routes return independent, route-correct results. Route selection is itself optional; see Q-143. [#115] `[EDGE]`
- **Q-38** Trips whose `direction_id` is NULL or absent are included, and `direction_id` is never required to identify a trip. [#72][#90] `[EDGE]` The 0/1 assignment is arbitrary per provider and static and realtime data can disagree for the same trip.
- **Q-39** A trip whose service is defined in both `calendar.txt` and `calendar_dates.txt` appears exactly once for a given service day; the two sources are unioned and de-duplicated on trip identity plus instant. [#118] `[EDGE]` Otherwise every departure is listed twice.
- **Q-40** Service records with all seven weekday flags zero are valid and active only via `calendar_dates` exceptions; composite hyphen-joined `service_id` values are treated as opaque strings. [#71] `[EDGE]`
- **Q-41** Each departure exposes the first and last stop names of its own trip, so a human-readable terminus never requires splitting `route_long_name` on a guessed separator. [#72]
- **Q-42** A feed that omits `trip_headsign` entirely still produces usable departures, with the destination derived from the trip's last stop. [#72][#3]
- **Q-43** Route, trip and stop identifiers containing colons, spaces or other punctuation round-trip through configuration, selection and display without being split or mangled. Display labels are constructed separately and are never parsed back. [#90][#113] `[EDGE]` `StopPoint:OCETrain TER-87334508` and `8595142:0:10000`.
- **Q-44** A route served by more than one vehicle type returns all its departures, each labelled with the route and trip actually selected rather than with the mode chosen at configuration. [#107] `[EDGE]` One Amtrak route id carries both trains and connecting coaches.
- **Q-45** An empty departure result is handled without raising, with no unguarded access to fields of a missing row, and the entity settles into a defined descriptive state. [#103]
- **Q-46** A nearby-departures query over a metro-scale feed completes in a few hundred milliseconds, well inside the update interval. [#88]
- **Q-47** The query layer returns explicit `truncated`, `exhausted` and `window_end_utc` flags, so "the materialisation window ran out" is structurally distinguishable from "the timetable is quiet". [B] `[EDGE]`
- **Q-48** Service-date lookahead over the whole feed validity (up to 400 days) answers "when does this route/pair next run" without materialising anything, so a seasonal or weekend-only service can be validated out of season. [A; #5][#104] `[EDGE]`
- **Q-140** Departure selection starts at `now - lookback` (configurable, default 15 minutes) as well as running forward. A departure whose scheduled instant has passed is retained only while realtime evidence places its effective instant in the future; one with no realtime data, or whose realtime instant has also passed, is dropped. This qualifies Q-34, which otherwise discards every passed instant unconditionally. [#55] `[EDGE]` A delayed vehicle that has not yet arrived is exactly the departure a user checking "can I still catch it" needs to see.
- **Q-141** Ordering and selection of the next departure use each departure's effective instant - realtime where matched, scheduled otherwise - not the scheduled instant alone. A departure delayed past a later on-time one is ordered after it, and the state does not advance past a delayed departure until its realtime instant has passed. [#48] `[EDGE]` The reported symptom was the entity rolling forward while realtime still had the delayed bus inbound.
- **Q-142** `calendar_dates.txt` exception_type 1 adds a service date and exception_type 2 removes one; a removal overrides the weekly pattern in `calendar.txt` for that date, and a trip whose service is removed produces no departure on it. [#40] `[EDGE]` A Saturday-only exception surfaced as a phantom Friday departure alongside the real one.
- **Q-143** Route selection on an origin/destination entry is optional. With no route chosen the pair returns every service calling at both stops in the configured direction; with a route chosen, the flow states plainly that services on other route ids serving the same physical stops are excluded. This relaxes Q-37, which assumes a route is always configured. [#60] `[EDGE]` Translink SEQ splits one visible line across several route ids for night and off-peak services, so pinning one hides real trains.

### 2.3 Realtime

- **R-49** Realtime endpoints are consumed as GTFS-Realtime protobuf regardless of the URL's extension or path, and the response body is never handed to zip extraction. [#15][#18][#106] `[EDGE]` `.aspx` endpoints and extension-less URLs.
- **R-50** A response that is not a parseable `FeedMessage` produces an explicit "this is not a GTFS-RT feed" error naming the URL, the detected content type and the first bytes, and leaves the scheduled sensors fully functional. [#15][#18][#61]
- **R-51** Trip updates, vehicle positions and service alerts are three independent, individually optional endpoints; the integration operates normally when only some are supplied. [#92][#93]
- **R-52** Credential injection applies only to URLs that are actually configured; an unset endpoint is skipped rather than concatenated with a key. [#122] `[EDGE]` A `None` URL plus a key is a `TypeError` that fails the whole entry.
- **R-53** The API key is placeable either in a request header whose name the user chooses or as a URL query parameter whose name the user chooses, and the stored value is sent under exactly that name. [#21][#80][#99][#109] `[EDGE]` Looking the value up under a fixed literal name silently sends null and yields HTTP 401.
- **R-54** When no key is configured, no placeholder value is sent under any header. [#21] `[EDGE]` Sending `Authorization: na` is rejected by some agencies with a different error than sending nothing.
- **R-55** Submitting the realtime options step with a blank key or blank key name completes successfully and is interpreted as "this feed needs no authentication". [#92]
- **R-56** Optional request headers, including an `Accept: application/x-protobuf` toggle, are always initialised to an empty mapping and applied consistently. [#123]
- **R-57** A sensor can read realtime from a locally stored file previously fetched by a service call, not only from a live URL. [#15][#21]
- **R-58** A realtime fetch or parse failure (protobuf error, connection reset, timeout, non-200, empty body) is caught per fetch, logged once with the affected origin stop and URL, and leaves the scheduled sensor and its full attribute set available with its previous data. The entity never becomes unavailable because of realtime. [#17][#20][#22][#63][#65][#108]
- **R-59** Error handling around realtime always reports the actual exception text; a failure in the error path itself never replaces the real diagnosis. [#63][#65] `[EDGE]` An unbound variable in a log message hid the real cause for every reporter.
- **R-60** Realtime entities that omit optional sub-messages (no `trip_update`, no `vehicle`, no `alert`) are skipped silently; presence is probed in a way that works on whichever representation the decoder produces and is never assumed. [#63][#65] `[EDGE]`
- **R-61** Vendor extension blocks are ignored without error, and the absence of standard descriptive data alongside them is not a failure. [#82] `[EDGE]` NYC subway carries most useful data in a proprietary extension.
- **R-62** A `stop_time_update` is applied only when it resolves to the sensor's own stop. Updates for other stops on the same trip, including stop ids absent from the static feed, are ignored without error. [#22] `[EDGE]`
- **R-63** Matching uses stop id first, and falls back to `stop_sequence` within the trip when the realtime `stop_id` is empty or absent. A feed reporting `stop_sequence 0` on every entry still resolves by stop id. [#90][#119] `[EDGE]` Both pathologies exist and each breaks the other's naive implementation.
- **R-64** Trip descriptors with empty `route_id`, empty `start_time` or empty `start_date` still match using `trip_id` plus stop. [#61][#90] `[EDGE]`
- **R-65** `trip_id` values are matched as opaque strings. They are never parsed for meaning, truncated at an arbitrary offset, or assumed stable in format between feed versions; the only permitted transformation is the bounded affix normalisation of R-66 and R-144. [#90] `[EDGE]` SNCF trip ids embed a timestamp that is not the service date and whose format changed between releases.
- **R-66** Where the realtime feed prefixes identifiers that the static feed does not, matching may strip an affix, but only on a `:`, `_` or space boundary and only when the normalised value resolves to exactly one static trip or stop. [#99] `[EDGE]` `MTA NYCT_JG_A5-...` against static `JG_A5-...`, and `MTA_308209` against `308209`.
- **R-67** The full match ladder is, in order: exact trip id plus stop id; exact trip id plus stop sequence; affix-normalised trip id plus stop; route id plus direction plus stop with the nearest scheduled instant inside a bounded window; `trip_short_name` plus stop. When none matches, the scheduled departure is still published with the realtime fields empty. The rule that matched is recorded per departure as `match_rule`. [#93][#100]
- **R-68** An absolute `time` takes precedence over a `delay`; a `time` of 0 or absent means "not provided" and the delay is applied to the scheduled instant instead; a zero epoch is never rendered as a 1970 departure; `departure` is preferred over `arrival` and each falls back to the other. [#18][#90][#119] `[EDGE]` `arrival {delay: 0, time: 0}` next to a valid departure is common.
- **R-69** A trip-level delay applies when no stop-level value is present. [#18]
- **R-70** A provider delay of 0, empty, null or absent is reported as unknown rather than as "on time"; a derived delay is computed as `realtime - scheduled`, exposed as a separate attribute, formatted `h:mm:ss` and able to be negative. [#112] `[EDGE]` Several providers emit 0 by default whether or not a delay exists.
- **R-71** Provider-supplied delay and provider-supplied realtime instant are both preserved as reported even when they contradict each other. [#112]
- **R-72** When the feed parses but contains nothing for the configured trip and stop, the realtime attributes are still present with an explicit no-data placeholder, the scheduled time continues to be published, and the last successful fetch time is recorded. [#22][#63][#100][#119]
- **R-73** A trip far enough in the future that the provider publishes nothing for it is not an error condition. [#100]
- **R-74** A matched realtime entry expires after a TTL (default 20 minutes past its fetch time), so a vehicle that vanishes from the feed stops donating its delay to a scheduled departure. [C]
- **R-75** A trip in the realtime feed may cover a different stop set than the static trip; departures for stops present in static but missing from the realtime update fall back to their scheduled instant rather than being dropped. [#90] `[EDGE]`
- **R-76** Service alerts whose `informed_entity` references a trip or stop id that does not match any known record are ignored without error and without dropping other alerts. [#90] `[EDGE]` A truncated trip prefix in an alert must not lose the whole feed.
- **R-77** Vehicle positions are matched to the configured route by `trip_id` when the descriptor omits `route_id`. [#20] `[EDGE]`
- **R-78** A per-route, per-direction GeoJSON `FeatureCollection` is written on every realtime cycle, containing an empty collection when nothing matched, to the Home Assistant `www` directory so it is retrievable at `/local/<path>`. The path and URL pair are documented and stable across refreshes. Its absence never affects departure calculation. [#20][#93][#117]
- **R-79** Realtime work per refresh is bounded, so a location entry covering many stops cannot make the instance unresponsive. Realtime feeds are fetched once per datasource per cycle and matched against all subscribing entities from one snapshot. [#109]
- **R-80** When the ratio of realtime trip ids that match nothing static exceeds a threshold, one repair issue is raised stating that the identifiers do not correlate, rather than a per-poll warning. [C]
- **R-81** A SIRI StopMonitoring or EstimatedTimetable JSON document is accepted as an alternative realtime input and normalised into the same snapshot, tolerating `DatedVehicleJourneyRef` versus `DatedVehicleJourneySAERef`. [#99]
- **R-82** Scheduled and realtime instants within one attribute set always use the same format and the same zone. [#63][#112] `[EDGE]` A local wall-clock string next to a UTC ISO string makes any delay calculation impossible.
- **R-144** Affix normalisation is symmetric: it applies where the static identifier carries an affix the realtime feed lacks as well as the reverse, under the same boundary and uniqueness constraints - split only on `:`, `_` or space, and only when the normalised value resolves to exactly one counterpart. [#34] `[EDGE]` SNCF's static ids append a dated qualifier its realtime feed omits, so R-65's exact-string rule alone yields no realtime for any trip.
- **R-145** A realtime update whose effective instant has already passed is discarded from the match rather than donating a stale instant to a scheduled departure. [#34][#48] `[EDGE]` At least two providers keep publishing hours-old stop times in a freshly fetched feed, which R-74's fetch-age TTL does not catch.
- **R-146** `schedule_relationship` is honoured: a SKIPPED stop time update produces no departure and marks the scheduled row cancelled, a trip-level CANCELED cancels every departure of that trip, and NO_DATA leaves the scheduled instant untouched. Where a trip update carries several updates resolving to the same stop, the non-skipped one with a usable time wins; if several survive, the lowest `stop_sequence` wins. The choice is deterministic and recorded in `match_rule`. [#48] `[EDGE]` Spokane Transit emits the same stop twice, once skipped and once with a real timestamp.
- **R-147** Realtime matching and enrichment run for every departure published in the list, not only the one that becomes the state; each entry carries its own `match_rule` and realtime fields. [#40] `[EDGE]` A dashboard used to pick among the next several buses is theoretical, and at peak materially wrong, if only the head of the list is live.
- **R-148** A GTFS-Realtime document served as JSON (the official JSON mapping, `{"header": {"gtfsRealtimeVersion": "2.0", ...}, "entity": [...]}`) is accepted and normalised into the same snapshot as the protobuf form, selected by the bytes received rather than by URL or configuration. [#52] `[EDGE]` RNV publishes a `/tripupdates/decoded` endpoint that R-49 and R-50 would classify as an error.
- **R-149** The realtime poll and the static departure requery run on independent intervals; a realtime cycle never triggers a static requery, an index rebuild or a feed re-read. [#40] `[EDGE]` Coupling them forced a full static requery per realtime tick, which was the root cause of both the request volume and the sensor instability reported.

### 2.4 Sensor

- **S-83** Every datetime-valued attribute is emitted timezone-aware. UTC ISO-8601 attributes carry `+00:00`; local attributes carry the feed-zone offset. No attribute is a naive local time, a bare time of day masquerading as a timestamp, or an ISO string with an empty offset. [#7][#16][#63]
- **S-84** Alongside the UTC form, each departure exposes a local ISO form and a bare `HH:MM` in the feed zone, so templates and text-to-speech need no conversion. [#16]
- **S-85** An absent optional GTFS text field renders as an empty string or is omitted; the literal string `"None"` cannot appear in any attribute. [#3][#13] `[EDGE]` `"2023-12-19T14:48:00+00:00 (None)"` was the reported symptom.
- **S-86** The per-departure line label falls back to `route_short_name` when `route_long_name` is empty and to `route_long_name` when `route_short_name` is empty. [#13]
- **S-87** Each departure exposes `trip_headsign` alongside the line label. [#3]
- **S-88** `route_type` is surfaced as a human-readable mode name and drives the entity icon, covering the extended 100-1700 range and non-standard values. The agency name is surfaced as the attribution. [#13][#15]
- **S-89** The route id and name reported are those of the trip actually selected for the next departure. [#115]
- **S-90** A sensor's state changes as departures change and is never constant for a whole day. The primary entity is a `timestamp` device class whose state is the next departure instant; a separate diagnostic entity carries the descriptive status. [#66]
- **S-91** An entry with no computable departure reports an explicit descriptive status - one of `no_departures`, `no_service_today`, `feed_expired`, `feed_future`, `window_exhausted`, `extracting`, `datasource_failed` - rather than a bare `unknown`. [#66][#103][#113][#135] `[EDGE]` These four causes are indistinguishable from an empty list.
- **S-92** When the loaded feed's last service date is in the past, the sensor signals the stale state explicitly and logs once naming the feed and its last service date. [#103][#135]
- **S-93** A sensor with no upcoming departures still exposes its full attribute set with an empty list; no attribute ever disappears. [#72] `[EDGE]` A vanishing list attribute crashes every dashboard template iterating it.
- **S-94** Attribute names are frozen for v1; any change requires a release note and a repair issue. [#63]
- **S-95** During a re-extraction, existing sensors do not raise, expose an `extracting` indicator, retain their previous data, and emit at most one warning per datasource per extraction rather than one per sensor per poll. [#103] `[EDGE]` A 4.5 hour import produced hundreds of duplicate warnings across three loggers.
- **S-96** An entry that yields no departures still creates its entity with an explicit empty state and a reason; the flow never reports success and produce no entity. [#113]
- **S-97** Requesting an entity update through Home Assistant's standard update mechanism re-queries departures and re-reads the cached realtime snapshot without re-initialising the config entry and without making the entity unavailable. [#62]
- **S-98** The heavy list attributes are declared in the recorder exclusion list. [B]
- **S-99** The entity is unavailable only when its datasource is missing or failed; never during a rebuild, never during a realtime outage. [#63][#108]
- **S-150** The entity update interval is independent of the configured lead-time offset; changing the offset does not change how often departures are recomputed, and the published set never lags the clock by more than one update interval. [#31] `[EDGE]` A reporter's offset appeared inert because he had set the refresh interval equal to it.
- **S-151** The published attribute set is bounded in serialised size (default budget 12 KiB, under Home Assistant's 16384-byte state attribute limit). The list is truncated to fit, `truncated` is set, and the published count is exposed. [#54] `[EDGE]` A busy stop with realtime exceeded the recorder limit at peak and lost all attribute history.
- **S-152** Entities within an entry are isolated: one that raises, times out or yields nothing does not prevent the others being computed and published, and an entity that has become empty repopulates by itself within one update interval once departures reappear, with no reload, service call or re-creation. [#57] `[EDGE]` A stuck empty entity appeared to block its siblings from populating at all.

### 2.5 Config flow

- **C-100** The route picker labels each route with `route_id`, `route_short_name` and `route_long_name` together. [#1] `[EDGE]` `route_id 44` is publicly line 74 in one Brussels feed; other feeds have no short name at all.
- **C-101** Selection lists present human-readable labels beside raw GTFS codes: route type 2 as train, 3 as bus, direction 0 as outbound, 1 as return. [#104]
- **C-102** Setup offers two route-definition modes: explicit origin and destination stop ids, and origin/destination by station or city name where the name resolves to the many stop ids a station carries. [#25]
- **C-103** Validation of a route/direction/origin/destination combination succeeds when a departure exists on the current service day or on any later service day found by lookahead, and never rejects a valid pair merely because nothing runs at the moment of configuration. [#5][#104] `[EDGE]` Configuring a weekday service on a Sunday, or any service at 23:50.
- **C-104** A validation failure returns to the same step with the previously chosen values pre-filled, never restarting the sequence. [#104][#105] `[EDGE]` Feeds commonly present four identically named stops, so several attempts are normal.
- **C-105** An existing entry is reconfigurable in place, including its route direction, without deleting the entry or re-extracting the datasource. [#26][#75]
- **C-106** Any flow step or service call that touches a datasource still being extracted returns a user-facing "extraction in progress, try again later" message with the current phase, and never raises. Datasource pickers list such a source as unavailable with an explanation. [#67][#102]
- **C-107** Removing a datasource from the UI succeeds and offers removal of the extracted database as well as the config entry. [#110]
- **C-108** Multiple static datasources are configurable simultaneously with independent refresh schedules; each departures or local-stop configuration is scoped to exactly one datasource. [#81][#87]
- **C-109** Selector values are raw identifiers and labels are built separately, so identifiers containing colons or spaces are never split. [#90][#113] `[EDGE]`
- **C-110** The setup UI states plainly that the static feed is not refreshed automatically unless a refresh interval is configured, and shows the loaded feed's validity window. [#27][#85]
- **C-153** A route selection resolving to zero stops, or a stop selection resolving to zero trips, returns to the same step with a message naming the route and datasource and stating the feed has no usable stops for it. It never raises and never surfaces as "Unknown error occurred". [#35] `[EDGE]` The Belgian TEC feed binds 209,605 trips to one route id, producing an empty stop list and an `IndexError`.
- **C-154** Route and stop pickers are search-driven and bounded: the user types a fragment, the flow queries with that fragment and a result limit, and no step materialises a full list of a six-figure feed into a selector. [#35]
- **C-155** Removing a datasource deletes only files belonging to that exact datasource. Cleanup is scoped to that datasource's own directory or keyed on its identifier, never on a name prefix or glob. [#42] `[EDGE]` Deleting "dublin" destroyed the working "dublin-bus-gtfs" database and archive.
- **C-156** Removal succeeds and reports success when some or all expected files are already absent - a database never created, a partial build file, a deleted archive, a leftover temporary zip. [#41] `[EDGE]` A `FileNotFoundError` on the delete path left a permanently unusable entry in the UI, which is the state a user most needs to delete from.
- **C-157** When the source for a dataset update is a local zip or a local extracted directory, the URL field is optional and an update with it blank succeeds; the URL is required only for a remote download. [#56] `[EDGE]` Users entered a meaningless URL that was never fetched purely to satisfy the form.

### 2.6 Local stops

- **L-111** Latitude and longitude are validated independently; a tracked entity with one but not the other aborts with a logged error naming the entity. [#73] `[EDGE]` The reported defect tested latitude twice.
- **L-112** The radius-to-degrees conversion uses 360 degrees per 40,000 km (about 9e-6 degrees per metre) and converts longitude separately from latitude by dividing by `cos(latitude)`. [#74][#75] `[EDGE]` A fixed 1/130000 factor understates the box by roughly 15 percent.
- **L-113** The bounding box clamps latitude to plus or minus 90 and, when it crosses the 180th meridian, searches two longitude ranges rather than clamping. Results are post-filtered by haversine so every returned stop is genuinely inside the requested radius. [#74] `[EDGE]`
- **L-114** Local-stop entities use deterministic, stable unique ids so a moving device reuses or retires entities instead of accumulating one permanent entity per stop ever passed. [#75] `[EDGE]` A two-day trip produced 30-plus orphaned entities.
- **L-115** The maximum number of stops returned is user-configurable, not a fixed limit. [#109]
- **L-116** Setup can restrict which stops and which routes within the radius become entities. [#87]
- **L-117** When the radius matches more stops than can be presented, the step is still completable: it reports how many were found and offers to narrow the radius or raise the cap. [#67][#87]
- **L-118** Refreshes are rate-limited: an update is skipped when the tracked location has moved less than a threshold (default 100 m) or the previous update was less than a threshold ago (default 30 s). [#75]
- **L-119** Location-based sensors expose a per-departure list where each entry carries instant, stop name, route short and long name, headsign, trip id, direction and icon, and entity names follow a documented, stable pattern so they can be selected by wildcard in dashboard cards. [#95]
- **L-120** Location-based departures apply the same timezone resolution as start/end schedules, with a per-entry timezone override available where the feed supplies no usable zone. [#107]
- **L-158** A location-derived entity returns departures whose stop matches that entity's own stop id and nothing else; stop name, stop id, platform and departures always describe the same physical stop. [#38] `[EDGE]` The sensor for HLO001 published the timetable and stop name of FAY002, giving wrong times in the wrong direction.
- **L-159** Distinct stop ids are never collapsed by shared `stop_name` or shared `parent_station`. Opposite-direction platform pairs metres apart each get their own entity, named distinguishably by platform code or, failing that, stop id. [#38] `[EDGE]` This is the inverse of the station-name mode of C-102 and the two modes share the same stop tables.
- **L-160** Within one location query a given (trip, stop, service date) appears at most once, including when a stop is reached through several matched rows - a parent station and its children both in radius, or duplicate stop rows in the box. A trip calling at two stops in range appears once per distinct stop. [#40] `[EDGE]` Every trip appeared twice, which is a different cause from the calendar union of Q-39.
- **L-161** The entity set for a location entry is a function of geography and configuration alone, never of whether a stop currently has departures. A stop that goes quiet keeps its entity, and the set is re-derived on the configured interval so a stop returning to scope regains one without a reload. [#57]
- **L-162** A location entry supports the same per-entry options as an origin/destination entry: the lead-time offset of Q-36, realtime enrichment from the datasource's configured endpoints through the same match ladder, and route/direction restriction. [#50][#58] `[EDGE]` Realtime worked for individually configured stops while every local stop reported a parse error.
- **L-163** The search radius is settable during initial setup and afterwards in the options flow, with a conservative default of the order of 200 m and an accepted range reaching at least 5000 m. [#46][#47][#53][#59] `[EDGE]` Users were told to reduce a radius that first-time setup gave them no way to change.
- **L-164** When more stops fall inside the radius than the configured cap allows, the cap selects the N nearest by great-circle distance in a deterministic order, not an arbitrary or query-order subset, and each selected stop's distance is exposed. [#46][#47] `[EDGE]` An arbitrary subset drops the stop outside the user's door in favour of a further one, and churns between refreshes.

### 2.7 Services and actions

- **V-121** `gtfsie.refresh_datasource` re-downloads the configured static feed and rebuilds the dataset in place, without re-running the config flow and without losing entities. Dependent sensors resume by themselves once the rebuild promotes, with no restart, reload or reconfiguration. [#27][#103][#116][#135]
- **V-122** A manual refresh and the scheduled refresh can never touch the same datasource concurrently; the second is deferred or skipped with a clear result, and "database is locked" cannot surface. [#4] `[EDGE]`
- **V-123** `gtfsie.download_realtime` fetches a realtime feed on demand, writes the raw body and a decoded human-readable dump to disk, and returns the entity count. [#93][#99][#109]
- **V-124** The realtime download accepts an arbitrary header name for the credential, including `Authorization` with a `Bearer` value, as well as a query parameter name, and does not require a key name when no key is supplied. [#80]
- **V-125** A realtime download that yields zero entities or an empty body is reported as a failure with the HTTP status and byte count, and never overwrites a previously good cached file. [#80][#89] `[EDGE]` Reporting success while writing a 0-byte file is worse than failing.
- **V-126** When a realtime payload cannot be converted, the call raises a descriptive error naming the URL and the detected content type, and never falls through into a path referencing uninitialised state. [#89][#90] `[EDGE]` A zip containing a `.bin` produced only "Unknown error".
- **V-127** `gtfsie.download_static` fetches and validates a static archive, optionally dropping nominated members, and reports the validated path. [#17]
- **V-128** `gtfsie.extract_departures` returns today's and tomorrow's departures for an entity as a service response. `gtfsie.extract_trip_stops` returns a trip instance's ordered stops with absolute instants. Both declare `SupportsResponse.ONLY`. [#93]
- **V-129** `gtfsie.refresh_local_stops` re-evaluates a location entry on demand. [#62]
- **V-165** `gtfsie.download_realtime` accepts a Home Assistant template for the credential value, rendered at call time, and a caller-supplied destination filename that a sensor can then be pointed at under R-57. [#51][#54] `[EDGE]` RNV rotates its token hourly through an OAuth call, so no static config entry value can work.

### 2.8 Platform and packaging

- **P-130** The config flow loads even when the GTFS-Realtime protobuf bindings are not importable; realtime imports are deferred so a missing binding degrades realtime only. A static-only configuration remains fully usable, and the missing dependency is named in the log and in a repair issue. [#29] `[EDGE]` The reported symptom was "Config flow could not be loaded: Invalid handler specified", which points nowhere.
- **P-131** The integration declares and enforces a minimum Home Assistant version so an unsupported install reports a version error rather than an import-time syntax error. [#96]
- **P-132** A malformed manifest, an unavailable dependency or a setup failure fails this integration alone and never prevents Home Assistant from starting; recovery never requires hand-editing files. [#69]
- **P-133** Entry setup decides readiness before forwarding to platforms; `ConfigEntryNotReady` is raised only from `async_setup_entry`, never from inside a forwarded platform. [#109]
- **P-134** Setup never blocks startup: entities are created immediately, and the first full departure query is scheduled after `EVENT_HOMEASSISTANT_STARTED`. [#75]
- **P-135** All user-facing strings come from translation files, so a new language is a data contribution. [#30]
- **P-136** When the Home Assistant timezone changes and it was the resolved fallback zone at build time, an index rebuild is forced automatically. [B] `[EDGE]` Stored instants silently become wrong otherwise.
- **P-137** Diagnostics expose datasource state, feed window, row counts, import problems, per-feed last realtime fetch, and a `match_rule` histogram, with URLs and keys redacted.

---

## 3. BUILD ORDER

Each phase is independently testable and ends with a stated acceptance test. Phases 1 to 4 deliver a working next-departure sensor.

### Phase 0 - skeleton and invariants (0.5 week)

`manifest.json`, `const.py`, `exceptions.py`, `helpers/text.py`, `helpers/tz.py`, `helpers/modes.py`, `helpers/ids.py`, translation scaffolding, CI with ruff and mypy.

Also ship the two lint tests now, before there is anything to lint: `test_no_date_sql.py` (greps `db/queries.py` for `date(`, `datetime(`, `strftime(`, `julianday(`, `'now'`) and `test_worker_isolation.py` (imports `ingest/worker.py` in a bare interpreter and asserts `homeassistant` is absent from `sys.modules`).

*Done when:* `service_day_anchor_utc` passes a table of cases covering both DST transitions in `Europe/Berlin` and `America/Los_Angeles`, `25:30:00`, `00:00:00` on a spring-forward day, and a zone with a historical offset change. `clean()` maps `None`, `"None"`, `"nan"` and `"  "` to `""`.

*Status against `pygtfsie` at commit `5a31619`.* Built and tested:
`service_day_anchor_utc` (as `day_origin_utc`), `parse_gtfs_time`,
`parse_gtfs_date`, `resolve_zone`, `event_utc`, `tzdata_version`, all of
`helpers/text.py`, all of `helpers/geo.py`, `helpers/logthrottle.py`,
`exceptions.py` and the route-type table. The DST work goes beyond the criterion
above: rather than a table of hardcoded transition dates, the suite walks a full
year in eight zones asserting every service day is 23 to 25 hours and that
shortened and lengthened days pair up, and it derives the skipped-midnight and
repeated-midnight dates from the tz database at run time so a tzdata update
cannot silently turn those tests into no-ops.

Outstanding for this phase:

1. `clean()` does not yet map the literal strings `"None"` and `"nan"` to `""`.
   It handles `None` itself, whitespace and a leading BOM. The sentinel strings
   are a separate case -- they arrive from publishers whose export tooling
   stringified a null -- and closing it means accepting that a stop genuinely
   named "None" would be blanked. That trade is worth making; a pandas-authored
   `"nan"` is common and a stop named None is not.
2. `helpers/ids.py` is not written: `name_key` and `affixes`.
3. `helpers/modes.py` does not exist as a module; `route_type_name` and
   `route_type_icon` currently live in `const.py`. Cosmetic, but the spec's
   layout should win.
4. The tz formatting helpers are not written: `utc_iso`, `local_iso`,
   `local_hhmm`, `day_label`.
5. `resolve_zone` returns `(zone, source)` rather than `(zone, name, source)`,
   and `signed_hms` takes seconds rather than a `timedelta`. Both need
   reconciling with section 1.4 before anything depends on them.
6. Neither lint test is written yet: `test_no_date_sql.py` and
   `test_worker_isolation.py`. The second is partly subsumed by the package
   boundary and its CI gate, but the subprocess-scope assertion is still worth
   making directly.
7. `America/Los_Angeles` and a zone with a historical offset change are not
   named explicitly in the anchor tests, though the year-walk covers the former
   in substance.

### Phase 1 - archive acquisition and validation (1 week)

`ingest/download.py`, `ingest/archive.py`, `ingest/reader.py`, `ingest/estimate.py`.

*Done when:* fixtures for an HTML error page, a raw protobuf, a gzip, a zip of zips, a zip missing `stop_times.txt`, a zip with quoted CSV values, a cp1252 member, and a member containing byte `0xa2` each produce the correct named exception or the correct tolerant parse, and no fixture leaves a temporary file behind. The fingerprint is stable across two re-zips of identical content with different timestamps.

### Phase 2 - `feed.sqlite` loader (1.5 weeks)

`db/schema_feed.py`, `ingest/load_feed.py`, `ingest/calendar.py`, `ingest/worker.py` (job `full`, stopping after the feed phase), `ingest/manager.py` (locking, progress tailing, state machine).

*Done when:* the pathology fixture set - MBTA-shaped coordinate-less nodes, `transfer_type=4`, blank `feed_info` dates, missing `agency_id` with one agency, all-zero weekday flags, composite service ids, colon-bearing identifiers, umlauts and diacritics, one deliberately corrupt row - all import to completion with the corrupt row recorded in `import_problem` and everything else present. A killed worker leaves no promoted database and a `failed` state.

### Phase 3 - `index.sqlite` materialisation (1.5 weeks)

`db/schema_index.py`, `ingest/materialise.py`, generation promotion, `db/store.py`.

*Done when:* rebuilding the index from a checked-in fixture `feed.sqlite` produces byte-identical departure instants across runs; a departure at `25:30:00` on the day before a spring-forward lands at the correct absolute instant; a `calendar_dates`-only service materialises; a trip listed in both calendar sources materialises once. Promotion under a concurrently scanning reader does not error and the reader observes the new generation within 60 s.

### Phase 4 - read path and a working sensor (1.5 weeks)

`db/queries.py`, `datasource.py`, `coordinator.py` (schedule only), `presenter.py`, `sensor.py`, minimal `config_flow.py` (datasource by URL, then origin/destination stop ids), `__init__.py`, `strings.json`.

*Done when:* against a real medium feed, a configured stop pair produces a `timestamp` sensor whose state is the next departure, whose `departures` list is correct and ordered, whose elapsed departures disappear on the next poll, and whose full attribute contract from section 1.5 is present and free of the string `"None"`. Setup does not block startup. This is the first shippable alpha.

### Phase 5 - lifecycle, services and repairs (1 week)

Scheduled static refresh, nightly window roll, future-dating guard, `services.py` with `refresh_datasource`, `download_static`, `extract_departures`, `extract_trip_stops`, `repairs.py`, `diagnostics.py`, `recorder.py`, options flow and reconfigure.

*Done when:* a refresh against an unchanged archive is a no-op by fingerprint; a refresh against an entirely future-dated archive is refused and the old data survives; a concurrent manual and scheduled refresh serialise with no lock error; an expired feed raises the repair issue and flips the status attribute; removing a datasource offers and performs database deletion.

### Phase 6 - realtime trip updates (2 weeks)

`rt/client.py`, `rt/model.py`, `rt/protobuf.py`, `rt/matcher.py`, `RealtimeCoordinator`, `download_realtime` with the decoded dump.

*Done when:* recorded protobuf fixtures reproducing the reported pathologies all resolve correctly: absolute time only, delay only, `time: 0` beside a valid sibling, trip-level delay only, empty `route_id`, empty `stop_id` with `stop_sequence`, `stop_sequence 0` everywhere, agency-prefixed identifiers, updates for stops absent from static, a missing `trip_update` sub-message, an HTML error page, a JSON body, and a connection reset. Every one of them leaves the scheduled sensor available.

### Phase 7 - vehicle positions and alerts (1 week)

`rt/geojson.py`, alert matching and the alerts attribute.

*Done when:* an empty `FeatureCollection` is written when nothing matched; vehicles with empty `route_id` match by trip; an alert whose `informed_entity` matches nothing is dropped without losing the others; the written file is readable at `/local/<path>`.

### Phase 8 - local stops (1.5 weeks)

`local_stops.py`, `LocalStopsCoordinator`, the local-stop config steps and filters.

*Done when:* the degrees conversion, pole clamping and antimeridian split have unit tests; a tracked entity with latitude but no longitude aborts with the entity named; a moving fixture track reuses unique ids rather than accumulating entities; a radius matching 200 stops still completes setup with a stated count and a narrowing option.

### Phase 9 - station-name mode, frequencies, SIRI (1.5 weeks)

Station-name resolution for both static selection and realtime expansion, `frequencies.txt` expansion, `rt/siri.py`.

*Done when:* a city-name origin resolves to its platform stop ids and matches realtime against all of them; a headway-only fixture produces a full day of departures; a SIRI document with each of the two journey-reference field spellings normalises identically.

### Phase 10 - hardening and release (1 week)

Full translation extraction, German translation as the second language, documentation of the GeoJSON path, the backup exclusion advice, the DST anchor note, and the attribute contract. Long-running soak on a Raspberry Pi with a country-scale feed.

---

## 4. OPEN QUESTIONS

**4.1 Where does the database live by default?**
Requirement I-22 says outside the backup-heavy paths, but Home Assistant OS backs up all of `/config`. Options are `<config>/gtfsie/` (simple, backed up), `<config>/gtfsie/` plus documented backup exclusion, or `/share/gtfsie/` (excluded from a config-only backup, but absent on Core installs).
**Recommendation:** default to `<config>/gtfsie/`, expose `db_dir` as a datasource option, and detect at setup whether `/share` exists and is writable; if so, offer it in the flow with a note that it keeps multi-gigabyte databases out of config backups. Do not silently choose a path outside `/config`.

**4.2 Frequency-based feeds: expand or refuse?**
The mined requirement is marked unclear. Expansion is more work and can multiply row counts substantially; refusing leaves whole agencies unusable.
**Recommendation:** expand. The rule is well specified in GTFS and the alternative silently shows one departure per day, which is the failure mode we are trying to eliminate. Record `frequencies_expanded` in `meta` and expose it as an attribute so the behaviour is visible. Where `exact_times=1`, generate at exact headway offsets; where `exact_times=0`, generate at the same offsets and mark those departures `frequency_based: true` in the payload so a consumer knows the times are nominal.

**4.3 `ConfigSubentry` or independent config entries? DECIDED: subentries.**

A datasource is one GTFS feed: a URL, a refresh schedule, and two SQLite files that may run to hundreds of megabytes and take hours to build. A route watch and a vicinity watch are cheap children pointing at one. The structure is hub-and-children; the only question was whether Home Assistant is told so.

Independent entries would mean each watch carrying a `datasource_id` naming its parent. Nothing then prevents deleting the datasource and silently orphaning its watches, startup ordering becomes ours to hand-roll, and the cross-entry reference is invisible to the framework -- which is exactly the "which datasource is this entry using" class of defect in the mined history.

Checked against the developer documentation and the core source rather than assumed:

- Support is declared with `async_get_supported_subentry_types()`, returning `{type: ConfigSubentryFlow}`.
- Reconfigure is supported, through `async_step_reconfigure()` with `self._get_entry()` and `self._get_reconfigure_subentry()`.
- Subentry flows support only the `user` and `reconfigure` steps: **no reauth, no discovery**. Neither is needed. GTFS feeds are not discoverable, and the only credential -- a realtime API key -- belongs to the datasource, which is the parent entry, where reauth behaves normally.
- `ConfigSubentryFlow` first appears in `homeassistant/config_entries.py` at tag **2025.3.0** and is absent at 2025.2.0. Reconfigure support is present from that same release, so the version floor this costs is known rather than guessed.

Committed to before phase 4 writes a config flow rather than during. The entry structure is persisted in `.storage/core.config_entries`, and entity `unique_id`s conventionally encode the entry they belong to, so a later migration that goes even slightly wrong renames every entity and costs users their history, their customisations and their dashboard references.

**4.4 The spring-forward anchor artefact.**
Noon minus twelve hours puts the service-day origin at 23:00 the previous evening on a spring-forward day. GTFS defines stop times as elapsed seconds from that anchor, which is what the specification says and what the anchor implements. Most publishers, however, author `stop_times.txt` as wall-clock times, so a `00:00:00` entry on that one day reads an hour early.
**Recommendation:** implement the specification exactly (elapsed-seconds semantics) as the default, document the artefact plainly, and expose a per-datasource `dst_mode` option with values `spec` (default) and `wallclock`. Do not make `wallclock` the default; a spec-conformant publisher would then be wrong twice a year instead, and there is no way to detect which convention a feed uses.

**4.5 Default materialisation window and scope.**
Three days keeps a national feed tractable but limits "include tomorrow" plus a long-horizon automation, and forces reliance on the nightly roll actually running.
**Recommendation:** `back_days = 1`, `horizon_days = 3` by default, raised automatically to `horizon_days = 7` when the projected size is under 200 MB. Always expose the window in attributes and always distinguish `window_exhausted` from `no_departures`. Scope defaults to `all`; offer agency/route-type filtering in the flow when the projection exceeds the confirmation threshold.

**4.6 Affix-normalised realtime matching: on by default?**
It rescues NYC MTA and similar prefix-adding providers, and it is the only heuristic in the design that can produce a confidently wrong answer.
**Recommendation:** on by default, but gated on resolving to exactly one static trip and one static stop, with the rule recorded per departure as `match_rule` and surfaced in the attributes and diagnostics. Add a per-entry option to restrict matching to exact rules only, for users who would rather see no realtime than a possibly wrong one.

**4.7 SIRI in v1?**
It rescues two named large providers but is a second input format with its own parsing surface and its own per-provider field naming.
**Recommendation:** ship it in phase 9 as v1.1, not in the first release. Structure `rt/model.py` from the start so the snapshot is format-neutral, so adding SIRI is one module and no changes elsewhere.

**4.8 Vehicle positions: files in `www`, or a `geo_location` platform?**
Writing GeoJSON to `www` matches how users already build map cards through `geo_json_events`, but writing into the web root from an integration is a pattern core reviewers dislike, and it means world-readable files.
**Recommendation:** do both. Ship a `geo_location` platform as the native path, and keep the `www` GeoJSON writer behind an explicit per-entry option, defaulting off, documented with its `/local/<path>` URL. Do not make the file the only way to see vehicles.

**4.9 Local realtime file path: how is it constrained?**
Requirement R-57 lets a sensor read realtime from a local file, which is a path traversal surface if unconstrained.
**Recommendation:** restrict readable paths to the integration's own datasource directory and to `hass.config.path("gtfsie_rt")`, resolve symlinks before checking, and reject anything outside. The service that writes these files writes only into the same locations.

**4.10 Minimum Home Assistant version.**
Driven by subentry reconfigure support (4.3), `OptionsFlowWithReload`, and the current `ConfigEntryNotReady` deprecation. Subentry reconfigure is no longer the unknown: it ships from **2025.3.0** (4.3 records how that was established), so on that count alone the floor plus one release of margin is 2025.4.0. The other two drivers are unpinned and may well set a higher floor; whichever is highest wins, and each should be checked the same way rather than inferred.
**Recommendation:** pin the floor to the highest of the drivers plus one further release of margin, and add an explicit version check in `async_setup` that logs a named error rather than failing at import. Revisit only if the maintainer has evidence of a meaningful user population on older cores.

**Where the floor is declared.** Not in `manifest.json`. Home Assistant's `Manifest` TypedDict in `homeassistant/loader.py` has no `homeassistant` key and the loader never reads one, so a minimum version placed there is silently ignored. It belongs in `hacs.json`, which documents `homeassistant` as the minimum-version key and enforces it at download time. `hacs.json` also carries `hacs` for a minimum HACS version and `persistent_directory` for a path inside the integration that survives upgrades -- the latter is not needed here, since the databases live under the configuration directory rather than inside the integration.

**Measured, as of the 2025 releases:** subentry reconfigure ships from **2025.3.0** (absent at 2025.2.0) and `OptionsFlowWithReload` from **2025.8.0** (absent at 2025.7.0), each established by reading `homeassistant/config_entries.py` at the release tag. `OptionsFlowWithReload` is therefore the binding driver and the floor is **2025.9.0**. The `ConfigEntryNotReady` deprecation is a deprecation rather than a requirement and does not raise it.

**A gap worth stating.** The test harness pins one exact Home Assistant, currently 2026.7.4, so the suite verifies the current release and not the floor. Support for 2025.9.0 is a claim about which API surface exists, not one these tests exercise. Closing it would mean a second CI job against an older `pytest-homeassistant-custom-component` and an older Python; worth doing if anyone reports trouble on an older core, and not worth doing speculatively.

**4.11 Over-fetch for effective-instant ordering.**
Q-141 orders departures by effective instant and Q-140 admits a lookback window,
but `index.sqlite` is ordered by *scheduled* instant and the query applies its
`limit` in SQL, while the realtime overlay happens afterwards in the
coordinator. A departure delayed into the visible window can therefore be one
SQL has already cut off, and Q-47's `truncated` and `exhausted` flags would then
describe the pre-overlay page rather than the page actually published.
**Recommendation:** the query functions take a lower bound of `now - lookback`
and a limit padded above the presentation limit; the presenter re-sorts by
effective instant, re-trims, and owns Q-47's flags. Choose the padding from the
largest delay worth honouring rather than a round number. Cheap to build in at
phase 4 and expensive to retrofit at phase 6, which is when it would otherwise
be found.

**As built, with one deviation.** The lower bound, the padding (`over_fetch()`)
and the trim are as recommended, but the filter, the sort and Q-47's flags live
in `coordinator.py` rather than in `presenter.py`. This section predates the
library/integration split, after which `presenter.py` became pure formatting and
the coordinator became what assembles `WatchData` and its `truncated` flag. Same
order of operations, different module.

The first cut of phase 4 took the lower bound and the padding and then applied
neither the filter nor the sort, so the lookback widened the query and nothing
narrowed the result -- Q-140's window without Q-34's discard. The state became
whatever the scan found first, which on any service whose headway is no longer
than the lookback is the departure the user has just missed, on every update.
It survived review and the whole suite because it is not time-*dependent*: it is
wrong identically at every instant, so `time-sweep.sh` compares two equally wrong
runs and correctly reports no variation. It was found by running against a live
feed, which is the second time dogfooding has caught what static reading did not.
Guarded now by a test that pins an instant between two departures and asserts the
head is the later one, and by a second that injects a prediction at
`effective_departure_utc` so the lookback's real purpose stays covered.

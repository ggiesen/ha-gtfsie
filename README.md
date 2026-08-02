# ha-gtfsie

A Home Assistant integration for public transit departures from GTFS static
timetables and GTFS-Realtime feeds.

> **Status: pre-alpha.** The config flow works -- a datasource can be added and
> route and vicinity watches attached to it -- but nothing yet reads a feed or
> produces an entity, so there is nothing to install. The engine it builds on,
> [pygtfsie](https://gitlab.com/ggiesen/pygtfsie), handles fetch, validation,
> import, materialisation and departure queries.
>
> `manifest.json` declares `pygtfsie` as a requirement and that package is not
> yet on PyPI, so the integration cannot be installed through HACS until it is.
> Tests install the engine from git; see `requirements_test.txt`.

## What it is

Point it at an agency's GTFS archive, pick an origin and a destination, and get a
sensor whose state is the next departure as a timestamp, with the following
departures, delays, route and trip detail in its attributes. Where the agency
publishes GTFS-Realtime, departures carry live predictions, and vehicle positions
and service alerts are available too.

There is also a proximity mode: instead of a fixed stop pair, watch whatever
stops are near a device tracker, so a phone leaving the house produces departures
for the stops it is actually near.

## Why it exists

This is a clean-room reimplementation rather than a fork. The specification was
built by mining public issue reports for behaviour that a GTFS integration has to
get right, and by reading the GTFS and GTFS-Realtime specifications directly. No
existing implementation's source was used.

The result is a different architecture rather than a rewrite of the same one.
Two decisions account for most of the difference:

**Time is resolved when the feed is imported, not when a sensor updates.** Every
stop event becomes an absolute UTC instant at ingest, and no SQL statement
contains a date function. SQLite evaluates `date('now')` in C against the process
timezone in UTC, which no Python-level clock control can reach -- so a feed that
looks correct in a test can still file departures on the wrong day in
production, and the two cases are indistinguishable from the outside. Removing
date arithmetic from the query path removes the whole class.

**The stored data is split in two, and neither half is ever written after it is
promoted.** `feed.sqlite` is a pure function of the downloaded archive.
`index.sqlite` is a pure function of that feed plus the departure window, the
resolved timezone and the configured scope. Those change at completely different
rates -- an agency republishes every few weeks, the window rolls every night --
so fusing them means re-parsing every CSV to move a window, and makes it
impossible to recompute stored instants after a timezone correction without
downloading the archive again. Because promotion is an `os.replace()` and nothing
mutates a live file, there is exactly one writer and "database is locked" is
unreachable rather than merely handled.

## Layout

The integration is the Home Assistant half of a pair:

- **[pygtfsie](https://gitlab.com/ggiesen/pygtfsie)** -- the engine. Archive
  handling, CSV tolerance, calendar expansion, the time model, SQLite schema and
  queries, protobuf and SIRI decoding. No Home Assistant dependency, enforced by
  a CI gate.
- **ha-gtfsie** (this repository) -- config flow, coordinators, entities,
  services, diagnostics and repairs. Declares `pygtfsie` in `manifest.json`.

[`docs/SPEC.md`](docs/SPEC.md) section 1.3a records exactly which module lives
where and why the two signatures that need a `HomeAssistant` are split rather
than moved.

## Installation

Not yet installable. When it is, it will be through HACS as a custom repository,
and then as a default HACS integration.

## Development

```
python3 -m venv .venv
./.venv/bin/pip install -r requirements_test.txt
./scripts/run-tests.sh
```

`scripts/time-sweep.sh` runs the suite at a range of instants and timezones and
fails if the results differ. No test here may depend on the current time; that
script is what keeps the rule from decaying, because a test that quietly builds
an expectation from "today" passes for months before failing on a DST boundary
or after 22:00 in one hemisphere.

## Branching

`master` is the release branch and is what tags are cut from. Work happens on
topic branches merged back into `master`.

## Licence

Mozilla Public License 2.0. See [LICENSE](LICENSE).

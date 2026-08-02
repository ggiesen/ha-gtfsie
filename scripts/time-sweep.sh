#!/usr/bin/env bash
# Run the whole suite at many instants and fail if the result ever changes.
#
# Why this exists
# ---------------
# The most persistent class of defect in this suite has not been a wrong
# assertion. It has been a test whose outcome depends on when it runs.
#
# Three separate cases have been found and fixed:
#
#   * test_calendar_and_exceptions.py compared Python-local dates against
#     SQLite's UTC date('now'), so four of five tests failed after 20:00 Toronto
#     and passed the rest of the day.
#   * test_next_departure.py skipped itself after 22:00 local, which silently
#     removed the offset filter from coverage for part of every day.
#   * test_calendar_and_exceptions.py's issue #164 reproduction PASSED, because
#     reproducing needs "now" to precede the trip's clock time. It could not
#     fail after 08:00 local. That one is the worst of the three: a green test
#     that cannot fail looks exactly like a healthy one, so nothing draws
#     attention to it, and it was cited upstream as evidence a real bug was
#     not reproducible.
#
# Individually each looked like an ordinary bug. Together they are a property
# the suite should hold and be checked for: the result must not depend on the
# clock. This script checks that property directly instead of hoping the next
# instance is noticed by hand.
#
# Usage:  scripts/time-sweep.sh            # default instants
#         scripts/time-sweep.sh --quick    # times of day only
#
# Requires libfaketime (scripts/run-tests.sh finds it). Without it every run is
# at the real clock, the sweep is meaningless, and this exits non-zero.

set -uo pipefail
cd "$(dirname "$0")/.."

QUICK=0
[ "${1:-}" = "--quick" ] && QUICK=1

# Times of day: the boundaries that matter are where local and UTC dates
# disagree, which for the Americas is late evening.
INSTANTS=(
  "2026-08-02 00:30:00"  "2026-08-02 06:00:00"  "2026-08-02 12:00:00"
  "2026-08-02 18:00:00"  "2026-08-02 21:00:00"  "2026-08-02 23:45:00"
)

if [ "$QUICK" -eq 0 ]; then
  INSTANTS+=(
    # Every weekday: calendar.txt is keyed by weekday column, so a test that
    # builds a calendar without covering today's column yields nothing at all.
    "2026-08-03 14:00:00"  "2026-08-04 14:00:00"  "2026-08-05 14:00:00"
    "2026-08-06 14:00:00"  "2026-08-07 14:00:00"  "2026-08-08 14:00:00"
    "2026-08-09 14:00:00"
    # DST in both directions, and month and year boundaries.
    "2026-03-08 06:30:00"  "2026-03-08 07:30:00"  "2026-11-01 05:30:00"
    "2026-02-28 23:30:00"  "2026-08-31 23:30:00"  "2026-12-31 23:30:00"
    "2027-12-30 12:00:00"
    # Years out, and the reason matters. This list used to stop just *inside*
    # the feed builder's default validity window -- it knew the fixture had an
    # end date and deliberately stayed before it. Testing up to a boundary but
    # never past it is exactly how an expiring fixture stays invisible: every
    # instant passes, right up until the real date crosses the line and the
    # timetable silently empties. The default calendar is now unbounded, and
    # these two instants are here to keep it that way.
    "2030-11-11 22:00:00"  "2035-01-01 00:00:01"
  )
fi

if ! ls /usr/lib/*/faketime/libfaketime.so.1 >/dev/null 2>&1 \
   && ! ls /usr/lib/faketime/libfaketime.so.1 >/dev/null 2>&1 \
   && ! ls /opt/homebrew/lib/faketime/libfaketime.1.dylib >/dev/null 2>&1; then
  echo "libfaketime not found; every run would use the real clock and the sweep" >&2
  echo "would prove nothing. Install it (apt install faketime / brew install libfaketime)." >&2
  exit 1
fi

baseline=""
failures=0
printf '%-22s %s\n' "INSTANT (UTC)" "RESULT"
printf '%-22s %s\n' "----------------------" "------"

for t in "${INSTANTS[@]}"; do
  # Counts only. Durations are meaningless under libfaketime (pytest subtracts
  # a faked start from a faked end) and would differ every run.
  result=$(FAKETIME="@$t" ./scripts/run-tests.sh --tb=no -q -p no:randomly 2>&1 \
           | grep -E '^[0-9]+ (passed|failed)' | tail -1 \
           | sed -E 's/,? [0-9]+ warnings?//; s/ in .*//; s/[[:space:]]+$//')
  printf '%-22s %s\n' "$t" "${result:-<no result>}"
  if [ -z "$baseline" ]; then
    baseline="$result"
  elif [ "$result" != "$baseline" ]; then
    echo "    ^^ DIFFERS from baseline: $baseline" >&2
    failures=$((failures + 1))
  fi
done

# A sweep looks for *variation*, so a suite that fails identically at every
# instant passes it. Announcing "identical at all N instants" against a red
# baseline reads as a clean bill of health and is worse than saying nothing.
case "$baseline" in
  *failed*|*error*)
    echo "" >&2
    echo "the baseline is not green ($baseline), so this sweep only established" >&2
    echo "that it fails consistently. Fix the suite first." >&2
    exit 1
    ;;
esac

echo
if [ "$failures" -ne 0 ]; then
  echo "FAIL: the suite's result depends on the clock ($failures of ${#INSTANTS[@]} instants differ)." >&2
  echo "A test that behaves differently by time of day is not a test, it is a coin flip." >&2
  exit 1
fi
echo "OK: identical at all ${#INSTANTS[@]} instants -> $baseline"

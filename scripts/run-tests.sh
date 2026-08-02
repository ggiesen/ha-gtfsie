#!/usr/bin/env bash
#
# Run the test suite with a controllable clock.
#
# get_next_departure compares dates computed in Python against dates computed by
# SQLite's date('now'). freezegun cannot reconcile those: it patches Python's time
# functions, while SQLite reads the system clock through libc. libfaketime
# intercepts at the libc level, so both see the same instant.
#
# Without this wrapper the suite still runs and still passes; the tests that need
# a specific instant skip instead. That keeps libfaketime an optional dependency
# rather than a barrier to running the tests at all.
#
#   apt install faketime        # Debian/Ubuntu
#   brew install libfaketime    # macOS
#
# TZ=UTC is set so a pinned instant means what it says. Home Assistant's timezone
# is configured per test with hass.config.async_set_time_zone, which is what lets a
# test sit at "01:30 local on a spring-forward morning".
set -euo pipefail

# Prefer a local virtualenv when one exists, since that is the usual developer
# setup, but fall back to whatever python is on PATH. Hardcoding ./.venv made the
# script unusable anywhere without one -- CI, or a contributor using uv, pyenv or
# a system install -- and the failure was a bare "No such file or directory" and
# exit 127 rather than anything that named the cause.
if [ -z "${PYTHON:-}" ]; then
  if [ -x ./.venv/bin/python ]; then
    PYTHON=./.venv/bin/python
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON=python3
  else
    PYTHON=python
  fi
fi

if ! command -v "$PYTHON" >/dev/null 2>&1 && [ ! -x "$PYTHON" ]; then
  echo "no usable interpreter: PYTHON=$PYTHON is neither on PATH nor executable" >&2
  exit 1
fi

for candidate in \
  /usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1 \
  /usr/lib/aarch64-linux-gnu/faketime/libfaketime.so.1 \
  /usr/lib/faketime/libfaketime.so.1 \
  /usr/local/lib/faketime/libfaketime.so.1 \
  /opt/homebrew/lib/faketime/libfaketime.1.dylib ; do
  [ -e "$candidate" ] && FAKETIME_LIB="$candidate" && break
done

if [ -z "${FAKETIME_LIB:-}" ]; then
  echo "libfaketime not found; running without a controllable clock." >&2
  echo "Clock-dependent tests will skip. Install with: apt install faketime" >&2
  exec "$PYTHON" -m pytest "$@"
fi

echo "using libfaketime at $FAKETIME_LIB" >&2
export LD_PRELOAD="$FAKETIME_LIB"
export FAKETIME_NO_CACHE=1
export TZ=UTC
exec "$PYTHON" -m pytest "$@"

#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
case ${JOLT_BIN:-} in
  /*) jolt=$JOLT_BIN ;;
  "") jolt=jolt ;;
  *) jolt=$repo/$JOLT_BIN ;;
esac

version=$("$jolt" --version)
case $version in
  "jolt v0.8."*|"jolt v0.9."*|"jolt v1."*) ;;
  *)
    echo "ERROR: preset smoke requires Jolt 0.8.0 or newer; found: $version" >&2
    exit 2
    ;;
esac

if ! env JOLT_NO_USER_DEPS=1 JOLT_PWD=${TMPDIR:-/tmp} \
  "$jolt" -Sdeps '{:deps {}}' -e \
  "(require 'jolt.aspects) (System/exit (if (resolve 'jolt.aspects/expand-selection) 0 1))"
then
  echo "ERROR: preset smoke requires package preset expansion in jolt.aspects" >&2
  exit 2
fi

fixture=$repo/test-app-presets
tmp=${TMPDIR:-/tmp}/jolt-http-server-presets-$$
trap 'rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp"
cp "$fixture/deps.edn" "$tmp/deps.edn"
cp -R "$repo/src" "$tmp/src"
cp -R "$repo/test" "$tmp/test"

for profile in basic detailed debug; do
  sed -i "s@http-server/basic@http-server/$profile@" "$tmp/deps.edn"
  (cd "$tmp" && env JOLT_PWD="$tmp" \
    JOLT_CACHE_DIR="$tmp/cache" \
    JOLT_GITLIBS_DIR=${JOLT_GITLIBS_DIR:-$tmp/gitlibs} \
    "$jolt" build -m otel.instrumentation.http-server-build-smoke \
      -o "target/$profile")
  report="$tmp/target/http-server-preset-aspects.edn"
  grep -q "http-server/$profile-aspect-provider" "$report"
  grep -q 'META-INF/jolt/aspects/http-server.edn' "$report"
  "$tmp/target/$profile"
  sed -i "s@http-server/$profile@http-server/basic@" "$tmp/deps.edn"
done

echo "PASS: HTTP server package-owned capture presets"

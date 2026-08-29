#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
case ${JOLT_BIN:-} in
  /*) jolt=$JOLT_BIN ;;
  "") jolt=jolt ;;
  *) jolt=$repo/$JOLT_BIN ;;
esac
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
    "$jolt" aspects plan) >"$tmp/$profile-plan.edn"
  grep -q ":id :otel.http-server/$profile" "$tmp/$profile-plan.edn"
  grep -q "http-server/$profile.edn" "$tmp/$profile-plan.edn"
  (cd "$tmp" && env JOLT_PWD="$tmp" \
    JOLT_CACHE_DIR="$tmp/cache" \
    JOLT_GITLIBS_DIR=${JOLT_GITLIBS_DIR:-$tmp/gitlibs} \
    "$jolt" build -m otel.instrumentation.http-server-build-smoke \
      -o "target/$profile")
  "$tmp/target/$profile"
  sed -i "s@http-server/$profile@http-server/basic@" "$tmp/deps.edn"
done

echo "PASS: HTTP server package-owned capture presets"

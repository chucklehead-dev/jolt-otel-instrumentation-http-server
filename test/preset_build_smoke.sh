#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
case ${JOLT_BIN:-} in
  /*) jolt=$JOLT_BIN ;;
  "") jolt=jolt ;;
  *) jolt=$repo/$JOLT_BIN ;;
esac
toolchain=${JOLT_TOOLCHAIN:-}

run_jolt() {
  if [ -n "$toolchain" ]; then
    "$toolchain" "$jolt" "$@"
  else
    "$jolt" "$@"
  fi
}

version=$(run_jolt --version)
case $version in
  "jolt v0.8."*|"jolt v0.9."*|"jolt v1."*) ;;
  *)
    echo "ERROR: preset smoke requires Jolt 0.8.0 or newer; found: $version" >&2
    exit 2
    ;;
esac

if ! (
  export JOLT_NO_USER_DEPS=1
  JOLT_PWD=${TMPDIR:-/tmp}
  export JOLT_PWD
  run_jolt -Sdeps '{:deps {}}' -e \
    "(require 'jolt.aspects) (System/exit (if (resolve 'jolt.aspects/expand-selection) 0 1))"
)
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
  (cd "$tmp"
    export JOLT_PWD="$tmp"
    export JOLT_CACHE_DIR="$tmp/cache"
    JOLT_GITLIBS_DIR=${JOLT_GITLIBS_DIR:-$tmp/gitlibs}
    export JOLT_GITLIBS_DIR
    run_jolt build -m otel.instrumentation.http-server-build-smoke \
      -o "target/$profile")
  report="$tmp/target/http-server-preset-aspects.edn"
  grep -q "http-server/$profile-aspect-provider" "$report"
  grep -q 'META-INF/jolt/aspects/http-server.edn' "$report"
  output=$("$tmp/target/$profile")
  printf '%s\n' "$output"
  printf '%s\n' "$output" | grep -Fqx \
    'OK: woven asynchronous HTTP server lifecycle'
  sed -i "s@http-server/$profile@http-server/basic@" "$tmp/deps.edn"
done

echo "PASS: HTTP server package-owned capture presets"

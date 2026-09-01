# jolt-otel-instrumentation-http-server

Build-selected OpenTelemetry server instrumentation for jolt-http. jolt-http
owns the inert `META-INF/jolt/aspects/http-server.edn` manifest; applications
explicitly select this separate provider when building an executable.

The source and unit-test surface requires Jolt 0.8.0 or newer. Compiler-woven
builds additionally require an explicitly selected aspect-capable compiler;
the released Jolt 0.8.0 remains the plain/unit compatibility floor.

Preset selection is staged but not yet an end-to-end validated public path.
It requires a compiler that combines Jolt 0.8 runtime semantics with package
preset expansion; those capabilities currently exist on separate development
lines. `test/preset_build_smoke.sh` fails fast until a compiler supplies both.
Once consolidated, applications can select a package-owned capture policy
without copying the manifest/provider wiring:

```clojure
{:jolt/build
 {:aspects
  [{:preset
    "META-INF/jolt/instrumentation/http-server/basic.edn"}]}}
```

Three explicit presets ship in this package:

| Preset | Captured span data |
| --- | --- |
| `basic` | Standard method, route/path, protocol, addresses, status, errors, propagation, and duration metric. No arbitrary headers, body sizes, or bodies. |
| `detailed` | Basic plus bounded allowlisted content metadata, request IDs, user agent, and declared request/response body sizes. |
| `debug` | Detailed plus up to 4096 characters of textual request/response body content when already available without consuming a stream. |

`debug` is an explicit opt-in to potentially sensitive payload telemetry.
Streaming request bodies are never read, repositioned, closed, or intercepted
by this join point; consequently their content is absent. Authorization,
proxy-authorization, cookie, set-cookie, API-key, Trace Context, tracestate, and
baggage headers remain denied in every preset. Header values are bounded and
must not contain line breaks.

The emitted header, body-size, and body-content names follow the current
[OpenTelemetry HTTP span semantic conventions](https://opentelemetry.io/docs/specs/semconv/http/http-spans/).

```clojure
{:jolt/build
 {:aspects [{:resource "META-INF/jolt/aspects/http-server.edn"
             :provider otel.instrumentation.http-server}]}}
```

The provider extracts W3C Trace Context and baggage, starts a server span with
bounded HTTP attributes, and keeps it open when a three-arity Ring handler
returns before calling `respond` or `raise`. The callback runs with the server
span current, so child work created there is parented correctly. The first
callback finishes the span after jolt-http processes it; duplicate callbacks do
not finish it twice. A companion response seam observes jolt-http's sanitized
response, so invalid handler metadata replaced by a safe 500 is reported as the
actual accepted 500 rather than the handler's rejected value.

Terminal log emission, span completion, duration recording, and the optional
completion hook are independent fail-open observations. A failure in any one
cannot replace a successful Ring callback result, mask the original object
thrown by application code, or prevent the remaining terminal observations.

The lifecycle deliberately means **Ring response callback completed**. It does
not mean that the kernel flushed the final write or that the peer received it.
Transport-delivery instrumentation needs an outcome-bearing final-write seam in
jolt-tcp and is outside this manifest's contract.

The provider records method, path, protocol version, server/client address and
response status. It also records the stable `http.server.request.duration`
histogram in seconds with the standard advisory buckets; its duration ends at
the same accepted Ring response-callback boundary as the span. It does not
record query strings, request or response bodies, authorization/cookie headers,
or arbitrary header values. The duration metric defaults to the required and
recommended low-cardinality dimensions; the span-only server/client/peer
addresses do not become metric dimensions. Failures emit a correlated
`http.server.request.exception` log event at ERROR severity with only the
bounded exception type, not exception messages, stack traces, or ex-data.

The default inbound propagator is the standard Trace Context plus baggage
composite. Applications with a stricter boundary can supply any
`otel.propagation/TextMapPropagator`; for example, this accepts distributed
trace parentage and tracestate without accepting baggage:

```clojure
(http/run-server handler
  :otel.instrumentation.http-server/propagator
  otel.propagation/trace-context)
```

An explicitly invalid propagator, or one that throws while parsing untrusted
headers, fails closed to a fresh root span without changing the HTTP result.

Methods in the semantic-conventions registry retain their standard value.
Other methods use `_OTHER` and the span name `HTTP`. jolt-http currently
normalizes request-line methods before constructing the Ring map, so this
consumer does not fabricate `http.request.method_original` from a lossy value.
The standard `OTEL_INSTRUMENTATION_HTTP_KNOWN_METHODS` comma-separated setting
provides the required case-sensitive full override for extension methods.

Embedded receivers and viewers should prevent self-observation before handler
dispatch by supplying an exclusion predicate to jolt-http:

```clojure
(http/run-server handler
  :otel.instrumentation.http-server/exclude?
  (fn [request]
    (contains? #{"/v1/traces" "/v1/logs" "/v1/metrics" "/live"}
               (:uri request))))
```

This bypasses header extraction and span creation for matching requests. A
configured exclusion predicate fails closed: if it throws, the application
request proceeds but is not instrumented. The existing generic OTel context
suppression remains available for storage work performed after dispatch.

An application can also provide a bounded low-cardinality route resolver. This
adds `http.route` and names the span `METHOD /route/:parameter` without putting
OTel calls inside a Ring handler or router:

```clojure
(http/run-server handler
  :otel.instrumentation.http-server/route
  (fn [request] (my.router/route-template (:uri request))))
```

Routes longer than 256 characters, containing a query/fragment/newline, or
whose resolver throws are ignored. Raw query strings remain unrecorded.

Every string attribute has a closed syntax and length bound; an invalid or
oversized value is omitted. Local/embedded applications can additionally omit
physical network identity while retaining method, route, path, protocol, and
status telemetry:

```clojure
(http/run-server handler
  :otel.instrumentation.http-server/capture-network-addresses? false)
```

## WIP verification

Run the unit gate, then build and execute both sides of the opt-in contract
with the aspect-enabled compiler. The woven fixture proves remote parentage,
async child context, accepted status, and delayed completion over real loopback
HTTP. The plain fixture has the provider and inert manifest on its classpath but
does not select it; only its explicit child span is exported.

```sh
env JOLT_GITLIBS_DIR=/home/chuck/.cache/jolt-http-server-gitlibs \
  /home/chuck/ai-src/tools/jolt-with-chez-10.4.1 jolt -M:test

env JOLT_BIN=/path/to/aspect-capable/jolt \
  JOLT_GITLIBS_DIR=/home/chuck/.cache/jolt-http-server-gitlibs \
  /home/chuck/ai-src/tools/jolt-with-chez-10.4.1 "$JOLT_BIN" \
  -A:test build -m otel.instrumentation.http-server-build-smoke \
  -o target/http-server-build-smoke
target/http-server-build-smoke

# Staged: this intentionally fails unless the compiler has both Jolt 0.8
# runtime semantics and package preset expansion.
env JOLT_BIN=/path/to/consolidated-preset-capable-jolt \
  JOLT_GITLIBS_DIR=/home/chuck/.cache/jolt-http-server-gitlibs \
  test/preset_build_smoke.sh

(cd test-app-plain && \
  env JOLT_GITLIBS_DIR=/home/chuck/.cache/jolt-http-server-gitlibs \
  /home/chuck/ai-src/tools/jolt-with-chez-10.4.1 jolt \
  build -m otel.instrumentation.http-server-build-smoke \
    -o target/plain-http-server-smoke)
test-app-plain/target/plain-http-server-smoke plain
```

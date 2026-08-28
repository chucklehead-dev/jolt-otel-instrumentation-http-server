# jolt-otel-instrumentation-http-server

Build-selected OpenTelemetry server instrumentation for jolt-http. jolt-http
owns the inert `META-INF/jolt/aspects/http-server.edn` manifest; applications
explicitly select this separate provider when building an executable.

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

The lifecycle deliberately means **Ring response callback completed**. It does
not mean that the kernel flushed the final write or that the peer received it.
Transport-delivery instrumentation needs an outcome-bearing final-write seam in
jolt-tcp and is outside this manifest's contract.

The provider records method, path, protocol version, server/client address and
response status. It does not record query strings, request or response bodies,
authorization/cookie headers, or arbitrary header values.
Exception events retain a bounded type and escaped flag, not exception messages
or ex-data.

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
jolt -M:test

jolt -A:test build -m otel.instrumentation.http-server-build-smoke \
  -o target/http-server-build-smoke
target/http-server-build-smoke

(cd test-app-plain && \
  jolt build -m otel.instrumentation.http-server-build-smoke \
    -o target/plain-http-server-smoke)
test-app-plain/target/plain-http-server-smoke plain
```

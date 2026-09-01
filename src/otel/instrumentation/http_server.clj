(ns otel.instrumentation.http-server
  "Build-selected OpenTelemetry consumer for jolt-http's normalized Ring
  handler lifecycle.

  The compiler advice replaces only the normalized handler argument. It starts
  one server span before handler dispatch, keeps that span open when an async
  handler returns, restores its context when `respond` or `raise` later runs,
  and ends it after the first callback finishes. This is Ring response-callback
  completion, not proof that bytes reached the peer."
  (:require [clojure.string :as str]
            [jolt.host :as host]
            [otel.context :as context]
            [otel.logs :as logs]
            [otel.metrics :as metrics]
            [otel.propagation :as propagation]
            [otel.sdk :as sdk]
            [otel.trace :as trace]))

(def http-build-id
  "Compatibility id of the exact jolt-http entry seam selected by the
  library-owned manifest."
  "3ef772262308bbf6039412366ae80690cec348b0")

(def ^:private instrumentation-version "0.1.0")
(def ^:private scope-name
  "io.github.chucklehead-dev/jolt-otel-instrumentation-http-server")
(def ^:private active-context-key
  ::active-request)
(def ^:private metric-attributes-context-key
  ::metric-attributes)
(def ^:private capture-profile-context-key
  ::capture-profile)
(def ^:private duration-boundaries
  [0.005 0.01 0.025 0.05 0.075 0.1 0.25 0.5 0.75 1.0 2.5 5.0 7.5 10.0])
(defonce ^:private duration-instrument-cache (atom nil))

(def ^:private basic-capture
  {:name :basic
   :request-headers []
   :response-headers []
   :body-sizes? false
   :bodies? false
   :max-body-chars 0})

(def ^:private detailed-capture
  {:name :detailed
   :request-headers ["content-length" "content-type" "user-agent"
                     "x-request-id"]
   :response-headers ["content-length" "content-type" "x-request-id"]
   :body-sizes? true
   :bodies? false
   :max-body-chars 0})

(def ^:private debug-capture
  (assoc detailed-capture
         :name :debug
         :bodies? true
         :max-body-chars 4096))

(def ^:private sensitive-header-names
  #{"authorization" "proxy-authorization" "cookie" "set-cookie"
    "x-api-key" "traceparent" "tracestate" "baggage"})

(defn active?
  "True while application code or a response callback runs inside this
  provider's generic server boundary.

  Embedders can use this request-scoped capability marker to retain explicit
  source-mode fallback instrumentation without duplicating compiler-woven
  server or client spans. It carries with the OTel context and never becomes
  process-global build state."
  []
  (true? (context/get-value (context/current) active-context-key)))

(def exclusion-option
  "jolt-http option naming a `(fn [request] boolean)` predicate. Matching
  requests bypass server instrumentation before trace headers are extracted.
  Receivers and embedded telemetry viewers should exclude their own ingest,
  query, asset, and live-update endpoints to prevent self-observation loops."
  :otel.instrumentation.http-server/exclude?)

(def route-option
  "jolt-http option naming a `(fn [request] route-template-or-nil)` resolver.
  A valid bounded route becomes `http.route` and enriches the server span name
  without requiring application handlers or routers to call OTel APIs."
  :otel.instrumentation.http-server/route)

(def network-addresses-option
  "jolt-http boolean option controlling `server.address`, `server.port`, and
  `client.address`. It defaults to true; embedded/local demos may set false so
  physical machine identity never reaches stored telemetry or screenshots."
  :otel.instrumentation.http-server/capture-network-addresses?)

(def propagator-option
  "jolt-http option naming the inbound TextMapPropagator. When absent, the
  standard Trace Context plus baggage composite is used. An explicit invalid
  value, or a configured propagator that throws, fails closed to a root
  context: the HTTP request still runs and receives a fresh server span, but
  untrusted propagation headers are not inherited."
  :otel.instrumentation.http-server/propagator)

(def on-end-option
  "jolt-http option naming a zero-argument completion hook. The provider calls
  it exactly once after the accepted response callback has ended the server
  span. Hook failures are observational and cannot change the HTTP result.
  The ended span context remains installed during the hook, so hook-owned
  telemetry/export work should use generic instrumentation suppression.
  Embedded collectors can use it to flush the just-completed span before a
  redirect causes the next viewer query."
  :otel.instrumentation.http-server/on-end)

(def ^:private default-known-methods
  ;; Stable HTTP semantic-convention values plus QUERY, which is already in the
  ;; current registry as a development value.
  #{"CONNECT" "DELETE" "GET" "HEAD" "OPTIONS" "PATCH"
    "POST" "PUT" "QUERY" "TRACE"})

(defn parse-known-methods
  "Parse the standard case-sensitive known-method full override."
  [raw]
  (if (string? raw)
    (into #{}
          (filter #(and (<= 1 (count %) 32)
                        (re-matches #"[!#$%&'*+.^_`|~0-9A-Za-z-]+" %)))
          (map str/trim (str/split raw #",")))
    default-known-methods))

(def ^:private configured-known-methods
  (parse-known-methods
   (host/getenv "OTEL_INSTRUMENTATION_HTTP_KNOWN_METHODS")))

(defn- original-method [request]
  (let [method (:request-method request)]
    (cond
      (keyword? method) (str/upper-case (name method))
      (string? method)  (str/upper-case method)
      :else             nil)))

(defn- method-values [request]
  (let [original (original-method request)]
    (if (contains? configured-known-methods original)
      {:method original :span-name original}
      ;; jolt-http's Ring map has already normalized the request-line method to
      ;; a lower-case keyword, so its original spelling/casing is unavailable.
      ;; Do not fabricate `http.request.method_original` from that lossy value.
      {:method "_OTHER" :span-name "HTTP"})))

(defn- safe-route [value]
  (when (and (string? value)
             (<= 1 (count value) 256)
             (.startsWith value "/")
             (not (re-find #"[?#\r\n]" value)))
    value))

(defn- safe-scheme [value]
  (let [candidate (cond
                    (keyword? value) (name value)
                    (string? value) (str/lower-case value)
                    :else nil)]
    (when (contains? #{"http" "https"} candidate) candidate)))

(defn- safe-path [value]
  (when (and (string? value)
             (<= 1 (count value) 2048)
             (.startsWith value "/")
             (not (re-find #"[\r\n]" value)))
    value))

(defn- safe-address [value]
  (when (and (string? value)
             (<= 1 (count value) 255)
             (not (re-find #"[\r\n\s]" value)))
    value))

(defn- safe-protocol-version [value]
  (when (and (string? value) (<= 1 (count value) 32))
    (let [version (or (second (re-matches #"HTTP/([0-9]+(?:\.[0-9]+)?)" value))
                      (when (re-matches #"[0-9]+(?:\.[0-9]+)?" value) value))]
      version)))

(defn- safe-header-value [value]
  (when (and (string? value)
             (<= (count value) 2048)
             (not (re-find #"[\r\n]" value)))
    value))

(defn- normalized-headers [headers]
  (reduce-kv
   (fn [result name value]
     (let [name (when (or (string? name) (keyword? name))
                  (str/lower-case
                   (if (keyword? name) (clojure.core/name name) name)))]
       (if (and name (re-matches #"[!#$%&'*+.^_`|~0-9a-z-]+" name))
         (assoc result name value)
         result)))
   {}
   (if (map? headers) headers {})))

(defn- captured-header-attributes [kind headers names]
  (let [headers (normalized-headers headers)]
    (reduce
     (fn [attrs name]
       (if (contains? sensitive-header-names name)
         attrs
         (if-some [value (safe-header-value (get headers name))]
           (assoc attrs (str "http." kind ".header." name) [value])
           attrs)))
     {}
     names)))

(defn- content-length [headers]
  (let [raw (get (normalized-headers headers) "content-length")]
    (when (string? raw)
      (try
        (let [n (parse-long raw)]
          (when (and n (not (neg? n))) n))
        (catch :default _ nil)))))

(defn- textual-content? [headers]
  (when-some [content-type
              (some-> (get (normalized-headers headers) "content-type")
                      str/lower-case
                      (str/split #";" 2)
                      first
                      str/trim)]
    (or (str/starts-with? content-type "text/")
        (str/ends-with? content-type "/json")
        (str/ends-with? content-type "+json")
        (str/ends-with? content-type "/xml")
        (str/ends-with? content-type "+xml")
        (str/ends-with? content-type "/yaml")
        (str/ends-with? content-type "+yaml")
        (= content-type "application/x-www-form-urlencoded"))))

(defn- bounded-body [body headers {:keys [bodies? max-body-chars]}]
  (when (and bodies? (string? body) (textual-content? headers))
    (subs body 0 (min (count body) max-body-chars))))

(defn- resolved-route [opts request]
  (when-some [resolve-route (get opts route-option)]
    (when (fn? resolve-route)
      (try (safe-route (resolve-route request))
           (catch :default _ nil)))))

(defn- excluded? [opts request]
  (when-some [exclude? (get opts exclusion-option)]
    ;; A configured anti-feedback guard must fail closed. Instrumenting when it
    ;; throws can recursively observe the receiver/viewer path it was meant to
    ;; protect; the application request itself still proceeds unchanged.
    (if (fn? exclude?)
      (try (boolean (exclude? request))
           (catch :default _ true))
      true)))

(defn- request-attributes [request method route capture-addresses? capture]
  (let [scheme (safe-scheme (:scheme request))
        path (safe-path (:uri request))
        protocol (safe-protocol-version (:protocol request))
        server-address (when capture-addresses?
                         (safe-address (:server-name request)))
        client-address (when capture-addresses?
                         (safe-address (:remote-addr request)))
        server-port (when (and capture-addresses?
                               (integer? (:server-port request))
                               (<= 1 (:server-port request) 65535))
                      (:server-port request))
        headers (:headers request)
        user-agent (when (not= :basic (:name capture))
                     (safe-header-value
                      (get (normalized-headers headers) "user-agent")))
        body-size (when (:body-sizes? capture) (content-length headers))
        body-content (bounded-body (:body request) headers capture)]
    (cond-> (merge {:http.request.method method}
                   (captured-header-attributes
                    "request" headers (:request-headers capture)))
    route
    (assoc :http.route route)

    scheme
    (assoc :url.scheme scheme)

    path
    (assoc :url.path path)

    protocol
    (assoc :network.protocol.version protocol)

    server-address
    (assoc :server.address server-address)

    server-port
    (assoc :server.port server-port)

    client-address
    (assoc :client.address client-address
           ;; jolt-http exposes the connected peer, not a proxy-derived
           ;; original address, so these are intentionally equal here.
           :network.peer.address client-address)

    user-agent
    (assoc :user_agent.original user-agent)

    body-size
    (assoc :http.request.body.size body-size)

    body-content
    (assoc :http.request.body.content body-content))))

(defn- exception-type [error]
  (try
    (or (some-> error class .getName) "UnknownExceptionType")
    (catch :default _ "UnknownExceptionType")))

(defn- duration-instrument []
  (let [provider (sdk/meter-provider)]
    (if (nil? provider)
      metrics/noop-instrument
      (locking duration-instrument-cache
        (let [cached @duration-instrument-cache]
          (if (identical? provider (:provider cached))
            (:instrument cached)
            (let [instrument
                  (metrics/histogram
                   (sdk/meter scope-name {:version instrumentation-version})
                   "http.server.request.duration"
                   {:description "Duration of HTTP server requests."
                    :unit "s"
                    :boundaries duration-boundaries})]
              (reset! duration-instrument-cache
                      {:provider provider :instrument instrument})
              instrument)))))))

(defn- metric-attributes [attributes]
  (select-keys attributes
               [:http.request.method :url.scheme :http.route
                :network.protocol.version]))

(defn- observe!
  "Run one observational operation without letting it replace application
  control flow. Terminal operations are deliberately isolated from one another
  so a failed exporter or processor cannot prevent the remaining cleanup."
  [operation]
  (try
    (operation)
    (catch :default _ nil)))

(defn- mark-error! [span metric-attrs error]
  (let [error-type (exception-type error)]
    (observe! #(trace/set-attribute! span :error.type error-type))
    (observe! #(swap! metric-attrs assoc :error.type error-type))
    ;; Exception messages, stack traces, and ex-data routinely contain request
    ;; bodies, credentials, filesystem paths, and application data. The type
    ;; alone satisfies the event contract without weakening privacy defaults.
    (observe!
     #(logs/emit! (sdk/logger scope-name {:version instrumentation-version})
                  {:event-name "http.server.request.exception"
                   :body "HTTP server request exception"
                   :severity :error
                   :attributes {:exception.type error-type}}))
    (observe! #(trace/set-status! span :error))))

(defn- response-status [response]
  (let [status (:status response)]
    (when (integer? status) status)))

(defn- mark-response! [span metric-attrs response capture]
  (when-some [status (response-status response)]
    (if (<= 100 status 599)
      (do
        (trace/set-attribute! span :http.response.status_code status)
        (swap! metric-attrs assoc :http.response.status_code status)
        (when (>= (long status) 500)
          (trace/set-attribute! span :error.type (str status))
          (swap! metric-attrs assoc :error.type (str status))
          (trace/set-status! span :error)))
      (do
        (trace/set-attribute! span :error.type "_OTHER")
        (swap! metric-attrs assoc :error.type "_OTHER")
        (trace/set-status! span :error))))
  (let [headers (:headers response)
        body-size (when (:body-sizes? capture) (content-length headers))
        body-content (bounded-body (:body response) headers capture)]
    (trace/set-attributes!
     span
     (cond-> (captured-header-attributes
              "response" headers (:response-headers capture))
       body-size (assoc :http.response.body.size body-size)
       body-content (assoc :http.response.body.content body-content)))))

(defn- notify-end! [opts]
  (when-some [on-end (get opts on-end-option)]
    (when (fn? on-end)
      (try (on-end)
           (catch :default _ nil)))))

(defn- extracted-parent [opts headers]
  (let [configured? (contains? opts propagator-option)
        propagator (if configured?
                     (get opts propagator-option)
                     propagation/default-propagator)]
    (if (satisfies? propagation/TextMapPropagator propagator)
      (try
        (propagation/extract propagator context/root headers)
        (catch :default _ context/root))
      context/root)))

(defn- traced-handler [handler request opts capture]
  (let [{:keys [method span-name]} (method-values request)
        route        (resolved-route opts request)
        capture-addresses? (not (false? (get opts network-addresses-option)))
        span-name    (if route (str span-name " " route) span-name)
        parent       (extracted-parent opts (or (:headers request) {}))
        attributes   (request-attributes request method route capture-addresses?
                                         capture)
        metric-attrs (atom (metric-attributes attributes))
        started      (host/mono-nanos)
        start-wall   (host/wall-nanos)
        tracer       (sdk/tracer scope-name {:version instrumentation-version})
        span         (trace/start-span tracer span-name
                                       {:parent parent
                                        :kind :server
                                        :start-timestamp start-wall
                                        :attributes attributes})
        span-context (-> parent
                         (trace/context-with-span span)
                         (context/with-value active-context-key true)
                         (context/with-value metric-attributes-context-key
                                             metric-attrs)
                         (context/with-value capture-profile-context-key capture))
        ended?       (atom false)
        terminal-lock (Object.)
        terminal!
        (fn [operation success!]
          ;; jolt-http arbitrates duplicate callbacks inside `respond`. Keep
          ;; the provider's terminal decision serialized around that call so a
          ;; concurrent loser cannot end or mark the span before the accepted
          ;; callback completes. Losers still reach jolt-http, preserving its
          ;; return/error-logging behavior, but run outside the ended span.
          (locking terminal-lock
            (if @ended?
              (operation)
              (context/with-context span-context
                (try
                  (let [result (operation)]
                    (observe! success!)
                    result)
                  (catch :default error
                    (mark-error! span metric-attrs error)
                    (throw error))
                  (finally
                    (let [elapsed (- (host/mono-nanos) started)]
                      (reset! ended? true)
                      (observe! #(trace/end! span (+ start-wall elapsed)))
                      (observe! #(metrics/record! (duration-instrument)
                                                  (/ elapsed 1000000000.0)
                                                  @metric-attrs)))
                    (notify-end! opts)))))))]
    (fn [_request respond raise]
      (let [respond (fn [response async?]
                      (terminal! #(respond response async?) (fn [] nil)))
            raise   (fn [error]
                      (terminal! #(raise error)
                                 #(mark-error! span metric-attrs error)))]
        (context/with-context span-context
          (try
            (handler request respond raise)
            (catch :default error
              (terminal! #(throw error) (fn [] nil)))))))))

(defn- around-with-profile
  [capture _join-point
   [handler request socket done buffer read-buffer opts handled] proceed]
  (if (or (context/instrumentation-suppressed?)
          (excluded? opts request))
    (proceed)
    (proceed [(traced-handler handler request opts capture)
              request socket done buffer read-buffer opts handled])))

(defn around
  "Instrument the compiler's fixed-arity `invoke-handler` entry.

  `proceed` receives a replacement vector under `:replace-args-v1`. Only the
  normalized Ring handler is replaced; request/parser/socket identity and every
  application result or exception remain owned by jolt-http. Generic
  instrumentation suppression bypasses extraction and all telemetry work."
  [_join-point [handler request socket done buffer read-buffer opts handled] proceed]
  (around-with-profile basic-capture _join-point
                       [handler request socket done buffer read-buffer opts handled]
                       proceed))

(defn around-detailed
  "Detailed server advice with explicit bounded header and body-size capture."
  [join-point args proceed]
  (around-with-profile detailed-capture join-point args proceed))

(defn around-debug
  "Debug server advice with bounded textual body content capture in addition to
  the detailed profile. Selecting this provider is an explicit opt-in to
  potentially sensitive payload telemetry. Streaming request bodies remain
  untouched and therefore cannot be captured at this join point."
  [join-point args proceed]
  (around-with-profile debug-capture join-point args proceed))

(defn around-response
  "Observe the result of jolt-http response sanitization while the server span
  is current. The target's `[safe-response problems]` result is returned by
  identity; handler-supplied invalid metadata is never reported as if it went
  to the wire. Outside this provider's request-scoped marker the seam is inert,
  so an excluded viewer request cannot annotate an unrelated active span."
  [_join-point _evaluated-args proceed]
  (let [instrument? (and (not (context/instrumentation-suppressed?))
                         (active?))
        result (proceed)]
    (when instrument?
      (when-some [metric-attrs (context/get-value
                                (context/current)
                                metric-attributes-context-key)]
        (observe!
         #(mark-response! (trace/current-span) metric-attrs (first result)
                          (or (context/get-value (context/current)
                                                 capture-profile-context-key)
                              basic-capture)))))
    result))

(def basic-aspect-provider
  {:schema 1
   :libraries {'casselc/jolt-http http-build-id}
   :roles {:http/server {:fn 'otel.instrumentation.http-server/around
                         :contract :replace-args-v1}
           :http/server-response
           {:fn 'otel.instrumentation.http-server/around-response
            :contract :args-v1}}})

(def detailed-aspect-provider
  {:schema 1
   :libraries {'casselc/jolt-http http-build-id}
   :roles {:http/server
           {:fn 'otel.instrumentation.http-server/around-detailed
            :contract :replace-args-v1}
           :http/server-response
           {:fn 'otel.instrumentation.http-server/around-response
            :contract :args-v1}}})

(def debug-aspect-provider
  {:schema 1
   :libraries {'casselc/jolt-http http-build-id}
   :roles {:http/server
           {:fn 'otel.instrumentation.http-server/around-debug
            :contract :replace-args-v1}
           :http/server-response
           {:fn 'otel.instrumentation.http-server/around-response
            :contract :args-v1}}})

(def aspect-provider
  "Compatibility default. Equivalent to the basic preset/provider."
  basic-aspect-provider)

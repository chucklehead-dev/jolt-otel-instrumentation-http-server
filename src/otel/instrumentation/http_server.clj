(ns otel.instrumentation.http-server
  "Build-selected OpenTelemetry consumer for jolt-http's normalized Ring
  handler lifecycle.

  The compiler advice replaces only the normalized handler argument. It starts
  one server span before handler dispatch, keeps that span open when an async
  handler returns, restores its context when `respond` or `raise` later runs,
  and ends it after the first callback finishes. This is Ring response-callback
  completion, not proof that bytes reached the peer."
  (:require [clojure.string :as str]
            [otel.context :as context]
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

(def ^:private known-methods
  ;; Stable HTTP semantic-convention values plus QUERY, which is already in the
  ;; current registry as a development value.
  #{"CONNECT" "DELETE" "GET" "HEAD" "OPTIONS" "PATCH"
    "POST" "PUT" "QUERY" "TRACE"})

(defn- original-method [request]
  (let [method (:request-method request)]
    (cond
      (keyword? method) (str/upper-case (name method))
      (string? method)  (str/upper-case method)
      :else             nil)))

(defn- method-values [request]
  (let [original (original-method request)]
    (if (contains? known-methods original)
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

(defn- request-attributes [request method route capture-addresses?]
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
                      (:server-port request))]
    (cond-> {:http.request.method method}
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
    (assoc :client.address client-address))))

(defn- exception-type [error]
  (try
    (or (some-> error class .getName) "UnknownExceptionType")
    (catch :default _ "UnknownExceptionType")))

(defn- mark-error! [span error]
  (let [error-type (exception-type error)]
    (trace/set-attribute! span :error.type error-type)
    ;; Exception messages and ex-data routinely contain request bodies,
    ;; credentials, filesystem paths, and application data. Preserve only the
    ;; bounded type and escaped bit.
    (trace/add-event! span "exception"
                      {:exception.type error-type
                       :exception.escaped true})
    (trace/set-status! span :error)))

(defn- response-status [response]
  (let [status (:status response)]
    (when (integer? status) status)))

(defn- mark-response! [span response]
  (when-some [status (response-status response)]
    (trace/set-attribute! span :http.response.status_code status)
    (when (>= (long status) 500)
      (trace/set-attribute! span :error.type (str status))
      (trace/set-status! span :error))))

(defn- traced-handler [handler request opts]
  (let [{:keys [method span-name]} (method-values request)
        route        (resolved-route opts request)
        capture-addresses? (not (false? (get opts network-addresses-option)))
        span-name    (if route (str span-name " " route) span-name)
        parent       (propagation/extract-context (or (:headers request) {}))
        tracer       (sdk/tracer scope-name {:version instrumentation-version})
        span         (trace/start-span tracer span-name
                                       {:parent parent
                                        :kind :server
                                        :attributes
                                        (request-attributes request method route
                                                            capture-addresses?)})
        span-context (-> parent
                         (trace/context-with-span span)
                         (context/with-value active-context-key true))
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
                    (success!)
                    result)
                  (catch :default error
                    (mark-error! span error)
                    (throw error))
                  (finally
                    (reset! ended? true)
                    (trace/end! span)))))))]
    (fn [_request respond raise]
      (let [respond (fn [response async?]
                      (terminal! #(respond response async?) (fn [] nil)))
            raise   (fn [error]
                      (terminal! #(raise error) #(mark-error! span error)))]
        (context/with-context span-context
          (try
            (handler request respond raise)
            (catch :default error
              (terminal! #(throw error) (fn [] nil)))))))))

(defn around
  "Instrument the compiler's fixed-arity `invoke-handler` entry.

  `proceed` receives a replacement vector under `:replace-args-v1`. Only the
  normalized Ring handler is replaced; request/parser/socket identity and every
  application result or exception remain owned by jolt-http. Generic
  instrumentation suppression bypasses extraction and all telemetry work."
  [_join-point [handler request socket done buffer read-buffer opts handled] proceed]
  (if (or (context/instrumentation-suppressed?)
          (excluded? opts request))
    (proceed)
    (proceed [(traced-handler handler request opts)
              request socket done buffer read-buffer opts handled])))

(defn around-response
  "Observe the result of jolt-http response sanitization while the server span
  is current. The target's `[safe-response problems]` result is returned by
  identity; handler-supplied invalid metadata is never reported as if it went
  to the wire."
  [_join-point _evaluated-args proceed]
  (if (context/instrumentation-suppressed?)
    (proceed)
    (let [result (proceed)
          response (first result)]
      (mark-response! (trace/current-span) response)
      result)))

(def aspect-provider
  {:schema 1
   :libraries {'casselc/jolt-http http-build-id}
   :roles {:http/server {:fn 'otel.instrumentation.http-server/around
                         :contract :replace-args-v1}
           :http/server-response
           {:fn 'otel.instrumentation.http-server/around-response
            :contract :args-v1}}})

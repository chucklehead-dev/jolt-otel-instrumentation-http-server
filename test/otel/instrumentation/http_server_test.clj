(ns otel.instrumentation.http-server-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [otel.baggage :as baggage]
            [otel.context :as context]
            [otel.exporter.memory :as memory]
            [otel.instrumentation.http-server :as instrumentation]
            [otel.logs :as logs]
            [otel.metrics :as metrics]
            [otel.propagation :as propagation]
            [otel.sdk :as sdk]
            [otel.trace :as trace]))

(def ^:private remote-trace-id "0af7651916cd43dd8448eb211c80319c")
(def ^:private remote-span-id  "b7ad6b7169203331")

(defn- request
  ([] (request {}))
  ([extra]
   (merge {:request-method :get
           :scheme :http
           :uri "/orders/42"
           :protocol "HTTP/1.1"
           :server-name "example.test"
           :server-port 8080
           :remote-addr "192.0.2.10"
           :headers {"traceparent"
                     (str "00-" remote-trace-id "-" remote-span-id "-01")}}
          extra)))

(defn- join-point []
  {:id :http/server-ring-handler
   :advice-role :http/server
   :contract :replace-args-v1
   :match {:entry 'jolt.http.protocol/invoke-handler :arity 8}
   :library {:id 'casselc/jolt-http :version instrumentation/http-build-id}})

(defn- response-join-point []
  {:id :http/server-sanitized-response
   :advice-role :http/server-response
   :contract :args-v1
   :match {:entry 'jolt.http.protocol/sanitize-response :arity 1}
   :library {:id 'casselc/jolt-http :version instrumentation/http-build-id}})

(defn- observe-safe-response! [response]
  (instrumentation/around-response
   (response-join-point) [response] (fn [] [response nil])))

(defn- with-memory-sdk [f]
  (let [exporter (memory/multisignal-exporter)
        handle (sdk/init! {:service-name "http-server-instrumentation-test"
                           :exporter exporter
                           :processor :simple
                           :runtime-metrics? false
                           :logs? true
                           :bridge-logging? false})]
    (try
      (f exporter)
      (finally (sdk/shutdown! handle)))))

(defn- apply-profile-advice
  [advice handler request respond raise opts]
  (let [args [handler request :socket :done :buffer :read-buffer opts :handled]
        invoke (fn [xs]
                 ((nth xs 0) (nth xs 1) respond raise))]
    (advice
     (join-point) args
     (fn
       ([] (invoke args))
       ([replacement] (invoke replacement))))))

(defn- apply-advice
  ([handler request respond raise]
   (apply-advice handler request respond raise {}))
  ([handler request respond raise opts]
   (apply-profile-advice instrumentation/around
                         handler request respond raise opts)))

(deftest synchronous-response-preserves-result-and-remote-parent
  (with-memory-sdk
    (fn [exporter]
      (let [response {:status 201 :headers {} :body "private response"}
            callback-result (Object.)
            baggage-value (atom nil)
            active-values (atom [])
            completed-span-count (atom nil)
            observed (apply-advice
                      (fn [_ respond _]
                        (swap! active-values conj (instrumentation/active?))
                        (reset! baggage-value
                                (baggage/get-value (baggage/current) "tenant"))
                        (respond response false))
                      (request {:headers
                                {"traceparent"
                                 (str "00-" remote-trace-id "-"
                                      remote-span-id "-01")
                                 "baggage" "tenant=blue"}})
                      (fn [actual async?]
                        (swap! active-values conj (instrumentation/active?))
                        (is (identical? response actual))
                        (is (false? async?))
                        (observe-safe-response! actual)
                        callback-result)
                      (fn [_] (throw (ex-info "unexpected raise" {})))
                      {instrumentation/route-option
                       (fn [_] "/orders/:order-id")
                       instrumentation/on-end-option
                       (fn []
                         (swap! active-values conj (instrumentation/active?))
                         (reset! completed-span-count
                                 (count (memory/spans exporter))))})
            [span] (memory/spans exporter)
            attrs (:attributes span)]
        (is (identical? callback-result observed))
        (is (= :server (:kind span)))
        (is (= "GET /orders/:order-id" (:name span)))
        (is (= remote-trace-id (get-in span [:span-context :trace-id])))
        (is (= remote-span-id (:parent-span-id span)))
        (is (= "blue" @baggage-value))
        (is (= [true true true] @active-values)
            "the marker covers dispatch, response, and post-end completion")
        (is (= 1 @completed-span-count)
            "the completion hook observes the already-ended server span")
        (is (false? (instrumentation/active?))
            "the marker does not leak outside the request")
        (is (= "GET" (get attrs "http.request.method")))
        (is (= "/orders/:order-id" (get attrs "http.route")))
        (is (= "/orders/42" (get attrs "url.path")))
        (is (= "1.1" (get attrs "network.protocol.version")))
        (is (= "192.0.2.10" (get attrs "network.peer.address")))
        (is (= 201 (get attrs "http.response.status_code")))
        (is (not (.contains (pr-str span) "private response")))
        (is (not (.contains (pr-str span) "tenant=blue")))))))

(deftest build-selected-capture-profiles-are-bounded-and-explicit
  (with-memory-sdk
    (fn [exporter]
      (let [req (request
                 {:headers {"content-type" "application/json"
                            "content-length" "20"
                            "user-agent" "fixture-agent/1"
                            "x-request-id" "request-42"
                            "authorization" "Bearer request-secret"
                            "cookie" "session=request-secret"}
                  :body "{\"request\":\"body\"}"})
            response {:status 200
                      :headers {"Content-Type" "application/json"
                                "Content-Length" "23"
                                "X-Request-Id" "response-42"
                                "Set-Cookie" "session=response-secret"}
                      :body "{\"response\":\"details\"}"}
            run! (fn [advice]
                   (apply-profile-advice
                    advice
                    (fn [_ respond _] (respond response false))
                    req
                    (fn [safe _] (observe-safe-response! safe))
                    (fn [_] nil)
                    {}))]
        (run! instrumentation/around)
        (run! instrumentation/around-detailed)
        (run! instrumentation/around-debug)
        (let [[basic detailed debug] (memory/spans exporter)
              basic-attrs (:attributes basic)
              detailed-attrs (:attributes detailed)
              debug-attrs (:attributes debug)]
          (is (nil? (get basic-attrs "http.request.header.content-type")))
          (is (nil? (get basic-attrs "http.request.body.size")))
          (is (= ["application/json"]
                 (get detailed-attrs "http.request.header.content-type")))
          (is (= ["request-42"]
                 (get detailed-attrs "http.request.header.x-request-id")))
          (is (= ["application/json"]
                 (get detailed-attrs "http.response.header.content-type")))
          (is (= 20 (get detailed-attrs "http.request.body.size")))
          (is (= 23 (get detailed-attrs "http.response.body.size")))
          (is (= "fixture-agent/1"
                 (get detailed-attrs "user_agent.original")))
          (is (nil? (get detailed-attrs "http.request.body.content")))
          (is (nil? (get detailed-attrs "http.response.body.content")))
          (is (= "{\"request\":\"body\"}"
                 (get debug-attrs "http.request.body.content")))
          (is (= "{\"response\":\"details\"}"
                 (get debug-attrs "http.response.body.content")))
          (doseq [secret ["request-secret" "response-secret"]]
            (is (not (.contains (pr-str [basic detailed debug]) secret)))))))))

(deftest debug-body-capture-is-text-only-and-truncated
  (with-memory-sdk
    (fn [exporter]
      (let [large (apply str (repeat 5000 "x"))
            run! (fn [content-type body]
                   (apply-profile-advice
                    instrumentation/around-debug
                    (fn [_ respond _]
                      (respond {:status 200
                                :headers {"content-type" content-type}
                                :body body}
                               false))
                    (request {:headers {}})
                    (fn [safe _] (observe-safe-response! safe))
                    (fn [_] nil)
                    {}))]
        (run! "text/plain; charset=UTF-8" large)
        (run! "application/octet-stream" "binary-secret")
        (let [[text binary] (memory/spans exporter)]
          (is (= 4096
                 (count (get (:attributes text)
                             "http.response.body.content"))))
          (is (nil? (get (:attributes binary)
                         "http.response.body.content")))
          (is (not (.contains (pr-str binary) "binary-secret"))))))))

(deftest completion-hook-failure-cannot-change-the-http-result
  (with-memory-sdk
    (fn [exporter]
      (let [callback-result (Object.)
            observed
            (apply-advice
             (fn [_ respond _]
               (respond {:status 204 :headers {} :body nil} false))
             (request {:headers {}})
             (fn [response _]
               (observe-safe-response! response)
               callback-result)
             (fn [_] (throw (ex-info "unexpected raise" {})))
             {instrumentation/on-end-option
              (fn [] (throw (ex-info "observer failed" {})))})]
        (is (identical? callback-result observed))
        (is (= 1 (count (memory/spans exporter))))))))

(deftest configured-propagator-controls-inbound-context
  (with-memory-sdk
    (fn [exporter]
      (let [baggage-values (atom [])
            respond (fn [response _]
                      (observe-safe-response! response))
            handler (fn [_ respond _]
                      (swap! baggage-values conj
                             (baggage/get-value (baggage/current) "tenant"))
                      (respond {:status 200 :headers {} :body nil} false))]
        (apply-advice
         handler
         (request {:headers {"traceparent"
                             (str "00-" remote-trace-id "-"
                                  remote-span-id "-01")
                             "tracestate" "vendor=one"
                             "baggage" "tenant=private"}})
         respond
         (fn [_] nil)
         {instrumentation/propagator-option propagation/trace-context})
        (apply-advice
         handler
         (request {:headers {"traceparent"
                             (str "00-" remote-trace-id "-"
                                  remote-span-id "-01")
                             "baggage" "tenant=private"}})
         respond
         (fn [_] nil)
         {instrumentation/propagator-option :not-a-propagator})
        (let [[configured invalid] (memory/spans exporter)]
          (is (= [nil nil] @baggage-values)
              "trace-only and invalid configurations do not inherit baggage")
          (is (= remote-trace-id
                 (get-in configured [:span-context :trace-id])))
          (is (= remote-span-id (:parent-span-id configured)))
          (is (= [["vendor" "one"]]
                 (get-in configured [:span-context :trace-state])))
          (is (nil? (:parent-span-id invalid)))
          (is (not= remote-trace-id
                    (get-in invalid [:span-context :trace-id]))))))))

(deftest throwing-propagator-fails-closed-without-breaking-the-request
  (with-memory-sdk
    (fn [exporter]
      (let [throwing
            (reify propagation/TextMapPropagator
              (fields [_] [])
              (inject [_ _ carrier] carrier)
              (extract [_ _ _]
                (throw (ex-info "untrusted propagator failed" {}))))
            callback-result (Object.)
            observed
            (apply-advice
             (fn [_ respond _]
               (respond {:status 204 :headers {} :body nil} false))
             (request)
             (fn [response _]
               (observe-safe-response! response)
               callback-result)
             (fn [_] nil)
             {instrumentation/propagator-option throwing})
            [span] (memory/spans exporter)]
        (is (identical? callback-result observed))
        (is (nil? (:parent-span-id span)))
        (is (not= remote-trace-id
                  (get-in span [:span-context :trace-id])))))))

(deftest async-return-does-not-end-span-and-callback-restores-parent
  (with-memory-sdk
    (fn [exporter]
      (let [callbacks (promise)
            returned (Object.)
            response-result (Object.)
            observed (apply-advice
                      (fn [_ respond raise]
                        (deliver callbacks {:respond respond :raise raise})
                        returned)
                      (request {:headers {}})
                      (fn [response _]
                        (observe-safe-response! response)
                        response-result)
                      (fn [_] nil))]
        (is (identical? returned observed))
        (is (empty? (memory/spans exporter))
            "an async handler return is not Ring response completion")
        (let [respond (:respond @callbacks)
              callback-observed
              (respond {:status 202 :headers {} :body nil} true)]
          (is (identical? response-result callback-observed)))
        (let [child-tracer (sdk/tracer "async-child")]
          ;; A second call is ignored by jolt-http in production and must not
          ;; end or mutate the already-ended server span again.
          ((:respond @callbacks) {:status 503 :headers {} :body nil} true)
          (trace/with-span [_ child-tracer "outside-after-completion"]))
        (let [spans (memory/spans exporter)
              server (first (filter #(= "GET" (:name %)) spans))]
          (is (= 1 (count (filter #(= "GET" (:name %)) spans))))
          (is (= 202 (get (:attributes server) "http.response.status_code")))
          (is (= :unset (get-in server [:status :code]))))))))

(deftest child-work-inside-async-response-is-parented-to-server-span
  (with-memory-sdk
    (fn [exporter]
      (let [callback (promise)
            child-tracer (sdk/tracer "async-child")]
        (apply-advice
         (fn [_ respond _] (deliver callback respond) :returned)
         (request {:headers {}})
         (fn [response _]
           (observe-safe-response! response)
           (trace/with-span [_ child-tracer "serialize-response"])
           :done)
         (fn [_] nil))
        (@callback {:status 200 :headers {} :body nil} true)
        (let [spans (memory/spans exporter)
              server (first (filter #(= "GET" (:name %)) spans))
              child (first (filter #(= "serialize-response" (:name %)) spans))]
          (is (= (get-in server [:span-context :span-id])
                 (:parent-span-id child))))))))

(deftest concurrent-duplicate-callback-cannot-finish-the-accepted-response
  (with-memory-sdk
    (fn [exporter]
      (let [callbacks (promise)
            accepted-entered (promise)
            release-accepted (promise)
            handled? (atom false)]
        (apply-advice
         (fn [_ respond raise]
           (deliver callbacks {:respond respond :raise raise})
           :returned)
         (request {:headers {}})
         (fn [response _]
           (if (compare-and-set! handled? false true)
             (do
               (deliver accepted-entered true)
               @release-accepted
               (observe-safe-response! response)
               :accepted)
             :duplicate))
         (fn [_] :duplicate-raise))
        (let [accepted (future ((:respond @callbacks)
                                {:status 202 :headers {} :body nil} true))]
          @accepted-entered
          (let [duplicate (future ((:respond @callbacks)
                                   {:status 503 :headers {} :body nil} true))]
            (is (= ::still-running (deref duplicate 25 ::still-running)))
            (is (empty? (memory/spans exporter)))
            (deliver release-accepted true)
            (is (= :accepted (deref accepted 1000 ::timeout)))
            (is (= :duplicate (deref duplicate 1000 ::timeout)))))
        (let [[span] (memory/spans exporter)]
          (is (= 202 (get (:attributes span) "http.response.status_code")))
          (is (= :unset (get-in span [:status :code]))))))))

(deftest raise-and-thrown-handler-errors-end-once-with-original-identity
  (with-memory-sdk
    (fn [exporter]
      (let [raised (ex-info "private raised message" {:secret true})
            thrown (ex-info "private thrown message" {:secret true})
            observed-raise (apply-advice
                            (fn [_ _ raise] (raise raised))
                            (request {:headers {}})
                            (fn [_ _] nil)
                            (fn [error] error))
            observed-throw (try
                             (apply-advice
                              (fn [_ _ _] (throw thrown))
                              (request {:headers {}})
                              (fn [_ _] nil)
                              (fn [_] nil))
                             (catch :default error error))
            spans (memory/spans exporter)]
        (is (identical? raised observed-raise))
        (is (identical? thrown observed-throw))
        (is (= 2 (count spans)))
        (is (every? #(= :error (get-in % [:status :code])) spans))
        (is (every? #(some? (get (:attributes %) "error.type")) spans))
        (let [events (memory/records exporter)]
          (is (= 2 (count events)))
          (is (every? #(= "http.server.request.exception" (:event-name %))
                      events))
          (is (every? #(= 17 (:severity-number %)) events))
          (is (every? #(some? (get (:attributes %) "exception.type"))
                      events))
          (is (every? (fn [[span event]]
                        (and (= (get-in span [:span-context :trace-id])
                                (:trace-id event))
                             (= (get-in span [:span-context :span-id])
                                (:span-id event))))
                      (map vector spans events))
              "each exception log is correlated to its server span"))
        (doseq [secret ["private raised message" "private thrown message"]]
          (is (not (.contains (pr-str [spans (memory/records exporter)])
                              secret))))))))

(deftest exception-log-failure-preserves-the-original-thrown-object
  (with-memory-sdk
    (fn [exporter]
      (let [thrown (ex-info "application failure" {:owner :application})
            log-failure (ex-info "log exporter failure" {:owner :telemetry})
            observed
            (with-redefs [logs/emit! (fn [& _] (throw log-failure))]
              (try
                (apply-advice
                 (fn [_ _ _] (throw thrown))
                 (request {:headers {}})
                 (fn [_ _] nil)
                 (fn [_] nil))
                (catch :default error error)))
            [span] (memory/spans exporter)]
        (is (identical? thrown observed))
        (is (= :error (get-in span [:status :code])))
        (is (some? (get (:attributes span) "error.type")))))))

(deftest span-end-failure-preserves-application-control-and-attempts-the-metric
  (with-memory-sdk
    (fn [exporter]
      (let [callback-result (Object.)
            thrown (ex-info "application failure" {:owner :application})
            end-failure (ex-info "span processor failure" {})
            metric-calls (atom 0)
            record! metrics/record!
            [observed-result observed-throw]
            (with-redefs [trace/end! (fn [& _] (throw end-failure))
                          metrics/record! (fn [& args]
                                            (swap! metric-calls inc)
                                            (apply record! args))]
              [(apply-advice
                (fn [_ respond _]
                  (respond {:status 204 :headers {} :body nil} false))
                (request {:headers {}})
                (fn [response _]
                  (observe-safe-response! response)
                  callback-result)
                (fn [_] nil))
               (try
                 (apply-advice
                  (fn [_ _ _] (throw thrown))
                  (request {:headers {}})
                  (fn [_ _] nil)
                  (fn [_] nil))
                 (catch :default error error))])]
        (is (identical? callback-result observed-result))
        (is (identical? thrown observed-throw))
        (is (= 2 @metric-calls))
        (is (empty? (memory/spans exporter)))))))

(deftest duration-metric-failure-preserves-application-control-and-ended-spans
  (with-memory-sdk
    (fn [exporter]
      (let [callback-result (Object.)
            thrown (ex-info "application failure" {:owner :application})
            metric-failure (ex-info "metric exporter failure" {})
            [observed-result observed-throw]
            (with-redefs [metrics/record! (fn [& _] (throw metric-failure))]
              [(apply-advice
                (fn [_ respond _]
                  (respond {:status 202 :headers {} :body nil} false))
                (request {:headers {}})
                (fn [response _]
                  (observe-safe-response! response)
                  callback-result)
                (fn [_] nil))
               (try
                 (apply-advice
                  (fn [_ _ _] (throw thrown))
                  (request {:headers {}})
                  (fn [_ _] nil)
                  (fn [_] nil))
                 (catch :default error error))])]
        (is (identical? callback-result observed-result))
        (is (identical? thrown observed-throw))
        (let [[success-span error-span] (memory/spans exporter)]
          (is (= 202 (get (:attributes success-span)
                          "http.response.status_code")))
          (is (= :error (get-in error-span [:status :code]))))))))

(deftest response-annotation-failure-preserves-result-and-terminal-observations
  (with-memory-sdk
    (fn [exporter]
      (let [response {:status 206 :headers {} :body nil}
            sanitized-result [response nil]
            callback-result (Object.)
            annotation-failure (ex-info "response annotation failure" {})
            observed-sanitized (atom nil)
            end-calls (atom 0)
            metric-calls (atom 0)
            end! trace/end!
            record! metrics/record!
            observed-result
            (with-redefs
              [trace/set-attributes! (fn [& _] (throw annotation-failure))
               trace/end! (fn [& args]
                            (swap! end-calls inc)
                            (apply end! args))
               metrics/record! (fn [& args]
                                  (swap! metric-calls inc)
                                  (apply record! args))]
              (apply-advice
               (fn [_ respond _] (respond response false))
               (request {:headers {}})
               (fn [_ _]
                 (reset!
                  observed-sanitized
                  (instrumentation/around-response
                   (response-join-point) [response]
                   (fn [] sanitized-result)))
                 callback-result)
               (fn [_] nil)))]
        (is (identical? callback-result observed-result))
        (is (identical? sanitized-result @observed-sanitized))
        (is (= 1 @end-calls))
        (is (= 1 @metric-calls))
        (is (= 1 (count (memory/spans exporter))))))))

(deftest exception-error-type-agrees-across-span-log-and-duration-metric
  (let [exporter (memory/multisignal-exporter)
        handle (sdk/init! {:service-name "http-server-error-signals-test"
                           :exporter exporter :processor :simple
                           :runtime-metrics? false :logs? true
                           :bridge-logging? false})]
    (try
      (let [raised (ex-info "private metric error" {:secret true})]
        (is (identical?
             raised
             (apply-advice
              (fn [_ _ raise] (raise raised))
              (request {:headers {}})
              (fn [_ _] nil)
              (fn [error] error))))
        (is (sdk/force-flush! handle))
        (let [[span] (memory/spans exporter)
              [event] (memory/records exporter)
              metric (first
                      (filter #(= "http.server.request.duration" (:name %))
                              (memory/metrics exporter)))
              point (first (:data-points metric))
              error-type (get (:attributes span) "error.type")]
          (is (some? error-type))
          (is (= error-type (get (:attributes event) "exception.type")))
          (is (= error-type (get (:attributes point) "error.type")))
          (is (= (get-in span [:span-context :trace-id]) (:trace-id event)))
          (is (= (get-in span [:span-context :span-id]) (:span-id event)))
          (is (not (.contains (pr-str [span event point])
                              "private metric error")))))
      (finally
        (sdk/shutdown! handle)))))

(deftest duration-metric-uses-stable-name-unit-buckets-and-attributes
  (let [exporter (memory/multisignal-exporter)
        handle (sdk/init! {:service-name "http-server-metric-test"
                           :exporter exporter :processor :simple
                           :runtime-metrics? false :logs? true
                           :bridge-logging? false})]
    (try
      (apply-advice
       (fn [_ respond _] (respond {:status 201 :headers {} :body nil} false))
       (request {:headers {}})
       (fn [response _] (observe-safe-response! response))
       (fn [_] nil)
       {instrumentation/route-option (fn [_] "/orders/:id")})
      (is (sdk/force-flush! handle))
      (let [metric (first (filter #(= "http.server.request.duration" (:name %))
                                  (memory/metrics exporter)))
            point (first (:data-points metric))]
        (is (= "s" (:unit metric)))
        (is (= [0.005 0.01 0.025 0.05 0.075 0.1 0.25 0.5 0.75
                1.0 2.5 5.0 7.5 10.0]
               (:explicit-bounds metric)))
        (is (= "GET" (get (:attributes point) "http.request.method")))
        (is (= "http" (get (:attributes point) "url.scheme")))
        (is (= "/orders/:id" (get (:attributes point) "http.route")))
        (is (= "1.1" (get (:attributes point) "network.protocol.version")))
        (is (= 201 (get (:attributes point) "http.response.status_code")))
        (is (nil? (get (:attributes point) "server.address"))
            "opt-in server identity is not a default metric dimension")
        (is (nil? (get (:attributes point) "server.port"))
            "opt-in server identity is not a default metric dimension")
        (is (not (neg? (:sum point)))))
      (finally
        (sdk/shutdown! handle)))))

(deftest response-observer-reports-the-sanitized-wire-status
  (with-memory-sdk
    (fn [exporter]
      (let [handler-response {:status 99 :headers {"bad\nname" "secret"}}
            safe-response {:status 500 :headers {} :body "Internal Server Error"}]
        (apply-advice
         (fn [_ respond _] (respond handler-response false))
         (request {:headers {}})
         (fn [_ _]
           (let [result (instrumentation/around-response
                         (response-join-point) [handler-response]
                         (fn [] [safe-response [{:kind :invalid-status}]]))]
             (is (identical? safe-response (first result)))))
         (fn [_] nil))
        (let [[span] (memory/spans exporter)]
          (is (= 500 (get (:attributes span) "http.response.status_code")))
          (is (= "500" (get (:attributes span) "error.type")))
          (is (= :error (get-in span [:status :code])))
          (is (nil? (get-in span [:status :description])))
          (is (not (.contains (pr-str span) "bad")))
          (is (not (.contains (pr-str span) "secret"))))))))

(deftest unknown-method-uses-other-and-http-span-name
  (with-memory-sdk
    (fn [exporter]
      (apply-advice
       (fn [_ respond _] (respond {:status 200 :headers {} :body nil} false))
       (request {:request-method :brew :headers {}})
       (fn [response _] (observe-safe-response! response))
       (fn [_] nil))
      (let [[span] (memory/spans exporter)]
        (is (= "HTTP" (:name span)))
        (is (= "_OTHER" (get (:attributes span) "http.request.method")))
        (is (nil? (get (:attributes span) "http.request.method_original")))))))

(deftest known-method-setting-is-a-case-sensitive-full-override
  (is (contains? (instrumentation/parse-known-methods nil) "GET"))
  (is (= #{} (instrumentation/parse-known-methods "")))
  (is (= #{"GET" "PROPFIND" "get"}
         (instrumentation/parse-known-methods "GET, PROPFIND, get")))
  (is (= #{"GET"}
         (instrumentation/parse-known-methods
          "GET, bad method, TOO_LONG_TO_BE_A_BOUNDED_METHOD_NAME"))))

(deftest suppression-bypasses-extraction-and-handler-replacement
  (with-memory-sdk
    (fn [exporter]
      (let [handler (fn [_ _ _] :plain)
            replacement (atom nil)
            args [handler (request) :socket :done :buffer :read-buffer :opts :handled]
            observed
            (context/with-instrumentation-suppressed
              (instrumentation/around
               (join-point) args
               (fn
                 ([] :plain)
                 ([xs] (reset! replacement xs) :replaced))))]
        (is (= :plain observed))
        (is (nil? @replacement))
        (is (empty? (memory/spans exporter)))))))

(deftest response-observer-is-inert-outside-this-provider-request
  (with-memory-sdk
    (fn [exporter]
      (let [tracer (sdk/tracer "unrelated-owner")]
        (trace/with-span [_ tracer "unrelated-operation"]
          (let [response {:status 503 :headers {} :body nil}
                result (instrumentation/around-response
                        (response-join-point) [response]
                        (fn [] [response nil]))]
            (is (identical? response (first result)))))
        (let [[span] (memory/spans exporter)]
          (is (= "unrelated-operation" (:name span)))
          (is (nil? (get (:attributes span) "http.response.status_code")))
          (is (nil? (get (:attributes span) "error.type")))
          (is (= :unset (get-in span [:status :code]))))))))

(deftest request-exclusion-bypasses-receiver-and-viewer-routes
  (with-memory-sdk
    (fn [exporter]
      (let [handler (fn [_ _ _] :plain)
            excluded? (atom [])
            request (request {:uri "/internal/telemetry/live"})
            opts {instrumentation/exclusion-option
                  (fn [req]
                    (swap! excluded? conj (:uri req))
                    true)}
            args [handler request :socket :done :buffer :read-buffer opts :handled]
            replacement (atom nil)
            observed (instrumentation/around
                      (join-point) args
                      (fn
                        ([] :plain)
                        ([xs] (reset! replacement xs) :replaced)))]
        (is (= :plain observed))
        (is (= ["/internal/telemetry/live"] @excluded?))
        (is (nil? @replacement))
        (is (empty? (memory/spans exporter)))))))

(deftest request-exclusion-fails-closed-without-breaking-the-request
  (with-memory-sdk
    (fn [exporter]
      (let [handler (fn [_ _ _] :plain)
            request (request {:uri "/v1/traces"})
            opts {instrumentation/exclusion-option
                  (fn [_] (throw (ex-info "broken guard" {})))}
            args [handler request :socket :done :buffer :read-buffer opts :handled]
            observed (instrumentation/around
                      (join-point) args
                      (fn
                        ([] :plain)
                        ([_] :unexpected-replacement)))]
        (is (= :plain observed))
        (is (empty? (memory/spans exporter)))))))

(deftest route-resolver-is-bounded-and-fails-soft
  (with-memory-sdk
    (fn [exporter]
      (doseq [resolver [(fn [_] (str "/" (apply str (repeat 300 "x"))))
                        (fn [_] "/orders?customer=secret")
                        (fn [_] (throw (ex-info "bad route" {})))]]
        (apply-advice
         (fn [_ respond _] (respond {:status 200 :headers {} :body nil} false))
         (request {:headers {}})
         (fn [response _] (observe-safe-response! response))
         (fn [_] nil)
         {instrumentation/route-option resolver}))
      (let [spans (memory/spans exporter)]
        (is (= 3 (count spans)))
        (is (every? #(= "GET" (:name %)) spans))
        (is (every? #(nil? (get (:attributes %) "http.route")) spans))))))

(deftest request-attributes-are-bounded-and-address-capture-is-configurable
  (with-memory-sdk
    (fn [exporter]
      (apply-advice
       (fn [_ respond _] (respond {:status 200 :headers {} :body nil} false))
       (request {:uri (str "/" (apply str (repeat 3000 "x")))
                 :server-name "private-host"
                 :remote-addr "192.0.2.44"
                 :headers {}})
       (fn [response _] (observe-safe-response! response))
       (fn [_] nil)
       {instrumentation/network-addresses-option false})
      (let [[span] (memory/spans exporter)
            attrs (:attributes span)]
        (is (nil? (get attrs "url.path")))
        (is (nil? (get attrs "server.address")))
        (is (nil? (get attrs "server.port")))
        (is (nil? (get attrs "client.address")))
        (is (nil? (get attrs "network.peer.address")))
        (is (not (.contains (pr-str span) "private-host")))))))

(deftest provider-contract-matches-the-fetched-library-manifest
  (let [resource (io/resource "META-INF/jolt/aspects/http-server.edn")
        manifest (some-> resource slurp edn/read-string)]
    (is (some? resource))
    (is (= {:schema 1
            :libraries {'casselc/jolt-http instrumentation/http-build-id}
            :roles {:http/server
                    {:fn 'otel.instrumentation.http-server/around
                     :contract :replace-args-v1}
                    :http/server-response
                    {:fn 'otel.instrumentation.http-server/around-response
                     :contract :args-v1}}}
           instrumentation/aspect-provider))
    (is (= 'casselc/jolt-http (get-in manifest [:library :id])))
    (is (= instrumentation/http-build-id
           (get-in manifest [:library :version])))
    (is (= {:entry 'jolt.http.protocol/invoke-handler :arity 8}
           (get-in manifest [:aspects 0 :match])))
    (is (= {:entry 'jolt.http.protocol/sanitize-response :arity 1}
           (get-in manifest [:aspects 1 :match])))))

(deftest package-owned-presets-select-the-versioned-provider-variants
  (doseq [[profile provider]
          [["basic" 'otel.instrumentation.http-server/basic-aspect-provider]
           ["detailed" 'otel.instrumentation.http-server/detailed-aspect-provider]
           ["debug" 'otel.instrumentation.http-server/debug-aspect-provider]]]
    (let [resource-name
          (str "META-INF/jolt/instrumentation/http-server/" profile ".edn")
          resource (io/resource resource-name)
          preset (some-> resource slurp edn/read-string)]
      (is (some? resource))
      (is (= 1 (:schema preset)))
      (is (= (keyword "otel.http-server" profile) (:id preset)))
      (is (= "META-INF/jolt/aspects/http-server.edn"
             (get-in preset [:selections 0 :resource])))
      (is (= provider (get-in preset [:selections 0 :provider]))))))

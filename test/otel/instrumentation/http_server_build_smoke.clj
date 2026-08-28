(ns otel.instrumentation.http-server-build-smoke
  (:require [jolt.http.server :as http]
            [otel.baggage :as baggage]
            [otel.exporter.memory :as memory]
            [otel.propagation :as propagation]
            [otel.sdk :as sdk]
            [otel.trace :as trace]
            [teensyp.ffi-net :as net]))

(def ^:private remote-trace-id "0af7651916cd43dd8448eb211c80319c")
(def ^:private remote-span-id "b7ad6b7169203331")

(defn- utf8 [s] (.getBytes ^String s "UTF-8"))

(defn- ensure! [pred message data]
  (when-not pred (throw (ex-info message data))))

(defn- recv-all [fd]
  (loop [chunks []]
    (if-let [chunk (net/client-recv fd 8192)]
      (recur (conj chunks (String. ^bytes chunk "UTF-8")))
      (apply str chunks))))

(defn -main [& args]
  (let [plain?   (= "plain" (first args))
        entered  (promise)
        release  (promise)
        exporter (memory/exporter)
        sdk-handle (sdk/init! {:service-name "http-server-weave-smoke"
                               :exporter exporter
                               :processor :simple
                               :metrics? false})
        child-tracer (sdk/tracer "http-server-weave-child")
        observed-baggage (promise)
        handler (fn [_request respond _raise]
                  (deliver observed-baggage
                           (baggage/get-value (baggage/current) "tenant"))
                  (deliver entered true)
                  (future
                    @release
                    (trace/with-span [_ child-tracer "async-work"])
                    (respond {:status 202 :headers {} :body "woven"}))
                  :returned)
        server (http/run-server
                handler :async? true :port 0 :reuse-address? true
                :otel.instrumentation.http-server/propagator
                propagation/trace-context)
        fd (net/connect-loopback (:port server))]
    (try
      (net/client-send-all
       fd
       (utf8 (str "GET /woven HTTP/1.1\r\n"
                  "Host: localhost\r\n"
                  "traceparent: 00-" remote-trace-id "-" remote-span-id "-01\r\n"
                  "tracestate: vendor=one\r\n"
                  "baggage: tenant=private\r\n"
                  "Connection: close\r\n\r\n")))
      @entered
      (ensure! (empty? (memory/spans exporter))
               "async handler return ended the woven server span" {})
      (deliver release true)
      (let [wire (recv-all fd)
            spans (memory/spans exporter)
            server-span (first (filter #(= "GET" (:name %)) spans))
            child-span (first (filter #(= "async-work" (:name %)) spans))]
        (ensure! (.contains wire "HTTP/1.1 202")
                 "server returned the wrong response" {:wire wire})
        (if plain?
          (do
            (ensure! (= 1 (count spans))
                     "plain build emitted generated server telemetry"
                     {:spans spans})
            (ensure! (nil? server-span)
                     "plain build selected the inert manifest" {:spans spans})
            (ensure! (nil? (:parent-span-id child-span))
                     "plain child acquired a synthetic server parent"
                     {:child child-span})
            (ensure! (not= remote-trace-id
                           (get-in child-span [:span-context :trace-id]))
                     "plain build extracted tracecontext without selection"
                     {:child child-span})
            (println "OK: plain HTTP server manifest remains inert"))
          (do
            (ensure! (= 2 (count spans))
                     "woven lifecycle did not export exactly server and child spans"
                     {:spans spans})
            (ensure! (= remote-trace-id
                        (get-in server-span [:span-context :trace-id]))
                     "woven server span did not extract tracecontext"
                     {:span server-span})
            (ensure! (= remote-span-id (:parent-span-id server-span))
                     "woven server span has the wrong remote parent"
                     {:span server-span})
            (ensure! (= [["vendor" "one"]]
                        (get-in server-span [:span-context :trace-state]))
                     "woven server span lost ordered tracestate"
                     {:span server-span})
            (ensure! (nil? @observed-baggage)
                     "trace-only server configuration inherited baggage"
                     {:baggage @observed-baggage})
            (ensure! (= (get-in server-span [:span-context :span-id])
                        (:parent-span-id child-span))
                     "async child was not parented to the woven server span"
                     {:server server-span :child child-span})
            (ensure! (= 202 (get (:attributes server-span)
                                 "http.response.status_code"))
                     "woven server span did not observe response completion"
                     {:span server-span})
            (println "OK: woven asynchronous HTTP server lifecycle"))))
      (finally
        (net/close! fd)
        (http/stop-server server)
        (sdk/shutdown! sdk-handle)))))

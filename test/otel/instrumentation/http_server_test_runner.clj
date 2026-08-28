(ns otel.instrumentation.http-server-test-runner
  (:require [clojure.test :as test]
            [otel.instrumentation.http-server-test]))

(defn -main [& _]
  (let [result (test/run-tests 'otel.instrumentation.http-server-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (System/exit 1))))

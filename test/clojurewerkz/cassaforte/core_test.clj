(ns clojurewerkz.cassaforte.core-test
  (require [clojurewerkz.cassaforte.core :as cc])
  (:use clojure.test))

(def ^String keyspace "CassaforteTest1")


(deftest test-connection-with-explicitly-specified-host
  (let [host (or (System/getenv "CASSANDRA_HOST") "127.0.0.1")
        port cc/default-port]
    (println (cc/connect host port keyspace))))

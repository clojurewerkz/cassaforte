(ns clojurewerkz.cassaforte.core-test
  (require [clojurewerkz.cassaforte.core :as cc])
  (:use clojure.test)
  (:import clojurewerkz.cassaforte.CassandraClient))

(def ^String keyspace "CassaforteTest1")


(deftest test-connection-with-explicitly-specified-host
  (let [host   (or (System/getenv "CASSANDRA_HOST") "127.0.0.1")
        port   cc/default-port
        ^CassandraClient client (cc/connect host port keyspace)]
    (is (= host (.getHost client)))
    (is (= keyspace (.getKeyspace client)))))

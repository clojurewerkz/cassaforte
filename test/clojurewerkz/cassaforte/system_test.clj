(ns clojurewerkz.cassaforte.system-test
  (require [clojurewerkz.cassaforte.core :as cc]
           [clojurewerkz.cassaforte.system :as sys])
  (:use clojure.test
        clojurewerkz.cassaforte.conversion))

(cc/connect! "127.0.0.1" "CassaforteTest1")


(deftest test-describe-keyspace
  (let [keyspace "CassaforteTest1"
        ks-def   (sys/describe-keyspace keyspace)]
    (is (= (.getName ks-def) keyspace))))


(deftest test-add-and-drop-keyspace
  (let [keyspace       "CassaforteTest2"
        strategy-class "org.apache.cassandra.locator.SimpleStrategy"
        strategy-opts  {:replication_factor "1"}
        cf-defs        [(to-column-family-definition keyspace "movies")]]
    (is (sys/add-keyspace keyspace strategy-class cf-defs :strategy-opts strategy-opts))
    (is (sys/drop-keyspace keyspace))))

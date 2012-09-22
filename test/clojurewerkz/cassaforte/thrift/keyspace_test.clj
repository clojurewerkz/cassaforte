(ns clojurewerkz.cassaforte.thrift.keyspace-test
  (:require [clojurewerkz.cassaforte.client :as cc])
  (:use clojure.test
        clojurewerkz.cassaforte.test-helper
        clojurewerkz.cassaforte.thrift.keyspace
        clojurewerkz.cassaforte.utils
        clojurewerkz.cassaforte.conversion)
  (:require [clojurewerkz.cassaforte.thrift.column-definition :as column-def]
            [clojurewerkz.cassaforte.thrift.column-family-definition :as column-family-def]
            [clojurewerkz.cassaforte.thrift.keyspace-definition :as keyspace-def]))

(use-fixtures :once initialize-cql)

(deftest test-add-and-drop-keyspace
  (with-thrift-exception-handling
    (drop-keyspace "keyspace2"))
  (let [keyspace-name "keyspace2"
        ks-def        (build-kd
                       keyspace-name
                       "org.apache.cassandra.locator.SimpleStrategy"
                       [(build-cfd "keyspace2" "ColumnFamilyName" [(build-cd "name" "UTF8Type") (build-cd "occupation" "UTF8Type")])]
                       :strategy-opts {:replication_factor "1"})
        ks-count      (count-keyspaces)]
    (is (not (nil? (add-keyspace ks-def))))
    (is (= (inc ks-count) (count-keyspaces)))
    (drop-keyspace keyspace-name)
    (is (= ks-count (count-keyspaces)))))

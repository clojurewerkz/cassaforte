(ns clojurewerkz.cassaforte.thrift.core-test
  (:refer-clojure :exclude [get])
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.conversion :as conv]
            [clojurewerkz.cassaforte.thrift.keyspace :as k]
            [clojurewerkz.cassaforte.thrift.column-or-super-column :as cosc]
            )
  (:use clojurewerkz.cassaforte.thrift.core
        clojurewerkz.cassaforte.test.helper
        clojurewerkz.cassaforte.conversion
        clojure.test))

(def *consistency-level* (conv/to-consistency-level :one))
(cc/connect! "127.0.0.1" "CassaforteTest1")

(deftest t-batch-mutate
  (with-thrift-exception-handling
    (k/drop-keyspace "keyspace_name"))

  (k/add-keyspace
   (build-keyspace-definition "keyspace_name"
                              "org.apache.cassandra.locator.SimpleStrategy"
                              [(build-cfd "keyspace_name" "ColumnFamily1" [(build-cd "first" "UTF8Type")
                                                                           (build-cd "second" "UTF8Type")
                                                                           (build-cd "third" "UTF8Type")])]
                              :strategy-opts {"replication_factor" "1"}))
  (k/set-keyspace "keyspace_name")

  (batch-mutate
   {"key1" {"ColumnFamily1" {:first "first" :second "second" :third "third"} }
    "key2" {"ColumnFamily1" {:first "first" :second "second" :third "third"} }}
   *consistency-level*)

  (let [column-map (to-map (cosc/get-column (get "key1" "ColumnFamily1" "first" *consistency-level*)))]
    (is (= :first (:name column-map)))))
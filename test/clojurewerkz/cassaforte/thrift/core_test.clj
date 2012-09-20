(ns clojurewerkz.cassaforte.thrift.core-test
  (:refer-clojure :exclude [get])
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.conversion :as conv]
            [clojurewerkz.cassaforte.thrift.keyspace :as k]
            [clojurewerkz.cassaforte.thrift.column-or-super-column :as cosc]

            [clojurewerkz.cassaforte.cql :as cql]
            )
  (:use clojurewerkz.cassaforte.thrift.core
        clojurewerkz.cassaforte.utils
        clojurewerkz.cassaforte.conversion
        clojure.test))

(def *consistency-level* (conv/to-consistency-level :one))
(cc/connect! "127.0.0.1" "CassaforteTest1")

;; (deftest t-batch-mutate
;;   (with-thrift-exception-handling
;;     (k/drop-keyspace "keyspace_name"))

;;   (k/add-keyspace
;;    (build-keyspace-definition "keyspace_name"
;;                               "org.apache.cassandra.locator.SimpleStrategy"
;;                               [(build-cfd "keyspace_name" "ColumnFamily2" [(build-cd "first" "UTF8Type")
;;                                                                            (build-cd "second" "UTF8Type")
;;                                                                            (build-cd "third" "UTF8Type")])]
;;                               :strategy-opts {"replication_factor" "1"}))

;;   (k/set-keyspace "keyspace_name")

;;   (batch-mutate
;;    {"key1" {"ColumnFamily2" {:first "a" :second "b"} }
;;     "key2" {"ColumnFamily2" {:first "c" :second "d"} }}
;;    *consistency-level*)

;;   (println  (map to-map (get-slice "ColumnFamily2" "key1" *consistency-level*)))
;;   (is (= {:first "a" :second "b"} (to-plain-hash (get-slice "ColumnFamily2" "key1" *consistency-level*))))
;;   (is (= {:first "c" :second "d"} (to-plain-hash (get-slice "ColumnFamily2" "key2" *consistency-level*)))))

;; (deftest t-batch-mutate-supercolumn
;;   (with-thrift-exception-handling
;;     (k/drop-keyspace "keyspace_name"))

;;   (k/add-keyspace
;;    (build-keyspace-definition "keyspace_name"
;;                               "org.apache.cassandra.locator.SimpleStrategy"
;;                               [(build-cfd "keyspace_name" "ColumnFamily1" [] :column-type "Super")]
;;                               :strategy-opts {"replication_factor" "1"}))
;;   (k/set-keyspace "keyspace_name")

;;   (batch-mutate
;;    {"key1" {"ColumnFamily1" {:name1 {:first "a" :second "b"} :name2 {:first "c" :second "d"} :name3 {:first "e" :second "f"}}}
;;     "key2" {"ColumnFamily1" {:name1 {:first "g" :second "h"} :name2 {:first "i" :second "j"} :name3 {:first "k" :second "l"}} }}
;;    *consistency-level*
;;    :type :super)

;;   (are [expected actual] (= expected (to-plain-hash actual))
;;        {:name1 {:first "a" :second "b"}}
;;        (get "ColumnFamily1" "key1" "name1" *consistency-level* :type :super)

;;        {:name1 {:first "a" :second "b"} :name2 {:first "c" :second "d"} :name3 {:first "e" :second "f"}}
;;        (get-slice "ColumnFamily1" "key1" *consistency-level*)

;;        {:name1 {:first "g" :second "h"} :name2 {:first "i" :second "j"} :name3 {:first "k" :second "l"}}
;;        (get-slice "ColumnFamily1" "key2" *consistency-level*)

;;        {:name1 {:first "a" :second "b"} :name2 {:first "c" :second "d"}}
;;        (get-slice "ColumnFamily1" "key1" "name1" "name2" *consistency-level*)

;;        {:name1 {:first "a" :second "b"} :name2 {:first "c" :second "d"}}
;;        (get-slice "ColumnFamily1" "key1" "" "name2" *consistency-level*)

;;        {:name2 {:first "i" :second "j"} :name3 {:first "k" :second "l"}}
;;        (get-slice "ColumnFamily1" "key2" "name2" "" *consistency-level*)))


;; (deftest t-mutate-types
;;   (with-thrift-exception-handling
;;     (k/drop-keyspace "keyspace_name"))

;;   (k/add-keyspace
;;    (build-keyspace-definition "keyspace_name"
;;                               "org.apache.cassandra.locator.SimpleStrategy"
;;                               [(build-cfd "keyspace_name" "ColumnFamily2" [])]
;;                               :strategy-opts {"replication_factor" "1"}))

;;   (k/set-keyspace "keyspace_name")

;;   (batch-mutate
;;    {"key1" {"ColumnFamily2" {:first "a" :second "b"} }}
;;    *consistency-level*)

;;   (is (= {:first "a" :second "b"} (to-plain-hash (get-slice "ColumnFamily2" "key1" *consistency-level*)))))
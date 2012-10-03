(ns clojurewerkz.cassaforte.thrift.core-test
  (:refer-clojure :exclude [get])
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.conversion :as conv]
            [clojurewerkz.cassaforte.thrift.keyspace-definition :as kd]
            [clojurewerkz.cassaforte.thrift.column-definition :as cd]
            [clojurewerkz.cassaforte.thrift.column-family-definition :as cfd]
            [clojurewerkz.cassaforte.thrift.column-or-super-column :as cosc]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.cql :as cql])
  (:use clojurewerkz.cassaforte.thrift.core
        clojurewerkz.cassaforte.test-helper
        clojurewerkz.cassaforte.utils
        clojurewerkz.cassaforte.conversion
        clojure.test))

(def *consistency-level* (conv/to-consistency-level :one))

(use-fixtures :once initialize-cql)

(deftest t-batch-mutate
  (with-thrift-exception-handling
    (sch/drop-keyspace "keyspace_name"))

  (sch/add-keyspace
   (kd/build-kd "keyspace_name"
                              "org.apache.cassandra.locator.SimpleStrategy"
                              [(cfd/build-cfd "keyspace_name" "ColumnFamily2" [(cd/build-cd "first" "UTF8Type")
                                                                              (cd/build-cd "second" "UTF8Type")
                                                                              (cd/build-cd "third" "UTF8Type")])]
                              :strategy-opts {"replication_factor" "1"}))

  (sch/set-keyspace "keyspace_name")

  (batch-mutate
   {"key1" {"ColumnFamily2" {:first "a" :second "b"} }
    "key2" {"ColumnFamily2" {:first "c" :second "d" :third "e"} }}
   *consistency-level*)

  (is (= {:first "a" :second "b"}
         (get-slice "ColumnFamily2" "key1" *consistency-level*
                    {:default-value-type "UTF8Type" :default-name-type "UTF8Type"})))

  (is (= {:first "c" :second "d"}
         (get-slice "ColumnFamily2" "key2" "first" "second" *consistency-level*
                    {:default-value-type "UTF8Type" :default-name-type "UTF8Type"}))))


(deftest t-batch-mutate-supercolumn
  (with-thrift-exception-handling
    (sch/drop-keyspace "keyspace_name"))

  (sch/add-keyspace
   (kd/build-kd "keyspace_name"
             "org.apache.cassandra.locator.SimpleStrategy"
             [(cfd/build-cfd "keyspace_name" "ColumnFamily1" [] :column-type "Super")]
             :strategy-opts {"replication_factor" "1"}))
  (sch/set-keyspace "keyspace_name")

  (batch-mutate
   {"key1" {"ColumnFamily1" {:name1 {:first "a" :second "b"} :name2 {:first "c" :second "d"} :name3 {:first "e" :second "f"}}}
    "key2" {"ColumnFamily1" {:name1 {:first "g" :second "h"} :name2 {:first "i" :second "j"} :name3 {:first "k" :second "l"}} }}
   *consistency-level*
   :type :super)

  (are [expected actual] (= expected actual)

       {:name1 {:first "a" :second "b"} :name2 {:first "c" :second "d"}}
       (get-slice "ColumnFamily1" "key1" "" "name2" *consistency-level*
                  {:default-value-type "UTF8Type" :default-name-type "UTF8Type"})

       {:name1 {:first "a" :second "b"} :name2 {:first "c" :second "d"} :name3 {:first "e" :second "f"}}
       (get-slice "ColumnFamily1" "key1" *consistency-level*
                  {:default-value-type "UTF8Type" :default-name-type "UTF8Type"})

       {:name1 {:first "g" :second "h"} :name2 {:first "i" :second "j"} :name3 {:first "k" :second "l"}}
       (get-slice "ColumnFamily1" "key2" *consistency-level*
                  {:default-value-type "UTF8Type" :default-name-type "UTF8Type"})

       {:name1 {:first "a" :second "b"} :name2 {:first "c" :second "d"}}
       (get-slice "ColumnFamily1" "key1" "name1" "name2" *consistency-level*
                  {:default-value-type "UTF8Type" :default-name-type "UTF8Type"})

       {:name1 {:first "a" :second "b"} :name2 {:first "c" :second "d"}}
       (get-slice "ColumnFamily1" "key1" "" "name2" *consistency-level*
                  {:default-value-type "UTF8Type" :default-name-type "UTF8Type"})

       {:name2 {:first "i" :second "j"} :name3 {:first "k" :second "l"}}
       (get-slice "ColumnFamily1" "key2" "name2" "" *consistency-level*
                  {:default-value-type "UTF8Type" :default-name-type "UTF8Type"})))
